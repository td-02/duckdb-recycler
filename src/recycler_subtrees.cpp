//===----------------------------------------------------------------------===//
// recycler_subtrees.cpp - the post hook's subtree cache
//
// Top-down over the optimized plan: exact hits replace subtrees, admissible misses get a RecyclerMaterialize, at
// most one materialization per root-to-leaf path; the pagination site's sorted prefix is handled on the way.
//===----------------------------------------------------------------------===//
#include "recycler_subtrees.hpp"
#include "recycler_operators.hpp"
#include "recycler_pagination.hpp"
#include "recycler_policy.hpp"
#include "recycler_rewrite.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

//! Which candidates below the current node may still be materialized. One materialization per root-to-leaf path is
//! the rule; the exception is a single nested aggregation, because a shared aggregate (a CTE-like
//! "GROUP BY person_id" feeding different consumers) is exactly the unit that recurs while its consumers differ.
enum class WritePolicy : uint8_t { NONE, AGGREGATES_ONLY, ALL };

struct SubtreeWalker {
	SubtreeWalker(ClientContext &context, Binder &binder, shared_ptr<RecyclerCache> cache,
	              const RecyclerSettings &settings, PlanSignatures &sig, const TableSnapshot &snapshot)
	    : context(context), binder(binder), cache(std::move(cache)), settings(settings), sig(sig), snapshot(snapshot) {
	}

	ClientContext &context;
	Binder &binder;
	shared_ptr<RecyclerCache> cache;
	const RecyclerSettings &settings;
	PlanSignatures &sig;
	const TableSnapshot &snapshot;
	ColumnBindingReplacer replacer;
	bool replaced = false;
	//! The pagination recorded by the pre hook; set only when the bound and the optimized plan each hold exactly one
	const PaginationSite *pagination_site = nullptr;

	//! The sorted rows below the LIMIT of the pagination site are the same for every page, so they are the cache
	//! unit and every page only skips/scans its own slice. The key comes from the bound plan (see PaginationSite); the
	//! optimized ORDER_BY must still be this site. Returns true when the subtree below the LIMIT has been handled.
	bool TryPagination(unique_ptr<LogicalOperator> &slot, WritePolicy policy, bool under_delim_lhs) {
		idx_t limit_rows, offset_rows;
		auto order_slot = pagination_site ? MatchPagination(*slot, limit_rows, offset_rows) : nullptr;
		if (!order_slot || limit_rows != pagination_site->limit || offset_rows != pagination_site->offset) {
			return false;
		}
		auto &order = (*order_slot)->Cast<LogicalOrder>();
		if (OrderSpec(order) != pagination_site->order_spec) {
			return false;
		}
		// the unit is the whole order-preserving chain under the LIMIT: above the sort's decompression it emits
		// logical types in every plan, so the entry written by one page can be replayed by the next
		auto &unit_slot = (*slot).children[0];
		auto &info = sig.Get(*unit_slot);
		if (!info.Cacheable() || !info.ordered) {
			return false;
		}
		// which bound ORDER BY output each column of this plan's unit carries; unprovable means no pagination
		vector<idx_t> positions;
		if (!UnitPositions(*unit_slot, pagination_site->projection_index, positions)) {
			return false;
		}
		auto canonical = PaginationCanonical(*pagination_site, order, *unit_slot);
		auto hash = HashPaginationKey(canonical);
		// rows of the sorted result this page needs
		auto need = offset_rows > NumericLimits<idx_t>::Maximum() - limit_rows ? NumericLimits<idx_t>::Maximum()
		                                                                       : limit_rows + offset_rows;
		idx_t existing_rows = 0;
		auto entry = cache->LookupPagination(hash, canonical, pagination_site->tables, need, existing_rows);
		if (entry) {
			// the entry stores the columns in the order of the plan that wrote it, this plan's unit may order them
			// differently
			vector<idx_t> stored;
			if (!StoredColumns(*entry, positions, stored)) {
				return false;
			}
			bool in_order = true;
			for (idx_t j = 0; j < stored.size(); j++) {
				in_order = in_order && stored[j] == j;
			}
			if (in_order ? SubstituteScan(unit_slot, entry, true, info.tables, binder, replacer)
			             : SubstituteReorderedScan(unit_slot, entry, true, stored, info.tables, binder, replacer)) {
				cache->stats.subtree_hits++;
				replaced = true;
				return true;
			}
		}
		if (policy != WritePolicy::ALL || !AdmitPaginationWrite(*cache, settings, info, hash) ||
		    !cache->Admissible(info.tables, snapshot)) {
			return false;
		}
		// the prefix to store (see PrefixRows); below a delim join's LHS the row cap is the smaller one used there
		auto cap_rows = under_delim_lhs ? MinValue<idx_t>(settings.entry_cap_rows, 200000) : settings.entry_cap_rows;
		auto prefix = PrefixRows(need, existing_rows, cap_rows, settings.entry_cap_bytes,
		                         RecyclerEstimateRowWidth(unit_slot->types));
		if (prefix < need) {
			// the prefix could not even cover this page: leave the query as M5 plans it
			return false;
		}
		auto spec = RecyclerWriteSpec::Make(RecyclerEntryKind::PAGINATION, hash, std::move(canonical), true,
		                                    info.est_leaf_rows, pagination_site->tables, settings);
		spec.prefix_rows = prefix;
		for (auto position : positions) {
			ColumnIdentity column;
			column.table = 0;
			column.column = position;
			spec.columns.push_back(column);
		}
		cache->stats.writes_planned++;
		WrapMaterialize(unit_slot, std::move(spec), cache);
		// the pagination writer is transparent to the write policy: the sort is a pipeline breaker between it and
		// whatever is written below, so the subtree writes of the unpaginated plan (the joins feeding the sort) stay
		WalkChildren(*unit_slot->children[0], policy, under_delim_lhs);
		return true;
	}

	void Walk(unique_ptr<LogicalOperator> &slot, bool is_root, WritePolicy policy, bool under_delim_lhs,
	          bool force_candidate) {
		auto &op = *slot;
		if (AsRecyclerOperator(op)) {
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_LIMIT && TryPagination(slot, policy, under_delim_lhs)) {
			return;
		}
		auto &info = sig.Get(op);
		bool candidate =
		    !is_root && (force_candidate || IsSubtreeCandidateType(op.type)) && info.CacheableWithoutOuterFilters();
		if (candidate) {
			cache->NoteSeen(info.hash);
			auto entry = cache->Lookup(RecyclerEntryKind::SUBTREE, info, sig, op, snapshot);
			if (entry && SubstituteScan(slot, entry, entry->ordered, info.tables, binder, replacer)) {
				cache->stats.subtree_hits++;
				replaced = true;
				return;
			}
			bool may_write =
			    policy == WritePolicy::ALL || (policy == WritePolicy::AGGREGATES_ONLY && IsAggregation(op.type));
			if (may_write && AdmitSubtreeWrite(context, *cache, settings, op, info, under_delim_lhs, force_candidate) &&
			    cache->Admissible(info.tables, snapshot)) {
				auto spec =
				    RecyclerWriteSpec::Make(RecyclerEntryKind::SUBTREE, info.hash, sig.Render(op), info.ordered,
				                            info.est_leaf_rows, TableDependencies(info.tables, snapshot), settings);
				cache->stats.writes_planned++;
				// after rendering: the joins whose filters are removed are outside the subtree and its key
				cache->stats.stripped_runtime_filters += StripRuntimeFilters(op, info.pending_dynamic_owners);
				WrapMaterialize(slot, std::move(spec), cache);
				// below the wrapper: hits, and at most one more nested aggregation
				auto &wrapped = *slot->children[0];
				auto nested = policy == WritePolicy::ALL ? WritePolicy::AGGREGATES_ONLY : WritePolicy::NONE;
				WalkChildren(wrapped, nested, under_delim_lhs);
				return;
			}
		}
		WalkChildren(op, policy, under_delim_lhs);
	}

	void WalkChildren(LogicalOperator &op, WritePolicy policy, bool under_delim_lhs) {
		if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN && op.children.size() == 2) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			idx_t delim_side = join.delim_flipped ? 0 : 1;
			// the delim side depends on the other side's data (never cacheable); the other side of Family-A style
			// joins is cast_info-sized, so only small results may be written there
			Walk(op.children[1 - delim_side], false, policy, true, false);
			Walk(op.children[delim_side], false, WritePolicy::NONE, under_delim_lhs, false);
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op.children.size() == 2) {
			// the definition is an ordinary subtree whose type does not matter
			Walk(op.children[0], false, policy, under_delim_lhs, true);
			Walk(op.children[1], false, policy, under_delim_lhs, false);
			return;
		}
		for (auto &child : op.children) {
			Walk(child, false, policy, under_delim_lhs, false);
		}
	}
};

//! Subtrees: exact hits replace subtrees, admissible misses get a RecyclerMaterialize
void RewriteSubtrees(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                     unique_ptr<LogicalOperator> &root_slot, RecyclerState &state, const RecyclerClientState &client,
                     PlanSignatures &sig) {
	SubtreeWalker walker(input.context, input.optimizer.binder, state.cache, client.settings, sig, client.snapshot);
	// the pagination of the bound plan is only used when the optimized plan still holds exactly that one
	if (client.pagination_site.eligible && CountPaginationSites(*root_slot, nullptr) == 1) {
		walker.pagination_site = &client.pagination_site;
	}
	walker.Walk(root_slot, true, WritePolicy::ALL, false, false);
	if (walker.replaced) {
		walker.replacer.VisitOperator(*plan);
	}
}

} // namespace duckdb
