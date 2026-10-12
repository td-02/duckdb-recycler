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

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

//! Which candidates below the current node may still be materialized. One materialization per root-to-leaf path is
//! the rule; the exception is a single nested aggregation, because a shared aggregate (one grouped result feeding
//! different consumers, as a CTE does) is exactly the unit that recurs while its consumers differ.
enum class WritePolicy : uint8_t { NONE, AGGREGATES_ONLY, ALL };

struct SubtreeWalker : PlanPass {
	using PlanPass::PlanPass;

	//! The pagination recorded by the pre hook; set only when the bound and the optimized plan each hold exactly one
	const PaginationSite *pagination_site = nullptr;

	void Walk(unique_ptr<LogicalOperator> &slot, bool is_root, WritePolicy policy, bool under_delim_lhs,
	          bool force_candidate) {
		auto &op = *slot;
		if (AsRecyclerOperator(op)) {
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_LIMIT && pagination_site) {
			switch (RewritePagination(*this, slot, *pagination_site, policy == WritePolicy::ALL, under_delim_lhs)) {
			case PaginationRewrite::SERVED:
				return;
			case PaginationRewrite::WRITING:
				// the pagination writer is transparent to the write policy: the sort is a pipeline breaker between it
				// and whatever is written below, so the subtree writes of the unpaginated plan (the joins feeding the
				// sort) stay. Below the LIMIT now: the writer, then the unit it wraps.
				WalkChildren(*slot->children[0]->children[0], policy, under_delim_lhs);
				return;
			case PaginationRewrite::NONE:
				break;
			}
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
			// the delim side depends on the other side's data (never cacheable); the other side is the join's whole
			// input, often the largest table of the query, so only small results may be written there
			// (EntryCapRows)
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
	walker.ApplyReplacements(*plan);
}

} // namespace duckdb
