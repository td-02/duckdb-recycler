//===----------------------------------------------------------------------===//
// recycler_hooks.cpp - the pre- and post-optimizer passes
//
// Pre-optimize (before any built-in optimizer, i.e. on the bound plan):
//   * whole-result hit: the query's plan matches a cached result -> replace the
//     whole plan with a RecyclerScan; miss -> remember the key for the post hook
//   * nested-block hit: a sub-query block equals an earlier whole query
//     (before filter pushdown rewrites it) -> replace the block
// Post-optimize (after all built-in optimizers):
//   * whole-result write: wrap the root in an ordered RecyclerMaterialize
//   * subtree cache: top-down, one materialization per root-to-leaf path,
//     exact hits replace subtrees, admissible misses get a RecyclerMaterialize
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"
#include "recycler_operators.hpp"
#include "recycler_signature.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <chrono>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Settings & state access
//===----------------------------------------------------------------------===//
static bool GetBoolSetting(ClientContext &context, const char *name, bool default_value) {
	Value value;
	if (context.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
	}
	return default_value;
}

static idx_t GetIntSetting(ClientContext &context, const char *name, idx_t default_value) {
	Value value;
	if (context.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		auto v = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		return v < 0 ? 0 : idx_t(v);
	}
	return default_value;
}

RecyclerSettings LoadRecyclerSettings(ClientContext &context) {
	RecyclerSettings s;
	s.enabled = GetBoolSetting(context, "recycler_enabled", s.enabled);
	s.whole_result = GetBoolSetting(context, "recycler_whole_result", s.whole_result);
	s.blocks = GetBoolSetting(context, "recycler_blocks", s.blocks);
	s.subtrees = GetBoolSetting(context, "recycler_subtrees", s.subtrees);
	s.budget_bytes = GetIntSetting(context, "recycler_budget_mb", s.budget_bytes / (1024 * 1024)) * 1024 * 1024;
	s.entry_cap_bytes =
	    GetIntSetting(context, "recycler_entry_cap_mb", s.entry_cap_bytes / (1024 * 1024)) * 1024 * 1024;
	s.entry_cap_rows = GetIntSetting(context, "recycler_entry_cap_rows", s.entry_cap_rows);
	s.min_input_rows = GetIntSetting(context, "recycler_min_input_rows", s.min_input_rows);
	s.speculate_bytes =
	    GetIntSetting(context, "recycler_speculate_mb", s.speculate_bytes / (1024 * 1024)) * 1024 * 1024;
	return s;
}

static shared_ptr<RecyclerClientState> GetClientState(ClientContext &context) {
	return context.registered_state->GetOrCreate<RecyclerClientState>("recycler");
}

static RecyclerState *StateFromInput(OptimizerExtensionInput &input) {
	if (!input.info) {
		return nullptr;
	}
	return static_cast<RecyclerState *>(input.info.get());
}

struct ScopedTimer {
	explicit ScopedTimer(std::atomic<uint64_t> &target) : target(target), start(std::chrono::steady_clock::now()) {
	}
	~ScopedTimer() {
		auto end = std::chrono::steady_clock::now();
		target += uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
	}
	std::atomic<uint64_t> &target;
	std::chrono::steady_clock::time_point start;
};

//===----------------------------------------------------------------------===//
// Root classification
//===----------------------------------------------------------------------===//
//! Statements that may change data or catalog state: drop everything we cached
static bool IsModifyingRoot(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
	case LogicalOperatorType::LOGICAL_CREATE_SECRET:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_ATTACH:
	case LogicalOperatorType::LOGICAL_DETACH:
	case LogicalOperatorType::LOGICAL_COPY_DATABASE:
	case LogicalOperatorType::LOGICAL_VACUUM:
	case LogicalOperatorType::LOGICAL_LOAD:
	case LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS:
		return true;
	default:
		return false;
	}
}

//! Plain read queries whose result we may cache and serve
static bool IsReadRoot(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	case LogicalOperatorType::LOGICAL_PIVOT:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		return true;
	default:
		return false;
	}
}

//! Locate the query root: the child of EXPLAIN ANALYZE, or the plan itself. Returns nullptr when the statement
//! is not a candidate at all (plain EXPLAIN, non-read statements).
static unique_ptr<LogicalOperator> *FindQueryRoot(unique_ptr<LogicalOperator> &plan) {
	if (plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		auto &explain = plan->Cast<LogicalExplain>();
		if (explain.explain_type != ExplainType::EXPLAIN_ANALYZE || plan->children.size() != 1) {
			return nullptr;
		}
		return &plan->children[0];
	}
	return &plan;
}

//===----------------------------------------------------------------------===//
// Rewrites
//===----------------------------------------------------------------------===//
//! Replace the operator in `slot` by a scan over `entry`, recording the binding replacements
static bool SubstituteScan(unique_ptr<LogicalOperator> &slot, shared_ptr<CachedEntry> entry, bool ordered,
                           Binder &binder, ColumnBindingReplacer &replacer) {
	auto &old_op = *slot;
	if (old_op.types != entry->types) {
		return false;
	}
	auto old_bindings = old_op.GetColumnBindings();
	if (old_bindings.size() != entry->types.size()) {
		return false;
	}
	auto scan = make_uniq<LogicalRecyclerScan>(binder.GenerateTableIndex(), old_op.types, std::move(entry), ordered);
	scan->ResolveOperatorTypes();
	auto new_bindings = scan->GetColumnBindings();
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
	}
	slot = std::move(scan);
	return true;
}

static void WrapMaterialize(unique_ptr<LogicalOperator> &slot, RecyclerWriteSpec spec, shared_ptr<RecyclerCache> cache) {
	auto child = std::move(slot);
	auto wrapper = make_uniq<LogicalRecyclerMaterialize>(std::move(spec), std::move(cache), std::move(child));
	wrapper->ResolveOperatorTypes();
	slot = std::move(wrapper);
}

static bool IsRecyclerOperator(const LogicalOperator &op) {
	return op.type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR &&
	       (dynamic_cast<const LogicalRecyclerScan *>(&op) || dynamic_cast<const LogicalRecyclerMaterialize *>(&op));
}

//===----------------------------------------------------------------------===//
// Pre-optimize: whole-result hits, nested-block hits
//===----------------------------------------------------------------------===//
struct BlockMatcher {
	BlockMatcher(ClientContext &context, Binder &binder, RecyclerCache &cache, const RecyclerSettings &settings,
	             PlanSignatures &sig, bool preserve_order)
	    : context(context), binder(binder), cache(cache), settings(settings), sig(sig),
	      preserve_order(preserve_order) {
	}

	ClientContext &context;
	Binder &binder;
	RecyclerCache &cache;
	const RecyclerSettings &settings;
	PlanSignatures &sig;
	bool preserve_order;
	ColumnBindingReplacer replacer;
	bool replaced = false;

	void Walk(unique_ptr<LogicalOperator> &slot) {
		auto &op = *slot;
		if (IsRecyclerOperator(op)) {
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE || op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			// the definition (children[0]) may be deep-copied by CTE inlining, which cannot copy our operators;
			// only look at the consumer side
			if (op.children.size() == 2) {
				Walk(op.children[1]);
			}
			return;
		}
		auto &info = sig.Get(op);
		if (info.Cacheable() && info.est_leaf_rows >= settings.min_input_rows && op.children.size() > 0) {
			auto entry = cache.Lookup(RecyclerEntryKind::WHOLE_RESULT, info.hash, [&]() { return sig.Render(op); });
			if (entry && (entry->ordered || !preserve_order) &&
			    SubstituteScan(slot, entry, entry->ordered, binder, replacer)) {
				cache.stats.block_hits++;
				replaced = true;
				return;
			}
		}
		for (auto &child : op.children) {
			Walk(child);
		}
	}
};

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto state = StateFromInput(input);
	if (!state) {
		return;
	}
	auto &cache = *state->cache;
	auto &context = input.context;
	ScopedTimer timer(cache.stats.pre_ns);

	auto client = GetClientState(context);
	client->Reset();
	if (plan->type == LogicalOperatorType::LOGICAL_SET || plan->type == LogicalOperatorType::LOGICAL_RESET ||
	    plan->type == LogicalOperatorType::LOGICAL_PRAGMA) {
		client->settings_loaded = false;
	}

	auto root_slot = FindQueryRoot(plan);
	if (!root_slot) {
		cache.stats.skip_not_query++;
		return;
	}
	auto &root = **root_slot;
	if (IsModifyingRoot(root.type)) {
		cache.stats.skip_modifying++;
		cache.Clear();
		return;
	}
	if (!IsReadRoot(root.type)) {
		cache.stats.skip_not_read++;
		return;
	}
	if (!client->settings_loaded) {
		client->settings = LoadRecyclerSettings(context);
		client->settings_loaded = true;
	}
	auto &settings = client->settings;
	if (!settings.enabled) {
		cache.stats.skip_disabled++;
		return;
	}
	// statements that modify data are detected at bind time; a running write transaction must not be served
	// from or written into the cache either
	if (!input.optimizer.binder.GetStatementProperties().modified_databases.empty()) {
		cache.stats.skip_modified_databases++;
		cache.Clear();
		return;
	}
	if (context.transaction.HasActiveTransaction() && MetaTransaction::Get(context).ModifiedDatabase()) {
		cache.stats.skip_write_transaction++;
		return;
	}
	cache.stats.pre_calls++;
	client->active = true;
	// with preserve_insertion_order the order of an unordered query's rows is observable, so only results that
	// were stored in a defined order may be served
	bool preserve_order = Settings::Get<PreserveInsertionOrderSetting>(context);

	PlanSignatures sig(context);
	try {
		plan->ResolveOperatorTypes();
		sig.Analyze(root);
	} catch (std::exception &) {
		cache.stats.skip_analyze_failed++;
		client->active = false;
		return;
	}

	auto &root_info = sig.Get(root);
	if (!root_info.Cacheable()) {
		cache.stats.root_not_cacheable++;
	}
	if (settings.whole_result && root_info.Cacheable()) {
		auto entry = cache.Lookup(RecyclerEntryKind::WHOLE_RESULT, root_info.hash, [&]() { return sig.Render(root); });
		if (entry && (entry->ordered || !preserve_order)) {
			ColumnBindingReplacer unused;
			if (SubstituteScan(*root_slot, entry, entry->ordered, input.optimizer.binder, unused)) {
				cache.stats.whole_hits++;
				return;
			}
		}
		// unordered results are written by a parallel sink; with preserve_insertion_order that would change the
		// observable row order, and a serial sink would run the whole pipeline on one thread: neither is acceptable
		bool writable = root_info.ordered || !preserve_order;
		if (writable && root_info.est_leaf_rows >= settings.min_input_rows) {
			client->has_pending_whole = true;
			client->whole_hash = root_info.hash;
			client->whole_canonical = sig.Render(root);
			client->whole_est_input_rows = root_info.est_leaf_rows;
		}
	}
	if (settings.blocks) {
		BlockMatcher matcher(context, input.optimizer.binder, cache, settings, sig, preserve_order);
		for (auto &child : root.children) {
			matcher.Walk(child);
		}
		if (matcher.replaced) {
			matcher.replacer.VisitOperator(*plan);
		}
	}
}

//===----------------------------------------------------------------------===//
// Post-optimize: subtree hits/writes, whole-result writes
//===----------------------------------------------------------------------===//
static bool IsSubtreeCandidateType(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return true;
	default:
		return false;
	}
}

//! Whether we trust the optimizer's cardinality estimate enough to materialize on first sight
static bool SpeculationAllowed(const LogicalOperator &op, const NodeInfo &info, bool force_candidate) {
	if (info.unreliable_estimate) {
		// non-equality joins below make every estimate above them worthless (a self join with an inequality
		// was 60x off in the workload); wait for a repeat before paying for a materialization
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return true;
	default:
		return force_candidate;
	}
}

static bool IsAggregation(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || type == LogicalOperatorType::LOGICAL_DISTINCT;
}

//! Which candidates below the current node may still be materialized. One materialization per root-to-leaf path is
//! the rule; the exception is a single nested aggregation, because a shared aggregate (a CTE-like
//! "GROUP BY person_id" feeding different consumers) is exactly the unit that recurs while its consumers differ.
enum class WritePolicy : uint8_t { NONE, AGGREGATES_ONLY, ALL };

struct SubtreeWalker {
	SubtreeWalker(ClientContext &context, Binder &binder, shared_ptr<RecyclerCache> cache,
	              const RecyclerSettings &settings, PlanSignatures &sig)
	    : context(context), binder(binder), cache(std::move(cache)), settings(settings), sig(sig) {
	}

	ClientContext &context;
	Binder &binder;
	shared_ptr<RecyclerCache> cache;
	const RecyclerSettings &settings;
	PlanSignatures &sig;
	ColumnBindingReplacer replacer;
	bool replaced = false;

	bool Admit(LogicalOperator &op, const NodeInfo &info, bool under_delim_lhs, bool force_candidate) {
		if (cache->IsTooBig(info.hash)) {
			return false;
		}
		auto est_in = info.est_leaf_rows;
		if (est_in < settings.min_input_rows) {
			return false;
		}
		auto est_out = op.EstimateCardinality(context);
		if (!IsAggregation(op.type) && est_out > est_in / 2) {
			// a join result that is not much smaller than its inputs saves little when scanned back; an aggregate
			// always folds its input and its estimate is bounded by the input anyway
			return false;
		}
		auto width = RecyclerEstimateRowWidth(op.types);
		auto est_bytes = est_out * width;
		auto cap_rows = under_delim_lhs ? MinValue<idx_t>(settings.entry_cap_rows, 200000) : settings.entry_cap_rows;
		if (est_out > cap_rows || est_bytes > settings.entry_cap_bytes) {
			return false;
		}
		auto seen_before = cache->Seen(info.hash) > 1;
		if (!seen_before) {
			if (!SpeculationAllowed(op, info, force_candidate)) {
				return false;
			}
			// aggregates are the safest speculation (their output is bounded by their input and usually far
			// smaller), plain joins the riskiest
			auto speculate_bytes = IsAggregation(op.type) ? settings.speculate_bytes * 2
			                       : op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
			                           ? settings.speculate_bytes / 8
			                           : settings.speculate_bytes;
			if (est_bytes > speculate_bytes) {
				return false;
			}
		}
		return true;
	}

	void Walk(unique_ptr<LogicalOperator> &slot, bool is_root, WritePolicy policy, bool under_delim_lhs,
	          bool force_candidate) {
		auto &op = *slot;
		if (IsRecyclerOperator(op)) {
			return;
		}
		auto &info = sig.Get(op);
		bool candidate = !is_root && (force_candidate || IsSubtreeCandidateType(op.type)) && info.Cacheable();
		if (candidate) {
			cache->NoteSeen(info.hash);
			auto entry = cache->Lookup(RecyclerEntryKind::SUBTREE, info.hash, [&]() { return sig.Render(op); });
			if (entry && SubstituteScan(slot, entry, entry->ordered, binder, replacer)) {
				cache->stats.subtree_hits++;
				replaced = true;
				return;
			}
			bool may_write = policy == WritePolicy::ALL ||
			                 (policy == WritePolicy::AGGREGATES_ONLY && IsAggregation(op.type));
			if (may_write && Admit(op, info, under_delim_lhs, force_candidate)) {
				RecyclerWriteSpec spec;
				spec.kind = RecyclerEntryKind::SUBTREE;
				spec.hash = info.hash;
				spec.canonical = sig.Render(op);
				spec.ordered = info.ordered;
				spec.est_input_rows = info.est_leaf_rows;
				spec.cap_rows = settings.entry_cap_rows;
				spec.cap_bytes = settings.entry_cap_bytes;
				spec.budget_bytes = settings.budget_bytes;
				cache->stats.writes_planned++;
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

void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto state = StateFromInput(input);
	if (!state) {
		return;
	}
	auto &cache = state->cache;
	auto &context = input.context;
	ScopedTimer timer(cache->stats.post_ns);

	auto client = GetClientState(context);
	if (!client->active) {
		return;
	}
	client->active = false;
	auto settings = client->settings;

	auto root_slot = FindQueryRoot(plan);
	if (!root_slot) {
		return;
	}
	if (IsRecyclerOperator(**root_slot)) {
		// whole-result hit at the pre hook
		return;
	}
	cache->stats.post_calls++;

	PlanSignatures sig(context);
	try {
		plan->ResolveOperatorTypes();
		sig.Analyze(**root_slot);
	} catch (std::exception &) {
		return;
	}
	// the root's analysis (rewrites below the root do not touch the analysis map)
	const auto &root_info = sig.Get(**root_slot);

	if (settings.subtrees) {
		SubtreeWalker walker(context, input.optimizer.binder, cache, settings, sig);
		walker.Walk(*root_slot, true, WritePolicy::ALL, false, false);
		if (walker.replaced) {
			walker.replacer.VisitOperator(*plan);
		}
	}

	if (settings.whole_result && client->has_pending_whole && !cache->IsTooBig(client->whole_hash)) {
		auto &root = **root_slot;
		bool admissible = !root_info.refused && !root_info.nondeterministic && !root_info.unresolved_delim_get;
		if (admissible) {
			auto est_out = root.EstimateCardinality(context);
			if (est_out > settings.whole_cap_rows) {
				admissible = false;
			}
		}
		if (admissible) {
			RecyclerWriteSpec spec;
			spec.kind = RecyclerEntryKind::WHOLE_RESULT;
			spec.hash = client->whole_hash;
			spec.canonical = std::move(client->whole_canonical);
			// an ordered result is written by a serial sink (only the sort's output stage is serialized) and
			// replayed in that order; anything else is written and replayed in parallel
			spec.ordered = root_info.ordered;
			spec.est_input_rows = client->whole_est_input_rows;
			spec.cap_rows = MinValue<idx_t>(settings.whole_cap_rows, settings.entry_cap_rows);
			spec.cap_bytes = settings.entry_cap_bytes;
			spec.budget_bytes = settings.budget_bytes;
			cache->stats.writes_planned++;
			WrapMaterialize(*root_slot, std::move(spec), cache);
		}
	}
	client->Reset();
}

} // namespace duckdb
