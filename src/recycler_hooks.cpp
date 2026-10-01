//===----------------------------------------------------------------------===//
// recycler_hooks.cpp - the pre- and post-optimizer passes
//
// Pre-optimize (before any built-in optimizer, i.e. on the bound plan):
//   * whole-result hit: the query's plan matches a cached result -> replace the
//     whole plan with a RecyclerScan; miss -> remember the key for the post hook
//   * nested-block hit: a sub-query block equals an earlier whole query
//     (before filter pushdown rewrites it) -> replace the block
//   * subsumption: an SPJ block narrower than a cached block -> scan the cached
//     rows plus the residual predicates; admissible misses get a block marker
// Post-optimize (after all built-in optimizers):
//   * block markers learn which base columns their pruned child produces
//   * subtree cache: top-down, one materialization per root-to-leaf path,
//     exact hits replace subtrees, admissible misses get a RecyclerMaterialize
//   * whole-result write: wrap the root in a RecyclerMaterialize
//
// The decisions live in recycler_policy, the plan surgery in recycler_rewrite;
// this file only orders the steps.
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"
#include "recycler_operators.hpp"
#include "recycler_policy.hpp"
#include "recycler_rewrite.hpp"
#include "recycler_signature.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <algorithm>
#include <chrono>
#include <unordered_set>

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
	s.subsumption = GetBoolSetting(context, "recycler_subsumption", s.subsumption);
	s.budget_bytes = GetIntSetting(context, "recycler_budget_mb", s.budget_bytes / (1024 * 1024)) * 1024 * 1024;
	s.entry_cap_bytes =
	    GetIntSetting(context, "recycler_entry_cap_mb", s.entry_cap_bytes / (1024 * 1024)) * 1024 * 1024;
	s.entry_cap_rows = GetIntSetting(context, "recycler_entry_cap_rows", s.entry_cap_rows);
	s.min_input_rows = GetIntSetting(context, "recycler_min_input_rows", s.min_input_rows);
	s.speculate_bytes =
	    GetIntSetting(context, "recycler_speculate_mb", s.speculate_bytes / (1024 * 1024)) * 1024 * 1024;
	return s;
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
// Pre-optimize: nested-block hits
//===----------------------------------------------------------------------===//
struct BlockMatcher {
	BlockMatcher(Binder &binder, RecyclerCache &cache, const RecyclerSettings &settings, PlanSignatures &sig,
	             bool preserve_order)
	    : binder(binder), cache(cache), settings(settings), sig(sig), preserve_order(preserve_order) {
	}

	Binder &binder;
	RecyclerCache &cache;
	const RecyclerSettings &settings;
	PlanSignatures &sig;
	bool preserve_order;
	ColumnBindingReplacer replacer;
	bool replaced = false;

	void Walk(unique_ptr<LogicalOperator> &slot) {
		auto &op = *slot;
		if (AsRecyclerOperator(op)) {
			return;
		}
		auto consumer = CteConsumerSlot(op);
		if (consumer) {
			Walk(*consumer);
			return;
		}
		auto &info = sig.Get(op);
		if (info.Cacheable() && info.est_leaf_rows >= settings.min_input_rows && op.children.size() > 0) {
			auto entry = cache.Lookup(RecyclerEntryKind::WHOLE_RESULT, info.hash, sig, op);
			if (entry && CanServe(*entry, preserve_order) &&
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

//===----------------------------------------------------------------------===//
// Pre-optimize: predicate / column subsumption on SPJ blocks
//===----------------------------------------------------------------------===//
using binding_set_t = std::unordered_set<ColumnBinding, ColumnBindingHashFunction, ColumnBindingEquality>;

//! Collects every column binding referenced by operators outside of `block`
class ExternalReferenceCollector : public LogicalOperatorVisitor {
public:
	explicit ExternalReferenceCollector(const LogicalOperator &block) : block(block) {
	}

	void VisitOperator(LogicalOperator &op) override {
		if (&op == &block) {
			return;
		}
		VisitOperatorExpressions(op);
		VisitOperatorChildren(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		references.insert(expr.binding);
		return nullptr;
	}

	const LogicalOperator &block;
	binding_set_t references;
};

static void CollectBindings(const Expression &expr, vector<ColumnBinding> &out) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		out.push_back(expr.Cast<BoundColumnRefExpression>().binding);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { CollectBindings(child, out); });
}

struct SubsumptionPass {
	SubsumptionPass(ClientContext &context, Binder &binder, shared_ptr<RecyclerCache> cache,
	                const RecyclerSettings &settings, PlanSignatures &sig, LogicalOperator &plan)
	    : context(context), binder(binder), cache(std::move(cache)), settings(settings), sig(sig), plan(plan) {
	}

	ClientContext &context;
	Binder &binder;
	shared_ptr<RecyclerCache> cache;
	const RecyclerSettings &settings;
	PlanSignatures &sig;
	LogicalOperator &plan;
	ColumnBindingReplacer replacer;
	bool replaced = false;

	void Walk(unique_ptr<LogicalOperator> &slot) {
		auto &op = *slot;
		if (AsRecyclerOperator(op)) {
			return;
		}
		auto consumer = CteConsumerSlot(op);
		if (consumer) {
			Walk(*consumer);
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_FILTER && sig.Get(op).Cacheable()) {
			auto shape = ComputeBlockShape(context, op, sig);
			if (shape.ok) {
				if (TryHit(slot, shape)) {
					replaced = true;
					return;
				}
				if (AdmitBlockWrite(*cache, settings, shape)) {
					Plant(slot, shape);
					return;
				}
			}
		}
		for (auto &child : op.children) {
			Walk(child);
		}
	}

	void Plant(unique_ptr<LogicalOperator> &slot, BlockShape &shape) {
		// a narrower consumer re-applies its predicates on the stored rows, so the constrained columns must be part
		// of the stored block even when the payer itself only uses them in its WHERE clause
		vector<unique_ptr<Expression>> keep_alive;
		{
			auto bindings = slot->GetColumnBindings();
			auto &types = slot->types;
			vector<idx_t> kept;
			for (auto &constraint : shape.constraints) {
				for (idx_t pos = 0; pos < shape.output.size(); pos++) {
					if (shape.output[pos] == constraint.column &&
					    std::find(kept.begin(), kept.end(), pos) == kept.end() && pos < types.size()) {
						kept.push_back(pos);
						keep_alive.push_back(make_uniq<BoundColumnRefExpression>(types[pos], bindings[pos]));
					}
				}
			}
		}
		auto spec = RecyclerWriteSpec::Make(RecyclerEntryKind::BLOCK, shape.hash, std::move(shape.canonical), false,
		                                    shape.est_leaf_rows, settings);
		spec.constraints = std::move(shape.constraints);
		spec.conjuncts = std::move(shape.conjuncts);
		spec.table_ids = std::move(shape.table_ids);
		cache->stats.blocks_planted++;
		WrapMaterialize(slot, std::move(spec), cache, std::move(keep_alive));
	}

	bool TryHit(unique_ptr<LogicalOperator> &slot, const BlockShape &shape) {
		auto entries = cache->LookupBlocks(shape.hash);
		if (entries.empty()) {
			return false;
		}
		auto &root = *slot;
		auto bindings = root.GetColumnBindings();
		auto &types = root.types;
		// which of the block's output columns does the rest of the query need?
		ExternalReferenceCollector collector(root);
		collector.VisitOperator(plan);
		for (auto &entry : entries) {
			if (entry->canonical != shape.canonical) {
				continue;
			}
			if (!RangeConstraintsImply(shape.constraints, entry->constraints)) {
				continue;
			}
			if (entry->rows > shape.est_leaf_rows / 2) {
				// scanning the cached rows back would not save enough
				continue;
			}
			// conjuncts of the query that the cached rows do not already satisfy exactly
			std::unordered_set<string> cached_conjuncts(entry->conjuncts.begin(), entry->conjuncts.end());
			vector<idx_t> residual;
			for (idx_t i = 0; i < shape.conjuncts.size(); i++) {
				if (cached_conjuncts.find(shape.conjuncts[i]) == cached_conjuncts.end()) {
					residual.push_back(i);
				}
			}
			binding_set_t needed = collector.references;
			for (auto i : residual) {
				vector<ColumnBinding> refs;
				CollectBindings(*root.expressions[i], refs);
				needed.insert(refs.begin(), refs.end());
			}
			// map every needed binding to a stored column of the same identity and type
			vector<column_t> scan_columns;
			vector<LogicalType> scan_types;
			vector<std::pair<ColumnBinding, idx_t>> mapping; // old binding -> position in the scan output
			bool usable = true;
			for (idx_t pos = 0; pos < bindings.size() && usable; pos++) {
				if (needed.find(bindings[pos]) == needed.end()) {
					continue;
				}
				auto &identity = shape.output[pos];
				if (!identity.IsValid()) {
					usable = false;
					break;
				}
				idx_t stored = DConstants::INVALID_INDEX;
				for (idx_t j = 0; j < entry->columns.size(); j++) {
					if (entry->columns[j] == identity && entry->types[j] == types[pos]) {
						stored = j;
						break;
					}
				}
				if (stored == DConstants::INVALID_INDEX) {
					usable = false;
					break;
				}
				idx_t k = DConstants::INVALID_INDEX;
				for (idx_t j = 0; j < scan_columns.size(); j++) {
					if (scan_columns[j] == stored) {
						k = j;
					}
				}
				if (k == DConstants::INVALID_INDEX) {
					k = scan_columns.size();
					scan_columns.push_back(stored);
					scan_types.push_back(entry->types[stored]);
				}
				mapping.emplace_back(bindings[pos], k);
			}
			if (!usable) {
				continue;
			}
			if (scan_columns.empty()) {
				// nothing above references the block (e.g. count(*)): keep one column so the row count survives
				if (entry->types.empty()) {
					continue;
				}
				scan_columns.push_back(0);
				scan_types.push_back(entry->types[0]);
			}
			// rewrite: scan of the stored columns, plus the residual predicates
			auto new_index = binder.GenerateTableIndex();
			auto scan = make_uniq<LogicalRecyclerScan>(new_index, scan_types, entry, false, scan_columns);
			scan->ResolveOperatorTypes();
			for (auto &m : mapping) {
				replacer.replacement_bindings.emplace_back(m.first, ColumnBinding(new_index, m.second));
			}
			unique_ptr<LogicalOperator> replacement = std::move(scan);
			if (!residual.empty()) {
				auto filter = make_uniq<LogicalFilter>();
				for (auto i : residual) {
					filter->expressions.push_back(root.expressions[i]->Copy());
				}
				filter->children.push_back(std::move(replacement));
				filter->ResolveOperatorTypes();
				replacement = std::move(filter);
			}
			cache->NoteHit(*entry);
			cache->stats.subsumption_hits++;
			slot = std::move(replacement);
			return true;
		}
		return false;
	}
};

//===----------------------------------------------------------------------===//
// Pre-optimize: the hook
//===----------------------------------------------------------------------===//
//! Gate: which statements take part. Returns the query root's slot, or nullptr with the reason counted.
static unique_ptr<LogicalOperator> *GateStatement(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                                  RecyclerCache &cache, RecyclerClientState &client) {
	auto &context = input.context;
	if (plan->type == LogicalOperatorType::LOGICAL_SET || plan->type == LogicalOperatorType::LOGICAL_RESET ||
	    plan->type == LogicalOperatorType::LOGICAL_PRAGMA) {
		client.settings_loaded = false;
	}
	auto root_slot = FindQueryRoot(plan);
	if (!root_slot) {
		cache.stats.skip_not_query++;
		return nullptr;
	}
	auto &root = **root_slot;
	if (IsModifyingRoot(root.type)) {
		cache.stats.skip_modifying++;
		cache.Clear();
		return nullptr;
	}
	if (!IsReadRoot(root.type)) {
		cache.stats.skip_not_read++;
		return nullptr;
	}
	if (!client.settings_loaded) {
		client.settings = LoadRecyclerSettings(context);
		client.settings_loaded = true;
	}
	if (!client.settings.enabled) {
		cache.stats.skip_disabled++;
		return nullptr;
	}
	// statements that modify data are detected at bind time; a running write transaction must not be served
	// from or written into the cache either
	if (!input.optimizer.binder.GetStatementProperties().modified_databases.empty()) {
		cache.stats.skip_modified_databases++;
		cache.Clear();
		return nullptr;
	}
	if (context.transaction.HasActiveTransaction() && MetaTransaction::Get(context).ModifiedDatabase()) {
		cache.stats.skip_write_transaction++;
		return nullptr;
	}
	return root_slot;
}

//! Whole result: serve a cached result (true), or remember the key so the post hook can write one
static bool ServeOrPlanWholeResult(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &root_slot,
                                   RecyclerCache &cache, RecyclerClientState &client, PlanSignatures &sig,
                                   bool preserve_order) {
	auto &root = *root_slot;
	auto &root_info = sig.Get(root);
	auto entry = cache.Lookup(RecyclerEntryKind::WHOLE_RESULT, root_info.hash, sig, root);
	if (entry && CanServe(*entry, preserve_order)) {
		ColumnBindingReplacer unused;
		if (SubstituteScan(root_slot, entry, entry->ordered, input.optimizer.binder, unused)) {
			cache.stats.whole_hits++;
			return true;
		}
	}
	// unordered results are written by a parallel sink; with preserve_insertion_order that would change the
	// observable row order, and a serial sink would run the whole pipeline on one thread: neither is acceptable
	bool writable = root_info.ordered || !preserve_order;
	if (writable && root_info.est_leaf_rows >= client.settings.min_input_rows) {
		client.has_pending_whole = true;
		client.whole_hash = root_info.hash;
		client.whole_canonical = sig.Render(root);
		client.whole_est_input_rows = root_info.est_leaf_rows;
	}
	return false;
}

//! Nested blocks: sub-queries below the root that equal an earlier whole query
static void ServeNestedBlocks(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan, LogicalOperator &root,
                              RecyclerCache &cache, const RecyclerSettings &settings, PlanSignatures &sig,
                              bool preserve_order) {
	BlockMatcher matcher(input.optimizer.binder, cache, settings, sig, preserve_order);
	for (auto &child : root.children) {
		matcher.Walk(child);
	}
	if (matcher.replaced) {
		matcher.replacer.VisitOperator(*plan);
	}
}

//! Subsumption: SPJ blocks served from wider cached blocks, admissible misses planted with a marker
static void ServeSubsumedBlocks(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                unique_ptr<LogicalOperator> &root_slot, RecyclerState &state,
                                const RecyclerSettings &settings, PlanSignatures &sig) {
	SubsumptionPass pass(input.context, input.optimizer.binder, state.cache, settings, sig, *plan);
	pass.Walk(root_slot);
	if (pass.replaced) {
		pass.replacer.VisitOperator(*plan);
	}
}

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto state = StateFromInput(input);
	if (!state) {
		return;
	}
	auto &cache = *state->cache;
	auto &context = input.context;
	ScopedTimer timer(cache.stats.pre_ns);

	auto &client = RecyclerClientState::Get(context);
	client.Reset();
	auto root_slot = GateStatement(input, plan, cache, client);
	if (!root_slot) {
		return;
	}
	cache.stats.pre_calls++;
	client.active = true;
	auto &settings = client.settings;
	bool preserve_order = Settings::Get<PreserveInsertionOrderSetting>(context);

	auto &root = **root_slot;
	PlanSignatures sig(context);
	try {
		plan->ResolveOperatorTypes();
		sig.Analyze(root);
	} catch (std::exception &) {
		cache.stats.skip_analyze_failed++;
		client.active = false;
		return;
	}
	auto &root_info = sig.Get(root);
	if (!root_info.Cacheable()) {
		cache.stats.root_not_cacheable++;
		cache.NoteRefused(root_info.hash, root_info.Reason());
	}

	if (settings.whole_result && root_info.Cacheable()) {
		if (ServeOrPlanWholeResult(input, *root_slot, cache, client, sig, preserve_order)) {
			return;
		}
	}
	if (settings.blocks) {
		ServeNestedBlocks(input, plan, root, cache, settings, sig, preserve_order);
	}
	if (settings.subsumption && !preserve_order) {
		// block results are written by a parallel sink, so their row order is arbitrary: only usable when the
		// session does not promise insertion order
		ServeSubsumedBlocks(input, plan, *root_slot, *state, settings, sig);
	}
}

//===----------------------------------------------------------------------===//
// Post-optimize: subtree hits/writes
//===----------------------------------------------------------------------===//
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

	void Walk(unique_ptr<LogicalOperator> &slot, bool is_root, WritePolicy policy, bool under_delim_lhs,
	          bool force_candidate) {
		auto &op = *slot;
		if (AsRecyclerOperator(op)) {
			return;
		}
		auto &info = sig.Get(op);
		bool candidate = !is_root && (force_candidate || IsSubtreeCandidateType(op.type)) && info.Cacheable();
		if (candidate) {
			cache->NoteSeen(info.hash);
			auto entry = cache->Lookup(RecyclerEntryKind::SUBTREE, info.hash, sig, op);
			if (entry && SubstituteScan(slot, entry, entry->ordered, binder, replacer)) {
				cache->stats.subtree_hits++;
				replaced = true;
				return;
			}
			bool may_write =
			    policy == WritePolicy::ALL || (policy == WritePolicy::AGGREGATES_ONLY && IsAggregation(op.type));
			if (may_write && AdmitSubtreeWrite(context, *cache, settings, op, info, under_delim_lhs, force_candidate)) {
				auto spec = RecyclerWriteSpec::Make(RecyclerEntryKind::SUBTREE, info.hash, sig.Render(op), info.ordered,
				                                    info.est_leaf_rows, settings);
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

//===----------------------------------------------------------------------===//
// Post-optimize: the hook
//===----------------------------------------------------------------------===//
//! Subtrees: exact hits replace subtrees, admissible misses get a RecyclerMaterialize
static void RewriteSubtrees(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                            unique_ptr<LogicalOperator> &root_slot, RecyclerState &state,
                            const RecyclerSettings &settings, PlanSignatures &sig) {
	SubtreeWalker walker(input.context, input.optimizer.binder, state.cache, settings, sig);
	walker.Walk(root_slot, true, WritePolicy::ALL, false, false);
	if (walker.replaced) {
		walker.replacer.VisitOperator(*plan);
	}
}

//! Whole result: wrap the root so that the result the pre hook missed is written
static void WriteWholeResult(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &root_slot,
                             RecyclerState &state, RecyclerClientState &client, const RecyclerSettings &settings,
                             const NodeInfo &root_info) {
	auto &root = *root_slot;
	if (!AdmitWholeWrite(input.context, *state.cache, settings, root, root_info, client.whole_hash)) {
		return;
	}
	// an ordered result is written by a serial sink (only the sort's output stage is serialized) and replayed in
	// that order; anything else is written and replayed in parallel
	auto spec =
	    RecyclerWriteSpec::Make(RecyclerEntryKind::WHOLE_RESULT, client.whole_hash, std::move(client.whole_canonical),
	                            root_info.ordered, client.whole_est_input_rows, settings);
	state.cache->stats.writes_planned++;
	WrapMaterialize(root_slot, std::move(spec), state.cache);
}

void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto state = StateFromInput(input);
	if (!state) {
		return;
	}
	auto &cache = state->cache;
	auto &context = input.context;
	ScopedTimer timer(cache->stats.post_ns);

	auto &client = RecyclerClientState::Get(context);
	if (!client.active) {
		return;
	}
	client.active = false;
	client.profiled = QueryProfiler::Get(context).IsEnabled();
	auto settings = client.settings;

	auto root_slot = FindQueryRoot(plan);
	if (!root_slot) {
		return;
	}
	if (AsRecyclerOperator(**root_slot)) {
		// whole-result hit at the pre hook
		return;
	}
	cache->stats.post_calls++;

	// planted block markers learn which base columns their (now pruned and reordered) child produces
	ResolveBlockColumns(*root_slot);

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
		RewriteSubtrees(input, plan, *root_slot, *state, settings, sig);
	}
	if (settings.whole_result && client.has_pending_whole) {
		WriteWholeResult(input, *root_slot, *state, client, settings, root_info);
	}
	client.Reset();
}

} // namespace duckdb
