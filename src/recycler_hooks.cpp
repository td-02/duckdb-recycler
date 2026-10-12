//===----------------------------------------------------------------------===//
// recycler_hooks.cpp - the pre- and post-optimizer passes
//
// Pre-optimize (before any built-in optimizer, i.e. on the bound plan):
//   * writes announce their target table (or the database they modify) before
//     they run; statements outside any database (ATTACH, LOAD) clear the cache
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
// The decisions live in recycler_policy, the plan surgery in recycler_rewrite,
// the passes in recycler_subsumption, recycler_pagination and recycler_subtrees;
// this file classifies the statement and orders the steps.
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"
#include "recycler_exists.hpp"
#include "recycler_operators.hpp"
#include "recycler_pagination.hpp"
#include "recycler_policy.hpp"
#include "recycler_rewrite.hpp"
#include "recycler_signature.hpp"
#include "recycler_subsumption.hpp"
#include "recycler_subtrees.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <chrono>

namespace duckdb {

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
//! Statements that write the rows of one table (TRUNCATE is a DELETE, COPY FROM an INSERT, RETURNING a projection
//! over the write): the target is announced to the cache, which withholds entries over it until the transaction
//! ends
static optional_ptr<TableCatalogEntry> WriteTarget(LogicalOperator &root) {
	switch (root.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return root.children.size() == 1 ? WriteTarget(*root.children[0]) : nullptr;
	case LogicalOperatorType::LOGICAL_INSERT:
		return root.Cast<LogicalInsert>().table;
	case LogicalOperatorType::LOGICAL_DELETE:
		return root.Cast<LogicalDelete>().table;
	case LogicalOperatorType::LOGICAL_UPDATE:
		return root.Cast<LogicalUpdate>().table;
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		return root.Cast<LogicalMergeInto>().table;
	default:
		return nullptr;
	}
}

//! Statements that may change state the cache does not track and that modify no database the cache can stamp
//! (ATTACH, DETACH, LOAD, ...): drop everything we cached
//! Statements that change this connection's settings: the cached copy is dropped once they ran (QueryEnd)
static bool IsSettingsRoot(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_SET || type == LogicalOperatorType::LOGICAL_RESET ||
	       type == LogicalOperatorType::LOGICAL_PRAGMA;
}

static bool IsModifyingRoot(LogicalOperatorType type) {
	switch (type) {
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
	BlockMatcher(Binder &binder, RecyclerCache &cache, const RecyclerClientState &client, PlanSignatures &sig,
	             bool preserve_order)
	    : binder(binder), cache(cache), settings(client.settings), snapshot(client.snapshot), sig(sig),
	      preserve_order(preserve_order) {
	}

	Binder &binder;
	RecyclerCache &cache;
	const RecyclerSettings &settings;
	const TableSnapshot &snapshot;
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
			auto entry = cache.Lookup(RecyclerEntryKind::WHOLE_RESULT, info, sig, op, snapshot);
			if (entry && CanServe(*entry, preserve_order) &&
			    SubstituteScan(slot, entry, entry->ordered, info.tables, binder, replacer)) {
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
// Pre-optimize: the hook
//===----------------------------------------------------------------------===//
//! Gate: which statements take part. Returns the query root's slot, or nullptr with the reason counted.
static unique_ptr<LogicalOperator> *GateStatement(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                                  RecyclerCache &cache, RecyclerClientState &client) {
	auto &context = input.context;
	if (IsSettingsRoot(plan->type)) {
		client.settings_change_pending = true;
	}
	// announced before the write runs, whatever this session's settings say: a reader that starts after the commit
	// must never find an entry the cache has not yet heard about (the stamp follows at commit). A database the
	// statement modifies without a table named here is announced as a whole: DDL, and EXECUTE, whose root
	// (LOGICAL_EXECUTE) carries the prepared statement's modified databases. PREPARE (LOGICAL_PREPARE) runs
	// nothing and modifies nothing. A plain EXPLAIN writes nothing, but DuckDB marks the databases the explained
	// statement modifies as modified, and so commits them.
	auto explained = plan->type == LogicalOperatorType::LOGICAL_EXPLAIN && plan->children.size() == 1;
	auto &statement = explained ? *plan->children[0] : *plan;
	auto target = WriteTarget(statement);
	auto &modified = input.optimizer.binder.GetStatementProperties().modified_databases;
	client.AnnounceStatement(context, cache, target, modified);
	auto root_slot = FindQueryRoot(plan);
	if (!root_slot) {
		cache.stats.skip_not_query++;
		return nullptr;
	}
	auto &root = **root_slot;
	if (target) {
		cache.stats.announced_table_writes++;
		return nullptr;
	}
	if (!modified.empty()) {
		cache.stats.skip_modified_databases++;
		return nullptr;
	}
	if (IsModifyingRoot(root.type)) {
		cache.stats.skip_modifying++;
		cache.Clear();
		return nullptr;
	}
	if (!IsReadRoot(root.type)) {
		cache.stats.skip_not_read++;
		return nullptr;
	}
	if (!client.GetSettings(context).enabled) {
		cache.stats.skip_disabled++;
		return nullptr;
	}
	// a running write transaction must not be served from or written into the cache
	if (context.transaction.HasActiveTransaction() && MetaTransaction::Get(context).ModifiedDatabase()) {
		cache.stats.skip_write_transaction++;
		return nullptr;
	}
	if (!client.transaction_tracked) {
		cache.stats.skip_untracked_transaction++;
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
	auto entry = cache.Lookup(RecyclerEntryKind::WHOLE_RESULT, root_info, sig, root, client.snapshot);
	if (entry && CanServe(*entry, preserve_order)) {
		ColumnBindingReplacer unused;
		if (SubstituteScan(root_slot, entry, entry->ordered, root_info.tables, input.optimizer.binder, unused)) {
			cache.stats.whole_hits++;
			return true;
		}
	}
	// unordered results are written by a parallel sink; with preserve_insertion_order that would change the
	// observable row order, and a serial sink would run the whole pipeline on one thread: neither is acceptable
	bool writable = root_info.ordered || !preserve_order;
	if (writable && root_info.est_leaf_rows >= client.settings.min_input_rows &&
	    cache.Admissible(root_info.tables, client.snapshot)) {
		client.has_pending_whole = true;
		client.whole_hash = root_info.hash;
		client.whole_canonical = sig.Render(root);
		client.whole_est_input_rows = root_info.est_leaf_rows;
		client.whole_tables = TableDependencies(root_info.tables, client.snapshot);
	}
	return false;
}

//! Nested blocks: sub-queries below the root that equal an earlier whole query
static void ServeNestedBlocks(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan, LogicalOperator &root,
                              RecyclerCache &cache, const RecyclerClientState &client, PlanSignatures &sig,
                              bool preserve_order) {
	BlockMatcher matcher(input.optimizer.binder, cache, client, sig, preserve_order);
	for (auto &child : root.children) {
		matcher.Walk(child);
	}
	if (matcher.replaced) {
		matcher.replacer.VisitOperator(*plan);
	}
}

//! The cache's part of the pre hook: announcements, hits and the markers of admissible misses
static void RecyclePreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                               RecyclerState &state) {
	auto &cache = *state.cache;
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
	client.settings_salt = PlanSignatures::SettingsSalt(context);
	PlanSignatures sig(context, client.settings_salt);
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
		// the hash of a refused subtree is undefined, so the refusal is counted but not remembered per signature
		auto reason = root_info.Reason();
		D_ASSERT(reason != RefusalReason::NONE);
		cache.stats.root_not_cacheable++;
		cache.stats.refused[static_cast<idx_t>(reason)]++;
	}
	client.snapshot = TakeTableSnapshot(context, root_info.tables);
	if (settings.subtrees && settings.pagination) {
		DetectPagination(client, sig, root);
	}

	if (settings.whole_result && root_info.Cacheable()) {
		if (ServeOrPlanWholeResult(input, *root_slot, cache, client, sig, preserve_order)) {
			return;
		}
	}
	if (settings.blocks) {
		ServeNestedBlocks(input, plan, root, cache, client, sig, preserve_order);
	}
	if (settings.subsumption && !preserve_order) {
		// block results are written by a parallel sink, so their row order is arbitrary: only usable when the
		// session does not promise insertion order
		ServeSubsumedBlocks(input, plan, *root_slot, state, client, sig);
	}
}

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto state = StateFromInput(input);
	if (!state) {
		return;
	}
	RecyclePreOptimize(input, plan, *state);
	// after every key was taken and every marker planted: the keys of this statement and of the ones it may serve
	// are those of the plan as bound, and the rewrite below is the same for every statement of a shape, so a payer
	// and the queries it serves still render alike. Independent of the gate above: a statement the cache declines
	// (a write, EXPLAIN, a transaction it does not track) still gets the better plan
	auto &client = RecyclerClientState::Get(input.context);
	if (client.GetSettings(input.context).exists_rewrite) {
		auto &stats = state->cache->stats;
		ScopedTimer timer(stats.exists_ns);
		RewriteExistsJoins(plan, stats);
	}
}

//===----------------------------------------------------------------------===//
// Post-optimize: the hook
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
	auto spec = RecyclerWriteSpec::Make(RecyclerEntryKind::WHOLE_RESULT, client.whole_hash,
	                                    std::move(client.whole_canonical), root_info.ordered,
	                                    client.whole_est_input_rows, std::move(client.whole_tables), settings);
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
	if (!root_slot || AsRecyclerOperator(**root_slot)) {
		// nothing to rewrite: the pre hook served the whole result
		client.Reset();
		return;
	}
	cache->stats.post_calls++;

	// planted block markers learn which base columns their (now pruned and reordered) child produces
	ResolveBlockColumns(*root_slot);

	PlanSignatures sig(context, client.settings_salt);
	try {
		plan->ResolveOperatorTypes();
		sig.Analyze(**root_slot);
	} catch (std::exception &) {
		client.Reset();
		return;
	}
	// the root's analysis (rewrites below the root do not touch the analysis map)
	const auto &root_info = sig.Get(**root_slot);
	client.snapshot = TakeTableSnapshot(context, root_info.tables);

	if (settings.subtrees) {
		RewriteSubtrees(input, plan, *root_slot, *state, client, sig);
	}
	if (settings.whole_result && client.has_pending_whole) {
		WriteWholeResult(input, *root_slot, *state, client, settings, root_info);
	}
	client.Reset();
}

} // namespace duckdb