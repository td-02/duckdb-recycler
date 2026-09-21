//===----------------------------------------------------------------------===//
// RecyclerOptimizer — walks the logical plan, extracts join skeletons, and
// either injects a cache-hit scan or marks the subtree for cache-write.
//===----------------------------------------------------------------------===//
#include "recycler_optimizer.hpp"
#include "logical_cache_writer.hpp"

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

#include <algorithm>
#include <set>
#include <sstream>
#include <unordered_map>

namespace duckdb {

// ---------------------------------------------------------------------------
// Helpers: collect base tables and join conditions from a subtree
// ---------------------------------------------------------------------------

static void CollectTableNames(const LogicalOperator &op,
                              unordered_map<idx_t, string> &table_map) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op.Cast<LogicalGet>();
        string name = get.GetName();
        table_map[get.table_index] = name;
    }
    for (auto &child : op.children) {
        CollectTableNames(*child, table_map);
    }
}

static void CollectJoinConditions(const LogicalOperator &op,
                                   const unordered_map<idx_t, string> &table_map,
                                   set<string> &conditions) {
    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto &join = op.Cast<LogicalComparisonJoin>();
        for (auto &cond : join.conditions) {
            if (cond.left && cond.right &&
                cond.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
                cond.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {

                auto &lref = cond.left->Cast<BoundColumnRefExpression>();
                auto &rref = cond.right->Cast<BoundColumnRefExpression>();

                auto lit = table_map.find(lref.binding.table_index);
                auto rit = table_map.find(rref.binding.table_index);

                string ltable = (lit != table_map.end()) ? lit->second
                                                          : "t" + to_string(lref.binding.table_index);
                string rtable = (rit != table_map.end()) ? rit->second
                                                          : "t" + to_string(rref.binding.table_index);

                string lcol = ltable + "." + to_string(lref.binding.column_index);
                string rcol = rtable + "." + to_string(rref.binding.column_index);

                if (lcol > rcol) { swap(lcol, rcol); }
                conditions.insert(lcol + "=" + rcol);
            }
        }
    }
    for (auto &child : op.children) {
        CollectJoinConditions(*child, table_map, conditions);
    }
}

// ---------------------------------------------------------------------------
// Public: ComputeJoinSkeleton
// ---------------------------------------------------------------------------

string ComputeJoinSkeleton(const LogicalOperator &op, ClientContext & /*context*/) {
    unordered_map<idx_t, string> table_map;
    CollectTableNames(op, table_map);

    set<string> tables;
    for (auto &kv : table_map) { tables.insert(kv.second); }

    set<string> conditions;
    CollectJoinConditions(op, table_map, conditions);

    std::ostringstream sig;
    sig << "tables:";
    for (auto &t : tables) { sig << t << ","; }
    sig << "|joins:";
    for (auto &c : conditions) { sig << c << ","; }
    return sig.str();
}

// ---------------------------------------------------------------------------
// Walk the plan tree — bottom-up, one join subtree at a time
// ---------------------------------------------------------------------------

static bool IsJoinNode(LogicalOperatorType type) {
    return type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
           type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
           type == LogicalOperatorType::LOGICAL_ANY_JOIN;
}

static void WalkAndRecycle(unique_ptr<LogicalOperator> &op,
                            shared_ptr<RecyclerCache> cache,   // shared ownership
                            ClientContext &context,
                            Optimizer &optimizer,
                            ColumnBindingReplacer &replacer) {
    // Recurse into children first (bottom-up).
    for (auto &child : op->children) {
        WalkAndRecycle(child, cache, context, optimizer, replacer);
    }

    if (!IsJoinNode(op->type)) { return; }

    string sig = ComputeJoinSkeleton(*op, context);
    if (sig.empty() || sig == "tables:|joins:") { return; }

    shared_ptr<CachedResult> cached;
    if (cache->Lookup(sig, cached)) {
        // ── CACHE HIT ──────────────────────────────────────────────────────
        // Record old bindings before we replace the node.
        // Force type resolution if the node hasn't done it yet.
        if (op->types.empty()) { op->ResolveOperatorTypes(); }
        auto old_bindings = op->GetColumnBindings();
        auto col_types    = op->types;

        // Fresh table index for the replacement scan.
        idx_t new_idx = optimizer.binder.GenerateTableIndex();

        // Wrap shared_ptr<ColumnDataCollection> in an optionally_owned_ptr
        // (no copy; the cache keeps the data alive via the shared_ptr).
        auto new_op = make_uniq<LogicalColumnDataGet>(
            new_idx, col_types,
            optionally_owned_ptr<ColumnDataCollection>(cached->collection));
        new_op->ResolveOperatorTypes();

        // Register old→new binding rewrites for the post-walk pass.
        auto new_bindings = new_op->GetColumnBindings();
        // Register old→new binding rewrites.
        D_ASSERT(old_bindings.size() == new_bindings.size());
        for (idx_t i = 0; i < old_bindings.size(); i++) {
            replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
        }

        op = std::move(new_op);
        // ───────────────────────────────────────────────────────────────────

    } else {
        // ── CACHE MISS ─────────────────────────────────────────────────────
        // Wrap the join in a transparent LogicalCacheWrite node.
        // The wrapper exposes identical column bindings and types so the rest
        // of the plan is unaffected.  At execution time, PhysicalCacheWriter
        // will materialise the join result and store it in the cache.
        auto writer = make_uniq<LogicalCacheWrite>(sig, cache);
        writer->children.push_back(std::move(op));
        writer->ResolveOperatorTypes();
        op = std::move(writer);
        // ───────────────────────────────────────────────────────────────────
    }
}

// ---------------------------------------------------------------------------
// Public: RecyclerOptimize  (registered as optimize_function_t)
// ---------------------------------------------------------------------------

void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    auto *state = input.info ? reinterpret_cast<RecyclerState *>(input.info.get()) : nullptr;
    if (!state || !state->cache) { return; }

    ColumnBindingReplacer replacer;
    WalkAndRecycle(plan, state->cache, input.context, input.optimizer, replacer);

    // Fix up any column-binding references in parent operators that pointed
    // to join output columns now served by cache-hit scans.
    if (!replacer.replacement_bindings.empty()) {
        replacer.VisitOperator(*plan);
    }
}

} // namespace duckdb
