//==-------------------------------------------------------------------==//
// recycler_optimizer.cpp
// Skeleton analysis delegated to join_utils (no duplication).
// WalkAndRecycle logic is preserved exactly from main, just calling
// the shared helpers instead of file-local copies.
//==-------------------------------------------------------------------==//
#include "recycler_optimizer.hpp"
#include "logical_cache_writer.hpp"
#include "join_utils.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

#include <algorithm>
#include <set>
#include <sstream>
#include <unordered_map>

namespace duckdb {

// ── WalkAndRecycle ────────────────────────────────────────────────
// Fix #2: recurse into ALL children FIRST (bottom-up), then decide
// whether to act on the current node.  This ensures nested joins
// are all visited before their parent is considered.
static void WalkAndRecycle(unique_ptr<LogicalOperator> &op,
                            shared_ptr<RecyclerCache> cache,
                            ClientContext &context,
                            Optimizer &optimizer,
                            ColumnBindingReplacer &replacer) {
    // Bottom-up: process children before the current node.
    for (auto &child : op->children) {
        WalkAndRecycle(child, cache, context, optimizer, replacer);
    }

    // Only act on join nodes.
    if (!IsJoinNode(op->type)) { return; }

    // Compute skeleton (calls join_utils – no duplicate definition).
    string sig = ComputeJoinSkeleton(*op, context);

    // Fix #3: exact sentinel matching original line 125.
    if (sig.empty() || sig == "tables:|joins:") { return; }

    shared_ptr<CachedResult> cached;
    if (cache->Lookup(sig, cached)) {
        // ── CACHE HIT ──────────────────────────────────────────────
        if (op->types.empty()) { op->ResolveOperatorTypes(); }
        auto old_bindings = op->GetColumnBindings();
        auto col_types    = op->types;

        TableIndex new_idx = optimizer.binder.GenerateTableIndex();
        // optionally_owned_ptr: non-owning reference is safe because
        // the cache holds the shared_ptr keeping the collection alive.
        auto new_op = make_uniq<LogicalColumnDataGet>(
            new_idx, col_types,
            optionally_owned_ptr<ColumnDataCollection>(cached->collection));
        new_op->ResolveOperatorTypes();
        replacer.AddReplacements(old_bindings, new_op->GetColumnBindings());
        op = std::move(new_op);
        return;
    }

    // ── CACHE MISS ─────────────────────────────────────────────────
    // Fix #4: pass cache shared_ptr directly, no shared_from_this needed.
    auto writer = make_uniq<LogicalCacheWrite>(sig, cache);
    writer->children.push_back(std::move(op));
    writer->ResolveOperatorTypes();
    op = std::move(writer);
}

// ── RecyclerOptimize ──────────────────────────────────────────────
void RecyclerOptimize(OptimizerExtensionInput &input,
                      unique_ptr<LogicalOperator> &plan) {
    // Match original: reinterpret_cast, not dynamic_cast (line 172).
    auto *state = input.info
        ? reinterpret_cast<RecyclerState *>(input.info.get())
        : nullptr;
    if (!state || !state->cache) { return; }

    ColumnBindingReplacer replacer;
    WalkAndRecycle(plan, state->cache, input.context, input.optimizer, replacer);

    if (!replacer.replacement_bindings.empty()) {
        replacer.VisitOperator(*plan);
    }
}

} // namespace duckdb
