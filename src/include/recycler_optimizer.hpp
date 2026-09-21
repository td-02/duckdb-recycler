//===----------------------------------------------------------------------===//
// RecyclerOptimizer — OptimizerExtension hook that:
//   1. Walks the logical plan looking for join subtrees.
//   2. For each subtree, computes its join-skeleton signature.
//   3. Cache hit  → replace the subtree with a LogicalColumnDataGet scan.
//   4. Cache miss → wrap the subtree with a LogicalCacheWrite marker node
//      that materialises the results and stores them during execution.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "recycler_cache.hpp"

namespace duckdb {

//! Per-database state that holds the shared RecyclerCache.
//! Stored in OptimizerExtension::optimizer_info.
struct RecyclerState : public OptimizerExtensionInfo {
    shared_ptr<RecyclerCache> cache;
    explicit RecyclerState() : cache(make_shared_ptr<RecyclerCache>()) {}
};

//! Computes a canonical, order-independent signature string for a logical subtree.
string ComputeJoinSkeleton(const LogicalOperator &op, ClientContext &context);

//! The optimizer rule registered with DuckDB's OptimizerExtension API.
void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
