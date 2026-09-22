#pragma once
//==-------------------------------------------------------------------==//
// join_utils.hpp
// Single definition point for join-skeleton helpers shared across modules.
// Eliminates duplication from the original monolithic recycler_optimizer.cpp.
//==-------------------------------------------------------------------==//
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/main/client_context.hpp"

#include <set>
#include <string>
#include <unordered_map>

namespace duckdb {

// key type: idx_t matching get.table_index and binding.table_index
using TableMap = std::unordered_map<idx_t, string>;

//! True for join operator types we recognise (matches original IsJoinNode).
inline bool IsJoinNode(LogicalOperatorType type) {
    return type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
           type == LogicalOperatorType::LOGICAL_DELIM_JOIN     ||
           type == LogicalOperatorType::LOGICAL_ANY_JOIN;
}

//! Walk subtree, populate table_map with { table_index → name }.
void CollectTableNames(const LogicalOperator &op, TableMap &table_map);

//! Walk subtree, append canonical "tbl.col_idx=tbl.col_idx" strings.
//! Falls back to "t<idx>" for bindings not found in table_map.
//! Guards on cond.comparison being a comparison type and BOUND_COLUMN_REF on both sides.
void CollectJoinConditions(const LogicalOperator &op,
                           const TableMap &table_map,
                           std::set<string> &conditions);

//! Returns "tables:t1,t2,...|joins:c1,c2,...|filters:f1,f2,...|cols:c1,c2,...".
//! filters covers pushed-down LogicalGet::table_filters and any residual
//! LOGICAL_FILTER nodes in the subtree; cols is the *ordered* output column
//! identity of op itself. Both are part of the key: two joins can share the
//! same tables and join condition while scanning different rows (different
//! WHERE filters) or exposing a different projection, and a cache hit
//! substitutes cached columns into the caller's plan positionally.
//! Returns the sentinel "tables:|joins:|filters:|cols:" when the subtree has
//! no tables (WalkAndRecycle treats this as "nothing to cache").
string ComputeJoinSkeleton(LogicalOperator &op, ClientContext &context);

} // namespace duckdb
