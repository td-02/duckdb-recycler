#pragma once
//===----------------------------------------------------------------------===//
// recycler_exists.hpp - correlated EXISTS subqueries as plain joins the join order optimizer can place
//===----------------------------------------------------------------------===//
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct RecyclerStats;

//! Rewrites every correlated EXISTS / NOT EXISTS delim join of the bound plan whose correlation is a conjunction of
//! column equalities in the subquery's WHERE clause into a plain MARK join over the subquery, and moves that join
//! below the operators the built-in join order optimizer cannot reorder (see recycler_exists.cpp). The rows of every
//! operator are unchanged; anything else in the plan is left alone. `stats` counts rewrites, moves and refusals.
void RewriteExistsJoins(unique_ptr<LogicalOperator> &plan, RecyclerStats &stats);

} // namespace duckdb
