//==-------------------------------------------------------------------==//
// join_utils.cpp – single definition of join-skeleton helpers.
// Logic is identical to the original recycler_optimizer.cpp helpers;
// the only change is that they are now compiled once and shared.
//==-------------------------------------------------------------------==//
#include "join_utils.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

// ── CollectTableNames ─────────────────────────────────────────────
void CollectTableNames(const LogicalOperator &op, TableMap &table_map) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        const auto &get = op.Cast<LogicalGet>();
        // table_index is a plain idx_t (v1.5.5 API), matching original line 38.
        table_map[get.table_index] = get.GetName();
    }
    for (const auto &child : op.children) {
        CollectTableNames(*child, table_map);
    }
}

//! True for the comparison ExpressionTypes a JoinCondition can carry.
static bool IsComparisonExpressionType(ExpressionType type) {
    switch (type) {
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOTEQUAL:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    case ExpressionType::COMPARE_DISTINCT_FROM:
    case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
        return true;
    default:
        return false;
    }
}

// ── CollectJoinConditions ─────────────────────────────────────────
void CollectJoinConditions(const LogicalOperator &op,
                           const TableMap &table_map,
                           std::set<string> &conditions) {
    if (IsJoinNode(op.type)) {
        const auto &join = op.Cast<LogicalComparisonJoin>();
        for (const auto &cond : join.conditions) {
            // Guard 1: must be an equality/comparison predicate (line 52).
            if (!IsComparisonExpressionType(cond.comparison)) continue;
            // Guard 2: both sides must be column references (lines 53-54).
            if (cond.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) continue;
            if (cond.right->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) continue;

            const auto &lref = cond.left->Cast<BoundColumnRefExpression>();
            const auto &rref = cond.right->Cast<BoundColumnRefExpression>();

            // Table name lookup with fallback "t<idx>" (lines 59-64).
            auto lit = table_map.find(lref.binding.table_index);
            auto rit = table_map.find(rref.binding.table_index);
            string ltable = (lit != table_map.end()) ? lit->second
                            : "t" + to_string(lref.binding.table_index);
            string rtable = (rit != table_map.end()) ? rit->second
                            : "t" + to_string(rref.binding.table_index);

            // Column identity: numeric index, not alias-sensitive GetName() (lines 67-68).
            string lcol = ltable + "." + to_string(lref.binding.column_index);
            string rcol = rtable + "." + to_string(rref.binding.column_index);

            // Canonical order (line 70).
            if (lcol > rcol) { swap(lcol, rcol); }
            conditions.insert(lcol + "=" + rcol);
        }
    }
    for (const auto &child : op.children) {
        CollectJoinConditions(*child, table_map, conditions);
    }
}

// ── ComputeJoinSkeleton ───────────────────────────────────────────
string ComputeJoinSkeleton(const LogicalOperator &op,
                           ClientContext & /*context*/) {
    TableMap table_map;
    CollectTableNames(op, table_map);

    std::set<string> tables;
    for (const auto &kv : table_map) { tables.insert(kv.second); }

    std::set<string> conditions;
    CollectJoinConditions(op, table_map, conditions);

    // Key format exactly matches original lines 94-99.
    std::ostringstream sig;
    sig << "tables:";
    for (const auto &t : tables)    { sig << t << ","; }
    sig << "|joins:";
    for (const auto &c : conditions){ sig << c << ","; }
    return sig.str();
}

} // namespace duckdb
