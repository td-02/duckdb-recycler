//==-------------------------------------------------------------------==//
// join_utils.cpp – single definition of join-skeleton helpers.
// Logic is identical to the original recycler_optimizer.cpp helpers;
// the only change is that they are now compiled once and shared.
//==-------------------------------------------------------------------==//
#include "join_utils.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

// ── CollectTableNames ─────────────────────────────────────────────
void CollectTableNames(const LogicalOperator &op, TableMap &table_map) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        const auto &get = op.Cast<LogicalGet>();
        // get.GetName() returns the *scan function* name (e.g. "SEQ_SCAN")
        // in this DuckDB version, not the table -- every table would
        // collapse to the same string. GetTable() gives the actual
        // catalog entry; fall back to "t<idx>" if it's a table function
        // with no catalog entry (e.g. read_csv).
        auto table = get.GetTable();
        table_map[get.table_index] = table ? table->name : "t" + to_string(get.table_index);
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

// ── CollectFilters ─────────────────────────────────────────────────
// Two joins over the same tables with the same join condition can still
// scan entirely different rows: a WHERE clause on either side is pushed
// down into LogicalGet::table_filters (invisible to CollectJoinConditions),
// or -- if it can't be pushed down -- survives as a LOGICAL_FILTER above
// the join. Both must be part of the cache key, or a cache hit can return
// another query's rows.
static void CollectFilters(const LogicalOperator &op, const TableMap &table_map,
                           std::set<string> &filters) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        const auto &get = op.Cast<LogicalGet>();
        auto tit = table_map.find(get.table_index);
        string tname = (tit != table_map.end()) ? tit->second : "t" + to_string(get.table_index);
        for (const auto &entry : get.table_filters.filters) {
            string col_label = to_string(entry.first);
            filters.insert(tname + "#" + col_label + ":" + entry.second->ToString(col_label));
        }
    } else if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
        for (const auto &expr : op.expressions) {
            filters.insert("filter:" + expr->ToString());
        }
    }
    for (const auto &child : op.children) {
        CollectFilters(*child, table_map, filters);
    }
}

// ── ComputeJoinSkeleton ───────────────────────────────────────────
string ComputeJoinSkeleton(LogicalOperator &op,
                           ClientContext & /*context*/) {
    TableMap table_map;
    CollectTableNames(op, table_map);

    std::set<string> tables;
    for (const auto &kv : table_map) { tables.insert(kv.second); }

    std::set<string> conditions;
    CollectJoinConditions(op, table_map, conditions);

    std::set<string> filters;
    CollectFilters(op, table_map, filters);

    // Output columns, in order: two joins with identical tables/conditions/
    // filters can still be asked for a different set (or order) of columns,
    // and a cache hit substitutes a LogicalColumnDataGet whose columns are
    // matched to the caller's bindings *positionally* -- so the exact,
    // ordered column identity has to be part of the key, not just a set.
    std::ostringstream cols;
    for (const auto &binding : op.GetColumnBindings()) {
        auto it = table_map.find(binding.table_index);
        string tname = (it != table_map.end()) ? it->second : "t" + to_string(binding.table_index);
        cols << tname << "." << binding.column_index << ",";
    }

    std::ostringstream sig;
    sig << "tables:";
    for (const auto &t : tables)    { sig << t << ","; }
    sig << "|joins:";
    for (const auto &c : conditions){ sig << c << ","; }
    sig << "|filters:";
    for (const auto &f : filters)   { sig << f << ","; }
    sig << "|cols:" << cols.str();
    return sig.str();
}

} // namespace duckdb
