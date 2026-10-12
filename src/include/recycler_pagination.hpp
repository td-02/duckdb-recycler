#pragma once
//===----------------------------------------------------------------------===//
// recycler_pagination.hpp - LIMIT/OFFSET pages over one sorted result (M6)
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"
#include "recycler_signature.hpp"

namespace duckdb {

class LogicalOrder;

//! Pre hook: records the bound plan's single eligible pagination in `client.pagination_site`
void DetectPagination(RecyclerClientState &client, PlanSignatures &sig, LogicalOperator &root);
//! Number of pagination sites below `op`; `first` receives the first LIMIT found
idx_t CountPaginationSites(LogicalOperator &op, LogicalOperator **first);
//! The slot of the ORDER BY under a pagination LIMIT `op`, with its constants; nullptr when `op` is none
unique_ptr<LogicalOperator> *MatchPagination(LogicalOperator &op, idx_t &limit_rows, idx_t &offset_rows);
//! Direction and NULL order of every ORDER BY key
string OrderSpec(const LogicalOrder &order);
//! The cache key of a pagination whose optimized unit is `unit`
string PaginationCanonical(const PaginationSite &site, const LogicalOrder &order, const LogicalOperator &unit);
SigHash HashPaginationKey(const string &canonical);
//! The bound ORDER BY output each column of the optimized `unit` carries; false when not provable
bool UnitPositions(LogicalOperator &unit, idx_t projection_index, vector<idx_t> &positions);
//! The stored column of `entry` carrying each of `positions`; false when the entry holds other columns
bool StoredColumns(const CachedEntry &entry, const vector<idx_t> &positions, vector<idx_t> &stored);
//! Rows of the sorted result to store for a page that needs `need` leading rows
idx_t PrefixRows(idx_t need, idx_t existing_rows, idx_t cap_rows, idx_t cap_bytes, idx_t row_width);

} // namespace duckdb
