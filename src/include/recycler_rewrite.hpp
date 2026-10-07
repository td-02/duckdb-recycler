#pragma once
//===----------------------------------------------------------------------===//
// recycler_rewrite.hpp - plan rewrites shared by the pre- and post-optimizer passes
//===----------------------------------------------------------------------===//
#include "recycler_operators.hpp"

namespace duckdb {

class Binder;
class ColumnBindingReplacer;

//! Replace the operator in `slot`, which scans `tables`, by a scan over `entry`, recording the binding
//! replacements. False when the entry's columns do not match the operator's output.
bool SubstituteScan(unique_ptr<LogicalOperator> &slot, shared_ptr<CachedEntry> entry, bool ordered,
                    const vector<reference<const TableCatalogEntry>> &tables, Binder &binder,
                    ColumnBindingReplacer &replacer);

//! Wrap the operator in `slot` in a RecyclerMaterialize that writes `spec`. `keep_alive` are column references the
//! wrapper carries so that column pruning keeps those columns in the wrapped subtree's output even when nothing
//! above the wrapper needs them.
void WrapMaterialize(unique_ptr<LogicalOperator> &slot, RecyclerWriteSpec spec, shared_ptr<RecyclerCache> cache,
                     vector<unique_ptr<Expression>> keep_alive = vector<unique_ptr<Expression>>());

//! Remove from `owners`, joins outside `subtree`, the runtime filters they push into the scans of `subtree`, so that a
//! write of the subtree stores all of its rows. A runtime filter only drops probe rows its join drops anyway, so no
//! result changes. Returns the number of filters removed.
idx_t StripRuntimeFilters(const LogicalOperator &subtree, const vector<LogicalOperator *> &owners);

//! Operators that consume their child's output by position and rebind it: what they need from the child appears in
//! no expression of theirs, so a reference collector never sees it, and a rewrite below them must keep the child's
//! column order
bool ConsumesChildrenPositionally(const LogicalOperator &op);

//! The consumer side of a CTE, or nullptr when `op` is not a CTE. CTE inlining deep-copies a CTE's definition and
//! cannot copy our operators, so the pre-optimize walkers rewrite the consumer side only.
unique_ptr<LogicalOperator> *CteConsumerSlot(LogicalOperator &op);

//! Resolve the stored columns of every planted block marker below `slot` from its (pruned and reordered) child;
//! a marker whose child produces no plain base column is removed again
void ResolveBlockColumns(unique_ptr<LogicalOperator> &slot);

} // namespace duckdb
