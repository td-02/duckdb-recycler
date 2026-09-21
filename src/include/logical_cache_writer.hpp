#pragma once
//===----------------------------------------------------------------------===//
// LogicalCacheWrite — a transparent wrapper inserted around a join subtree on
// a cache miss.  It delegates all column-binding and type information to its
// single child (the original join) so the rest of the plan is unaffected.
// Its CreatePlan() generates the child's physical plan and wraps it with a
// PhysicalCacheWriter that materialises the results into the cache.
//===----------------------------------------------------------------------===//

#include "recycler_cache.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

class PhysicalPlanGenerator;

class LogicalCacheWrite : public LogicalExtensionOperator {
public:
    LogicalCacheWrite(string signature, shared_ptr<RecyclerCache> cache);

    //! The join-skeleton key for this subtree.
    string signature;
    //! Shared reference to the database-level cache.
    shared_ptr<RecyclerCache> cache;

    // LogicalExtensionOperator interface ------------------------------------------
    PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
    string GetExtensionName() const override;

    // Transparent forwarding to the child join ----------------------------------------
    vector<ColumnBinding> GetColumnBindings() override;
    vector<idx_t>         GetTableIndex() const override;
    string GetName() const override;

protected:
    void ResolveTypes() override;
};

} // namespace duckdb
