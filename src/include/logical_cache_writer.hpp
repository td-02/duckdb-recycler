#pragma once
//==-------------------------------------------------------------------==//
// LogicalCacheWrite – transparent wrapper inserted around a join subtree
// on a cache miss.  Its CreatePlan() produces a PhysicalCacheWriter.
//==-------------------------------------------------------------------==//
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

    // LogicalExtensionOperator interface
    PhysicalOperator &CreatePlan(ClientContext &context,
                                 PhysicalPlanGenerator &planner) override;
    // Fix: exact name from main ("recycler_cache_write", not "cache_write")
    string GetExtensionName() const override;

    // Transparent forwarding to the child join
    vector<ColumnBinding> GetColumnBindings() override;
    vector<idx_t>         GetTableIndex() const override;
    string GetName() const override;

protected:
    void ResolveTypes() override;
};

} // namespace duckdb
