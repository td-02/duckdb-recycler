#include "logical_cache_writer.hpp"
#include "physical_cache_writer.hpp"

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

LogicalCacheWrite::LogicalCacheWrite(string sig, shared_ptr<RecyclerCache> c)
    : LogicalExtensionOperator(), signature(std::move(sig)), cache(std::move(c)) {
}

string LogicalCacheWrite::GetExtensionName() const {
    return "recycler_cache_write";
}

string LogicalCacheWrite::GetName() const {
    return "RECYCLER_CACHE_WRITE";
}

vector<ColumnBinding> LogicalCacheWrite::GetColumnBindings() {
    D_ASSERT(!children.empty());
    return children[0]->GetColumnBindings();
}

vector<TableIndex> LogicalCacheWrite::GetTableIndex() const {
    D_ASSERT(!children.empty());
    return children[0]->GetTableIndex();
}

void LogicalCacheWrite::ResolveTypes() {
    D_ASSERT(!children.empty());
    types = children[0]->types;
}

PhysicalOperator &LogicalCacheWrite::CreatePlan(ClientContext &context,
                                                 PhysicalPlanGenerator &planner) {
    D_ASSERT(children.size() == 1);
    // Generate physical plan for the child (the real join).
    auto &child_phys = planner.CreatePlan(*children[0]);
    // Wrap it with a PhysicalCacheWriter that materialises and re-emits.
    return planner.Make<PhysicalCacheWriter>(
        types, signature, cache, child_phys);
}

} // namespace duckdb
