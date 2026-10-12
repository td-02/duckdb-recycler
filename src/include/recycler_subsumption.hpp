#pragma once
//===----------------------------------------------------------------------===//
// recycler_subsumption.hpp - predicate / column subsumption on SPJ blocks (M5)
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"

namespace duckdb {

class PlanSignatures;

//! Pre hook: SPJ blocks served from wider cached blocks, admissible misses planted with a marker
void ServeSubsumedBlocks(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                         unique_ptr<LogicalOperator> &root_slot, RecyclerState &state,
                         const RecyclerClientState &client, PlanSignatures &sig);

} // namespace duckdb
