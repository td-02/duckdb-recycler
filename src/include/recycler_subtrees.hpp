#pragma once
//===----------------------------------------------------------------------===//
// recycler_subtrees.hpp - the post hook's subtree cache
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"

namespace duckdb {

class PlanSignatures;

//! Post hook: exact hits replace subtrees, admissible misses get a RecyclerMaterialize
void RewriteSubtrees(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                     unique_ptr<LogicalOperator> &root_slot, RecyclerState &state, const RecyclerClientState &client,
                     PlanSignatures &sig);

} // namespace duckdb
