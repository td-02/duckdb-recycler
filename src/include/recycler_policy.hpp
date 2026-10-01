#pragma once
//===----------------------------------------------------------------------===//
// recycler_policy.hpp - which results are served and which writes are admitted
//===----------------------------------------------------------------------===//
#include "recycler_cache.hpp"
#include "recycler_signature.hpp"

namespace duckdb {

class ClientContext;

//! Operators whose output the subtree cache considers (joins and aggregates: the expensive, recurring units)
bool IsSubtreeCandidateType(LogicalOperatorType type);
bool IsAggregation(LogicalOperatorType type);

//! With preserve_insertion_order the order of an unordered query's rows is observable, so only results that were
//! stored in a defined order may be served
bool CanServe(const CachedEntry &entry, bool preserve_order);

//! Whether the subtree rooted at `op` is worth materializing on this sight
bool AdmitSubtreeWrite(ClientContext &context, const RecyclerCache &cache, const RecyclerSettings &settings,
                       LogicalOperator &op, const NodeInfo &info, bool under_delim_lhs, bool force_candidate);

//! Whether an SPJ block of this shape is worth materializing for later subsumption
bool AdmitBlockWrite(const RecyclerCache &cache, const RecyclerSettings &settings, const BlockShape &shape);

//! Whether the whole result of the optimized plan rooted at `root` (keyed by `hash`) may be written
bool AdmitWholeWrite(ClientContext &context, const RecyclerCache &cache, const RecyclerSettings &settings,
                     LogicalOperator &root, const NodeInfo &root_info, const SigHash &hash);

} // namespace duckdb
