#pragma once
//===----------------------------------------------------------------------===//
// PhysicalCacheWriter — a pipeline operator that materialises a join result
// into the RecyclerCache while re-emitting the same rows to the consumer.
//
// Pipeline model (mirrors PhysicalResultCollector):
//   child pipeline:  Join → PhysicalCacheWriter (sink)
//   parent pipeline: PhysicalCacheWriter (source) → ... → query root
//===----------------------------------------------------------------------===//

#include "recycler_cache.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalCacheWriter : public PhysicalOperator {
public:
    PhysicalCacheWriter(PhysicalPlan &plan, vector<LogicalType> types,
                        string signature, shared_ptr<RecyclerCache> cache,
                        PhysicalOperator &child);

    //! Cache key for this subtree.
    string signature;
    //! Shared cache reference.
    shared_ptr<RecyclerCache> cache;
    //! The physical plan for the join (stored here, NOT in children vector).
    PhysicalOperator &child_plan;

    // ── Pipeline plumbing ─────────────────────────────────────────────────────
    bool IsSink()   const override { return true;  }
    bool IsSource() const override { return true;  }
    bool ParallelSink() const override { return false; }

    void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
    vector<const_reference<PhysicalOperator>> GetChildren() const override;

    // ── Sink interface ────────────────────────────────────────────────────────
    SinkResultType     Sink(ExecutionContext &context, DataChunk &chunk,
                            OperatorSinkInput &input) const override;
    SinkCombineResultType Combine(ExecutionContext &context,
                                  OperatorSinkCombineInput &input) const override;
    SinkFinalizeType   Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                OperatorSinkFinalizeInput &input) const override;
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    unique_ptr<LocalSinkState>  GetLocalSinkState(ExecutionContext &context) const override;

    // ── Source interface ──────────────────────────────────────────────────────
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    unique_ptr<LocalSourceState>  GetLocalSourceState(ExecutionContext &context,
                                                      GlobalSourceState &gstate) const override;
    SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                     OperatorSourceInput &input) const override;
};

} // namespace duckdb
