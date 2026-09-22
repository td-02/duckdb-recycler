#pragma once
#include "recycler_cache.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalCacheWriter : public PhysicalOperator {
public:
    PhysicalCacheWriter(PhysicalPlan &plan,
                        vector<LogicalType> types,
                        string signature,
                        shared_ptr<RecyclerCache> cache,
                        PhysicalOperator &child);

    bool IsSink()       const override { return true; }
    bool IsSource()     const override { return true; }
    bool ParallelSink() const override { return false; }

    void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
    vector<const_reference<PhysicalOperator>> GetChildren() const override;

    // Sink
    SinkResultType        Sink(ExecutionContext &context, DataChunk &chunk,
                               OperatorSinkInput &input) const override;
    SinkCombineResultType Combine(ExecutionContext &context,
                                  OperatorSinkCombineInput &input) const override;
    SinkFinalizeType      Finalize(Pipeline &pipeline, Event &event,
                                   ClientContext &context,
                                   OperatorSinkFinalizeInput &input) const override;
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    unique_ptr<LocalSinkState>  GetLocalSinkState(ExecutionContext &context) const override;

    // Source
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    unique_ptr<LocalSourceState>  GetLocalSourceState(ExecutionContext &context,
                                                      GlobalSourceState &gstate) const override;
    SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                     OperatorSourceInput &input) const override;

private:
    string                    signature_;
    shared_ptr<RecyclerCache> cache_;
    PhysicalOperator         &child_plan_;
};

} // namespace duckdb
