#include "physical_cache_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

struct CacheWriterGlobalSinkState : public GlobalSinkState {
    explicit CacheWriterGlobalSinkState(ClientContext &ctx,
                                        const vector<LogicalType> &types)
        : collection(make_shared_ptr<ColumnDataCollection>(ctx, types)) {}
    shared_ptr<ColumnDataCollection> collection;
};
struct CacheWriterLocalSinkState  : public LocalSinkState {};

struct CacheWriterGlobalSourceState : public GlobalSourceState {
    explicit CacheWriterGlobalSourceState(shared_ptr<ColumnDataCollection> coll)
        : collection(std::move(coll)) {
        collection->InitializeScan(scan_state);
    }
    shared_ptr<ColumnDataCollection> collection;
    ColumnDataScanState              scan_state;
};
struct CacheWriterLocalSourceState : public LocalSourceState {};

PhysicalCacheWriter::PhysicalCacheWriter(PhysicalPlan &plan,
                                         vector<LogicalType> types_p,
                                         string sig,
                                         shared_ptr<RecyclerCache> cache_p,
                                         PhysicalOperator &child)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types_p), 0),
      signature_(std::move(sig)), cache_(std::move(cache_p)), child_plan_(child) {}

void PhysicalCacheWriter::BuildPipelines(Pipeline &current,
                                          MetaPipeline &meta_pipeline) {
    sink_state.reset();
    D_ASSERT(children.empty());
    auto &state = meta_pipeline.GetState();
    state.SetPipelineSource(current, *this);
    auto &child_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    child_meta.Build(child_plan_);
}

vector<const_reference<PhysicalOperator>> PhysicalCacheWriter::GetChildren() const {
    return {child_plan_};
}

unique_ptr<GlobalSinkState>
PhysicalCacheWriter::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<CacheWriterGlobalSinkState>(context, types);
}
unique_ptr<LocalSinkState>
PhysicalCacheWriter::GetLocalSinkState(ExecutionContext &) const {
    return make_uniq<CacheWriterLocalSinkState>();
}
SinkResultType PhysicalCacheWriter::Sink(ExecutionContext &, DataChunk &chunk,
                                          OperatorSinkInput &input) const {
    input.global_state.Cast<CacheWriterGlobalSinkState>().collection->Append(chunk);
    return SinkResultType::NEED_MORE_INPUT;
}
SinkCombineResultType PhysicalCacheWriter::Combine(ExecutionContext &,
                                                    OperatorSinkCombineInput &) const {
    return SinkCombineResultType::FINISHED;
}
SinkFinalizeType PhysicalCacheWriter::Finalize(Pipeline &, Event &, ClientContext &,
                                                OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<CacheWriterGlobalSinkState>();
    auto result = make_shared_ptr<CachedResult>();
    result->types      = types;
    result->row_count  = gstate.collection->Count();
    result->collection = gstate.collection;  // shared_ptr keeps collection alive
    cache_->Store(signature_, std::move(result));
    return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState>
PhysicalCacheWriter::GetGlobalSourceState(ClientContext &) const {
    D_ASSERT(sink_state);
    return make_uniq<CacheWriterGlobalSourceState>(
        sink_state->Cast<CacheWriterGlobalSinkState>().collection);
}
unique_ptr<LocalSourceState>
PhysicalCacheWriter::GetLocalSourceState(ExecutionContext &, GlobalSourceState &) const {
    return make_uniq<CacheWriterLocalSourceState>();
}
SourceResultType PhysicalCacheWriter::GetDataInternal(ExecutionContext &,
                                                       DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
    auto &gs = input.global_state.Cast<CacheWriterGlobalSourceState>();
    chunk.Reset();
    bool has_more = gs.collection->Scan(gs.scan_state, chunk);
    return (!has_more || chunk.size() == 0) ? SourceResultType::FINISHED
                                            : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
