//===----------------------------------------------------------------------===//
// PhysicalCacheWriter — materialises a join result into RecyclerCache and
// re-emits the same rows to the parent pipeline.
//===----------------------------------------------------------------------===//
#include "physical_cache_writer.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

// ─────────────────────────────────────────────────────────────────────────────
// State structs
// ─────────────────────────────────────────────────────────────────────────────

struct CacheWriterGlobalSinkState : public GlobalSinkState {
    explicit CacheWriterGlobalSinkState(ClientContext &context, const vector<LogicalType> &types)
        : collection(make_shared_ptr<ColumnDataCollection>(context, types)) {
    }
    shared_ptr<ColumnDataCollection> collection; //!< accumulates all rows from the join
};

struct CacheWriterLocalSinkState : public LocalSinkState {};

struct CacheWriterGlobalSourceState : public GlobalSourceState {
    explicit CacheWriterGlobalSourceState(shared_ptr<ColumnDataCollection> coll)
        : collection(std::move(coll)) {
        collection->InitializeScan(scan_state);
    }
    shared_ptr<ColumnDataCollection> collection;
    ColumnDataScanState              scan_state;
};

struct CacheWriterLocalSourceState : public LocalSourceState {};

// ─────────────────────────────────────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────────────────────────────────────

PhysicalCacheWriter::PhysicalCacheWriter(PhysicalPlan &plan,
                                         vector<LogicalType> types_p,
                                         string sig,
                                         shared_ptr<RecyclerCache> cache_p,
                                         PhysicalOperator &child)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types_p), 0),
      signature(std::move(sig)), cache(std::move(cache_p)), child_plan(child) {
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline plumbing (mirrors PhysicalResultCollector)
// ─────────────────────────────────────────────────────────────────────────────

void PhysicalCacheWriter::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
    sink_state.reset();
    D_ASSERT(children.empty());

    // This operator is the SOURCE of the current (parent) pipeline —
    // the parent reads re-emitted rows from us after we have materialised.
    auto &state = meta_pipeline.GetState();
    state.SetPipelineSource(current, *this);

    // Create a child meta-pipeline whose SINK is this operator.
    // The child pipeline runs the join and feeds every chunk into our Sink().
    auto &child_meta = meta_pipeline.CreateChildMetaPipeline(current, *this);
    child_meta.Build(child_plan);
}

vector<const_reference<PhysicalOperator>> PhysicalCacheWriter::GetChildren() const {
    return {child_plan};
}

// ─────────────────────────────────────────────────────────────────────────────
// Sink
// ─────────────────────────────────────────────────────────────────────────────

unique_ptr<GlobalSinkState> PhysicalCacheWriter::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<CacheWriterGlobalSinkState>(context, types);
}

unique_ptr<LocalSinkState> PhysicalCacheWriter::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<CacheWriterLocalSinkState>();
}

SinkResultType PhysicalCacheWriter::Sink(ExecutionContext &context,
                                          DataChunk &chunk,
                                          OperatorSinkInput &input) const {
    auto &gstate = input.global_state.Cast<CacheWriterGlobalSinkState>();
    gstate.collection->Append(chunk);
    return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCacheWriter::Combine(ExecutionContext &context,
                                                    OperatorSinkCombineInput &input) const {
    return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCacheWriter::Finalize(Pipeline &pipeline, Event &event,
                                                ClientContext &context,
                                                OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<CacheWriterGlobalSinkState>();

    // Store in cache.
    auto result          = make_shared_ptr<CachedResult>();
    result->types        = types;
    result->row_count    = gstate.collection->Count();
    result->collection   = gstate.collection; // shared ownership

    cache->Store(signature, std::move(result));
    return SinkFinalizeType::READY;
}

// ─────────────────────────────────────────────────────────────────────────────
// Source  (re-emit the materialised rows to the parent pipeline)
// ─────────────────────────────────────────────────────────────────────────────

unique_ptr<GlobalSourceState> PhysicalCacheWriter::GetGlobalSourceState(ClientContext &context) const {
    // The sink must have completed before the source phase starts.
    D_ASSERT(sink_state);
    auto &gstate = sink_state->Cast<CacheWriterGlobalSinkState>();
    return make_uniq<CacheWriterGlobalSourceState>(gstate.collection);
}

unique_ptr<LocalSourceState>
PhysicalCacheWriter::GetLocalSourceState(ExecutionContext &context,
                                          GlobalSourceState & /*gstate*/) const {
    return make_uniq<CacheWriterLocalSourceState>();
}

SourceResultType PhysicalCacheWriter::GetDataInternal(ExecutionContext &context,
                                                       DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
    auto &gstate = input.global_state.Cast<CacheWriterGlobalSourceState>();
    chunk.Reset();
    bool has_more = gstate.collection->Scan(gstate.scan_state, chunk);
    if (!has_more || chunk.size() == 0) {
        return SourceResultType::FINISHED;
    }
    return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
