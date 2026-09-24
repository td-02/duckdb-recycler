#include "recycler_operators.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Shared scan state
//===----------------------------------------------------------------------===//
class RecyclerScanGlobalState : public GlobalSourceState {
public:
	RecyclerScanGlobalState(const ColumnDataCollection &collection, bool ordered)
	    : ordered(ordered), max_threads(ordered ? 1 : MaxValue<idx_t>(collection.ChunkCount(), 1)) {
		if (ordered) {
			collection.InitializeScan(sequential_state);
		} else {
			collection.InitializeScan(parallel_state);
		}
	}

	idx_t MaxThreads() override {
		return max_threads;
	}

	bool ordered;
	idx_t max_threads;
	ColumnDataScanState sequential_state;
	ColumnDataParallelScanState parallel_state;
};

class RecyclerScanLocalState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_state;
};

static SourceResultType ScanCollection(const ColumnDataCollection &collection, DataChunk &chunk,
                                       OperatorSourceInput &input) {
	auto &gstate = input.global_state.Cast<RecyclerScanGlobalState>();
	auto &lstate = input.local_state.Cast<RecyclerScanLocalState>();
	if (gstate.ordered) {
		collection.Scan(gstate.sequential_state, chunk);
	} else {
		collection.Scan(gstate.parallel_state, lstate.local_state, chunk);
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

//===----------------------------------------------------------------------===//
// LogicalRecyclerScan
//===----------------------------------------------------------------------===//
LogicalRecyclerScan::LogicalRecyclerScan(idx_t table_index, vector<LogicalType> types_p, shared_ptr<CachedEntry> entry_p,
                                         bool ordered)
    : LogicalExtensionOperator(), table_index(table_index), scan_types(std::move(types_p)), entry(std::move(entry_p)),
      ordered(ordered) {
	SetEstimatedCardinality(entry->rows);
}

PhysicalOperator &LogicalRecyclerScan::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.empty());
	return planner.Make<PhysicalRecyclerScan>(types, entry->rows, entry, ordered);
}

string LogicalRecyclerScan::GetExtensionName() const {
	return "recycler_scan";
}

string LogicalRecyclerScan::GetName() const {
	return "RECYCLER_SCAN";
}

InsertionOrderPreservingMap<string> LogicalRecyclerScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Kind"] = RecyclerEntryKindName(entry->kind);
	result["Key"] = entry->hash.ToString();
	result["Rows"] = to_string(entry->rows);
	result["Ordered"] = ordered ? "true" : "false";
	return result;
}

vector<ColumnBinding> LogicalRecyclerScan::GetColumnBindings() {
	return GenerateColumnBindings(table_index, scan_types.size());
}

vector<idx_t> LogicalRecyclerScan::GetTableIndex() const {
	return {table_index};
}

unique_ptr<LogicalOperator> LogicalRecyclerScan::Copy(ClientContext &context) const {
	auto result = make_uniq<LogicalRecyclerScan>(table_index, scan_types, entry, ordered);
	result->types = types;
	return std::move(result);
}

idx_t LogicalRecyclerScan::EstimateCardinality(ClientContext &context) {
	return entry->rows;
}

void LogicalRecyclerScan::ResolveTypes() {
	types = scan_types;
}

//===----------------------------------------------------------------------===//
// PhysicalRecyclerScan
//===----------------------------------------------------------------------===//
PhysicalRecyclerScan::PhysicalRecyclerScan(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                           idx_t estimated_cardinality, shared_ptr<CachedEntry> entry_p, bool ordered)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      entry(std::move(entry_p)), ordered(ordered) {
}

string PhysicalRecyclerScan::GetName() const {
	return "RECYCLER_SCAN";
}

InsertionOrderPreservingMap<string> PhysicalRecyclerScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Kind"] = RecyclerEntryKindName(entry->kind);
	result["Key"] = entry->hash.ToString();
	result["Rows"] = to_string(entry->rows);
	result["Ordered"] = ordered ? "true" : "false";
	return result;
}

unique_ptr<GlobalSourceState> PhysicalRecyclerScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RecyclerScanGlobalState>(*entry->data, ordered);
}

unique_ptr<LocalSourceState> PhysicalRecyclerScan::GetLocalSourceState(ExecutionContext &context,
                                                                       GlobalSourceState &gstate) const {
	return make_uniq<RecyclerScanLocalState>();
}

SourceResultType PhysicalRecyclerScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	return ScanCollection(*entry->data, chunk, input);
}

//===----------------------------------------------------------------------===//
// LogicalRecyclerMaterialize
//===----------------------------------------------------------------------===//
LogicalRecyclerMaterialize::LogicalRecyclerMaterialize(RecyclerWriteSpec spec_p, shared_ptr<RecyclerCache> cache_p,
                                                       unique_ptr<LogicalOperator> child)
    : LogicalExtensionOperator(), spec(std::move(spec_p)), cache(std::move(cache_p)) {
	children.push_back(std::move(child));
	if (children[0]->has_estimated_cardinality) {
		SetEstimatedCardinality(children[0]->estimated_cardinality);
	}
}

PhysicalOperator &LogicalRecyclerMaterialize::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	auto &child = planner.CreatePlan(*children[0]);
	auto estimated = children[0]->has_estimated_cardinality ? children[0]->estimated_cardinality
	                                                         : child.estimated_cardinality;
	return planner.Make<PhysicalRecyclerMaterialize>(types, estimated, spec, cache, child);
}

string LogicalRecyclerMaterialize::GetExtensionName() const {
	return "recycler_materialize";
}

string LogicalRecyclerMaterialize::GetName() const {
	return "RECYCLER_MATERIALIZE";
}

InsertionOrderPreservingMap<string> LogicalRecyclerMaterialize::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Kind"] = RecyclerEntryKindName(spec.kind);
	result["Key"] = spec.hash.ToString();
	result["Ordered"] = spec.ordered ? "true" : "false";
	return result;
}

vector<ColumnBinding> LogicalRecyclerMaterialize::GetColumnBindings() {
	D_ASSERT(!children.empty());
	return children[0]->GetColumnBindings();
}

vector<idx_t> LogicalRecyclerMaterialize::GetTableIndex() const {
	// transparent wrapper: it owns no table index of its own
	return {};
}

unique_ptr<LogicalOperator> LogicalRecyclerMaterialize::Copy(ClientContext &context) const {
	auto result = make_uniq<LogicalRecyclerMaterialize>(spec, cache, children[0]->Copy(context));
	result->types = types;
	return std::move(result);
}

idx_t LogicalRecyclerMaterialize::EstimateCardinality(ClientContext &context) {
	return children[0]->EstimateCardinality(context);
}

void LogicalRecyclerMaterialize::ResolveTypes() {
	types = children[0]->types;
}

//===----------------------------------------------------------------------===//
// PhysicalRecyclerMaterialize
//===----------------------------------------------------------------------===//
class RecyclerMaterializeGlobalSinkState : public GlobalSinkState {
public:
	RecyclerMaterializeGlobalSinkState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(make_shared_ptr<ColumnDataCollection>(context, types)) {
	}

	mutex glock;
	shared_ptr<ColumnDataCollection> collection;
	idx_t rows = 0;
	bool too_big = false;
};

class RecyclerMaterializeLocalSinkState : public LocalSinkState {
public:
	RecyclerMaterializeLocalSinkState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(make_uniq<ColumnDataCollection>(context, types)) {
		collection->InitializeAppend(append_state);
	}

	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

PhysicalRecyclerMaterialize::PhysicalRecyclerMaterialize(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                                         idx_t estimated_cardinality, RecyclerWriteSpec spec_p,
                                                         shared_ptr<RecyclerCache> cache_p, PhysicalOperator &child)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      spec(std::move(spec_p)), cache(std::move(cache_p)) {
	// the child lives in `children`, so the default BuildPipelines/GetSources/GetChildren and every planner walk
	// (delim scan gathering, order preservation, thread estimation) see it
	children.push_back(child);
}

string PhysicalRecyclerMaterialize::GetName() const {
	return "RECYCLER_MATERIALIZE";
}

InsertionOrderPreservingMap<string> PhysicalRecyclerMaterialize::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Kind"] = RecyclerEntryKindName(spec.kind);
	result["Key"] = spec.hash.ToString();
	result["Ordered"] = spec.ordered ? "true" : "false";
	return result;
}

unique_ptr<GlobalSinkState> PhysicalRecyclerMaterialize::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecyclerMaterializeGlobalSinkState>(context, types);
}

unique_ptr<LocalSinkState> PhysicalRecyclerMaterialize::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RecyclerMaterializeLocalSinkState>(context.client, types);
}

SinkResultType PhysicalRecyclerMaterialize::Sink(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<RecyclerMaterializeLocalSinkState>();
	lstate.collection->Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalRecyclerMaterialize::Combine(ExecutionContext &context,
                                                           OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<RecyclerMaterializeGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RecyclerMaterializeLocalSinkState>();
	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}
	lock_guard<mutex> guard(gstate.glock);
	gstate.rows += lstate.collection->Count();
	gstate.collection->Combine(*lstate.collection);
	if (gstate.rows > spec.cap_rows) {
		gstate.too_big = true;
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalRecyclerMaterialize::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<RecyclerMaterializeGlobalSinkState>();
	auto rows = gstate.collection->Count();
	idx_t bytes = gstate.collection->AllocationSize();
	if (gstate.too_big || rows > spec.cap_rows || bytes > spec.cap_bytes) {
		cache->NoteTooBig(spec.hash);
		return SinkFinalizeType::READY;
	}
	auto entry = make_shared_ptr<CachedEntry>();
	entry->kind = spec.kind;
	entry->hash = spec.hash;
	entry->canonical = spec.canonical;
	entry->data = gstate.collection;
	entry->types = types;
	entry->rows = rows;
	entry->bytes = bytes;
	entry->ordered = spec.ordered;
	entry->est_input_rows = spec.est_input_rows;
	cache->Store(std::move(entry), spec.budget_bytes);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalRecyclerMaterialize::GetGlobalSourceState(ClientContext &context) const {
	D_ASSERT(sink_state);
	auto &gstate = sink_state->Cast<RecyclerMaterializeGlobalSinkState>();
	return make_uniq<RecyclerScanGlobalState>(*gstate.collection, spec.ordered);
}

unique_ptr<LocalSourceState> PhysicalRecyclerMaterialize::GetLocalSourceState(ExecutionContext &context,
                                                                              GlobalSourceState &gstate) const {
	return make_uniq<RecyclerScanLocalState>();
}

SourceResultType PhysicalRecyclerMaterialize::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                              OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecyclerMaterializeGlobalSinkState>();
	return ScanCollection(*gstate.collection, chunk, input);
}

} // namespace duckdb
