#include "recycler_operators.hpp"
#include "recycler_state.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"

#include <chrono>

namespace duckdb {

optional_ptr<const LogicalRecyclerOperator> AsRecyclerOperator(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
		return nullptr;
	}
	return dynamic_cast<const LogicalRecyclerOperator *>(&op);
}

optional_ptr<const PhysicalRecyclerOperator> AsRecyclerOperator(const PhysicalOperator &op) {
	if (op.type != PhysicalOperatorType::EXTENSION) {
		return nullptr;
	}
	return dynamic_cast<const PhysicalRecyclerOperator *>(&op);
}

//===----------------------------------------------------------------------===//
// Shared scan state
//===----------------------------------------------------------------------===//
class RecyclerScanGlobalState : public GlobalSourceState {
public:
	//! `column_ids` empty = scan all columns
	RecyclerScanGlobalState(const ColumnDataCollection &collection, bool ordered, const vector<column_t> &column_ids)
	    : ordered(ordered), max_threads(ordered ? 1 : MaxValue<idx_t>(collection.ChunkCount(), 1)) {
		if (ordered) {
			if (column_ids.empty()) {
				collection.InitializeScan(sequential_state);
			} else {
				collection.InitializeScan(sequential_state, column_ids);
			}
		} else {
			if (column_ids.empty()) {
				collection.InitializeScan(parallel_state);
			} else {
				collection.InitializeScan(parallel_state, column_ids);
			}
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

//! ColumnDataCollection::Scan over a parallel state, without its D_ASSERT that the chunk holds every column of the
//! collection: in v1.5.5 that assertion also fires when the state projects a subset of the columns, which the scan
//! itself (ScanAtIndex reads the state's column_ids) supports
static void ScanParallel(const ColumnDataCollection &collection, ColumnDataParallelScanState &state,
                         ColumnDataLocalScanState &lstate, DataChunk &result) {
	result.Reset();
	idx_t chunk_index;
	idx_t segment_index;
	idx_t row_index;
	{
		lock_guard<mutex> guard(state.lock);
		if (!collection.NextScanIndex(state.scan_state, chunk_index, segment_index, row_index)) {
			return;
		}
	}
	collection.ScanAtIndex(state, lstate, result, chunk_index, segment_index, row_index);
}

static SourceResultType ScanCollection(const ColumnDataCollection &collection, DataChunk &chunk,
                                       OperatorSourceInput &input) {
	auto &gstate = input.global_state.Cast<RecyclerScanGlobalState>();
	auto &lstate = input.local_state.Cast<RecyclerScanLocalState>();
	if (gstate.ordered) {
		collection.Scan(gstate.sequential_state, chunk);
	} else {
		ScanParallel(collection, gstate.parallel_state, lstate.local_state, chunk);
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

//===----------------------------------------------------------------------===//
// LogicalRecyclerScan
//===----------------------------------------------------------------------===//
LogicalRecyclerScan::LogicalRecyclerScan(idx_t table_index, vector<LogicalType> types_p,
                                         shared_ptr<CachedEntry> entry_p, bool ordered,
                                         vector<reference<const TableCatalogEntry>> tables_p,
                                         vector<column_t> column_ids_p)
    : LogicalRecyclerOperator(RecyclerOperatorKind::SCAN), table_index(table_index), scan_types(std::move(types_p)),
      entry(std::move(entry_p)), ordered(ordered), tables(std::move(tables_p)), column_ids(std::move(column_ids_p)) {
	if (column_ids.empty()) {
		for (idx_t i = 0; i < scan_types.size(); i++) {
			column_ids.push_back(i);
		}
	}
	D_ASSERT(column_ids.size() == scan_types.size());
	SetEstimatedCardinality(entry->rows);
}

PhysicalOperator &LogicalRecyclerScan::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.empty());
	auto id = RecyclerClientState::Get(context).NextOperatorId();
	return planner.Make<PhysicalRecyclerScan>(types, entry->rows, id, entry, ordered, column_ids);
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
	result["Columns"] = to_string(column_ids.size()) + "/" + to_string(entry->types.size());
	return result;
}

vector<ColumnBinding> LogicalRecyclerScan::GetColumnBindings() {
	return GenerateColumnBindings(table_index, scan_types.size());
}

vector<idx_t> LogicalRecyclerScan::GetTableIndex() const {
	return {table_index};
}

unique_ptr<LogicalOperator> LogicalRecyclerScan::Copy(ClientContext &context) const {
	auto result = make_uniq<LogicalRecyclerScan>(table_index, scan_types, entry, ordered, tables, column_ids);
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
                                           idx_t estimated_cardinality, idx_t id, shared_ptr<CachedEntry> entry_p,
                                           bool ordered, vector<column_t> column_ids_p)
    : PhysicalRecyclerOperator(physical_plan, std::move(types_p), estimated_cardinality, id), entry(std::move(entry_p)),
      ordered(ordered), column_ids(std::move(column_ids_p)) {
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
	result["Id"] = to_string(id);
	return result;
}

unique_ptr<GlobalSourceState> PhysicalRecyclerScan::GetGlobalSourceState(ClientContext &context) const {
	RecyclerClientState::Get(context).NoteTouched(id, entry);
	return make_uniq<RecyclerScanGlobalState>(*entry->data, ordered, column_ids);
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
RecyclerWriteSpec RecyclerWriteSpec::Make(RecyclerEntryKind kind, const SigHash &hash, string canonical, bool ordered,
                                          idx_t est_input_rows, vector<TableDependency> tables,
                                          const RecyclerSettings &settings) {
	RecyclerWriteSpec spec;
	spec.kind = kind;
	spec.hash = hash;
	spec.canonical = std::move(canonical);
	spec.ordered = ordered;
	spec.est_input_rows = est_input_rows;
	spec.tables = std::move(tables);
	spec.cap_rows = settings.CapRows(kind);
	spec.cap_bytes = settings.entry_cap_bytes;
	spec.budget_bytes = settings.budget_bytes;
	spec.compact_rows = settings.compact_rows;
	return spec;
}

LogicalRecyclerMaterialize::LogicalRecyclerMaterialize(RecyclerWriteSpec spec_p, shared_ptr<RecyclerCache> cache_p,
                                                       unique_ptr<LogicalOperator> child)
    : LogicalRecyclerOperator(RecyclerOperatorKind::MATERIALIZE), spec(std::move(spec_p)), cache(std::move(cache_p)) {
	children.push_back(std::move(child));
	if (children[0]->has_estimated_cardinality) {
		SetEstimatedCardinality(children[0]->estimated_cardinality);
	}
}

PhysicalOperator &LogicalRecyclerMaterialize::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	auto &child = planner.CreatePlan(*children[0]);
	auto estimated =
	    children[0]->has_estimated_cardinality ? children[0]->estimated_cardinality : child.estimated_cardinality;
	auto id = RecyclerClientState::Get(context).NextOperatorId();
	return planner.Make<PhysicalRecyclerMaterialize>(types, estimated, id, spec, cache, child);
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
	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
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
	    : collection(make_shared_ptr<ColumnDataCollection>(context, types)), start(std::chrono::steady_clock::now()) {
	}

	//! Wall time from the sink's creation (executor start) to Finalize: stands in for the cost of producing the
	//! wrapped subtree when the query is not profiled (section 3.1 of the plan)
	uint64_t ElapsedNs() const {
		auto elapsed = std::chrono::steady_clock::now() - start;
		return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
	}

	mutex glock;
	shared_ptr<ColumnDataCollection> collection;
	std::chrono::steady_clock::time_point start;
	idx_t rows = 0;
	idx_t sink_calls = 0;
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
	idx_t sink_calls = 0;
};

PhysicalRecyclerMaterialize::PhysicalRecyclerMaterialize(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                                         idx_t estimated_cardinality, idx_t id,
                                                         RecyclerWriteSpec spec_p, shared_ptr<RecyclerCache> cache_p,
                                                         PhysicalOperator &child)
    : PhysicalRecyclerOperator(physical_plan, std::move(types_p), estimated_cardinality, id), spec(std::move(spec_p)),
      cache(std::move(cache_p)) {
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
	result["Id"] = to_string(id);
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
	lstate.sink_calls++;
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalRecyclerMaterialize::Combine(ExecutionContext &context,
                                                           OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<RecyclerMaterializeGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RecyclerMaterializeLocalSinkState>();
	if (lstate.sink_calls == 0) {
		return SinkCombineResultType::FINISHED;
	}
	lock_guard<mutex> guard(gstate.glock);
	gstate.sink_calls += lstate.sink_calls;
	gstate.rows += lstate.collection->Count();
	gstate.collection->Combine(*lstate.collection);
	if (gstate.rows > spec.cap_rows) {
		gstate.too_big = true;
	}
	return SinkCombineResultType::FINISHED;
}

//! The rows of `collection`, in order, in a collection on the in-memory allocator, which grows with the data: a
//! buffer-managed collection holds at least one block per writer thread however few rows it has
static shared_ptr<ColumnDataCollection> CompactCollection(ClientContext &context,
                                                          const ColumnDataCollection &collection) {
	auto result = make_shared_ptr<ColumnDataCollection>(Allocator::Get(context), collection.Types());
	ColumnDataAppendState append_state;
	result->InitializeAppend(append_state);
	for (auto &chunk : collection.Chunks()) {
		result->Append(append_state, chunk);
	}
	return result;
}

SinkFinalizeType PhysicalRecyclerMaterialize::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<RecyclerMaterializeGlobalSinkState>();
	auto rows = gstate.collection->Count();
	if (rows <= spec.compact_rows) {
		auto compacted = CompactCollection(context, *gstate.collection);
		// the in-memory allocator doubles its allocations from the first vector's size, so a few wide chunks can take
		// more than the blocks they filled. The source re-emits the kept collection: the other one is released here.
		if (compacted->AllocationSize() < gstate.collection->AllocationSize()) {
			gstate.collection = std::move(compacted);
		}
	}
	// the caps and the budget charge the allocated bytes, not the used ones: the allocation is what the buffer pool
	// (or the allocator, for a compacted entry) holds for the entry, whatever part of it the rows fill
	idx_t bytes = gstate.collection->AllocationSize();
	auto cost_ns = gstate.ElapsedNs();
	bool too_big = gstate.too_big || rows > spec.cap_rows || bytes > spec.cap_bytes;
	cache->NoteMaterialized(spec.hash, rows, bytes, cost_ns, too_big);
	if (too_big) {
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
	entry->used_bytes = gstate.collection->SizeInBytes();
	entry->sink_calls = gstate.sink_calls;
	entry->measured_cost_ns = cost_ns;
	entry->ordered = spec.ordered;
	entry->est_input_rows = spec.est_input_rows;
	entry->tables = spec.tables;
	bool stored;
	if (spec.kind == RecyclerEntryKind::BLOCK) {
		entry->columns = spec.columns;
		entry->constraints = spec.constraints;
		entry->conjuncts = spec.conjuncts;
		stored = cache->StoreBlock(entry, spec.budget_bytes);
	} else {
		stored = cache->Store(entry, spec.budget_bytes);
	}
	if (stored) {
		RecyclerClientState::Get(context).NoteTouched(id, std::move(entry));
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalRecyclerMaterialize::GetGlobalSourceState(ClientContext &context) const {
	D_ASSERT(sink_state);
	auto &gstate = sink_state->Cast<RecyclerMaterializeGlobalSinkState>();
	return make_uniq<RecyclerScanGlobalState>(*gstate.collection, spec.ordered, vector<column_t>());
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
