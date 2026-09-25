#include "recycler_operators.hpp"

#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/types/column/column_data_allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/binder.hpp"

#include <unordered_set>

namespace duckdb {

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
                                         bool ordered, vector<column_t> column_ids_p)
    : LogicalExtensionOperator(), table_index(table_index), scan_types(std::move(types_p)), entry(std::move(entry_p)),
      ordered(ordered), column_ids(std::move(column_ids_p)) {
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
	return planner.Make<PhysicalRecyclerScan>(types, entry->rows, entry, ordered, column_ids);
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
	auto result = make_uniq<LogicalRecyclerScan>(table_index, scan_types, entry, ordered, column_ids);
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
                                           idx_t estimated_cardinality, shared_ptr<CachedEntry> entry_p, bool ordered,
                                           vector<column_t> column_ids_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      entry(std::move(entry_p)), ordered(ordered), column_ids(std::move(column_ids_p)) {
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
	// an ordered result is written in parallel when every source below hands out batch indexes (a sort does);
	// a pipeline whose sink requires batch indexes from a source without them is an internal error
	bool use_batch_index = spec.ordered && TaskScheduler::GetScheduler(context).NumberOfThreads() > 1 &&
	                       child.AllSourcesSupportBatchIndex();
	return planner.Make<PhysicalRecyclerMaterialize>(types, estimated, spec, cache, child, use_batch_index);
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
//! Rows are collected either in a plain collection or, for parallel ordered writes, keyed by batch index
class RecyclerMaterializeGlobalSinkState : public GlobalSinkState {
public:
	RecyclerMaterializeGlobalSinkState(ClientContext &context, const vector<LogicalType> &types, bool use_batch_index) {
		if (use_batch_index) {
			batched = make_uniq<BatchedDataCollection>(context, types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
		} else {
			collection = make_shared_ptr<ColumnDataCollection>(context, types);
		}
	}

	mutex glock;
	//! Batch-indexed writes are merged here and become `collection` in Finalize
	unique_ptr<BatchedDataCollection> batched;
	shared_ptr<ColumnDataCollection> collection;
	idx_t rows = 0;
	bool too_big = false;
};

class RecyclerMaterializeLocalSinkState : public LocalSinkState {
public:
	RecyclerMaterializeLocalSinkState(ClientContext &context, const vector<LogicalType> &types, bool use_batch_index) {
		if (use_batch_index) {
			batched = make_uniq<BatchedDataCollection>(context, types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
		} else {
			collection = make_uniq<ColumnDataCollection>(context, types);
			collection->InitializeAppend(append_state);
		}
	}

	idx_t Count() const {
		return batched ? batched->Count() : collection->Count();
	}

	unique_ptr<BatchedDataCollection> batched;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

//! Allocated bytes of a collection. ColumnDataCollection::AllocationSize() assumes that every segment owns its
//! allocator, which batch-indexed writes violate (consecutive batches of one thread share their blocks).
static idx_t CollectionBytes(const ColumnDataCollection &collection) {
	idx_t bytes = 0;
	std::unordered_set<const ColumnDataAllocator *> allocators;
	for (auto &segment : collection.GetSegments()) {
		if (allocators.insert(segment->allocator.get()).second) {
			bytes += segment->allocator->AllocationSize();
		}
		bytes += segment->heap->AllocationSize();
	}
	return bytes;
}

PhysicalRecyclerMaterialize::PhysicalRecyclerMaterialize(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                                         idx_t estimated_cardinality, RecyclerWriteSpec spec_p,
                                                         shared_ptr<RecyclerCache> cache_p, PhysicalOperator &child,
                                                         bool use_batch_index)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      spec(std::move(spec_p)), cache(std::move(cache_p)), use_batch_index(use_batch_index) {
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
	result["Batch Index"] = use_batch_index ? "true" : "false";
	return result;
}

unique_ptr<GlobalSinkState> PhysicalRecyclerMaterialize::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecyclerMaterializeGlobalSinkState>(context, types, use_batch_index);
}

unique_ptr<LocalSinkState> PhysicalRecyclerMaterialize::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RecyclerMaterializeLocalSinkState>(context.client, types, use_batch_index);
}

SinkResultType PhysicalRecyclerMaterialize::Sink(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<RecyclerMaterializeLocalSinkState>();
	if (lstate.batched) {
		lstate.batched->Append(chunk, lstate.partition_info.batch_index.GetIndex());
	} else {
		lstate.collection->Append(lstate.append_state, chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalRecyclerMaterialize::Combine(ExecutionContext &context,
                                                           OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<RecyclerMaterializeGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RecyclerMaterializeLocalSinkState>();
	auto count = lstate.Count();
	if (count == 0) {
		return SinkCombineResultType::FINISHED;
	}
	lock_guard<mutex> guard(gstate.glock);
	gstate.rows += count;
	if (gstate.batched) {
		gstate.batched->Merge(*lstate.batched);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}
	if (gstate.rows > spec.cap_rows) {
		gstate.too_big = true;
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalRecyclerMaterialize::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<RecyclerMaterializeGlobalSinkState>();
	if (gstate.batched) {
		// the batches in index order form the ordered result; their segments are moved, not copied
		gstate.collection = gstate.batched->FetchCollection();
		gstate.batched.reset();
	}
	auto rows = gstate.collection->Count();
	idx_t bytes = CollectionBytes(*gstate.collection);
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
	if (spec.kind == RecyclerEntryKind::BLOCK) {
		entry->columns = spec.columns;
		entry->constraints = spec.constraints;
		entry->conjuncts = spec.conjuncts;
		cache->StoreBlock(std::move(entry), spec.budget_bytes);
	} else {
		cache->Store(std::move(entry), spec.budget_bytes);
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
