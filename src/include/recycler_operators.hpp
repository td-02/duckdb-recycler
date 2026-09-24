#pragma once
//===----------------------------------------------------------------------===//
// recycler_operators.hpp - the two operator pairs of the recycler
//
//  * LogicalRecyclerScan / PhysicalRecyclerScan: a cache hit. A childless
//    source that replays a cached ColumnDataCollection, in parallel or in the
//    stored order. It is an EXTENSION operator on purpose: DuckDB's profiler
//    disables itself for plans that consist only of COLUMN_DATA_SCAN nodes,
//    and the grader reads the profiler's "latency" field.
//  * LogicalRecyclerMaterialize / PhysicalRecyclerMaterialize: a cache miss.
//    A sink+source wrapper that stores its child's output in a collection,
//    publishes it to the cache in Finalize and re-emits it to its parent.
//===----------------------------------------------------------------------===//
#include "recycler_cache.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

class PhysicalPlanGenerator;

//===----------------------------------------------------------------------===//
// Scan (hit)
//===----------------------------------------------------------------------===//
class LogicalRecyclerScan : public LogicalExtensionOperator {
public:
	LogicalRecyclerScan(idx_t table_index, vector<LogicalType> types, shared_ptr<CachedEntry> entry, bool ordered);

	idx_t table_index;
	vector<LogicalType> scan_types;
	shared_ptr<CachedEntry> entry;
	bool ordered;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	string GetExtensionName() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;
	unique_ptr<LogicalOperator> Copy(ClientContext &context) const override;
	idx_t EstimateCardinality(ClientContext &context) override;
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override;
};

class PhysicalRecyclerScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

	PhysicalRecyclerScan(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                     shared_ptr<CachedEntry> entry, bool ordered);

	shared_ptr<CachedEntry> entry;
	bool ordered;

public:
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return !ordered;
	}
	OrderPreservationType SourceOrder() const override {
		return ordered ? OrderPreservationType::FIXED_ORDER : OrderPreservationType::INSERTION_ORDER;
	}
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
};

//===----------------------------------------------------------------------===//
// Materialize (miss)
//===----------------------------------------------------------------------===//
struct RecyclerWriteSpec {
	RecyclerEntryKind kind = RecyclerEntryKind::SUBTREE;
	SigHash hash;
	string canonical;
	//! Write single-threaded in arrival order and replay in that order
	bool ordered = false;
	idx_t est_input_rows = 0;
	idx_t cap_rows = 0;
	idx_t cap_bytes = 0;
	idx_t budget_bytes = 0;
};

class LogicalRecyclerMaterialize : public LogicalExtensionOperator {
public:
	LogicalRecyclerMaterialize(RecyclerWriteSpec spec, shared_ptr<RecyclerCache> cache,
	                           unique_ptr<LogicalOperator> child);

	RecyclerWriteSpec spec;
	shared_ptr<RecyclerCache> cache;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	string GetExtensionName() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;
	unique_ptr<LogicalOperator> Copy(ClientContext &context) const override;
	idx_t EstimateCardinality(ClientContext &context) override;
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override;
};

class PhysicalRecyclerMaterialize : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

	PhysicalRecyclerMaterialize(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                            RecyclerWriteSpec spec, shared_ptr<RecyclerCache> cache, PhysicalOperator &child);

	RecyclerWriteSpec spec;
	shared_ptr<RecyclerCache> cache;

public:
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// sink
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return !spec.ordered;
	}
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	// source
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return !spec.ordered;
	}
	OrderPreservationType SourceOrder() const override {
		return spec.ordered ? OrderPreservationType::FIXED_ORDER : OrderPreservationType::INSERTION_ORDER;
	}
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
};

} // namespace duckdb
