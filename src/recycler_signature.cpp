//===----------------------------------------------------------------------===//
// recycler_signature.cpp - canonical structural signatures (see header)
//===----------------------------------------------------------------------===//
#include "recycler_signature.hpp"
#include "recycler_operators.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Sinks
//===----------------------------------------------------------------------===//
class PlanSignatures::Sink {
public:
	virtual ~Sink() {
	}
	virtual void Write(const void *data, size_t len) = 0;

	void U8(uint8_t v) {
		Write(&v, 1);
	}
	void U64(uint64_t v) {
		Write(&v, sizeof(v));
	}
	void Bool(bool b) {
		U8(b ? 1 : 0);
	}
	void Str(const string &s) {
		U64(s.size());
		Write(s.data(), s.size());
	}
	void Tag(const char *t) {
		Write(t, std::strlen(t));
	}
	void Hash(const SigHash &h) {
		U64(h.h1);
		U64(h.h2);
	}
};

class PlanSignatures::HashSink : public PlanSignatures::Sink {
public:
	void Write(const void *data, size_t len) override {
		auto bytes = static_cast<const uint8_t *>(data);
		for (size_t i = 0; i < len; i++) {
			// FNV-1a
			h1 ^= bytes[i];
			h1 *= 0x100000001B3ULL;
			// independent multiplicative mix
			h2 = (h2 ^ bytes[i]) * 0x9E3779B97F4A7C15ULL;
			h2 ^= (h2 >> 29);
		}
	}
	SigHash Result() const {
		SigHash result;
		result.h1 = h1;
		result.h2 = h2;
		return result;
	}

private:
	uint64_t h1 = 0xCBF29CE484222325ULL;
	uint64_t h2 = 0x84222325CBF29CE4ULL;
};

class PlanSignatures::StringSink : public PlanSignatures::Sink {
public:
	explicit StringSink(string &out) : out(out) {
	}
	void Write(const void *data, size_t len) override {
		out.append(static_cast<const char *>(data), len);
	}

private:
	string &out;
};

string SigHash::ToString() const {
	char buf[40];
	snprintf(buf, sizeof(buf), "%016llx%016llx", (unsigned long long)h1, (unsigned long long)h2);
	return string(buf);
}

bool SigHash::TryParse(const string &text, SigHash &result) {
	if (text.size() != 32 || text.find_first_not_of("0123456789abcdef") != string::npos) {
		return false;
	}
	result.h1 = std::strtoull(text.substr(0, 16).c_str(), nullptr, 16);
	result.h2 = std::strtoull(text.substr(16).c_str(), nullptr, 16);
	return true;
}

const char *RefusalReasonName(RefusalReason reason) {
	switch (reason) {
	case RefusalReason::NONE:
		return "none";
	case RefusalReason::TABLE_FUNCTION:
		return "table_function";
	case RefusalReason::SAMPLED_SCAN:
		return "sampled_scan";
	case RefusalReason::SCAN_OPTIONS:
		return "scan_options";
	case RefusalReason::CORRELATED_COLUMN:
		return "correlated_column";
	case RefusalReason::VOLATILE_FUNCTION:
		return "volatile_function";
	case RefusalReason::PARAMETER:
		return "parameter";
	case RefusalReason::EXPRESSION:
		return "expression";
	case RefusalReason::OPERATOR:
		return "operator";
	case RefusalReason::CONSTANT_COLLECTION:
		return "constant_collection";
	case RefusalReason::EXTERNAL_REFERENCE:
		return "external_reference";
	case RefusalReason::DELIM_GET:
		return "delim_get";
	case RefusalReason::LIMIT_WITHOUT_ORDER:
		return "limit_without_order";
	case RefusalReason::RUNTIME_FILTER:
		return "runtime_filter";
	default:
		return "unknown";
	}
}

RefusalReason NodeInfo::Reason() const {
	if (refused) {
		return refusal;
	}
	if (external_ref) {
		return RefusalReason::EXTERNAL_REFERENCE;
	}
	if (unresolved_delim_get) {
		return RefusalReason::DELIM_GET;
	}
	if (nondeterministic) {
		// SAMPLE refuses with its own reason, so what is left is a LIMIT whose input has no order
		return RefusalReason::LIMIT_WITHOUT_ORDER;
	}
	if (runtime_filtered || !pending_dynamic_owners.empty()) {
		return RefusalReason::RUNTIME_FILTER;
	}
	return RefusalReason::NONE;
}

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//
static void Refuse(NodeInfo &info, RefusalReason reason) {
	info.refused = true;
	if (info.refusal == RefusalReason::NONE) {
		info.refusal = reason;
	}
}

static bool ContainsDynamicFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::DYNAMIC_FILTER:
		return true;
	case TableFilterType::OPTIONAL_FILTER: {
		auto &opt = filter.Cast<OptionalFilter>();
		return opt.child_filter && ContainsDynamicFilter(*opt.child_filter);
	}
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj = static_cast<const ConjunctionFilter &>(filter);
		for (auto &child : conj.child_filters) {
			if (ContainsDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &sf = filter.Cast<StructFilter>();
		return sf.child_filter && ContainsDynamicFilter(*sf.child_filter);
	}
	default:
		return false;
	}
}

static void RenderColumnIndex(const ColumnIndex &index, PlanSignatures::Sink &sink);

idx_t RecyclerEstimateRowWidth(const vector<LogicalType> &types) {
	idx_t width = 0;
	for (auto &type : types) {
		switch (type.id()) {
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::BLOB:
		case LogicalTypeId::BIT:
			width += 32;
			break;
		case LogicalTypeId::LIST:
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::MAP:
		case LogicalTypeId::ARRAY:
		case LogicalTypeId::UNION:
			width += 64;
			break;
		default:
			width += GetTypeIdSize(type.InternalType());
			break;
		}
	}
	return MaxValue<idx_t>(width, 1);
}

//===----------------------------------------------------------------------===//
// PlanSignatures
//===----------------------------------------------------------------------===//
PlanSignatures::PlanSignatures(ClientContext &context, bool shape_mode) : context(context), shape_mode(shape_mode) {
}

const NodeInfo &PlanSignatures::Get(const LogicalOperator &op) const {
	auto entry = infos.find(&op);
	if (entry == infos.end()) {
		throw InternalException("recycler: node was not analyzed");
	}
	return entry->second;
}

void PlanSignatures::Analyze(LogicalOperator &root) {
	ids.clear();
	infos.clear();
	gets.clear();
	dynamic_owners.clear();
	next_id = 0;
	joins = 0;
	CollectDynamicOwners(root);
	AssignIds(root);
	Compute(root);
}

void PlanSignatures::CollectDynamicOwners(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.filter_pushdown) {
			for (auto &probe : join.filter_pushdown->probe_info) {
				dynamic_owners[probe.dynamic_filters.get()] = &op;
			}
		}
	}
	for (auto &child : op.children) {
		CollectDynamicOwners(*child);
	}
}

void PlanSignatures::AssignIds(const LogicalOperator &op) {
	auto &info = infos[&op];
	info.start = next_id;
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		gets[op.Cast<LogicalGet>().table_index] = &op.Cast<LogicalGet>();
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_ANY_JOIN || op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		joins++;
	}
	if (op.type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		auto &empty = op.Cast<LogicalEmptyResult>();
		for (auto &binding : empty.bindings) {
			if (ids.find(binding.table_index) == ids.end()) {
				ids[binding.table_index] = next_id++;
			}
		}
	} else {
		for (auto idx : op.GetTableIndex()) {
			if (ids.find(idx) == ids.end()) {
				ids[idx] = next_id++;
			}
		}
	}
	for (auto &child : op.children) {
		AssignIds(*child);
	}
	info.count = next_id - info.start;
}

void PlanSignatures::RenderOwnIndex(idx_t table_index, const Frame &frame, Sink &sink) const {
	auto entry = ids.find(table_index);
	if (entry == ids.end()) {
		sink.Tag("?I");
		return;
	}
	sink.Tag("I");
	sink.U64(entry->second - frame.start);
}

void PlanSignatures::RenderBinding(ColumnBinding binding, const Frame &frame, Sink &sink, NodeInfo &info) const {
	auto entry = ids.find(binding.table_index);
	if (entry == ids.end() || entry->second < frame.start || entry->second - frame.start >= frame.count) {
		info.external_ref = true;
		sink.Tag("X");
		sink.U64(binding.column_index);
		return;
	}
	if (shape_mode) {
		auto get = gets.find(binding.table_index);
		if (get != gets.end()) {
			auto &column_ids = get->second->GetColumnIds();
			sink.Tag("P");
			sink.U64(entry->second - frame.start);
			sink.U64(binding.column_index < column_ids.size() ? column_ids[binding.column_index].GetPrimaryIndex()
			                                                   : DConstants::INVALID_INDEX);
			return;
		}
	}
	sink.Tag("B");
	sink.U64(entry->second - frame.start);
	sink.U64(binding.column_index);
}

ColumnIdentity PlanSignatures::IdentityOf(ColumnBinding binding) const {
	ColumnIdentity result;
	auto id = ids.find(binding.table_index);
	auto get = gets.find(binding.table_index);
	if (id == ids.end() || get == gets.end()) {
		return result;
	}
	auto &column_ids = get->second->GetColumnIds();
	if (binding.column_index >= column_ids.size()) {
		return result;
	}
	result.table = id->second;
	result.column = column_ids[binding.column_index].GetPrimaryIndex();
	return result;
}

static void RenderColumnIndex(const ColumnIndex &index, PlanSignatures::Sink &sink) {
	sink.U64(index.GetPrimaryIndex());
	auto &children = index.GetChildIndexes();
	sink.U64(children.size());
	for (auto &child : children) {
		RenderColumnIndex(child, sink);
	}
}

//===----------------------------------------------------------------------===//
// Expressions
//===----------------------------------------------------------------------===//
void PlanSignatures::RenderExpression(const Expression &expr, const Frame &frame, Sink &sink, NodeInfo &info) const {
	sink.U8(static_cast<uint8_t>(expr.GetExpressionClass()));
	sink.U8(static_cast<uint8_t>(expr.GetExpressionType()));
	sink.Str(expr.return_type.ToString());
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		if (ref.depth != 0) {
			Refuse(info, RefusalReason::CORRELATED_COLUMN);
			info.external_ref = true;
		}
		RenderBinding(ref.binding, frame, sink, info);
		break;
	}
	case ExpressionClass::BOUND_CONSTANT: {
		auto &constant = expr.Cast<BoundConstantExpression>();
		sink.Bool(constant.value.IsNull());
		if (!constant.value.IsNull()) {
			sink.Str(constant.value.ToString());
			sink.U64(constant.value.Hash());
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.GetStability() != FunctionStability::CONSISTENT) {
			Refuse(info, RefusalReason::VOLATILE_FUNCTION);
		}
		sink.Str(func.function.name);
		sink.Bool(func.is_operator);
		sink.U64(func.children.size());
		for (auto &child : func.children) {
			RenderExpression(*child, frame, sink, info);
		}
		if (func.bind_info) {
			// bind data can carry semantics that are not visible in the children (e.g. struct field names);
			// the textual form is only used to make the key stricter, never looser
			sink.Str(expr.ToString());
		}
		break;
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr = expr.Cast<BoundAggregateExpression>();
		if (aggr.function.GetStability() != FunctionStability::CONSISTENT) {
			Refuse(info, RefusalReason::VOLATILE_FUNCTION);
		}
		sink.Str(aggr.function.name);
		sink.U8(static_cast<uint8_t>(aggr.aggr_type));
		sink.U64(aggr.children.size());
		for (auto &child : aggr.children) {
			RenderExpression(*child, frame, sink, info);
		}
		sink.Bool(aggr.filter != nullptr);
		if (aggr.filter) {
			RenderExpression(*aggr.filter, frame, sink, info);
		}
		sink.Bool(aggr.order_bys != nullptr);
		if (aggr.order_bys) {
			sink.U64(aggr.order_bys->orders.size());
			for (auto &order : aggr.order_bys->orders) {
				sink.U8(static_cast<uint8_t>(order.type));
				sink.U8(static_cast<uint8_t>(order.null_order));
				RenderExpression(*order.expression, frame, sink, info);
			}
		}
		if (aggr.bind_info) {
			sink.Str(expr.ToString());
		}
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		sink.Bool(cast.try_cast);
		RenderExpression(*cast.child, frame, sink, info);
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &cmp = expr.Cast<BoundComparisonExpression>();
		RenderExpression(*cmp.left, frame, sink, info);
		RenderExpression(*cmp.right, frame, sink, info);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		sink.U64(conj.children.size());
		for (auto &child : conj.children) {
			RenderExpression(*child, frame, sink, info);
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		sink.U64(op.children.size());
		for (auto &child : op.children) {
			RenderExpression(*child, frame, sink, info);
		}
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &c = expr.Cast<BoundCaseExpression>();
		sink.U64(c.case_checks.size());
		for (auto &check : c.case_checks) {
			RenderExpression(*check.when_expr, frame, sink, info);
			RenderExpression(*check.then_expr, frame, sink, info);
		}
		RenderExpression(*c.else_expr, frame, sink, info);
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &b = expr.Cast<BoundBetweenExpression>();
		sink.Bool(b.lower_inclusive);
		sink.Bool(b.upper_inclusive);
		RenderExpression(*b.input, frame, sink, info);
		RenderExpression(*b.lower, frame, sink, info);
		RenderExpression(*b.upper, frame, sink, info);
		break;
	}
	case ExpressionClass::BOUND_PARAMETER:
		Refuse(info, RefusalReason::PARAMETER);
		break;
	default:
		// BOUND_REF, BOUND_SUBQUERY, BOUND_WINDOW, BOUND_UNNEST, BOUND_LAMBDA, BOUND_LAMBDA_REF, BOUND_DEFAULT, ...
		Refuse(info, RefusalReason::EXPRESSION);
		break;
	}
}

//===----------------------------------------------------------------------===//
// Operators
//===----------------------------------------------------------------------===//
void PlanSignatures::RenderJoin(const LogicalComparisonJoin &join, const Frame &frame, Sink &sink,
                                NodeInfo &info) const {
	sink.U8(static_cast<uint8_t>(join.join_type));
	sink.U64(join.conditions.size());
	for (auto &cond : join.conditions) {
		sink.U8(static_cast<uint8_t>(cond.comparison));
		RenderExpression(*cond.left, frame, sink, info);
		RenderExpression(*cond.right, frame, sink, info);
	}
	sink.U64(join.left_projection_map.size());
	for (auto idx : join.left_projection_map) {
		sink.U64(idx);
	}
	sink.U64(join.right_projection_map.size());
	for (auto idx : join.right_projection_map) {
		sink.U64(idx);
	}
	if (join.join_type == JoinType::MARK) {
		RenderOwnIndex(join.mark_index, frame, sink);
	}
	sink.U64(join.mark_types.size());
	for (auto &type : join.mark_types) {
		sink.Str(type.ToString());
	}
	sink.U64(join.duplicate_eliminated_columns.size());
	for (auto &expr : join.duplicate_eliminated_columns) {
		RenderExpression(*expr, frame, sink, info);
	}
	sink.Bool(join.delim_flipped);
	sink.Bool(join.predicate != nullptr);
	if (join.predicate) {
		RenderExpression(*join.predicate, frame, sink, info);
	}
}

void PlanSignatures::RenderNode(const LogicalOperator &op, const Frame &frame, Sink &sink, NodeInfo &info,
                                bool render_children) const {
	sink.Tag("{");
	sink.U8(static_cast<uint8_t>(op.type));
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (!table) {
			Refuse(info, RefusalReason::TABLE_FUNCTION);
			break;
		}
		if (get.extra_info.sample_options) {
			Refuse(info, RefusalReason::SAMPLED_SCAN);
			info.nondeterministic = true;
		}
		if (!get.projected_input.empty() || !get.children.empty() || get.ordinality_idx.IsValid() ||
		    get.row_group_order_options) {
			Refuse(info, RefusalReason::SCAN_OPTIONS);
		}
		RenderOwnIndex(get.table_index, frame, sink);
		sink.Str(get.function.name);
		sink.Str(table->catalog.GetName());
		sink.Str(table->schema.name);
		sink.Str(table->name);
		if (!shape_mode) {
			// the shape of a block does not depend on which columns a query happens to reference
			sink.U64(get.GetColumnIds().size());
			for (auto &col : get.GetColumnIds()) {
				RenderColumnIndex(col, sink);
			}
			sink.U64(get.projection_ids.size());
			for (auto idx : get.projection_ids) {
				sink.U64(idx);
			}
		}
		sink.U64(get.table_filters.filters.size());
		for (auto &entry : get.table_filters.filters) {
			sink.U64(entry.first);
			sink.U8(static_cast<uint8_t>(entry.second->filter_type));
			sink.Str(entry.second->ToString("c"));
			if (ContainsDynamicFilter(*entry.second)) {
				info.runtime_filtered = true;
			}
		}
		if (get.dynamic_filters) {
			auto owner = dynamic_owners.find(get.dynamic_filters.get());
			if (owner == dynamic_owners.end()) {
				info.runtime_filtered = true;
			} else {
				info.pending_dynamic_owners.push_back(owner->second);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		RenderJoin(op.Cast<LogicalComparisonJoin>(), frame, sink, info);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalAnyJoin>();
		sink.U8(static_cast<uint8_t>(join.join_type));
		RenderExpression(*join.condition, frame, sink, info);
		sink.U64(join.left_projection_map.size());
		for (auto idx : join.left_projection_map) {
			sink.U64(idx);
		}
		sink.U64(join.right_projection_map.size());
		for (auto idx : join.right_projection_map) {
			sink.U64(idx);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		RenderOwnIndex(aggr.group_index, frame, sink);
		RenderOwnIndex(aggr.aggregate_index, frame, sink);
		sink.Bool(aggr.groupings_index != DConstants::INVALID_INDEX);
		if (aggr.groupings_index != DConstants::INVALID_INDEX) {
			RenderOwnIndex(aggr.groupings_index, frame, sink);
		}
		sink.U64(aggr.groups.size());
		for (auto &group : aggr.groups) {
			RenderExpression(*group, frame, sink, info);
		}
		sink.U64(aggr.expressions.size());
		for (auto &expr : aggr.expressions) {
			RenderExpression(*expr, frame, sink, info);
		}
		sink.U64(aggr.grouping_sets.size());
		for (auto &set : aggr.grouping_sets) {
			sink.U64(set.size());
			for (auto idx : set) {
				sink.U64(idx);
			}
		}
		sink.U64(aggr.grouping_functions.size());
		for (auto &gf : aggr.grouping_functions) {
			sink.U64(gf.size());
			for (auto idx : gf) {
				sink.U64(idx);
			}
		}
		sink.U8(static_cast<uint8_t>(aggr.distinct_validity));
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op.Cast<LogicalProjection>();
		RenderOwnIndex(proj.table_index, frame, sink);
		sink.U64(proj.expressions.size());
		for (auto &expr : proj.expressions) {
			RenderExpression(*expr, frame, sink, info);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		if (shape_mode && &op == block_root) {
			// the parameterized conjuncts are the block's constraints, not part of its shape
			sink.U64(root_conjuncts.size());
			for (auto &conjunct : root_conjuncts) {
				sink.Str(conjunct);
			}
		} else {
			sink.U64(filter.expressions.size());
			for (auto &expr : filter.expressions) {
				RenderExpression(*expr, frame, sink, info);
			}
		}
		sink.U64(filter.projection_map.size());
		for (auto idx : filter.projection_map) {
			sink.U64(idx);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		sink.U64(order.orders.size());
		for (auto &o : order.orders) {
			sink.U8(static_cast<uint8_t>(o.type));
			sink.U8(static_cast<uint8_t>(o.null_order));
			RenderExpression(*o.expression, frame, sink, info);
		}
		sink.U64(order.projection_map.size());
		for (auto idx : order.projection_map) {
			sink.U64(idx);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &topn = op.Cast<LogicalTopN>();
		sink.U64(topn.orders.size());
		for (auto &o : topn.orders) {
			sink.U8(static_cast<uint8_t>(o.type));
			sink.U8(static_cast<uint8_t>(o.null_order));
			RenderExpression(*o.expression, frame, sink, info);
		}
		sink.U64(topn.limit);
		sink.U64(topn.offset);
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op.Cast<LogicalLimit>();
		const BoundLimitNode *nodes[2] = {&limit.limit_val, &limit.offset_val};
		for (auto node : nodes) {
			sink.U8(static_cast<uint8_t>(node->Type()));
			switch (node->Type()) {
			case LimitNodeType::UNSET:
				break;
			case LimitNodeType::CONSTANT_VALUE:
				sink.U64(node->GetConstantValue());
				break;
			case LimitNodeType::CONSTANT_PERCENTAGE: {
				double pct = node->GetConstantPercentage();
				sink.Write(&pct, sizeof(pct));
				break;
			}
			default:
				Refuse(info, RefusalReason::EXPRESSION);
				break;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		sink.U8(static_cast<uint8_t>(distinct.distinct_type));
		sink.U64(distinct.distinct_targets.size());
		for (auto &expr : distinct.distinct_targets) {
			RenderExpression(*expr, frame, sink, info);
		}
		sink.Bool(distinct.order_by != nullptr);
		if (distinct.order_by) {
			sink.U64(distinct.order_by->orders.size());
			for (auto &o : distinct.order_by->orders) {
				sink.U8(static_cast<uint8_t>(o.type));
				sink.U8(static_cast<uint8_t>(o.null_order));
				RenderExpression(*o.expression, frame, sink, info);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_CHUNK_GET: {
		auto &cdg = op.Cast<LogicalColumnDataGet>();
		RenderOwnIndex(cdg.table_index, frame, sink);
		sink.U64(cdg.chunk_types.size());
		for (auto &type : cdg.chunk_types) {
			sink.Str(type.ToString());
		}
		if (!cdg.collection) {
			Refuse(info, RefusalReason::OPERATOR);
			break;
		}
		auto &collection = *cdg.collection;
		if (collection.Count() > 4096) {
			Refuse(info, RefusalReason::CONSTANT_COLLECTION);
			break;
		}
		sink.U64(collection.Count());
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state);
		DataChunk chunk;
		collection.InitializeScanChunk(chunk);
		while (collection.Scan(scan_state, chunk)) {
			for (idx_t row = 0; row < chunk.size(); row++) {
				for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
					auto value = chunk.GetValue(col, row);
					sink.Bool(value.IsNull());
					if (!value.IsNull()) {
						sink.Str(value.ToString());
					}
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		auto &cte = op.Cast<LogicalMaterializedCTE>();
		RenderOwnIndex(cte.table_index, frame, sink);
		sink.U64(cte.column_count);
		sink.U8(static_cast<uint8_t>(cte.materialize));
		break;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &ref = op.Cast<LogicalCTERef>();
		if (ref.is_recurring || ref.correlated_columns > 0) {
			Refuse(info, RefusalReason::OPERATOR);
		}
		RenderOwnIndex(ref.table_index, frame, sink);
		// the referenced CTE must be part of the subtree for the result to be self-contained
		RenderBinding(ColumnBinding(ref.cte_index, 0), frame, sink, info);
		sink.U64(ref.chunk_types.size());
		for (auto &type : ref.chunk_types) {
			sink.Str(type.ToString());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		auto &dg = op.Cast<LogicalDelimGet>();
		RenderOwnIndex(dg.table_index, frame, sink);
		sink.U64(dg.chunk_types.size());
		for (auto &type : dg.chunk_types) {
			sink.Str(type.ToString());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		auto &dummy = op.Cast<LogicalDummyScan>();
		RenderOwnIndex(dummy.table_index, frame, sink);
		break;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &eg = op.Cast<LogicalExpressionGet>();
		RenderOwnIndex(eg.table_index, frame, sink);
		sink.U64(eg.expr_types.size());
		for (auto &type : eg.expr_types) {
			sink.Str(type.ToString());
		}
		sink.U64(eg.expressions.size());
		for (auto &row : eg.expressions) {
			sink.U64(row.size());
			for (auto &expr : row) {
				RenderExpression(*expr, frame, sink, info);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &setop = op.Cast<LogicalSetOperation>();
		RenderOwnIndex(setop.table_index, frame, sink);
		sink.U64(setop.column_count);
		sink.Bool(setop.setop_all);
		sink.Bool(setop.allow_out_of_order);
		break;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		auto &empty = op.Cast<LogicalEmptyResult>();
		sink.U64(empty.bindings.size());
		for (auto &binding : empty.bindings) {
			RenderOwnIndex(binding.table_index, frame, sink);
			sink.U64(binding.column_index);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR: {
		auto ours = AsRecyclerOperator(op);
		if (!ours) {
			Refuse(info, RefusalReason::OPERATOR);
			break;
		}
		if (ours->kind == RecyclerOperatorKind::MATERIALIZE) {
			// a planted block marker is a transparent wrapper around its child
			sink.Tag("RMAT");
			break;
		}
		auto &scan = op.Cast<LogicalRecyclerScan>();
		RenderOwnIndex(scan.table_index, frame, sink);
		sink.Hash(scan.entry->hash);
		sink.U64(scan.column_ids.size());
		for (auto col : scan.column_ids) {
			sink.U64(col);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_SAMPLE:
		info.nondeterministic = true;
		Refuse(info, RefusalReason::SAMPLED_SCAN);
		break;
	default:
		Refuse(info, RefusalReason::OPERATOR);
		break;
	}
	// output types (a block's output columns depend on what the query references, so not in shape mode)
	if (!shape_mode) {
		sink.Tag("T");
		sink.U64(op.types.size());
		for (auto &type : op.types) {
			sink.Str(type.ToString());
		}
	}
	// children
	sink.U64(op.children.size());
	for (auto &child : op.children) {
		auto &child_info = Get(*child);
		if (render_children || child_info.external_ref) {
			// full rendering, or a child whose own hash is context-dependent: inline it in our frame
			RenderNode(*child, frame, sink, info, render_children);
		} else {
			sink.Hash(child_info.hash);
		}
	}
	sink.Tag("}");
}

void PlanSignatures::Compute(LogicalOperator &op) {
	for (auto &child : op.children) {
		Compute(*child);
	}
	auto &info = infos[&op];
	Frame frame {info.start, info.count};
	HashSink sink;
	RenderNode(op, frame, sink, info, false);
	info.hash = sink.Result();

	// propagate child flags
	for (auto &child : op.children) {
		auto &child_info = Get(*child);
		info.refused = info.refused || child_info.refused;
		if (info.refusal == RefusalReason::NONE) {
			info.refusal = child_info.refusal;
		}
		info.nondeterministic = info.nondeterministic || child_info.nondeterministic;
		info.unreliable_estimate = info.unreliable_estimate || child_info.unreliable_estimate;
		info.runtime_filtered = info.runtime_filtered || child_info.runtime_filtered;
		info.est_leaf_rows += child_info.est_leaf_rows;
		info.delim_gets += child_info.delim_gets;
		info.delim_joins += child_info.delim_joins;
		for (auto owner : child_info.pending_dynamic_owners) {
			if (owner != &op) {
				info.pending_dynamic_owners.push_back(owner);
			}
		}
	}
	// operator-specific derived flags
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		info.est_leaf_rows += op.EstimateCardinality(context);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		info.delim_gets++;
		info.unresolved_delim_get = true;
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		info.unreliable_estimate = true;
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL &&
			    cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				info.unreliable_estimate = true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		info.delim_joins++;
		auto delim_side = join.delim_flipped ? 0 : 1;
		auto &delim_info = Get(*op.children[delim_side]);
		auto &other_info = Get(*op.children[1 - delim_side]);
		// the delim gets on the delim side belong to this join unless there are nested delim joins there,
		// in which case we cannot tell them apart and stay conservative
		bool own_gets_resolved = delim_info.delim_joins == 0 && delim_info.delim_gets > 0;
		info.unresolved_delim_get = other_info.unresolved_delim_get || (!own_gets_resolved && delim_info.delim_gets > 0);
		info.delim_gets = other_info.delim_gets + (own_gets_resolved ? 0 : delim_info.delim_gets);
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
		info.ordered = true;
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		info.ordered = !op.children.empty() && Get(*op.children[0]).ordered;
		break;
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op.Cast<LogicalLimit>();
		bool child_ordered = !op.children.empty() && Get(*op.children[0]).ordered;
		bool has_limit = limit.limit_val.Type() != LimitNodeType::UNSET || limit.offset_val.Type() != LimitNodeType::UNSET;
		if (has_limit && !child_ordered) {
			// which rows survive depends on execution order
			info.nondeterministic = true;
		}
		info.ordered = child_ordered;
		break;
	}
	default:
		break;
	}
	if (op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN && op.type != LogicalOperatorType::LOGICAL_DELIM_GET) {
		for (auto &child : op.children) {
			if (Get(*child).unresolved_delim_get) {
				info.unresolved_delim_get = true;
			}
		}
	}
}

string PlanSignatures::Render(const LogicalOperator &op) const {
	auto &info = Get(op);
	Frame frame {info.start, info.count};
	string result;
	StringSink sink(result);
	NodeInfo scratch;
	RenderNode(op, frame, sink, scratch, true);
	return result;
}

//===----------------------------------------------------------------------===//
// Block shapes and range constraints
//===----------------------------------------------------------------------===//
static bool IsRangeComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

static ExpressionType FlipRangeComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExpressionType::COMPARE_LESSTHAN;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	case ExpressionType::COMPARE_LESSTHAN:
		return ExpressionType::COMPARE_GREATERTHAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	default:
		return type;
	}
}

//! Fold a constant expression (constants may still carry unfolded casts on the bound plan)
static bool TryFoldConstant(ClientContext &context, const Expression &expr, const LogicalType &target, Value &result) {
	if (!expr.IsFoldable() || expr.HasParameter()) {
		return false;
	}
	Value value;
	if (!ExpressionExecutor::TryEvaluateScalar(context, expr, value) || value.IsNull()) {
		return false;
	}
	try {
		result = value.DefaultCastAs(target);
	} catch (std::exception &) {
		return false;
	}
	return !result.IsNull();
}

bool PlanSignatures::TryParseConstraint(const Expression &expr, vector<RangeConstraint> &out) const {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON: {
		if (!IsRangeComparison(expr.GetExpressionType())) {
			return false;
		}
		auto &cmp = expr.Cast<BoundComparisonExpression>();
		const Expression *column = nullptr;
		const Expression *constant = nullptr;
		auto op = expr.GetExpressionType();
		if (cmp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			column = cmp.left.get();
			constant = cmp.right.get();
		} else if (cmp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			column = cmp.right.get();
			constant = cmp.left.get();
			op = FlipRangeComparison(op);
		} else {
			return false;
		}
		auto &ref = column->Cast<BoundColumnRefExpression>();
		auto identity = IdentityOf(ref.binding);
		if (!identity.IsValid() || ref.depth != 0) {
			return false;
		}
		RangeConstraint constraint;
		if (!TryFoldConstant(context, *constant, ref.return_type, constraint.value)) {
			return false;
		}
		constraint.column = identity;
		constraint.op = op;
		out.push_back(std::move(constraint));
		return true;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		if (expr.GetExpressionType() != ExpressionType::OPERATOR_IS_NOT_NULL) {
			return false;
		}
		auto &oper = expr.Cast<BoundOperatorExpression>();
		if (oper.children.size() != 1 || oper.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &ref = oper.children[0]->Cast<BoundColumnRefExpression>();
		auto identity = IdentityOf(ref.binding);
		if (!identity.IsValid() || ref.depth != 0) {
			return false;
		}
		RangeConstraint constraint;
		constraint.column = identity;
		constraint.op = ExpressionType::OPERATOR_IS_NOT_NULL;
		out.push_back(std::move(constraint));
		return true;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between = expr.Cast<BoundBetweenExpression>();
		if (between.input->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &ref = between.input->Cast<BoundColumnRefExpression>();
		auto identity = IdentityOf(ref.binding);
		if (!identity.IsValid() || ref.depth != 0) {
			return false;
		}
		RangeConstraint lower;
		RangeConstraint upper;
		if (!TryFoldConstant(context, *between.lower, ref.return_type, lower.value) ||
		    !TryFoldConstant(context, *between.upper, ref.return_type, upper.value)) {
			return false;
		}
		lower.column = identity;
		lower.op = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
		                                   : ExpressionType::COMPARE_GREATERTHAN;
		upper.column = identity;
		upper.op = between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
		                                   : ExpressionType::COMPARE_LESSTHAN;
		out.push_back(std::move(lower));
		out.push_back(std::move(upper));
		return true;
	}
	default:
		return false;
	}
}

BlockShape ComputeBlockShape(ClientContext &context, const LogicalOperator &root, const PlanSignatures &plan_info) {
	BlockShape shape;
	if (root.type != LogicalOperatorType::LOGICAL_FILTER || root.children.size() != 1) {
		return shape;
	}
	auto &root_info = plan_info.Get(root);
	if (!root_info.Cacheable()) {
		return shape;
	}
	PlanSignatures block(context, true);
	block.AssignIds(root);
	if (block.joins == 0) {
		return shape;
	}
	PlanSignatures::Frame frame {0, block.next_id};

	// classify the root conjuncts
	vector<string> regular;
	for (idx_t i = 0; i < root.expressions.size(); i++) {
		auto &expr = *root.expressions[i];
		string rendered;
		PlanSignatures::StringSink sink(rendered);
		NodeInfo scratch;
		block.RenderExpression(expr, frame, sink, scratch);
		if (scratch.refused || scratch.external_ref) {
			return shape;
		}
		shape.conjuncts.push_back(rendered);
		vector<RangeConstraint> constraints;
		if (block.TryParseConstraint(expr, constraints)) {
			shape.is_constraint.push_back(true);
			for (auto &constraint : constraints) {
				shape.constraints.push_back(std::move(constraint));
			}
		} else {
			shape.is_constraint.push_back(false);
			regular.push_back(rendered);
		}
	}
	std::sort(regular.begin(), regular.end());
	block.block_root = &root;
	block.root_conjuncts = std::move(regular);

	// render the shape (full rendering, block-relative frame)
	{
		PlanSignatures::StringSink sink(shape.canonical);
		NodeInfo scratch;
		block.RenderNode(root, frame, sink, scratch, true);
		if (scratch.refused || scratch.external_ref) {
			return shape;
		}
	}
	PlanSignatures::HashSink hasher;
	hasher.Write(shape.canonical.data(), shape.canonical.size());
	shape.hash = hasher.Result();
	shape.table_ids = block.ids;
	for (auto &binding : const_cast<LogicalOperator &>(root).GetColumnBindings()) {
		shape.output.push_back(block.IdentityOf(binding));
	}
	shape.est_leaf_rows = root_info.est_leaf_rows;
	shape.joins = block.joins;
	shape.ok = true;
	return shape;
}

//! Does the query constraint q guarantee the cached constraint c on the same column?
static bool ConstraintImplied(const RangeConstraint &c, const RangeConstraint &q) {
	if (c.op == ExpressionType::OPERATOR_IS_NOT_NULL) {
		// every comparison with a constant rejects NULL
		return true;
	}
	if (q.op == ExpressionType::OPERATOR_IS_NOT_NULL || q.value.type() != c.value.type()) {
		return false;
	}
	auto &qv = q.value;
	auto &cv = c.value;
	auto ge = [&]() { return !(qv < cv); };
	auto gt = [&]() { return cv < qv; };
	auto le = [&]() { return !(cv < qv); };
	auto lt = [&]() { return qv < cv; };
	switch (c.op) {
	case ExpressionType::COMPARE_EQUAL:
		return q.op == ExpressionType::COMPARE_EQUAL && qv == cv;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return (q.op == ExpressionType::COMPARE_GREATERTHANOREQUALTO && ge()) ||
		       (q.op == ExpressionType::COMPARE_GREATERTHAN && ge()) || (q.op == ExpressionType::COMPARE_EQUAL && ge());
	case ExpressionType::COMPARE_GREATERTHAN:
		return (q.op == ExpressionType::COMPARE_GREATERTHAN && ge()) ||
		       (q.op == ExpressionType::COMPARE_GREATERTHANOREQUALTO && gt()) ||
		       (q.op == ExpressionType::COMPARE_EQUAL && gt());
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return (q.op == ExpressionType::COMPARE_LESSTHANOREQUALTO && le()) ||
		       (q.op == ExpressionType::COMPARE_LESSTHAN && le()) || (q.op == ExpressionType::COMPARE_EQUAL && le());
	case ExpressionType::COMPARE_LESSTHAN:
		return (q.op == ExpressionType::COMPARE_LESSTHAN && le()) ||
		       (q.op == ExpressionType::COMPARE_LESSTHANOREQUALTO && lt()) ||
		       (q.op == ExpressionType::COMPARE_EQUAL && lt());
	default:
		return false;
	}
}

bool RangeConstraintsImply(const vector<RangeConstraint> &query, const vector<RangeConstraint> &cached) {
	for (auto &c : cached) {
		bool implied = false;
		for (auto &q : query) {
			if (q.column == c.column && ConstraintImplied(c, q)) {
				implied = true;
				break;
			}
		}
		if (!implied) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
