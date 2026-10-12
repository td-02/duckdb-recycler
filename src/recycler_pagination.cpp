//===----------------------------------------------------------------------===//
// recycler_pagination.cpp - LIMIT/OFFSET pages over one sorted result (M6)
//
// The pre hook finds the pagination of the bound plan and proves its ORDER BY a total order (DetectPagination);
// the post hook's subtree walker keys, serves and writes the sorted prefix with the helpers below.
//===----------------------------------------------------------------------===//
#include "recycler_pagination.hpp"
#include "recycler_rewrite.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Pagination: a sorted prefix may only be replayed for later pages when the ORDER BY is a total order
//===----------------------------------------------------------------------===//
//! LIMIT + OFFSET above which DuckDB keeps ORDER BY + LIMIT instead of a Top-N (topn_optimizer.cpp): only then does
//! sorting the whole input pay for later pages
static constexpr idx_t PAGINATION_MIN_ROWS = 5000;
//! A pagination stores this many times the rows the current page needs, so that the following pages hit
static constexpr idx_t PAGINATION_PREFIX_FACTOR = 4;

static idx_t SaturatingMultiply(idx_t value, idx_t factor) {
	return value > NumericLimits<idx_t>::Maximum() / factor ? NumericLimits<idx_t>::Maximum() : value * factor;
}

//! The plain column behind an expression, seeing through DuckDB's lossless compress/decompress wrappers
static bool ResolvePlainColumn(const Expression &expr, ColumnBinding &result, idx_t depth = 0) {
	if (depth > 8) {
		return false;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		result = expr.Cast<BoundColumnRefExpression>().binding;
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (!func.children.empty() && (StringUtil::StartsWith(func.function.name, "__internal_compress_") ||
		                               StringUtil::StartsWith(func.function.name, "__internal_decompress_"))) {
			return ResolvePlainColumn(*func.children[0], result, depth + 1);
		}
	}
	return false;
}

//! Whether no two output rows of `op` agree on all of `keys` (output bindings of `op`). A sound but deliberately
//! small proof: primary keys of base tables, carried through filters, projections of plain columns, aggregate group
//! keys and joins that cannot multiply the rows of the side the keys come from. Anything else is "not proven".
static bool UniqueOn(LogicalOperator &op, const binding_set_t &keys, idx_t depth) {
	if (keys.empty() || depth > 64) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (!table || !get.children.empty()) {
			return false;
		}
		auto &column_ids = get.GetColumnIds();
		std::unordered_set<idx_t> present;
		for (auto &key : keys) {
			if (key.table_index == get.table_index && key.column_index < column_ids.size() &&
			    !column_ids[key.column_index].HasChildren()) {
				present.insert(column_ids[key.column_index].GetPrimaryIndex());
			}
		}
		// primary keys only: UNIQUE columns may hold several NULLs, which would tie
		for (auto &constraint : table->GetConstraints()) {
			if (constraint->type != ConstraintType::UNIQUE) {
				continue;
			}
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (!unique.IsPrimaryKey()) {
				continue;
			}
			auto indexes = unique.GetLogicalIndexes(table->GetColumns());
			bool all_present = !indexes.empty();
			for (auto &index : indexes) {
				all_present = all_present && present.count(index.index) > 0;
			}
			if (all_present) {
				return true;
			}
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
		return op.children.size() == 1 && UniqueOn(*op.children[0], keys, depth + 1);
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op.Cast<LogicalProjection>();
		if (op.children.size() != 1) {
			return false;
		}
		binding_set_t mapped;
		for (auto &key : keys) {
			ColumnBinding column;
			if (key.table_index == proj.table_index && key.column_index < proj.expressions.size() &&
			    ResolvePlainColumn(*proj.expressions[key.column_index], column)) {
				mapped.insert(column);
			}
		}
		return UniqueOn(*op.children[0], mapped, depth + 1);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = op.Cast<LogicalAggregate>();
		if (agg.groups.empty() || agg.grouping_sets.size() > 1 ||
		    (agg.grouping_sets.size() == 1 && agg.grouping_sets[0].size() != agg.groups.size())) {
			return false;
		}
		for (idx_t i = 0; i < agg.groups.size(); i++) {
			if (keys.find(ColumnBinding(agg.group_index, i)) == keys.end()) {
				return false;
			}
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (op.children.size() != 2 || (join.join_type != JoinType::INNER && join.join_type != JoinType::LEFT)) {
			return false;
		}
		auto &left = *op.children[0];
		auto &right = *op.children[1];
		binding_set_t left_outputs, right_outputs;
		for (auto &binding : left.GetColumnBindings()) {
			left_outputs.insert(binding);
		}
		for (auto &binding : right.GetColumnBindings()) {
			right_outputs.insert(binding);
		}
		binding_set_t left_keys, right_keys, left_join_columns, right_join_columns;
		for (auto &key : keys) {
			if (left_outputs.count(key)) {
				left_keys.insert(key);
			} else if (right_outputs.count(key)) {
				right_keys.insert(key);
			}
		}
		// equality conditions on plain columns: if one side is unique on them, each row of the other side finds at
		// most one partner (more conditions only remove partners)
		for (auto &cond : join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL &&
			    cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				continue;
			}
			ColumnBinding column;
			if (ResolvePlainColumn(*cond.left, column)) {
				left_join_columns.insert(column);
			}
			if (ResolvePlainColumn(*cond.right, column)) {
				right_join_columns.insert(column);
			}
		}
		// the output holds each left row at most once when the right side is unique on the join columns (a LEFT
		// join keeps unmatched left rows once), so uniqueness of the left keys carries over; INNER joins are symmetric
		if (UniqueOn(left, left_keys, depth + 1) && UniqueOn(right, right_join_columns, depth + 1)) {
			return true;
		}
		return join.join_type == JoinType::INNER && UniqueOn(right, right_keys, depth + 1) &&
		       UniqueOn(left, left_join_columns, depth + 1);
	}
	default:
		return false;
	}
}

//! The ORDER BY is a total order on its input: ties (whose order is unspecified, and differs between vanilla runs)
//! cannot occur, so a cached sorted result is the same answer every later page would compute
static bool OrderKeysAreUnique(LogicalOrder &order) {
	if (order.children.size() != 1) {
		return false;
	}
	binding_set_t keys;
	for (auto &node : order.orders) {
		ColumnBinding column;
		if (ResolvePlainColumn(*node.expression, column)) {
			keys.insert(column);
		}
	}
	return UniqueOn(*order.children[0], keys, 0);
}

//! `LIMIT n OFFSET m` with constants, n + m above the Top-N threshold, over an ORDER BY reached through
//! order-preserving projections/filters. Returns the slot of the ORDER_BY, or nullptr.
unique_ptr<LogicalOperator> *MatchPagination(LogicalOperator &op, idx_t &limit_rows, idx_t &offset_rows) {
	if (op.type != LogicalOperatorType::LOGICAL_LIMIT || op.children.size() != 1) {
		return nullptr;
	}
	auto &limit = op.Cast<LogicalLimit>();
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE ||
	    (limit.offset_val.Type() != LimitNodeType::UNSET && limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE)) {
		return nullptr;
	}
	limit_rows = limit.limit_val.GetConstantValue();
	offset_rows = limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE ? limit.offset_val.GetConstantValue() : 0;
	auto total = offset_rows > NumericLimits<idx_t>::Maximum() - limit_rows ? NumericLimits<idx_t>::Maximum()
	                                                                        : limit_rows + offset_rows;
	if (total <= PAGINATION_MIN_ROWS) {
		return nullptr;
	}
	auto order_slot = &op.children[0];
	while ((*order_slot)->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	       (*order_slot)->type == LogicalOperatorType::LOGICAL_FILTER) {
		if ((*order_slot)->children.size() != 1) {
			return nullptr;
		}
		order_slot = &(*order_slot)->children[0];
	}
	if ((*order_slot)->type != LogicalOperatorType::LOGICAL_ORDER_BY) {
		return nullptr;
	}
	return order_slot;
}

//! Number of pagination sites below `op`; `first` receives the first LIMIT found
idx_t CountPaginationSites(LogicalOperator &op, LogicalOperator **first) {
	idx_t count = 0;
	idx_t limit_rows, offset_rows;
	if (MatchPagination(op, limit_rows, offset_rows)) {
		if (first && !*first) {
			*first = &op;
		}
		count++;
	}
	for (auto &child : op.children) {
		count += CountPaginationSites(*child, first);
	}
	return count;
}

//! Direction and NULL order of every ORDER BY key
string OrderSpec(const LogicalOrder &order) {
	string result;
	for (auto &node : order.orders) {
		result += char('0' + static_cast<uint8_t>(node.type));
		result += char('0' + static_cast<uint8_t>(node.null_order));
	}
	return result;
}

//! What the cached unit (the top of the order-preserving chain under the LIMIT, above any compressed-materialization
//! decompression) emits: ORDER BY key count/directions/NULL orders and the logical output types/column count. The
//! ORDER_BY's own types are deliberately not part of it: they depend on column statistics, which a plan reading a
//! cached block does not have.
static string UnitLayout(const LogicalOrder &order, const LogicalOperator &unit) {
	string result = OrderSpec(order);
	result += "|n:" + std::to_string(unit.types.size());
	for (auto &type : unit.types) {
		result += "|t:" + type.ToString();
	}
	return result;
}

//! The cache key of a pagination: the bound ORDER BY subtree, which of its output columns the query demands, and the
//! layout the optimized unit actually materializes. Both the hash and the canonical string cover all of it.
string PaginationCanonical(const PaginationSite &site, const LogicalOrder &order, const LogicalOperator &unit) {
	string result = site.canonical;
	result += "\x1f"
	          "demanded";
	for (auto idx : site.demanded) {
		result += ":" + std::to_string(idx);
	}
	result += "\x1f"
	          "layout" +
	          UnitLayout(order, unit);
	return result;
}

//! Rows of the sorted result to store for a page that needs `need` leading rows: a multiple of `need`, at least twice
//! an existing shorter prefix (geometric growth), clamped to `cap_rows` (less one chunk, because the sink appends
//! whole chunks) and to what `cap_bytes` holds at `row_width` bytes per row. A result below `need` means the prefix
//! cannot even cover the page.
idx_t PrefixRows(idx_t need, idx_t existing_rows, idx_t cap_rows, idx_t cap_bytes, idx_t row_width) {
	auto prefix = SaturatingMultiply(need, PAGINATION_PREFIX_FACTOR);
	if (existing_rows > 0) {
		prefix = MaxValue(prefix, SaturatingMultiply(existing_rows, 2));
	}
	auto rows_cap = cap_rows > STANDARD_VECTOR_SIZE ? cap_rows - STANDARD_VECTOR_SIZE : 0;
	auto bytes_cap = cap_bytes / MaxValue<idx_t>(row_width, 1);
	return MinValue(prefix, MinValue(rows_cap, bytes_cap));
}

SigHash HashPaginationKey(const string &canonical) {
	uint64_t h1 = 0xCBF29CE484222325ULL;
	uint64_t h2 = 0x84222325CBF29CE4ULL;
	for (unsigned char byte : canonical) {
		h1 ^= byte;
		h1 *= 0x100000001B3ULL;
		h2 = (h2 ^ byte) * 0x9E3779B97F4A7C15ULL;
		h2 ^= (h2 >> 29);
	}
	SigHash result;
	result.h1 = h1;
	result.h2 = h2;
	return result;
}

//! For every output column of the cached unit (the chain under the LIMIT), the position of the bound ORDER BY output it
//! carries. The unit's own column order is not fixed (a debug build deliberately permutes the columns under a sort), so
//! each column is followed down through the plan's projections (compress/decompress and whatever else an optimizer
//! puts there) to the bound ORDER BY's input projection, which keeps its table index. False (fail closed) when a column
//! cannot be followed there or two columns carry the same position.
bool UnitPositions(LogicalOperator &unit, idx_t projection_index, vector<idx_t> &positions) {
	std::unordered_map<idx_t, LogicalProjection *> projections;
	for (auto op = &unit;; op = op->children[0].get()) {
		if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = op->Cast<LogicalProjection>();
			projections[projection.table_index] = &projection;
			if (projection.table_index == projection_index) {
				break;
			}
		} else if (op->type != LogicalOperatorType::LOGICAL_ORDER_BY &&
		           op->type != LogicalOperatorType::LOGICAL_FILTER) {
			return false;
		}
		if (op->children.size() != 1) {
			return false;
		}
	}
	positions.clear();
	for (auto binding : unit.GetColumnBindings()) {
		for (idx_t steps = 0; binding.table_index != projection_index; steps++) {
			auto projection = projections.find(binding.table_index);
			if (steps > 64 || projection == projections.end() ||
			    binding.column_index >= projection->second->expressions.size() ||
			    !ResolvePlainColumn(*projection->second->expressions[binding.column_index], binding)) {
				return false;
			}
		}
		if (std::find(positions.begin(), positions.end(), binding.column_index) != positions.end()) {
			return false;
		}
		positions.push_back(binding.column_index);
	}
	return true;
}

//! For each of `positions` (a consumer's columns), the stored column of `entry` that carries it. False when the entry
//! does not hold exactly these bound ORDER BY outputs.
bool StoredColumns(const CachedEntry &entry, const vector<idx_t> &positions, vector<idx_t> &stored) {
	if (entry.columns.size() != positions.size()) {
		return false;
	}
	stored.clear();
	for (auto position : positions) {
		idx_t index = 0;
		while (index < entry.columns.size() && entry.columns[index].column != position) {
			index++;
		}
		if (index == entry.columns.size()) {
			return false;
		}
		stored.push_back(index);
	}
	return true;
}

//! Pre hook: find the pagination of the bound plan and take its key and total-order proof from the bound plan,
//! before any M5 rewrite changes what lies below the ORDER BY. Only a plan with a single pagination is handled; the
//! client state is only set once every eligibility check has passed.
void DetectPagination(RecyclerClientState &client, PlanSignatures &sig, LogicalOperator &root) {
	LogicalOperator *limit_op = nullptr;
	if (CountPaginationSites(root, &limit_op) != 1) {
		return;
	}
	PaginationSite site;
	auto order_slot = MatchPagination(*limit_op, site.limit, site.offset);
	auto &order = (*order_slot)->Cast<LogicalOrder>();
	auto &info = sig.Get(order);
	if (!info.Cacheable() || !info.ordered || !OrderKeysAreUnique(order) ||
	    order.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	site.projection_index = order.children[0]->Cast<LogicalProjection>().table_index;
	// the session's result-affecting settings (SettingsSalt) are part of what the sorted rows are
	site.canonical = sig.Render(order) +
	                 "\x1f"
	                 "salt" +
	                 client.settings_salt;
	site.order_spec = OrderSpec(order);
	site.tables = TableDependencies(info.tables, client.snapshot);
	// the columns of the sorted result that the query's output actually demands decide what the optimizer keeps. A
	// mere reference is not enough (a subquery's projection mentions every column), so demand is propagated from
	// the query's output down through the projections above the LIMIT; any other operator there is not handled.
	binding_set_t demanded;
	for (auto &binding : root.GetColumnBindings()) {
		demanded.insert(binding);
	}
	for (auto *node = &root; node != limit_op;) {
		if (node->type != LogicalOperatorType::LOGICAL_PROJECTION || node->children.size() != 1) {
			return;
		}
		auto &proj = node->Cast<LogicalProjection>();
		binding_set_t next;
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (demanded.find(ColumnBinding(proj.table_index, i)) != demanded.end()) {
				vector<ColumnBinding> refs;
				CollectBindings(*proj.expressions[i], refs);
				next.insert(refs.begin(), refs.end());
			}
		}
		demanded = std::move(next);
		node = node->children[0].get();
	}
	auto bindings = order.GetColumnBindings();
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (demanded.find(bindings[i]) != demanded.end()) {
			site.demanded.push_back(i);
		}
	}
	site.eligible = true;
	client.pagination_site = std::move(site);
}

} // namespace duckdb
