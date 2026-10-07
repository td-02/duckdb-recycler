//===----------------------------------------------------------------------===//
// recycler_exists.cpp - correlated EXISTS subqueries as plain joins the join order optimizer can place
//
// DuckDB plans `WHERE [NOT] EXISTS (SELECT ... FROM inner WHERE inner.k = outer.c ...)` as a MARK delim join over the
// whole FROM clause: the outer rows are materialized, their correlated values deduplicated and joined into the
// subquery, and the subquery's matches joined back. Two built-in decisions keep it that way whatever the sizes are:
// the Deliminator turns a delim join into a plain join only when nothing in it filters (deliminator.cpp,
// HasSelection), and the join order optimizer never reorders a delim join, nor any join tree that contains one
// (relation_manager.cpp, LOGICAL_DELIM_JOIN). With one filter anywhere, the whole FROM is materialized before the
// EXISTS is evaluated, even when the subquery would keep one outer table in a thousand.
//
// This pass does the decorrelation itself, before the built-in optimizers run, for the one shape where it is exact:
//
//   DELIM_JOIN (MARK) [c_i IS NOT DISTINCT FROM delim_i]
//     outer FROM
//     PROJECTION [subquery select list ..., delim_1 .. delim_n]
//       FILTER [delim_j = k_j ..., other conjuncts]                   (= or IS NOT DISTINCT FROM, both bare columns)
//         CROSS_PRODUCT (inner, DELIM_GET)
//
// becomes
//
//   COMPARISON_JOIN (MARK) [c_j IS NOT DISTINCT FROM p_j]
//     outer FROM
//     PROJECTION [subquery select list ..., p_j = k_j]
//       FILTER [other conjuncts, k_j IS NOT NULL for the = conjuncts]
//         inner
//
// The outer row is referred to only through the delim columns, which are copies of c_i (the delim conditions hold
// them IS NOT DISTINCT FROM c_i). The mark of a row is then "some inner row satisfies the other conjuncts and every
// c_j = k_j" in both plans. A plain `=` never matches a NULL, which IS NOT DISTINCT FROM would, hence the IS NOT NULL
// on k_j: with no NULL on the inner side a NULL c_j matches nothing, as before; and with every condition NULL-safe the
// mark is TRUE or FALSE, never NULL, which is what lets filter pushdown make an ANTI join of NOT EXISTS
// (pushdown_mark_join.cpp). Both sides of a correlation are bare columns: an expression on the inner side would be
// evaluated for every inner row by the new projection, where the planner's plan evaluates it only against the outer
// values that exist, so an expression that fails on some row (a cast) would fail a query the planner answers.
// Comparisons other than these two, correlation outside the WHERE clause (in a join condition, an aggregate, a nested
// subquery), delim joins of other kinds, and a subquery holding one of the recycler's own operators are left as they
// are: a block marker inside the subquery would end up directly below the new filter, and the join order optimizer
// passes single-child operators it does not know through, pushing the filter's conjuncts below the marker into rows
// the marker's key does not describe.
//
// Filter pushdown then turns the MARK join into a SEMI (EXISTS) or ANTI (NOT EXISTS) join, and the join order
// optimizer, which does reorder plain semi and anti joins, puts it where its cost model says: on the correlated base
// table when that shrinks the join, above a selective join when it does not. The optimizer stops at operators it
// cannot reorder, so this pass first moves the new join below those: through the preserved side of an outer join,
// through filters, and through other joins that keep every outer row. A join that decorates or filters rows by their
// own values commutes with such operators (each output row carries exactly the columns the join reads), so the rows
// above are the same multiset in either order; the join never crosses an operator that would not preserve its rows
// (a null-supplying side, a positional join, an aggregate, a projection, a set operation, a recycler operator).
//===----------------------------------------------------------------------===//
#include "recycler_exists.hpp"

#include "recycler_cache.hpp"
#include "recycler_operators.hpp"
#include "recycler_rewrite.hpp"

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

//! Adds the table indexes the column references of `expr` name; false when one of them is correlated (depth > 0)
bool CollectReferences(const Expression &expr, unordered_set<idx_t> &tables) {
	bool plain = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &ref) {
		if (ref.depth != 0) {
			plain = false;
		}
		tables.insert(ref.binding.table_index);
	});
	return plain;
}

//! The binding of a bare, uncorrelated column reference; invalid for any other expression
optional_ptr<const BoundColumnRefExpression> AsColumn(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &ref = expr.Cast<BoundColumnRefExpression>();
	return ref.depth == 0 ? &ref : nullptr;
}

//! What a subtree refers to and contains, for the checks on the subquery's own plan
struct SubtreeScan {
	unordered_set<idx_t> referenced;
	bool correlated = false;
	bool delim = false;
	bool recycler = false;

	void Walk(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			delim = true;
		}
		if (AsRecyclerOperator(op)) {
			recycler = true;
		}
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			if (!CollectReferences(**expr, referenced)) {
				correlated = true;
			}
		});
		for (auto &child : op.children) {
			Walk(*child);
		}
	}
};

//! One correlated conjunct of the subquery's WHERE clause: delim column `outer` against inner column `inner`
struct Correlation {
	idx_t conjunct;
	idx_t outer;
	bool delim_is_left;
	ExpressionType comparison;
};

//! The parts of a delim join of the shape above; `ok` only when every check passed
struct ExistsShape {
	bool ok = false;
	LogicalComparisonJoin *join = nullptr;
	LogicalProjection *projection = nullptr;
	LogicalFilter *filter = nullptr;
	LogicalOperator *cross = nullptr;
	idx_t inner_side = 0;
	vector<Correlation> correlations;
	vector<idx_t> plain_conjuncts;
};

ExistsShape MatchShape(LogicalComparisonJoin &join) {
	ExistsShape shape;
	auto n = join.duplicate_eliminated_columns.size();
	if (join.children.size() != 2 || join.delim_flipped || join.predicate || n == 0 || join.conditions.size() != n ||
	    join.HasProjectionMap()) {
		return shape;
	}
	// the subquery side: PROJECTION -> FILTER -> CROSS_PRODUCT (inner, DELIM_GET)
	auto &rhs = *join.children[1];
	if (rhs.type != LogicalOperatorType::LOGICAL_PROJECTION || rhs.children.size() != 1) {
		return shape;
	}
	auto &projection = rhs.Cast<LogicalProjection>();
	if (projection.children[0]->type != LogicalOperatorType::LOGICAL_FILTER) {
		return shape;
	}
	auto &filter = projection.children[0]->Cast<LogicalFilter>();
	if (!filter.projection_map.empty() || filter.children.size() != 1 ||
	    filter.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return shape;
	}
	auto &cross = *filter.children[0];
	if (cross.children.size() != 2) {
		return shape;
	}
	idx_t delim_side;
	if (cross.children[1]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		delim_side = 1;
	} else if (cross.children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		delim_side = 0;
	} else {
		return shape;
	}
	auto &delim_get = cross.children[delim_side]->Cast<LogicalDelimGet>();
	if (delim_get.chunk_types.size() != n || projection.expressions.size() < n) {
		return shape;
	}
	auto delim_index = delim_get.table_index;
	// the delim conditions as the planner makes them: c_i IS NOT DISTINCT FROM the projection's trailing delim column i
	auto prefix = projection.expressions.size() - n;
	for (idx_t i = 0; i < n; i++) {
		auto &cond = join.conditions[i];
		auto left = AsColumn(*cond.left);
		auto right = AsColumn(*cond.right);
		auto outer = AsColumn(*join.duplicate_eliminated_columns[i]);
		auto carried = AsColumn(*projection.expressions[prefix + i]);
		if (cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM || !left || !right || !outer || !carried) {
			return shape;
		}
		if (left->binding != outer->binding || right->binding != ColumnBinding(projection.table_index, prefix + i) ||
		    carried->binding != ColumnBinding(delim_index, i)) {
			return shape;
		}
	}
	// the rest of the select list must not look at the outer row
	for (idx_t k = 0; k < prefix; k++) {
		unordered_set<idx_t> refs;
		if (!CollectReferences(*projection.expressions[k], refs) || refs.count(delim_index)) {
			return shape;
		}
	}
	// the WHERE clause: every conjunct over the outer row is `delim column = inner column` or IS NOT DISTINCT FROM
	for (idx_t j = 0; j < filter.expressions.size(); j++) {
		auto &conjunct = *filter.expressions[j];
		unordered_set<idx_t> refs;
		if (!CollectReferences(conjunct, refs)) {
			return shape;
		}
		if (!refs.count(delim_index)) {
			shape.plain_conjuncts.push_back(j);
			continue;
		}
		auto type = conjunct.GetExpressionType();
		if (conjunct.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON ||
		    (type != ExpressionType::COMPARE_EQUAL && type != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
			return shape;
		}
		auto &comparison = conjunct.Cast<BoundComparisonExpression>();
		auto left = AsColumn(*comparison.left);
		auto right = AsColumn(*comparison.right);
		if (!left || !right) {
			return shape;
		}
		bool delim_is_left = left->binding.table_index == delim_index;
		auto &delim_column = delim_is_left ? *left : *right;
		auto &inner_column = delim_is_left ? *right : *left;
		if (delim_column.binding.table_index != delim_index || inner_column.binding.table_index == delim_index ||
		    delim_column.binding.column_index >= n) {
			return shape;
		}
		shape.correlations.push_back(Correlation {j, delim_column.binding.column_index, delim_is_left, type});
	}
	if (shape.correlations.empty()) {
		return shape;
	}
	// the subquery itself refers to the outer row only through those conjuncts, holds no delim join of its own
	// (nested correlation is left to the planner's delim machinery) and none of the recycler's operators
	auto &inner = *cross.children[1 - delim_side];
	SubtreeScan scan;
	scan.Walk(inner);
	if (scan.correlated || scan.delim || scan.recycler || scan.referenced.count(delim_index)) {
		return shape;
	}
	shape.ok = true;
	shape.join = &join;
	shape.projection = &projection;
	shape.filter = &filter;
	shape.cross = &cross;
	shape.inner_side = 1 - delim_side;
	return shape;
}

//! Builds the plain MARK join of a matched shape, consuming the old operators
unique_ptr<LogicalOperator> Decorrelate(ExistsShape &shape) {
	auto &join = *shape.join;
	auto &projection = *shape.projection;
	auto &filter = *shape.filter;
	auto prefix = projection.expressions.size() - join.duplicate_eliminated_columns.size();

	vector<unique_ptr<Expression>> select_list;
	for (idx_t k = 0; k < prefix; k++) {
		select_list.push_back(std::move(projection.expressions[k]));
	}
	vector<unique_ptr<Expression>> conjuncts;
	for (auto j : shape.plain_conjuncts) {
		conjuncts.push_back(std::move(filter.expressions[j]));
	}
	vector<JoinCondition> conditions;
	for (auto &correlation : shape.correlations) {
		auto &comparison = filter.expressions[correlation.conjunct]->Cast<BoundComparisonExpression>();
		auto inner_column = std::move(correlation.delim_is_left ? comparison.right : comparison.left);
		if (correlation.comparison == ExpressionType::COMPARE_EQUAL) {
			auto not_null =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			not_null->children.push_back(inner_column->Copy());
			conjuncts.push_back(std::move(not_null));
		}
		JoinCondition cond;
		cond.left = join.duplicate_eliminated_columns[correlation.outer]->Copy();
		cond.right = make_uniq<BoundColumnRefExpression>(inner_column->return_type,
		                                                 ColumnBinding(projection.table_index, select_list.size()));
		cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		select_list.push_back(std::move(inner_column));
		conditions.push_back(std::move(cond));
	}

	unique_ptr<LogicalOperator> rhs = std::move(shape.cross->children[shape.inner_side]);
	if (!conjuncts.empty()) {
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(conjuncts);
		new_filter->children.push_back(std::move(rhs));
		rhs = std::move(new_filter);
	}
	auto new_projection = make_uniq<LogicalProjection>(projection.table_index, std::move(select_list));
	new_projection->children.push_back(std::move(rhs));

	auto result = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
	result->mark_index = join.mark_index;
	result->conditions = std::move(conditions);
	result->children.push_back(std::move(join.children[0]));
	result->children.push_back(std::move(new_projection));
	return std::move(result);
}

//! The child of `op` that keeps every row of its input and produces every table in `needed`, if there is one
optional_idx PreservedChild(LogicalOperator &op, const unordered_set<idx_t> &needed) {
	if (AsRecyclerOperator(op) || op.children.empty()) {
		return optional_idx();
	}
	// the children through which the rows of the input survive unchanged, in the order they are tried
	idx_t sides[2];
	idx_t side_count = 0;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		if (!op.Cast<LogicalFilter>().projection_map.empty()) {
			return optional_idx();
		}
		return optional_idx(0);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		sides[side_count++] = 0;
		sides[side_count++] = 1;
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		bool delim = op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN;
		if (join.HasProjectionMap() || (delim && op.Cast<LogicalComparisonJoin>().delim_flipped)) {
			return optional_idx();
		}
		switch (join.join_type) {
		case JoinType::INNER:
			sides[side_count++] = 0;
			if (!delim) {
				// the delim side of a delim join depends on the other side's rows
				sides[side_count++] = 1;
			}
			break;
		case JoinType::LEFT:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK:
		case JoinType::SINGLE:
			sides[side_count++] = 0;
			break;
		case JoinType::RIGHT:
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
			sides[side_count++] = 1;
			break;
		default:
			return optional_idx();
		}
		break;
	}
	default:
		return optional_idx();
	}
	if (op.children.size() != 2) {
		return optional_idx();
	}
	for (idx_t s = 0; s < side_count; s++) {
		unordered_set<idx_t> tables;
		LogicalJoin::GetTableReferences(*op.children[sides[s]], tables);
		bool all = true;
		for (auto table : needed) {
			all &= tables.count(table) > 0;
		}
		if (all) {
			return optional_idx(sides[s]);
		}
	}
	return optional_idx();
}

//! Moves the join in `slot` below every operator of its left input that preserves its rows and produces the columns
//! its conditions read; returns the number of operators crossed
idx_t PushDown(unique_ptr<LogicalOperator> &slot) {
	auto &join = slot->Cast<LogicalComparisonJoin>();
	unordered_set<idx_t> needed;
	for (auto &cond : join.conditions) {
		CollectReferences(*cond.left, needed);
	}
	if (needed.empty()) {
		return 0;
	}
	auto target = &join.children[0];
	idx_t crossed = 0;
	while (true) {
		auto side = PreservedChild(**target, needed);
		if (!side.IsValid()) {
			break;
		}
		target = &(*target)->children[side.GetIndex()];
		crossed++;
	}
	if (crossed == 0) {
		return 0;
	}
	// the join takes the place of the operator it stops at, and its old input takes the join's place; every operator
	// in between now sees the mark column among its input's columns, which its parent resolves by binding. Types are
	// left to the next operator that resolves them, as the built-in optimizers do
	auto moved = std::move(slot);
	slot = std::move(moved->children[0]);
	moved->children[0] = std::move(*target);
	*target = std::move(moved);
	return crossed;
}

struct ExistsRewriter {
	explicit ExistsRewriter(RecyclerStats &stats) : stats(stats) {
	}
	RecyclerStats &stats;

	void Walk(unique_ptr<LogicalOperator> &slot, optional_ptr<LogicalOperator> parent) {
		// bottom-up: a nested EXISTS is plain before the one above it is moved past it
		for (auto &child : slot->children) {
			Walk(child, slot.get());
		}
		if (slot->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			return;
		}
		auto &join = slot->Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::MARK) {
			// scalar and lateral subqueries are not candidates
			return;
		}
		auto shape = MatchShape(join);
		if (!shape.ok) {
			stats.exists_refused++;
			return;
		}
		slot = Decorrelate(shape);
		stats.exists_rewrites++;
		// a parent that reads its input by position would see the mark column move to the middle of the columns
		if (!parent || !ConsumesChildrenPositionally(*parent)) {
			if (PushDown(slot) > 0) {
				stats.exists_pushdowns++;
			}
		}
	}
};

} // namespace

void RewriteExistsJoins(unique_ptr<LogicalOperator> &plan, RecyclerStats &stats) {
	ExistsRewriter rewriter(stats);
	rewriter.Walk(plan, nullptr);
}

} // namespace duckdb
