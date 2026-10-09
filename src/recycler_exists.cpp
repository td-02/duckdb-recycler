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
//
// Worked example (benchmark/q0231.sql):
//
//   SELECT ... FROM cast_info ci JOIN title t ON t.id = ci.movie_id JOIN kind_type kt ... JOIN name n ...
//   WHERE ... AND EXISTS (SELECT 1 FROM movie_companies mcf JOIN company_name cnf ON cnf.id = mcf.company_id
//                         WHERE mcf.movie_id = t.id AND cnf.country_code = '[gb]')
//
// The planner hands us DELIM_JOIN(MARK)[t.id IS NOT DISTINCT FROM delim.0] over the five-table join, with the
// subquery's WHERE as FILTER[mcf.movie_id = delim.0, cnf.country_code = '[gb]'] over CROSS_PRODUCT(mcf JOIN cnf,
// DELIM_GET). After this pass it is COMPARISON_JOIN(MARK)[t.id IS NOT DISTINCT FROM p.0] whose right side is
// PROJECTION[p.0 = mcf.movie_id] -> FILTER[cnf.country_code = '[gb]', mcf.movie_id IS NOT NULL] -> (mcf JOIN cnf),
// and PushDown has moved it from above the five-table join to directly above the scan of `title`, the table that
// produces t.id. Filter pushdown makes it a SEMI join and the join order optimizer keeps it there: `title` shrinks
// before it meets the 36 M rows of cast_info (1.2 s -> 0.14 s).
//
// The pieces, in the order they run:
//   ExistsRewriter::Walk   finds every delim join bottom-up and drives the three steps below for each one
//   MatchShape             checks the delim join against the shape above and records its parts (ExistsShape)
//   Decorrelate            builds the plain MARK join from those parts
//   PushDown               moves the new join down to the operator that produces the columns it reads
//                          (helpers: PathToProducer finds the way there, SidePreserved says which steps are safe)
//===----------------------------------------------------------------------===//
#include "recycler_exists.hpp"

#include "recycler_cache.hpp"
#include "recycler_operators.hpp"
#include "recycler_rewrite.hpp"

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
//!
//! A column reference in a bound plan names its producer by (table index, column index); the table index belongs to
//! exactly one operator (a scan, a projection, an aggregate, a DELIM_GET, ...). Collecting the table indexes of an
//! expression therefore says which operators it reads from: the checks below use it to ask "does this expression
//! look at the outer row?", which is "does it name the DELIM_GET's table index?". A reference with depth > 0 points
//! out of the current subquery to an enclosing one; the planner removes those while decorrelating, so one that is
//! left means a correlation the planner kept, and the caller refuses.
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
//!
//! The rewrite only accepts correlations between two plain columns (`mcf.movie_id = t.id`), never between
//! expressions (`CAST(s AS INT) = t.id`, `x + 1 = t.id`): see the file comment for why an expression on the subquery
//! side could make a query fail that the planner's own plan answers. This is the test for "plain column".
optional_ptr<const BoundColumnRefExpression> AsColumn(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &ref = expr.Cast<BoundColumnRefExpression>();
	return ref.depth == 0 ? &ref : nullptr;
}

//! What a subtree refers to and contains, for the checks on the subquery's own plan
//!
//! MatchShape runs it over the subquery's FROM part (the `inner` side of the cross product with the DELIM_GET) to
//! prove that the subquery reaches the outer row only through the WHERE conjuncts it already examined. Any of the
//! flags below makes the delim join ineligible.
struct SubtreeScan {
	//! Table indexes named by any expression of any operator in the subtree (filters, join conditions, projections,
	//! aggregates, ...); the DELIM_GET's index among them means the outer row is read somewhere else
	unordered_set<idx_t> referenced;
	//! Some expression still holds a reference to an enclosing query (depth > 0)
	bool correlated = false;
	//! The subtree holds a delim join or a DELIM_GET of its own: a nested correlated subquery, left to the planner
	bool delim = false;
	//! The subtree holds one of the recycler's operators (a block marker or a cached-result scan planted by the pre
	//! hook); moving a filter onto it is unsafe, see the file comment
	bool recycler = false;

	//! Visits `op` and everything below it, filling the fields above
	void Walk(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			delim = true;
		}
		if (AsRecyclerOperator(op)) {
			recycler = true;
		}
		// every expression the operator owns: filter conjuncts, join conditions, projected and grouped expressions
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

//! One correlated conjunct of the subquery's WHERE clause: a delim column (a copy of an outer column) compared with a
//! column of the subquery, e.g. `mcf.movie_id = delim.0` where delim.0 stands for t.id
struct Correlation {
	//! Position of the conjunct in the subquery FILTER's expression list
	idx_t conjunct;
	//! Which delim column it compares, i.e. which entry of the delim join's duplicate_eliminated_columns (the outer
	//! column `c` it is a copy of)
	idx_t outer;
	//! Whether the delim column is the comparison's left operand (`delim.0 = mcf.movie_id`) or its right one
	bool delim_is_left;
	//! COMPARE_EQUAL (`=`, never true for a NULL) or COMPARE_NOT_DISTINCT_FROM (NULL matches NULL); Decorrelate adds
	//! an IS NOT NULL on the subquery column for the former so that both keep their meaning in the new join
	ExpressionType comparison;
};

//! The parts of a delim join of the shape in the file comment, as MatchShape found them; `ok` only when every check
//! passed. The pointers point into the plan and are only valid until Decorrelate consumes them.
struct ExistsShape {
	bool ok = false;
	//! The delim join itself (left child: the outer FROM; right child: the subquery)
	LogicalComparisonJoin *join = nullptr;
	//! The subquery's top: its select list followed by the n delim columns the planner appends
	LogicalProjection *projection = nullptr;
	//! The subquery's WHERE clause, one conjunct per expression
	LogicalFilter *filter = nullptr;
	//! CROSS_PRODUCT(inner, DELIM_GET): the subquery's FROM part joined with the deduplicated outer values
	LogicalOperator *cross = nullptr;
	//! Which child of `cross` is the subquery's FROM part (the other one is the DELIM_GET)
	idx_t inner_side = 0;
	//! The WHERE conjuncts that compare a delim column with a subquery column: they become the join's conditions
	vector<Correlation> correlations;
	//! The WHERE conjuncts that do not look at the outer row (`cnf.country_code = '[gb]'`): they stay in the subquery
	vector<idx_t> plain_conjuncts;
};

//! Checks whether the delim join `join` has exactly the shape the rewrite handles, and returns its parts when it does
//!
//! Every check mirrors how the planner builds a correlated EXISTS (FlattenDependentJoins in DuckDB's
//! planner/subquery/flatten_dependent_join.cpp) so that anything built differently, or carrying more meaning than the
//! rewrite can carry over, is refused rather than guessed at. The plan is not modified here.
ExistsShape MatchShape(LogicalComparisonJoin &join) {
	ExistsShape shape;
	// n = number of correlated outer columns (one for `mcf.movie_id = t.id`). The planner gives the delim join one
	// condition per correlated column and nothing else; an extra condition (IN / ANY), a residual predicate, a
	// flipped delim side or projection maps (set only by later optimizer passes) mean a join we do not handle
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
	// one child of the cross product is the DELIM_GET (the deduplicated outer values), the other the subquery's FROM
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
	// the table index under which the subquery reads the outer values: any reference to it is a look at the outer row
	auto delim_index = delim_get.table_index;
	// the delim conditions as the planner makes them: c_i IS NOT DISTINCT FROM the projection's trailing delim column
	// i. The projection is [subquery select list (prefix entries), delim.0, ..., delim.n-1]; the delim join matches
	// each outer row's c_i against those trailing columns. Verifying this exact wiring proves that delim.i is a copy of
	// c_i, which is what lets Decorrelate replace delim.i by c_i in the new join condition
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
	// the rest of the select list must not look at the outer row (`EXISTS (SELECT t.id FROM ...)`): the select list of
	// an EXISTS is never read, but it is kept in the new projection, which no longer has the outer row to read from
	for (idx_t k = 0; k < prefix; k++) {
		unordered_set<idx_t> refs;
		if (!CollectReferences(*projection.expressions[k], refs) || refs.count(delim_index)) {
			return shape;
		}
	}
	// the WHERE clause: every conjunct over the outer row is `delim column = inner column` or IS NOT DISTINCT FROM.
	// Each conjunct is sorted into one of two lists: plain (does not look at the outer row, stays in the subquery) or
	// correlation (becomes a join condition). A conjunct that looks at the outer row in any other way (`<`, OR, a
	// function of the outer column) refuses the whole delim join
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
		// it looks at the outer row: it must be `column = column` or `column IS NOT DISTINCT FROM column`
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
		// exactly one side is a delim column and the other a column of the subquery (`delim.0 = delim.0` or a
		// comparison of two subquery columns that happens to mention the delim table elsewhere are refused)
		bool delim_is_left = left->binding.table_index == delim_index;
		auto &delim_column = delim_is_left ? *left : *right;
		auto &inner_column = delim_is_left ? *right : *left;
		if (delim_column.binding.table_index != delim_index || inner_column.binding.table_index == delim_index ||
		    delim_column.binding.column_index >= n) {
			return shape;
		}
		shape.correlations.push_back(Correlation {j, delim_column.binding.column_index, delim_is_left, type});
	}
	// a delim join without a correlated WHERE conjunct correlates somewhere we do not look (a join condition inside
	// the subquery, an aggregate); nothing to rewrite
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
//!
//! The result is COMPARISON_JOIN(MARK)[c_j IS NOT DISTINCT FROM p_j] with the old outer FROM on the left and
//! PROJECTION[select list, p_j] -> FILTER[plain conjuncts, k_j IS NOT NULL] -> inner on the right. The expressions
//! and operators are moved out of the old plan, not copied; the old delim join, projection, filter, cross product and
//! DELIM_GET are destroyed when the caller overwrites its slot with the result. The new join keeps the old join's
//! mark_index and the new projection the old projection's table_index, so every operator above that reads the mark
//! column (the WHERE filter) or a subquery column still finds it under the same binding.
unique_ptr<LogicalOperator> Decorrelate(ExistsShape &shape) {
	auto &join = *shape.join;
	auto &projection = *shape.projection;
	auto &filter = *shape.filter;
	auto prefix = projection.expressions.size() - join.duplicate_eliminated_columns.size();

	// the subquery's own select list, without the trailing delim columns (the new projection appends p_j instead)
	vector<unique_ptr<Expression>> select_list;
	for (idx_t k = 0; k < prefix; k++) {
		select_list.push_back(std::move(projection.expressions[k]));
	}
	// the subquery's WHERE conjuncts that do not look at the outer row stay a filter on the subquery
	vector<unique_ptr<Expression>> conjuncts;
	for (auto j : shape.plain_conjuncts) {
		conjuncts.push_back(std::move(filter.expressions[j]));
	}
	// each correlated conjunct `k_j = delim.i` becomes one join condition `c_i IS NOT DISTINCT FROM p_j`, where p_j
	// is k_j exposed as a new column of the subquery's projection
	vector<JoinCondition> conditions;
	for (auto &correlation : shape.correlations) {
		auto &comparison = filter.expressions[correlation.conjunct]->Cast<BoundComparisonExpression>();
		auto inner_column = std::move(correlation.delim_is_left ? comparison.right : comparison.left);
		// `=` never matches a NULL k_j, IS NOT DISTINCT FROM would: dropping NULL k_j first gives the same matches
		if (correlation.comparison == ExpressionType::COMPARE_EQUAL) {
			auto not_null =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			not_null->children.push_back(inner_column->Copy());
			conjuncts.push_back(std::move(not_null));
		}
		// left: the outer column the delim column was a copy of (t.id); right: a reference to the projection column
		// p_j that is about to be appended (its index is the select list's current size)
		JoinCondition cond;
		cond.left = join.duplicate_eliminated_columns[correlation.outer]->Copy();
		cond.right = make_uniq<BoundColumnRefExpression>(inner_column->return_type,
		                                                 ColumnBinding(projection.table_index, select_list.size()));
		cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		select_list.push_back(std::move(inner_column));
		conditions.push_back(std::move(cond));
	}

	// assemble the new right side bottom-up: the subquery's FROM part (taken out of the cross product, which drops the
	// DELIM_GET), the remaining filter if there is one, then the projection
	unique_ptr<LogicalOperator> rhs = std::move(shape.cross->children[shape.inner_side]);
	if (!conjuncts.empty()) {
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(conjuncts);
		new_filter->children.push_back(std::move(rhs));
		rhs = std::move(new_filter);
	}
	auto new_projection = make_uniq<LogicalProjection>(projection.table_index, std::move(select_list));
	new_projection->children.push_back(std::move(rhs));

	// the plain MARK join: for each outer row, a boolean "some subquery row matches" under the old mark_index; filter
	// pushdown later turns it into a SEMI join (WHERE EXISTS) or an ANTI join (WHERE NOT EXISTS)
	auto result = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
	result->mark_index = join.mark_index;
	result->conditions = std::move(conditions);
	result->children.push_back(std::move(join.children[0]));
	result->children.push_back(std::move(new_projection));
	return std::move(result);
}

//! Whether every row of the input of `op` survives, with its columns, in `op`'s output when it comes from child `side`
//!
//! This is the safety test for one step of PushDown. Moving a MARK join from above `op` to below it, onto child
//! `side`, is only equivalent when `op` neither invents rows for that side (the null-supplying side of an outer join
//! does: its NULL rows never went through the moved join) nor loses the mark column on the way up (it must pass the
//! child's columns through: no projection, no projection map, no aggregate). Filters and inner joins only drop or
//! repeat rows by their own values, so a row's mark is the same whether it was computed before or after them.
bool SidePreserved(LogicalOperator &op, idx_t side) {
	if (AsRecyclerOperator(op) || side >= op.children.size()) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return op.Cast<LogicalFilter>().projection_map.empty();
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return op.children.size() == 2;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		bool delim = op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN;
		if (op.children.size() != 2 || join.HasProjectionMap() ||
		    (delim && op.Cast<LogicalComparisonJoin>().delim_flipped)) {
			return false;
		}
		switch (join.join_type) {
		case JoinType::INNER:
			// both sides keep their rows (each joined row carries both); the delim side of a delim join depends on the
			// other side's rows, so only its left side qualifies
			return side == 0 || !delim;
		// the preserved (left) side of a left outer, semi, anti, mark or single join keeps every row and its columns
		case JoinType::LEFT:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK:
		case JoinType::SINGLE:
			return side == 0;
		// their mirror images keep the right side; FULL OUTER keeps neither (both sides get padded with NULLs)
		case JoinType::RIGHT:
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
			return side == 1;
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

//! Appends to `path` the child indexes from `op` down to the operator whose table index is `table_index`; false when
//! no operator below produces it (one walk of the subtree)
//!
//! For q0231, with `op` the five-table join and `table_index` the one of the `title` scan, the path is the sequence
//! of "take child 0 / take child 1" choices that leads from the top join to that scan. PushDown descends along it.
bool PathToProducer(LogicalOperator &op, idx_t table_index, vector<idx_t> &path) {
	for (auto idx : op.GetTableIndex()) {
		if (idx == table_index) {
			return true;
		}
	}
	for (idx_t i = 0; i < op.children.size(); i++) {
		path.push_back(i);
		if (PathToProducer(*op.children[i], table_index, path)) {
			return true;
		}
		path.pop_back();
	}
	return false;
}

//! Moves the join in `slot` below every operator of its left input that preserves its rows and produces the columns
//! its conditions read; returns the number of operators crossed
//!
//! Why: DuckDB's join order optimizer can place the new SEMI/ANTI join anywhere inside a tree of inner joins, but it
//! stops at operators it does not reorder (outer joins, for one). In the pages family (q0230, q0292, ...) two LEFT
//! JOINs sit between the WHERE clause and the inner joins, so without this step the semi join would stay above them
//! and run on the fully joined rows. Moving it as deep as is safe hands the optimizer a tree it can work on; the
//! optimizer may still decide to put it higher again if that is cheaper.
//!
//! How: the join reads the outer columns named in its conditions (t.id). Starting at the join's left child, follow
//! the path to the operator(s) producing those columns for as long as all of them lie in the same child and every
//! operator on the way passes that child's rows through (SidePreserved), then re-link the plan so the join sits on
//! the last operator reached.
idx_t PushDown(unique_ptr<LogicalOperator> &slot) {
	auto &join = slot->Cast<LogicalComparisonJoin>();
	// the table indexes of the outer columns the join conditions read
	unordered_set<idx_t> needed;
	for (auto &cond : join.conditions) {
		CollectReferences(*cond.left, needed);
	}
	if (needed.empty()) {
		return 0;
	}
	// the columns the join reads flow up from their producers, so the join may follow the path shared by every
	// producer as long as each operator on it passes the rows of that child through
	vector<vector<idx_t>> paths;
	for (auto table : needed) {
		vector<idx_t> path;
		if (!PathToProducer(*join.children[0], table, path)) {
			return 0;
		}
		paths.push_back(std::move(path));
	}
	// walk down: `target` is the slot the join would be inserted into; `crossed` is how many operators it has passed
	auto target = &join.children[0];
	idx_t crossed = 0;
	while (true) {
		// all producers must lie below the same child, else the join needs columns from both sides of `target`
		bool agree = true;
		for (auto &path : paths) {
			agree &= crossed < path.size() && path[crossed] == paths[0][crossed];
		}
		if (!agree || !SidePreserved(**target, paths[0][crossed])) {
			break;
		}
		target = &(*target)->children[paths[0][crossed]];
		crossed++;
	}
	if (crossed == 0) {
		return 0;
	}
	// the join takes the place of the operator it stops at, and its old input takes the join's place; every operator
	// in between now sees the mark column among its input's columns, which its parent resolves by binding. Types are
	// left to the next operator that resolves them, as the built-in optimizers do.
	//
	//   before:  slot -> JOIN(left = A -> B -> ... -> T, right = subquery)
	//   after:   slot -> A -> B -> ... -> JOIN(left = T, right = subquery)
	auto moved = std::move(slot);
	slot = std::move(moved->children[0]);
	moved->children[0] = std::move(*target);
	*target = std::move(moved);
	return crossed;
}

//! Drives the pass over one plan: finds every delim join and, for each eligible one, runs MatchShape, Decorrelate
//! and PushDown, counting what it did in the cache's statistics (visible in recycler_stats())
struct ExistsRewriter {
	explicit ExistsRewriter(RecyclerStats &stats) : stats(stats) {
	}
	//! exists_rewrites / exists_pushdowns / exists_refused; shared by every connection, hence atomics
	RecyclerStats &stats;

	//! Rewrites the subtree in `slot`; `parent` is the operator that owns `slot` (null for the plan's root)
	void Walk(unique_ptr<LogicalOperator> &slot, optional_ptr<LogicalOperator> parent) {
		// bottom-up: a nested EXISTS is plain before the one above it is moved past it
		for (auto &child : slot->children) {
			Walk(child, slot.get());
		}
		if (slot->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			return;
		}
		auto &join = slot->Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::MARK || join.conditions.size() != join.duplicate_eliminated_columns.size()) {
			// scalar and lateral subqueries, and IN / ANY (which carry the compared expression as one more
			// condition), are not candidates
			return;
		}
		auto shape = MatchShape(join);
		if (!shape.ok) {
			// an EXISTS the rule declined: the planner's delim join stays as it is
			stats.exists_refused++;
			return;
		}
		// replacing the slot's content destroys the old delim join and everything Decorrelate did not move out of it
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

//! Entry point, called by the pre-optimize hook (RecyclerPreOptimize in recycler_hooks.cpp) after the cache's own
//! work on the statement and before DuckDB's built-in optimizers run
void RewriteExistsJoins(unique_ptr<LogicalOperator> &plan, RecyclerStats &stats) {
	ExistsRewriter rewriter(stats);
	rewriter.Walk(plan, nullptr);
}

} // namespace duckdb
