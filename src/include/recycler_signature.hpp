#pragma once
//===----------------------------------------------------------------------===//
// recycler_signature.hpp
//
// Canonical structural signatures of logical plan subtrees.
//
// Two plans that are the same query (or contain the same sub-query) get
// different per-query table indexes from the binder, so nothing that DuckDB
// offers out of the box (Expression::Hash, ToString, the binary serializer)
// is a usable cache key. This module renumbers every table index in
// pre-order, renders every operator and expression field that influences the
// result into a byte string, and hashes it compositionally so that one
// bottom-up pass yields a context-free 128-bit key for every subtree.
//
// The renderer is an allowlist: any operator or expression class it does not
// understand marks the subtree (and all of its ancestors) as not cacheable.
//
// The same renderer has a "shape" mode for SPJ blocks (a FILTER over joins
// and scans on the bound plan): column references are rendered by physical
// column id instead of position, scans do not list their columns, and range
// predicates on base columns are lifted out of the block-root filter as
// parameters. Two blocks with the same shape can share a result when one's
// parameters imply the other's (predicate subsumption) and the needed columns
// are present (column subsumption).
//===----------------------------------------------------------------------===//
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

class ClientContext;
class Expression;
class LogicalComparisonJoin;
class LogicalGet;

//! 128-bit compositional hash of a subtree
struct SigHash {
	uint64_t h1 = 0;
	uint64_t h2 = 0;

	bool operator==(const SigHash &other) const {
		return h1 == other.h1 && h2 == other.h2;
	}
	string ToString() const;
	//! Parse the output of ToString (as shown by EXPLAIN and the profiler); false for anything else
	static bool TryParse(const string &text, SigHash &result);
};

struct SigHashHasher {
	size_t operator()(const SigHash &h) const {
		return static_cast<size_t>(h.h1 ^ (h.h2 * 0x9E3779B97F4A7C15ULL));
	}
};

//! Why a subtree is not cacheable; counted per query root in recycler_stats() and kept in the signature history
enum class RefusalReason : uint8_t {
	NONE = 0,
	TABLE_FUNCTION,
	SAMPLED_SCAN,
	SCAN_OPTIONS,
	CORRELATED_COLUMN,
	VOLATILE_FUNCTION,
	PARAMETER,
	EXPRESSION,
	OPERATOR,
	CONSTANT_COLLECTION,
	EXTERNAL_REFERENCE,
	DELIM_GET,
	LIMIT_WITHOUT_ORDER,
	RUNTIME_FILTER,
	TEMPORARY_TABLE,
	FOREIGN_CATALOG,
	COUNT
};

const char *RefusalReasonName(RefusalReason reason);

//! Per-node analysis results
struct NodeInfo {
	//! Compositional hash of the subtree rooted at this node (valid iff !refused && !external_ref)
	SigHash hash;
	//! First canonical table-index id of the subtree and number of ids in it
	idx_t start = 0;
	idx_t count = 0;
	//! The subtree contains an operator/expression the renderer does not support (volatile functions,
	//! parameters, table functions, unknown operator types, ...); `refusal` is the first one met
	bool refused = false;
	RefusalReason refusal = RefusalReason::NONE;
	//! The subtree references a column binding that is produced outside of it
	bool external_ref = false;
	//! The subtree contains a LogicalDelimGet whose owning delim join is not inside the subtree
	bool unresolved_delim_get = false;
	//! Number of delim gets / delim joins in the subtree (used to resolve delim get ownership)
	idx_t delim_gets = 0;
	idx_t delim_joins = 0;
	//! The subtree's output depends on execution order (LIMIT without ORDER BY, SAMPLE)
	bool nondeterministic = false;
	//! The subtree's output has a defined order (ORDER BY / TOP N at the top, possibly under projections)
	bool ordered = false;
	//! Dynamic (runtime) filters flow into scans of this subtree from operators outside of it
	bool runtime_filtered = false;
	//! Sum of the estimated cardinalities of all base-table scans in the subtree
	idx_t est_leaf_rows = 0;
	//! The subtree contains a join whose cardinality estimate is known to be unreliable (non-equality conditions,
	//! cross products); nothing above it is materialized on first sight
	bool unreliable_estimate = false;
	//! Owners of dynamic filter sets that some scan in this subtree depends on but which are not in the subtree
	vector<const LogicalOperator *> pending_dynamic_owners;

	bool Cacheable() const {
		return !refused && !external_ref && !unresolved_delim_get && !nondeterministic && !runtime_filtered &&
		       pending_dynamic_owners.empty();
	}
	//! Why the subtree is not cacheable (NONE when it is)
	RefusalReason Reason() const;
};

//! A base-table column: block-relative canonical table id plus the physical column id in that table
struct ColumnIdentity {
	idx_t table = DConstants::INVALID_INDEX;
	idx_t column = DConstants::INVALID_INDEX;

	bool IsValid() const {
		return table != DConstants::INVALID_INDEX;
	}
	bool operator==(const ColumnIdentity &other) const {
		return table == other.table && column == other.column;
	}
};

//! A block-root conjunct of the form `column <op> constant` or `column IS NOT NULL` over a base column
struct RangeConstraint {
	ColumnIdentity column;
	//! COMPARE_EQUAL / COMPARE_GREATERTHAN / COMPARE_GREATERTHANOREQUALTO / COMPARE_LESSTHAN /
	//! COMPARE_LESSTHANOREQUALTO or OPERATOR_IS_NOT_NULL
	ExpressionType op = ExpressionType::INVALID;
	//! The constant, cast to the column's type (NULL for IS NOT NULL)
	Value value;
};

//! Normal form of an SPJ block for subsumption matching
struct BlockShape {
	bool ok = false;
	SigHash hash;
	//! Structure without the parameterized conjuncts, columns rendered by physical id
	string canonical;
	//! Canonical rendering of every block-root conjunct, index-aligned with the FILTER's expressions
	vector<string> conjuncts;
	vector<bool> is_constraint;
	vector<RangeConstraint> constraints;
	//! table_index -> block-relative canonical id
	std::unordered_map<idx_t, idx_t> table_ids;
	//! Identity of each output binding of the block root (invalid when it is not a plain base column)
	vector<ColumnIdentity> output;
	idx_t est_leaf_rows = 0;
	idx_t joins = 0;
};

//! Analyzes a plan once and exposes per-node signatures
class PlanSignatures {
public:
	//! Rendering of the session settings that change a result without changing the plan (see the definition); the
	//! pre hook reads it once per statement and both hooks analyze under it
	static string SettingsSalt(const ClientContext &context);
	//! Every signature of this analysis carries salt
	PlanSignatures(ClientContext &context, string salt);

	//! Analyze the plan rooted at root (assigns canonical ids, computes NodeInfo for every node)
	void Analyze(LogicalOperator &root);
	//! Get the analysis result of a node (must be part of the analyzed plan)
	const NodeInfo &Get(const LogicalOperator &op) const;
	//! Render the full canonical byte string of the subtree rooted at op (for storage / hit confirmation)
	string Render(const LogicalOperator &op) const;

	class Sink;
	class HashSink;
	class StringSink;

private:
	friend BlockShape ComputeBlockShape(ClientContext &context, const LogicalOperator &root,
	                                    const PlanSignatures &plan_info);

	struct Frame {
		idx_t start;
		idx_t count;
	};

	PlanSignatures(ClientContext &context, string salt, bool shape_mode);

	void AssignIds(const LogicalOperator &op);
	void Compute(LogicalOperator &op);
	void RenderNode(const LogicalOperator &op, const Frame &frame, Sink &sink, NodeInfo &info,
	                bool render_children) const;
	void RenderExpression(const Expression &expr, const Frame &frame, Sink &sink, NodeInfo &info) const;
	void RenderBinding(ColumnBinding binding, const Frame &frame, Sink &sink, NodeInfo &info) const;
	void RenderOwnIndex(idx_t table_index, const Frame &frame, Sink &sink) const;
	void RenderJoin(const LogicalComparisonJoin &join, const Frame &frame, Sink &sink, NodeInfo &info) const;
	void CollectDynamicOwners(const LogicalOperator &op);
	//! Identity of a binding that refers to a scan of the analyzed (sub)tree
	ColumnIdentity IdentityOf(ColumnBinding binding) const;
	//! Recognize `column <op> constant` / `column IS NOT NULL` conjuncts over base columns
	bool TryParseConstraint(const Expression &expr, vector<RangeConstraint> &out) const;

private:
	ClientContext &context;
	//! Shape mode: block normal form (see file comment)
	const bool shape_mode;
	//! Written at the start of every node, in the hash and in the stored rendering
	const string salt;
	const LogicalOperator *block_root = nullptr;
	//! Sorted canonical strings of the block root's non-parameterized conjuncts
	vector<string> root_conjuncts;
	//! table_index -> canonical id
	std::unordered_map<idx_t, idx_t> ids;
	idx_t next_id = 0;
	//! table_index -> scan (shape mode: to render column references by physical id)
	std::unordered_map<idx_t, const LogicalGet *> gets;
	idx_t joins = 0;
	//! node -> info
	std::unordered_map<const LogicalOperator *, NodeInfo> infos;
	//! DynamicTableFilterSet* -> owning join operator
	std::unordered_map<const void *, const LogicalOperator *> dynamic_owners;
};

//! Compute the shape of the SPJ block rooted at `root` (a LOGICAL_FILTER on the bound plan); `plan_info` is the
//! normal-mode analysis of the plan containing it. Returns ok=false when the block is not self-contained, has no
//! joins, or contains anything the renderer refuses.
BlockShape ComputeBlockShape(ClientContext &context, const LogicalOperator &root, const PlanSignatures &plan_info);

//! True when every cached constraint is implied by the query's constraints, i.e. every row that satisfies the query's
//! predicates also satisfies the cached ones (so the query's rows are a subset of the cached rows)
bool RangeConstraintsImply(const vector<RangeConstraint> &query, const vector<RangeConstraint> &cached);

//! Estimated bytes per row for a type list (fixed widths, allowance for strings)
idx_t RecyclerEstimateRowWidth(const vector<LogicalType> &types);

} // namespace duckdb
