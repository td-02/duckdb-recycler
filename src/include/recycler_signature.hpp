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
//===----------------------------------------------------------------------===//
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

class ClientContext;
class Expression;
class LogicalComparisonJoin;

//! 128-bit compositional hash of a subtree
struct SigHash {
	uint64_t h1 = 0;
	uint64_t h2 = 0;

	bool operator==(const SigHash &other) const {
		return h1 == other.h1 && h2 == other.h2;
	}
	string ToString() const;
};

struct SigHashHasher {
	size_t operator()(const SigHash &h) const {
		return static_cast<size_t>(h.h1 ^ (h.h2 * 0x9E3779B97F4A7C15ULL));
	}
};

//! Per-node analysis results
struct NodeInfo {
	//! Compositional hash of the subtree rooted at this node (valid iff !refused && !external_ref)
	SigHash hash;
	//! First canonical table-index id of the subtree and number of ids in it
	idx_t start = 0;
	idx_t count = 0;
	//! The subtree contains an operator/expression the renderer does not support (volatile functions,
	//! parameters, table functions, unknown operator types, ...)
	bool refused = false;
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
};

//! Analyzes a plan once and exposes per-node signatures
class PlanSignatures {
public:
	explicit PlanSignatures(ClientContext &context);

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
	struct Frame {
		idx_t start;
		idx_t count;
	};

	void AssignIds(LogicalOperator &op);
	void Compute(LogicalOperator &op);
	void RenderNode(const LogicalOperator &op, const Frame &frame, Sink &sink, NodeInfo &info,
	                bool render_children) const;
	void RenderExpression(const Expression &expr, const Frame &frame, Sink &sink, NodeInfo &info) const;
	void RenderBinding(ColumnBinding binding, const Frame &frame, Sink &sink, NodeInfo &info) const;
	void RenderOwnIndex(idx_t table_index, const Frame &frame, Sink &sink) const;
	void RenderJoin(const LogicalComparisonJoin &join, const Frame &frame, Sink &sink, NodeInfo &info) const;
	void CollectDynamicOwners(const LogicalOperator &op);

private:
	ClientContext &context;
	//! table_index -> canonical id
	std::unordered_map<idx_t, idx_t> ids;
	idx_t next_id = 0;
	//! node -> info
	std::unordered_map<const LogicalOperator *, NodeInfo> infos;
	//! DynamicTableFilterSet* -> owning join operator
	std::unordered_map<const void *, const LogicalOperator *> dynamic_owners;
};

//! Estimated bytes per row for a type list (fixed widths, allowance for strings)
idx_t RecyclerEstimateRowWidth(const vector<LogicalType> &types);

} // namespace duckdb
