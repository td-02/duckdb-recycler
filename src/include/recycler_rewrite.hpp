#pragma once
//===----------------------------------------------------------------------===//
// recycler_rewrite.hpp - plan rewrites shared by the pre- and post-optimizer passes
//===----------------------------------------------------------------------===//
#include "recycler_operators.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/column_binding_map.hpp"

#include <unordered_set>

namespace duckdb {

class Binder;
class ClientContext;
class PlanSignatures;

using binding_set_t = std::unordered_set<ColumnBinding, ColumnBindingHashFunction, ColumnBindingEquality>;

//! Appends every column binding `expr` references
void CollectBindings(const Expression &expr, vector<ColumnBinding> &out);

//! Replace the operator in `slot`, which scans `tables`, by a scan over `entry`, recording the binding
//! replacements. False when the entry's columns do not match the operator's output.
bool SubstituteScan(unique_ptr<LogicalOperator> &slot, shared_ptr<CachedEntry> entry, bool ordered,
                    const vector<reference<const TableCatalogEntry>> &tables, Binder &binder,
                    ColumnBindingReplacer &replacer);

//! SubstituteScan for an entry whose columns are the operator's, stored in another order: `stored[j]` is the stored
//! column that is output column j of the operator. The scan emits the stored columns as they are, and a projection
//! above it restores the operator's order. False when the types do not match.
bool SubstituteReorderedScan(unique_ptr<LogicalOperator> &slot, shared_ptr<CachedEntry> entry, bool ordered,
                             const vector<idx_t> &stored, const vector<reference<const TableCatalogEntry>> &tables,
                             Binder &binder, ColumnBindingReplacer &replacer);

//! Wrap the operator in `slot` in a RecyclerMaterialize that writes `spec`. `keep_alive` are column references the
//! wrapper carries so that column pruning keeps those columns in the wrapped subtree's output even when nothing
//! above the wrapper needs them.
void WrapMaterialize(unique_ptr<LogicalOperator> &slot, RecyclerWriteSpec spec, shared_ptr<RecyclerCache> cache,
                     vector<unique_ptr<Expression>> keep_alive = vector<unique_ptr<Expression>>());

//! Remove from `owners`, joins outside `subtree`, the runtime filters they push into the scans of `subtree`, so that a
//! write of the subtree stores all of its rows. A runtime filter only drops probe rows its join drops anyway, so no
//! result changes. Returns the number of filters removed.
idx_t StripRuntimeFilters(const LogicalOperator &subtree, const vector<LogicalOperator *> &owners);

//! Operators that consume their child's output by position and rebind it: what they need from the child appears in
//! no expression of theirs, so a reference collector never sees it, and a rewrite below them must keep the child's
//! column order
bool ConsumesChildrenPositionally(const LogicalOperator &op);

//! The consumer side of a CTE, or nullptr when `op` is not a CTE. CTE inlining deep-copies a CTE's definition and
//! cannot copy our operators, so the pre-optimize walkers rewrite the consumer side only.
unique_ptr<LogicalOperator> *CteConsumerSlot(LogicalOperator &op);

//! Resolve the stored columns of every planted block marker below `slot` from its (pruned and reordered) child;
//! a marker whose child produces no plain base column is removed again
void ResolveBlockColumns(unique_ptr<LogicalOperator> &slot);

//! What every pass over a plan works with, and the binding replacements its substitutions record: a substituted
//! subtree gets new bindings, which the operators above it learn once the pass is done (ApplyReplacements)
struct PlanPass {
	PlanPass(ClientContext &context, Binder &binder, shared_ptr<RecyclerCache> cache, const RecyclerSettings &settings,
	         PlanSignatures &sig, const TableSnapshot &snapshot)
	    : context(context), binder(binder), cache(std::move(cache)), settings(settings), sig(sig), snapshot(snapshot) {
	}

	ClientContext &context;
	Binder &binder;
	shared_ptr<RecyclerCache> cache;
	const RecyclerSettings &settings;
	PlanSignatures &sig;
	const TableSnapshot &snapshot;
	ColumnBindingReplacer replacer;
	//! Some subtree was substituted, so `replacer` holds replacements
	bool replaced = false;

	//! Rebinds every operator of `plan` that reads a substituted subtree
	void ApplyReplacements(LogicalOperator &plan) {
		if (replaced) {
			replacer.VisitOperator(plan);
		}
	}
};

//! Visits the bound plan top-down, as the pre-optimize passes do: the recycler's own operators are skipped, a CTE is
//! entered through its consumer side only (CteConsumerSlot), and at every other operator `visit(slot)` decides; when
//! it returns true it handled the slot, and the subtree below is not walked
template <class VISIT>
void WalkBoundPlan(unique_ptr<LogicalOperator> &slot, VISIT &&visit) {
	auto &op = *slot;
	if (AsRecyclerOperator(op)) {
		return;
	}
	auto consumer = CteConsumerSlot(op);
	if (consumer) {
		WalkBoundPlan(*consumer, visit);
		return;
	}
	if (visit(slot)) {
		return;
	}
	for (auto &child : op.children) {
		WalkBoundPlan(child, visit);
	}
}

} // namespace duckdb
