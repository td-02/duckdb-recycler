//===----------------------------------------------------------------------===//
// recycler_subsumption.cpp - predicate / column subsumption on SPJ blocks (M5)
//
// Pre hook: an SPJ block narrower than a cached block of the same shape is replaced by a scan of the cached rows
// plus the residual predicates; an admissible miss gets a block marker that writes the block while it runs.
//===----------------------------------------------------------------------===//
#include "recycler_subsumption.hpp"
#include "recycler_operators.hpp"
#include "recycler_policy.hpp"
#include "recycler_rewrite.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include <algorithm>
#include <unordered_set>

namespace duckdb {

//! Collects every column binding referenced by operators outside of `block`
class ExternalReferenceCollector : public LogicalOperatorVisitor {
public:
	ExternalReferenceCollector(const LogicalOperator &block, const vector<ColumnBinding> &block_bindings)
	    : block(block), block_bindings(block_bindings.begin(), block_bindings.end()) {
	}

	void VisitOperator(LogicalOperator &op) override {
		if (&op == &block) {
			return;
		}
		VisitOperatorExpressions(op);
		if (ConsumesChildrenPositionally(op)) {
			// the block's own columns still reach this operator (nothing in between rebound them), and it reads
			// them by position without naming them in an expression
			for (auto &child : op.children) {
				for (auto &binding : child->GetColumnBindings()) {
					if (block_bindings.find(binding) != block_bindings.end()) {
						opaque_parent = true;
					}
				}
			}
		}
		VisitOperatorChildren(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		references.insert(expr.binding);
		return nullptr;
	}

	const LogicalOperator &block;
	binding_set_t block_bindings;
	binding_set_t references;
	//! An operator above consumes the block's columns by position: `references` does not describe what the rest of
	//! the query needs from the block, so no column may be dropped on its word
	bool opaque_parent = false;
};

struct SubsumptionPass : PlanPass {
	SubsumptionPass(ClientContext &context, Binder &binder, shared_ptr<RecyclerCache> cache,
	                const RecyclerSettings &settings, PlanSignatures &sig, const TableSnapshot &snapshot,
	                LogicalOperator &plan)
	    : PlanPass(context, binder, std::move(cache), settings, sig, snapshot), plan(plan) {
	}

	//! The whole plan, whose references outside a block decide which of the block's columns a hit must provide
	LogicalOperator &plan;

	//! At an SPJ block (a cacheable FILTER whose shape ComputeBlockShape recognizes): serve it from a wider cached
	//! block, or plant a marker on an admissible miss. True when the slot was handled.
	bool Visit(unique_ptr<LogicalOperator> &slot) {
		auto &op = *slot;
		if (op.type != LogicalOperatorType::LOGICAL_FILTER || !sig.Get(op).Cacheable()) {
			return false;
		}
		auto shape = ComputeBlockShape(context, op, sig);
		if (!shape.ok) {
			return false;
		}
		auto &tables = sig.Get(op).tables;
		if (TryHit(slot, shape, tables)) {
			replaced = true;
			return true;
		}
		if (AdmitBlockWrite(*cache, settings, shape) && cache->Admissible(tables, snapshot)) {
			Plant(slot, shape);
			return true;
		}
		return false;
	}

	void Plant(unique_ptr<LogicalOperator> &slot, BlockShape &shape) {
		// a narrower consumer re-applies its predicates on the stored rows, so the constrained columns must be part
		// of the stored block even when the payer itself only uses them in its WHERE clause
		vector<unique_ptr<Expression>> keep_alive;
		{
			auto bindings = slot->GetColumnBindings();
			auto &types = slot->types;
			vector<idx_t> kept;
			for (auto &constraint : shape.constraints) {
				for (idx_t pos = 0; pos < shape.output.size(); pos++) {
					if (shape.output[pos] == constraint.column &&
					    std::find(kept.begin(), kept.end(), pos) == kept.end() && pos < types.size()) {
						kept.push_back(pos);
						keep_alive.push_back(make_uniq<BoundColumnRefExpression>(types[pos], bindings[pos]));
					}
				}
			}
		}
		auto spec =
		    RecyclerWriteSpec::Make(RecyclerEntryKind::BLOCK, shape.hash, std::move(shape.canonical), false,
		                            shape.est_leaf_rows, TableDependencies(sig.Get(*slot).tables, snapshot), settings);
		spec.constraints = std::move(shape.constraints);
		spec.conjuncts = std::move(shape.conjuncts);
		spec.table_ids = std::move(shape.table_ids);
		cache->stats.blocks_planted++;
		WrapMaterialize(slot, std::move(spec), cache, std::move(keep_alive));
	}

	bool TryHit(unique_ptr<LogicalOperator> &slot, const BlockShape &shape,
	            const vector<reference<const TableCatalogEntry>> &tables) {
		auto entries = cache->LookupBlocks(shape.hash);
		if (entries.empty()) {
			return false;
		}
		auto &root = *slot;
		auto bindings = root.GetColumnBindings();
		auto &types = root.types;
		// which of the block's output columns does the rest of the query need?
		ExternalReferenceCollector collector(root, bindings);
		collector.VisitOperator(plan);
		if (collector.opaque_parent) {
			return false;
		}
		for (auto &entry : entries) {
			if (entry->canonical != shape.canonical) {
				continue;
			}
			if (!RangeConstraintsImply(shape.constraints, entry->constraints)) {
				continue;
			}
			if (!cache->IsCurrent(*entry, tables, snapshot)) {
				continue;
			}
			if (entry->rows > shape.est_leaf_rows / 2) {
				// scanning the cached rows back would not save enough
				continue;
			}
			// conjuncts of the query that the cached rows do not already satisfy exactly
			std::unordered_set<string> cached_conjuncts(entry->conjuncts.begin(), entry->conjuncts.end());
			vector<idx_t> residual;
			for (idx_t i = 0; i < shape.conjuncts.size(); i++) {
				if (cached_conjuncts.find(shape.conjuncts[i]) == cached_conjuncts.end()) {
					residual.push_back(i);
				}
			}
			binding_set_t needed = collector.references;
			for (auto i : residual) {
				vector<ColumnBinding> refs;
				CollectBindings(*root.expressions[i], refs);
				needed.insert(refs.begin(), refs.end());
			}
			// map every needed binding to a stored column of the same identity and type
			vector<column_t> scan_columns;
			vector<LogicalType> scan_types;
			vector<std::pair<ColumnBinding, idx_t>> mapping; // old binding -> position in the scan output
			bool usable = true;
			for (idx_t pos = 0; pos < bindings.size() && usable; pos++) {
				if (needed.find(bindings[pos]) == needed.end()) {
					continue;
				}
				auto &identity = shape.output[pos];
				if (!identity.IsValid()) {
					usable = false;
					break;
				}
				idx_t stored = DConstants::INVALID_INDEX;
				for (idx_t j = 0; j < entry->columns.size(); j++) {
					if (entry->columns[j] == identity && entry->types[j] == types[pos]) {
						stored = j;
						break;
					}
				}
				if (stored == DConstants::INVALID_INDEX) {
					usable = false;
					break;
				}
				idx_t k = DConstants::INVALID_INDEX;
				for (idx_t j = 0; j < scan_columns.size(); j++) {
					if (scan_columns[j] == stored) {
						k = j;
					}
				}
				if (k == DConstants::INVALID_INDEX) {
					k = scan_columns.size();
					scan_columns.push_back(stored);
					scan_types.push_back(entry->types[stored]);
				}
				mapping.emplace_back(bindings[pos], k);
			}
			if (!usable) {
				continue;
			}
			if (scan_columns.empty()) {
				// nothing above references the block (e.g. count(*)): keep one column so the row count survives
				if (entry->types.empty()) {
					continue;
				}
				scan_columns.push_back(0);
				scan_types.push_back(entry->types[0]);
			}
			// rewrite: scan of the stored columns, plus the residual predicates
			auto new_index = binder.GenerateTableIndex();
			auto scan = make_uniq<LogicalRecyclerScan>(new_index, scan_types, entry, false, tables, scan_columns);
			scan->ResolveOperatorTypes();
			for (auto &m : mapping) {
				replacer.replacement_bindings.emplace_back(m.first, ColumnBinding(new_index, m.second));
			}
			unique_ptr<LogicalOperator> replacement = std::move(scan);
			if (!residual.empty()) {
				auto filter = make_uniq<LogicalFilter>();
				for (auto i : residual) {
					filter->expressions.push_back(root.expressions[i]->Copy());
				}
				filter->children.push_back(std::move(replacement));
				filter->ResolveOperatorTypes();
				replacement = std::move(filter);
			}
			cache->NoteHit(*entry);
			cache->stats.subsumption_hits++;
			slot = std::move(replacement);
			return true;
		}
		return false;
	}
};

//! Subsumption: SPJ blocks served from wider cached blocks, admissible misses planted with a marker
void ServeSubsumedBlocks(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                         unique_ptr<LogicalOperator> &root_slot, RecyclerState &state,
                         const RecyclerClientState &client, PlanSignatures &sig) {
	SubsumptionPass pass(input.context, input.optimizer.binder, state.cache, client.settings, sig, client.snapshot,
	                     *plan);
	WalkBoundPlan(root_slot, [&](unique_ptr<LogicalOperator> &slot) { return pass.Visit(slot); });
	pass.ApplyReplacements(*plan);
}

} // namespace duckdb
