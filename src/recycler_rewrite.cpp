#include "recycler_rewrite.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <unordered_map>

namespace duckdb {

bool SubstituteScan(unique_ptr<LogicalOperator> &slot, shared_ptr<CachedEntry> entry, bool ordered,
                    const vector<reference<const TableCatalogEntry>> &tables, Binder &binder,
                    ColumnBindingReplacer &replacer) {
	auto &old_op = *slot;
	if (old_op.types != entry->types) {
		return false;
	}
	auto old_bindings = old_op.GetColumnBindings();
	if (old_bindings.size() != entry->types.size()) {
		return false;
	}
	auto scan = make_uniq<LogicalRecyclerScan>(binder.GenerateTableIndex(), old_op.types, std::move(entry), ordered,
	                                           tables);
	scan->ResolveOperatorTypes();
	auto new_bindings = scan->GetColumnBindings();
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
	}
	slot = std::move(scan);
	return true;
}

void WrapMaterialize(unique_ptr<LogicalOperator> &slot, RecyclerWriteSpec spec, shared_ptr<RecyclerCache> cache,
                     vector<unique_ptr<Expression>> keep_alive) {
	auto child = std::move(slot);
	auto wrapper = make_uniq<LogicalRecyclerMaterialize>(std::move(spec), std::move(cache), std::move(child));
	wrapper->expressions = std::move(keep_alive);
	wrapper->ResolveOperatorTypes();
	slot = std::move(wrapper);
}

unique_ptr<LogicalOperator> *CteConsumerSlot(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE &&
	    op.type != LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return nullptr;
	}
	D_ASSERT(op.children.size() == 2);
	return &op.children[1];
}

//===----------------------------------------------------------------------===//
// Post-optimize: resolve the stored columns of planted block markers
//===----------------------------------------------------------------------===//
struct BlockFinalizer {
	struct Resolution {
		ColumnIdentity identity;
		bool compressed = false;
		bool valid = false;
	};

	std::unordered_map<idx_t, const LogicalOperator *> producers;
	const std::unordered_map<idx_t, idx_t> *table_ids = nullptr;

	void CollectProducers(const LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
			for (auto &binding : op.Cast<LogicalEmptyResult>().bindings) {
				producers[binding.table_index] = &op;
			}
		} else {
			for (auto idx : op.GetTableIndex()) {
				producers[idx] = &op;
			}
		}
		for (auto &child : op.children) {
			CollectProducers(*child);
		}
	}

	Resolution Resolve(ColumnBinding binding, idx_t depth) {
		Resolution result;
		if (depth > 32) {
			return result;
		}
		auto producer = producers.find(binding.table_index);
		if (producer == producers.end()) {
			return result;
		}
		auto &op = *producer->second;
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			auto &column_ids = get.GetColumnIds();
			auto id = table_ids->find(get.table_index);
			if (id == table_ids->end() || binding.column_index >= column_ids.size()) {
				return result;
			}
			result.identity.table = id->second;
			result.identity.column = column_ids[binding.column_index].GetPrimaryIndex();
			result.valid = true;
			return result;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = op.Cast<LogicalProjection>();
			if (binding.column_index >= proj.expressions.size()) {
				return result;
			}
			return ResolveExpression(*proj.expressions[binding.column_index], depth + 1);
		}
		default:
			return result;
		}
	}

	//! Follow column references through DuckDB's compressed-materialization projections: the value that leaves a
	//! decompress(compress(col)) pair is col again
	Resolution ResolveExpression(const Expression &expr, idx_t depth) {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			return Resolve(expr.Cast<BoundColumnRefExpression>().binding, depth);
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = expr.Cast<BoundFunctionExpression>();
			if (func.children.empty()) {
				return Resolution();
			}
			if (StringUtil::StartsWith(func.function.name, "__internal_decompress_")) {
				auto inner = ResolveExpression(*func.children[0], depth + 1);
				if (inner.valid && inner.compressed) {
					inner.compressed = false;
					return inner;
				}
				return Resolution();
			}
			if (StringUtil::StartsWith(func.function.name, "__internal_compress_")) {
				auto inner = ResolveExpression(*func.children[0], depth + 1);
				inner.compressed = true;
				return inner;
			}
		}
		return Resolution();
	}

	void Walk(unique_ptr<LogicalOperator> &slot) {
		auto ours = AsRecyclerOperator(*slot);
		if (ours && ours->kind == RecyclerOperatorKind::MATERIALIZE) {
			auto &marker = slot->Cast<LogicalRecyclerMaterialize>();
			if (marker.spec.kind == RecyclerEntryKind::BLOCK && marker.spec.columns.empty() &&
			    marker.children.size() == 1) {
				auto &child = *marker.children[0];
				producers.clear();
				table_ids = &marker.spec.table_ids;
				CollectProducers(child);
				idx_t usable = 0;
				for (auto &binding : child.GetColumnBindings()) {
					auto resolution = Resolve(binding, 0);
					if (resolution.valid && !resolution.compressed) {
						marker.spec.columns.push_back(resolution.identity);
						usable++;
					} else {
						marker.spec.columns.push_back(ColumnIdentity());
					}
				}
				if (usable == 0) {
					// nothing a later query could pick up: run the block as vanilla would
					auto child_ptr = std::move(marker.children[0]);
					slot = std::move(child_ptr);
					Walk(slot);
					return;
				}
			}
		}
		for (auto &child : slot->children) {
			Walk(child);
		}
	}
};

void ResolveBlockColumns(unique_ptr<LogicalOperator> &slot) {
	BlockFinalizer finalizer;
	finalizer.Walk(slot);
}

} // namespace duckdb
