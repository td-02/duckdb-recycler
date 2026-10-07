#include "recycler_policy.hpp"

namespace duckdb {

bool IsSubtreeCandidateType(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return true;
	default:
		return false;
	}
}

bool IsAggregation(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || type == LogicalOperatorType::LOGICAL_DISTINCT;
}

bool CanServe(const CachedEntry &entry, bool preserve_order) {
	return entry.ordered || !preserve_order;
}

//! Whether we trust the optimizer's cardinality estimate enough to materialize on first sight
static bool SpeculationAllowed(const LogicalOperator &op, const NodeInfo &info, bool force_candidate) {
	if (info.unreliable_estimate) {
		// non-equality joins below make every estimate above them worthless (a self join with an inequality
		// was 60x off in the workload); wait for a repeat before paying for a materialization
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return true;
	default:
		return force_candidate;
	}
}

bool AdmitSubtreeWrite(ClientContext &context, const RecyclerCache &cache, const RecyclerSettings &settings,
                       LogicalOperator &op, const NodeInfo &info, bool under_delim_lhs, bool force_candidate) {
	if (cache.TooBig(info.hash, settings.CapRows(RecyclerEntryKind::SUBTREE), settings.entry_cap_bytes)) {
		return false;
	}
	auto est_in = info.est_leaf_rows;
	if (est_in < settings.min_input_rows) {
		return false;
	}
	auto est_out = op.EstimateCardinality(context);
	if (!IsAggregation(op.type) && est_out > est_in / 2) {
		// a join result that is not much smaller than its inputs saves little when scanned back; an aggregate
		// always folds its input and its estimate is bounded by the input anyway
		return false;
	}
	auto width = RecyclerEstimateRowWidth(op.types);
	auto est_bytes = est_out * width;
	auto cap_rows = under_delim_lhs ? MinValue<idx_t>(settings.entry_cap_rows, 200000) : settings.entry_cap_rows;
	if (est_out > cap_rows || est_bytes > settings.entry_cap_bytes) {
		return false;
	}
	auto seen_before = cache.Seen(info.hash) > 1;
	if (!seen_before) {
		if (!SpeculationAllowed(op, info, force_candidate)) {
			return false;
		}
		// aggregates are the safest speculation (their output is bounded by their input and usually far
		// smaller), plain joins the riskiest
		auto speculate_bytes = IsAggregation(op.type)                                    ? settings.speculate_bytes * 2
		                       : op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ? settings.speculate_bytes / 8
		                                                                                 : settings.speculate_bytes;
		if (est_bytes > speculate_bytes) {
			return false;
		}
	}
	return true;
}

bool AdmitBlockWrite(const RecyclerCache &cache, const RecyclerSettings &settings, const BlockShape &shape) {
	// multi-table blocks only: the target is the FROM/WHERE part of dashboard-style queries, whose output is
	// far smaller than the tables they join
	return shape.joins >= 2 && shape.est_leaf_rows >= settings.min_input_rows &&
	       !cache.TooBig(shape.hash, settings.CapRows(RecyclerEntryKind::BLOCK), settings.entry_cap_bytes);
}

bool AdmitPaginationWrite(const RecyclerCache &cache, const RecyclerSettings &settings, const NodeInfo &info,
                          const SigHash &hash) {
	return info.est_leaf_rows >= settings.min_input_rows &&
	       !cache.TooBig(hash, settings.CapRows(RecyclerEntryKind::PAGINATION), settings.entry_cap_bytes);
}

bool AdmitWholeWrite(ClientContext &context, const RecyclerCache &cache, const RecyclerSettings &settings,
                     LogicalOperator &root, const NodeInfo &root_info, const SigHash &hash) {
	if (cache.TooBig(hash, settings.CapRows(RecyclerEntryKind::WHOLE_RESULT), settings.entry_cap_bytes)) {
		return false;
	}
	if (root_info.refused || root_info.nondeterministic || root_info.unresolved_delim_get) {
		return false;
	}
	// the estimate only guards against materializing table-sized results before the runtime row cap stops the
	// store; estimates above a planted block marker are coarse (the semi-join reduction is lost), so leave a wide
	// margin
	return root.EstimateCardinality(context) <= 4 * settings.whole_cap_rows;
}

} // namespace duckdb
