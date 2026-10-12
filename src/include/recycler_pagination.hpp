#pragma once
//===----------------------------------------------------------------------===//
// recycler_pagination.hpp - LIMIT/OFFSET pages over one sorted result (M6)
//===----------------------------------------------------------------------===//
#include "recycler_rewrite.hpp"
#include "recycler_state.hpp"

namespace duckdb {

class PlanSignatures;

//! Pre hook: records the bound plan's single eligible pagination in `client.pagination_site`. Its key and its
//! total-order proof come from the bound plan, before any rewrite changes what lies below the ORDER BY.
void DetectPagination(RecyclerClientState &client, PlanSignatures &sig, LogicalOperator &root);

//! Number of pagination sites below `op`; `first` receives the first LIMIT found
idx_t CountPaginationSites(LogicalOperator &op, LogicalOperator **first);

//! What RewritePagination did at a LIMIT
enum class PaginationRewrite : uint8_t {
	//! The LIMIT is not the site, or its sorted prefix is neither served nor written: the subtree walk goes on
	NONE,
	//! The unit below the LIMIT now scans a cached prefix
	SERVED,
	//! The unit below the LIMIT is wrapped in a writer of its sorted prefix (slot -> LIMIT -> writer -> unit)
	WRITING
};

//! Post hook, at the LIMIT in `slot`: the sorted rows below the pagination site's LIMIT are the same for every page,
//! so they are the cache unit and every page only skips and scans its own slice. The key comes from the bound plan
//! (`site`); the optimized ORDER_BY must still be that site. A write is planned only when `may_write`.
PaginationRewrite RewritePagination(PlanPass &pass, unique_ptr<LogicalOperator> &slot, const PaginationSite &site,
                                    bool may_write, bool under_delim_lhs);

} // namespace duckdb
