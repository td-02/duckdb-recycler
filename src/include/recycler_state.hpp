#pragma once
//===----------------------------------------------------------------------===//
// recycler_state.hpp - database-level and connection-level recycler state
//===----------------------------------------------------------------------===//
#include "recycler_cache.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

//! Database-wide state, owned by the OptimizerExtension registration
struct RecyclerState : public OptimizerExtensionInfo {
	RecyclerState() : cache(make_shared_ptr<RecyclerCache>()) {
	}
	shared_ptr<RecyclerCache> cache;
};

//! A write keyed on the pre-optimize plan that the pre hook requests and the post hook plants
struct PendingWrite {
	bool requested = false;
	SigHash hash;
	string canonical;
	idx_t est_input_rows = 0;

	void Request(const SigHash &hash_p, string canonical_p, idx_t est_input_rows_p) {
		requested = true;
		hash = hash_p;
		canonical = std::move(canonical_p);
		est_input_rows = est_input_rows_p;
	}
	void Reset() {
		requested = false;
		canonical.clear();
		est_input_rows = 0;
	}
};

//! Per-connection state carrying information from the pre-optimize hook to the post-optimize hook.
//! Only touched by the optimizer hooks of its own connection, which run one statement at a time.
struct RecyclerClientState : public ClientContextState {
	//! The pre hook decided this statement takes part in recycling
	bool active = false;
	//! Settings are re-read only after a SET/RESET/PRAGMA statement ran on this connection
	bool settings_loaded = false;
	RecyclerSettings settings;
	//! The complete result (a miss on an admissible query)
	PendingWrite whole;
	//! The complete ordered result a root LIMIT/OFFSET cuts its page from (a miss on a paginated query)
	PendingWrite page_source;

	void Reset() {
		active = false;
		whole.Reset();
		page_source.Reset();
	}
};

RecyclerSettings LoadRecyclerSettings(ClientContext &context);
optional_ptr<RecyclerState> GetRecyclerState(ClientContext &context);

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

void LoadRecycler(ExtensionLoader &loader);

} // namespace duckdb
