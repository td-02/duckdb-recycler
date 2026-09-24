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

//! Per-connection state carrying information from the pre-optimize hook to the post-optimize hook.
//! Only touched by the optimizer hooks of its own connection, which run one statement at a time.
struct RecyclerClientState : public ClientContextState {
	//! The pre hook decided this statement takes part in recycling
	bool active = false;
	//! Settings are re-read only after a SET/RESET/PRAGMA statement ran on this connection
	bool settings_loaded = false;
	RecyclerSettings settings;
	//! Whole-result write requested by the pre hook (a miss on an admissible query)
	bool has_pending_whole = false;
	SigHash whole_hash;
	string whole_canonical;
	idx_t whole_est_input_rows = 0;

	void Reset() {
		active = false;
		has_pending_whole = false;
		whole_canonical.clear();
		whole_est_input_rows = 0;
	}
};

RecyclerSettings LoadRecyclerSettings(ClientContext &context);
optional_ptr<RecyclerState> GetRecyclerState(ClientContext &context);

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

void LoadRecycler(ExtensionLoader &loader);

} // namespace duckdb
