#pragma once
//===----------------------------------------------------------------------===//
// recycler_state.hpp - database-level and connection-level recycler state
//===----------------------------------------------------------------------===//
#include "recycler_cache.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include <mutex>

namespace duckdb {

class ClientContext;
class ErrorData;
class ExtensionLoader;

//! Database-wide state, owned by the OptimizerExtension registration
struct RecyclerState : public OptimizerExtensionInfo {
	RecyclerState() : cache(make_shared_ptr<RecyclerCache>()) {
	}
	shared_ptr<RecyclerCache> cache;
};

//! A cache entry a recycler operator of the running statement touched, with the operator's id
struct TouchedEntry {
	idx_t operator_id;
	shared_ptr<CachedEntry> entry;
};

//! Per-connection state carrying information from the pre-optimize hook to the post-optimize hook and from the
//! recycler operators to QueryEnd. The hooks, physical planning and QueryEnd of a connection run one statement at
//! a time.
struct RecyclerClientState : public ClientContextState {
	static RecyclerClientState &Get(ClientContext &context);

	//! The pre hook decided this statement takes part in recycling
	bool active = false;
	//! Settings are re-read only after a SET/RESET/PRAGMA statement ran on this connection (the pre hook sees the
	//! statement before it executes and drops the cached copy). A SET GLOBAL on another connection therefore takes
	//! effect here only after this connection's next SET/RESET/PRAGMA.
	bool settings_loaded = false;
	RecyclerSettings settings;
	//! The result-affecting session settings as the pre hook read them (PlanSignatures::SettingsSalt); the post hook
	//! keys the same statement under the same values
	string settings_salt;
	//! Whole-result write requested by the pre hook (a miss on an admissible query)
	bool has_pending_whole = false;
	SigHash whole_hash;
	string whole_canonical;
	idx_t whole_est_input_rows = 0;

	//! Ids of this connection's recycler operators, drawn when the physical plan is built: a profiler node shows
	//! its operator's id ("Id" among the parameters), which is how QueryEnd tells two entries of one shape apart
	idx_t operator_ids = 0;
	//! The profiler is on for this statement (set by the post hook): the recycler operators register the entries
	//! they touch and QueryEnd pairs them with the profiler tree. Sinks finalize and sources initialize on worker
	//! threads, hence the lock.
	bool profiled = false;
	std::mutex touched_lock;
	vector<TouchedEntry> touched;

	void Reset() {
		active = false;
		has_pending_whole = false;
		whole_canonical.clear();
		whole_est_input_rows = 0;
	}
	idx_t NextOperatorId() {
		return ++operator_ids;
	}
	void NoteTouched(idx_t operator_id, shared_ptr<CachedEntry> entry);
	//! Records measured costs from the profiler tree on the touched entries and in the history
	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;
};

RecyclerSettings LoadRecyclerSettings(ClientContext &context);
optional_ptr<RecyclerState> GetRecyclerState(ClientContext &context);

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

void LoadRecycler(ExtensionLoader &loader);

} // namespace duckdb
