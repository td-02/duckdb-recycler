#pragma once
//===----------------------------------------------------------------------===//
// recycler_state.hpp - database-level and connection-level recycler state
//===----------------------------------------------------------------------===//
#include "recycler_cache.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include <atomic>
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
	//! One held by LOAD while it registers the open connections, and, when a transaction was open on a database at
	//! LOAD, one per connection open then until it ends the transaction it was in. Such a transaction may commit
	//! writes no hook announced, so a transaction that begins while any remains is not tracked
	//! (RecyclerClientState::transaction_tracked); one that begins later sees them all.
	std::atomic<idx_t> predating {1};
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
	RecyclerClientState() = default;
	//! The state LOAD gives a connection open before it while a transaction was open, counted in
	//! RecyclerState::predating
	explicit RecyclerClientState(RecyclerState &state);

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
	//! The tables of the statement's plan with the state of their catalogs' transactions, taken by each hook for
	//! the plan it analyzes (the transaction is the same in both)
	TableSnapshot snapshot;
	//! Whole-result write requested by the pre hook (a miss on an admissible query); the key and the tables are
	//! those of the bound plan, which later lookups see again
	bool has_pending_whole = false;
	SigHash whole_hash;
	string whole_canonical;
	idx_t whole_est_input_rows = 0;
	vector<TableDependency> whole_tables;

	//! The writes of the open transaction, each announced once (RecyclerCache::AnnounceWriter) before the first
	//! statement to name it runs, and settled by the transaction callbacks
	vector<PendingWrite> pending_writes;
	//! TransactionBegin ran for the open transaction when no transaction that predates LOAD remained
	//! (RecyclerState::predating). A transaction that began earlier may predate commits the cache never heard of: it
	//! is neither served nor stores.
	bool transaction_tracked = false;
	//! The connection holds RecyclerState::predating: it was open at LOAD and has not yet ended the transaction it was
	//! in
	bool predates_load = false;
	//! A commit of a database that no hook announced (an unplanned statement's, committed with it), which the
	//! connection's next transaction stamps: the commit callback must not allocate the database's version. Until
	//! then an unplanned writer withholds every entry.
	idx_t unstamped_catalog = DConstants::INVALID_INDEX;
	transaction_t unstamped_commit = 0;

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
		snapshot.clear();
		has_pending_whole = false;
		whole_canonical.clear();
		whole_est_input_rows = 0;
		whole_tables.clear();
	}
	idx_t NextOperatorId() {
		return ++operator_ids;
	}
	void NoteTouched(idx_t operator_id, shared_ptr<CachedEntry> entry);
	//! Adds the write to pending_writes and announces it, unless the transaction already did
	void AnnounceWrite(RecyclerCache &cache, PendingWrite write);
	//! Whether the open transaction may have written the catalog without an announcement: it ran an unplanned
	//! statement, or announced nothing for the catalog, and did not announce the whole catalog
	bool MayWriteUnannounced(idx_t catalog) const;
	//! Stamps the commit TransactionCommit could not, and releases the unplanned writer that stood in for it
	void StampUnstampedCommit(RecyclerCache &cache);
	//! The connection ended the transaction it was in at LOAD (or closes): it no longer holds RecyclerState::predating
	void EndPredating(RecyclerState &state);
	//! Announces an unplanned write for a statement the extension optimizer will not see, and the database the
	//! transaction modified without an announcement
	void QueryBegin(ClientContext &context) override;
	//! Announces the writes of a statement prepared through the client API, which runs without the pre hook, and
	//! asks for a rebind when the hooks rewrote its plan
	RebindQueryInfo OnExecutePrepared(ClientContext &context, PreparedStatementCallbackInfo &info,
	                                  RebindQueryInfo current_rebind) override;
	//! Records measured costs from the profiler tree on the touched entries and in the history
	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;
	void TransactionBegin(MetaTransaction &transaction, ClientContext &context) override;
	//! Stamps the written tables with the commit and evicts the entries the commit outdates
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;
};

RecyclerSettings LoadRecyclerSettings(ClientContext &context);
optional_ptr<RecyclerState> GetRecyclerState(ClientContext &context);

void RecyclerPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
void RecyclerOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

void LoadRecycler(ExtensionLoader &loader);

} // namespace duckdb
