#pragma once
//===----------------------------------------------------------------------===//
// recycler_state.hpp - database-level and connection-level recycler state
//===----------------------------------------------------------------------===//
#include "recycler_cache.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class ErrorData;
class ExtensionLoader;
class TableCatalogEntry;

//! Database-wide state, owned by the OptimizerExtension registration
struct RecyclerState : public OptimizerExtensionInfo {
	RecyclerState() : cache(make_shared_ptr<RecyclerCache>()) {
	}
	shared_ptr<RecyclerCache> cache;
	//! Per database the cache tracked at LOAD, the id of a transaction LOAD began on it once the hooks were
	//! registered: a transaction with an id up to it may have written without an announcement. Filled before
	//! load_horizon_set is.
	unordered_map<idx_t, transaction_t> load_horizon;
	std::atomic<bool> load_horizon_set {false};
	//! No transaction up to load_horizon remains (latched)
	std::atomic<bool> predating_ended {false};

	//! Whether every transaction that may have written before LOAD registered the hooks has ended. Such a
	//! transaction may commit writes no hook announced, so a transaction that begins while one remains is not tracked
	//! (RecyclerClientState::transaction_tracked); one that begins later sees them all.
	bool PredatingTransactionsEnded(DatabaseInstance &db);
};

//! A cache entry a recycler operator of the running statement touched, with the operator's id
struct TouchedEntry {
	idx_t operator_id;
	shared_ptr<CachedEntry> entry;
};

//! The pagination (LIMIT/OFFSET over an ORDER BY) of the bound plan, as the pre-optimize hook saw it. Its key is taken
//! from the bound plan because the optimized plan below the sort differs between the pages of one pagination (block
//! markers, block scans), while the bound plan does not.
struct PaginationSite {
	//! The bound ORDER BY subtree is deterministic and cacheable, and its ORDER BY is provably a total order
	bool eligible = false;
	//! Full canonical rendering of the bound ORDER BY subtree (LIMIT/OFFSET excluded) and the session's salt
	string canonical;
	idx_t limit = 0;
	idx_t offset = 0;
	//! Direction and NULL order of every ORDER BY key
	string order_spec;
	//! Positions of the bound ORDER BY's output columns that the query's output demands, ascending
	vector<idx_t> demanded;
	//! Table index of the bound ORDER BY's input projection, which optimization keeps: the optimized unit's columns are
	//! followed back to it to learn which bound ORDER BY output each carries (their order in the unit is not fixed)
	idx_t projection_index = DConstants::INVALID_INDEX;
	//! The tables of the bound ORDER BY subtree with the state of their catalogs' transactions at the pre hook: the
	//! entry written for this pagination depends on them, and an entry found for it must be current for them (the
	//! optimized plan may read them through a block scan, so its own tables are no stand-in)
	vector<TableDependency> tables;
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
	//! effect here only after this connection's next SET/RESET/PRAGMA; so does a memory_limit set there (a global
	//! option), which the budget follows.
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
	//! Set by the pre hook when the bound plan holds exactly one eligible pagination (M6)
	PaginationSite pagination_site;

	//! The writes of the open transaction, each announced once (RecyclerCache::AnnounceWriter) before the first
	//! statement to name it runs, and settled by the transaction callbacks
	vector<PendingWrite> pending_writes;
	//! TransactionBegin ran for the open transaction when no transaction that predates LOAD remained
	//! (RecyclerState::PredatingTransactionsEnded). A transaction that began earlier may predate commits the cache
	//! never heard of: it is neither served nor stores.
	bool transaction_tracked = false;

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
		pagination_site = PaginationSite();
	}
	idx_t NextOperatorId() {
		return ++operator_ids;
	}
	//! `settings`, loaded first if a SET/RESET/PRAGMA dropped them
	const RecyclerSettings &GetSettings(ClientContext &context);
	void NoteTouched(idx_t operator_id, shared_ptr<CachedEntry> entry);
	//! Adds the write to pending_writes and announces it, unless the transaction already did
	void AnnounceWrite(RecyclerCache &cache, PendingWrite write);
	//! Announces the writes of a statement: to `target` when it names the table it writes, and to every table of
	//! any other database it modifies (StatementProperties::modified_databases), for the databases the cache tracks
	void AnnounceStatement(ClientContext &context, RecyclerCache &cache, optional_ptr<TableCatalogEntry> target,
	                       const unordered_map<string, StatementProperties::ModificationInfo> &modified);
	//! Announces every database a statement the extension optimizer will not see may write
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
