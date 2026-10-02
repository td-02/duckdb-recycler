#pragma once
//===----------------------------------------------------------------------===//
// recycler_cache.hpp - the shared, bounded cache of materialized results
//===----------------------------------------------------------------------===//
#include "recycler_signature.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <atomic>
#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class DuckTransactionManager;
class TableCatalogEntry;

enum class RecyclerEntryKind : uint8_t {
	//! The complete result of a query (keyed on the pre-optimization plan)
	WHOLE_RESULT = 0,
	//! The output of a subtree of an optimized plan
	SUBTREE = 1,
	//! The output of an SPJ block keyed on its shape; reusable by narrower blocks (predicate/column subsumption)
	BLOCK = 2
};

const char *RecyclerEntryKindName(RecyclerEntryKind kind);

//! A base table a statement reads, with the state its catalog's transaction had at the pre hook: the start time
//! says which commits the statement sees, the catalog version which schema
struct TableDependency {
	//! TableKey(): the oid of the attached database, which a database attached again under the same name does not
	//! share, and the schema and table names
	string table;
	//! The oid of the table's attached database
	idx_t catalog = 0;
	transaction_t start_time = 0;
	idx_t catalog_version = 0;
};

//! A write a statement announced before it ran: one table, or every table of a catalog when no hook can name the
//! table the statement writes
struct PendingWrite {
	//! TableDependency::catalog
	idx_t catalog;
	//! TableDependency::table; empty for the whole catalog
	string table;
};

//! The dependencies of the running statement, one per table of the analyzed plan
using TableSnapshot = reference_map_t<const TableCatalogEntry, TableDependency>;

//! Whether the cache stamps the commits of the database: a DuckDB database other than the system and temporary ones,
//! the only databases whose tables the signature accepts
bool IsTrackedDatabase(AttachedDatabase &db);
//! TableDependency::table of a table, distinct for every table of every attached database
string TableKey(const TableCatalogEntry &table);
//! The dependencies of a statement scanning `tables`, read from the transactions of their catalogs
TableSnapshot TakeTableSnapshot(ClientContext &context, const vector<reference<const TableCatalogEntry>> &tables);
//! The dependencies of a subtree scanning `tables`, in that order, under the statement's snapshot
vector<TableDependency> TableDependencies(const vector<reference<const TableCatalogEntry>> &tables,
                                          const TableSnapshot &snapshot);

//! One cached, immutable result
struct CachedEntry {
	RecyclerEntryKind kind = RecyclerEntryKind::SUBTREE;
	SigHash hash;
	//! Full canonical rendering of the plan (sub)tree; compared on every hash match
	string canonical;
	shared_ptr<ColumnDataCollection> data;
	vector<LogicalType> types;
	idx_t rows = 0;
	idx_t bytes = 0;
	//! Rows were written in a defined order and must be replayed in that order
	bool ordered = false;
	//! Estimated rows scanned to produce the result (benefit proxy)
	idx_t est_input_rows = 0;
	//! The tables the result reads, in NodeInfo::tables order: a canonical match pairs them by position with the
	//! reader's tables
	vector<TableDependency> tables;
	//! Bytes holding rows; `bytes` is the allocation the buffer pool charges
	idx_t used_bytes = 0;
	//! Sink() calls that built the collection: many small chunks make the materialization expensive
	idx_t sink_calls = 0;
	//! Cache sequence at which the entry was stored
	idx_t stored_at = 0;
	std::atomic<idx_t> last_used {0};
	std::atomic<idx_t> hits {0};
	//! Operator thread time from the profiler tree of the query that stored (or last hit) the entry, written by
	//! that connection's QueryEnd while recycler_entries() may read on another: the cost of producing the
	//! wrapped subtree (the sink's wall time when the query was not profiled), the materialization's own time
	//! and the time of the last RECYCLER_SCAN over the entry
	std::atomic<uint64_t> measured_cost_ns {0};
	std::atomic<uint64_t> materialize_ns {0};
	std::atomic<uint64_t> hit_ns {0};

	//! BLOCK entries only: identity of every stored column (invalid = not a plain base column), the range
	//! constraints the rows satisfy, and the canonical rendering of every block-root conjunct
	vector<ColumnIdentity> columns;
	vector<RangeConstraint> constraints;
	vector<string> conjuncts;

	//! Benefit density used for eviction: work saved per reuse, per byte held
	double Benefit() const;
};

//! What the cache knows about the writes to one table, or to every table of a catalog
struct TableVersion {
	//! Transactions that announced a write and have not ended: while any exists, entries over the table (every
	//! table of the catalog) are neither served nor stored
	idx_t writers = 0;
	//! Upper bound of the commit ids that wrote the table (0: no commit since the version exists; a transaction older
	//! than the cache is refused by its connection, see RecyclerClientState::transaction_tracked). An entry or a
	//! reader whose start time does not exceed it may miss that commit.
	transaction_t last_commit = 0;
};

//! The versions of one attached database: of the database as a whole, and of each table a statement named
struct CatalogVersions {
	TableVersion whole;
	//! By TableDependency::table
	std::unordered_map<string, TableVersion> tables;
};

struct RecyclerSettings {
	bool enabled = true;
	bool whole_result = true;
	bool blocks = true;
	bool subtrees = true;
	bool subsumption = true;
	idx_t budget_bytes = 2000ULL * 1024 * 1024;
	idx_t entry_cap_bytes = 256ULL * 1024 * 1024;
	idx_t entry_cap_rows = 4000000;
	idx_t whole_cap_rows = 1000000;
	idx_t min_input_rows = 200000;
	idx_t speculate_bytes = 64ULL * 1024 * 1024;
	idx_t compact_rows = 8192;

	//! The row cap of an entry of this kind
	idx_t CapRows(RecyclerEntryKind kind) const {
		return kind == RecyclerEntryKind::WHOLE_RESULT ? MinValue<idx_t>(whole_cap_rows, entry_cap_rows)
		                                               : entry_cap_rows;
	}
};

//! What is known about a signature whether or not its result is in the cache: it survives eviction and a
//! too-big verdict (the recorded size against the caps of the moment)
struct SignatureHistory {
	//! Sightings as a subtree candidate
	idx_t seen = 0;
	idx_t last_seen = 0;
	//! Size of the last materialization, stored or not
	idx_t rows = 0;
	idx_t bytes = 0;
	//! Operator thread time, as on CachedEntry
	uint64_t measured_cost_ns = 0;
	uint64_t materialize_ns = 0;
	uint64_t hit_ns = 0;
};

struct RecyclerStats {
	std::atomic<uint64_t> pre_calls {0};
	std::atomic<uint64_t> post_calls {0};
	std::atomic<uint64_t> whole_hits {0};
	std::atomic<uint64_t> block_hits {0};
	std::atomic<uint64_t> subtree_hits {0};
	std::atomic<uint64_t> misses {0};
	std::atomic<uint64_t> whole_stores {0};
	std::atomic<uint64_t> subtree_stores {0};
	std::atomic<uint64_t> store_rejected {0};
	std::atomic<uint64_t> too_big {0};
	std::atomic<uint64_t> evictions {0};
	std::atomic<uint64_t> hash_collisions {0};
	std::atomic<uint64_t> writes_planned {0};
	std::atomic<uint64_t> blocks_planted {0};
	std::atomic<uint64_t> block_stores {0};
	std::atomic<uint64_t> subsumption_hits {0};
	std::atomic<uint64_t> clears {0};
	//! entries evicted because a writer of one of their tables (or of the whole catalog) committed
	std::atomic<uint64_t> invalidated_entries {0};
	//! lookups, admissions and stores refused by the table versions: an uncommitted writer, a commit the snapshot
	//! predates, or a catalog the reader does not share
	std::atomic<uint64_t> blocked_by_version {0};
	//! write statements whose target table the pre hook named (INSERT, UPDATE, DELETE, MERGE), also when their
	//! transaction had already announced that table
	std::atomic<uint64_t> announced_table_writes {0};
	std::atomic<uint64_t> pre_ns {0};
	std::atomic<uint64_t> post_ns {0};
	//! why the pre hook declined to take part (diagnostics)
	std::atomic<uint64_t> skip_not_query {0};
	//! statements that change state no table version tracks (ATTACH, DETACH, LOAD, ...) and clear the cache
	std::atomic<uint64_t> skip_modifying {0};
	std::atomic<uint64_t> skip_not_read {0};
	std::atomic<uint64_t> skip_disabled {0};
	//! statements that modify a database without a table the pre hook can name (DDL, EXECUTE of a prepared write),
	//! announced for every table of the database
	std::atomic<uint64_t> skip_modified_databases {0};
	std::atomic<uint64_t> skip_write_transaction {0};
	//! statements of a transaction that began before every transaction open at LOAD had ended
	std::atomic<uint64_t> skip_untracked_transaction {0};
	std::atomic<uint64_t> skip_analyze_failed {0};
	std::atomic<uint64_t> root_not_cacheable {0};
	//! root_not_cacheable by reason
	std::atomic<uint64_t> refused[static_cast<idx_t>(RefusalReason::COUNT)] {};
};

class RecyclerCache {
public:
	//! Look up the entry for the subtree rooted at `op` (analyzed as `info`) on behalf of a statement with this
	//! snapshot; a hash match is confirmed by rendering the subtree and comparing the canonical byte string, and
	//! the entry must be current for the reader (see IsCurrent)
	shared_ptr<CachedEntry> Lookup(RecyclerEntryKind kind, const NodeInfo &info, const PlanSignatures &sig,
	                               const LogicalOperator &op, const TableSnapshot &snapshot);
	//! Insert an entry (never overwrites); evicts lower-benefit entries to respect the budget. Refused when one of
	//! its tables has a writer or a commit the entry's snapshot predates. Returns success.
	bool Store(shared_ptr<CachedEntry> entry, idx_t budget_bytes);
	//! Insert a BLOCK entry under its shape hash (several entries per shape may coexist)
	bool StoreBlock(shared_ptr<CachedEntry> entry, idx_t budget_bytes);
	//! All BLOCK entries with this shape hash; the caller confirms a match and then asks IsCurrent
	vector<shared_ptr<CachedEntry>> LookupBlocks(const SigHash &hash) const;
	//! Whether an entry whose canonical rendering matched the subtree over `tables` answers a statement with this
	//! snapshot: every table is fresh for the entry and for the reader, and both see the same attached database
	//! at the same catalog version
	bool IsCurrent(const CachedEntry &entry, const vector<reference<const TableCatalogEntry>> &tables,
	               const TableSnapshot &snapshot);
	//! Whether a statement with this snapshot may store a result over `tables` now (Store checks again when the
	//! result is ready)
	bool Admissible(const vector<reference<const TableCatalogEntry>> &tables, const TableSnapshot &snapshot);

	//! A transaction is about to write (announced before the write runs, so that no reader can see the commit
	//! before the cache knows about it)
	void AnnounceWriter(const PendingWrite &write);
	//! The writer's transaction committed with a commit id of at most `commit`: the entries over the written tables
	//! are evicted. Allocates nothing, since it runs after DuckDB made the commit durable.
	void CommitWriter(const PendingWrite &write, transaction_t commit);
	//! The writer's transaction rolled back, or did not write the database. Allocates nothing.
	void RetractWriter(const PendingWrite &write);
	//! Drops the versions of the catalog that no snapshot needs any more: without a writer, and with no commit a
	//! transaction of `manager` (the catalog's) can predate. Called after a commit of the catalog; allocates
	//! nothing.
	void ForgetCommits(idx_t catalog, const DuckTransactionManager &manager);
	//! Account a reuse of an entry (also for entries found through LookupBlocks). Runs outside the cache lock, which is
	//! why `hits` and `last_used` are atomics
	void NoteHit(CachedEntry &entry);
	void Clear();

	//! The signature history (bounded to HISTORY_BOUND signatures, least recently noted first out)
	idx_t NoteSeen(const SigHash &hash);
	idx_t Seen(const SigHash &hash) const;
	//! A materialization finished with this size and the sink's wall time as its cost; `too_big` when the caps
	//! stopped the store
	void NoteMaterialized(const SigHash &hash, idx_t rows, idx_t bytes, uint64_t measured_cost_ns, bool too_big);
	//! The last materialization of this signature exceeded the given caps
	bool TooBig(const SigHash &hash, idx_t cap_rows, idx_t cap_bytes) const;
	//! Measured operator thread time of a materialization (the wrapped subtree and the operator's own time) and
	//! of a scan over the signature's result
	void NoteMaterializeTime(const SigHash &hash, uint64_t measured_cost_ns, uint64_t materialize_ns);
	void NoteHitTime(const SigHash &hash, uint64_t hit_ns);
	idx_t HistorySize() const;
	//! Every signature in the history, most recently noted first
	vector<std::pair<SigHash, SignatureHistory>> HistoryEntries() const;

	idx_t TotalBytes() const;
	idx_t UsedBytes() const;
	idx_t EntryCount() const;
	//! The databases and tables the cache holds a version for
	idx_t VersionCount() const;
	vector<shared_ptr<CachedEntry>> Entries() const;

	static constexpr idx_t HISTORY_BOUND = 4096;

	RecyclerStats stats;

private:
	idx_t NextSequence();
	using EntryMap = std::unordered_map<SigHash, shared_ptr<CachedEntry>, SigHashHasher>;
	using BlockMap = std::unordered_map<SigHash, vector<shared_ptr<CachedEntry>>, SigHashHasher>;
	EntryMap &Map(RecyclerEntryKind kind);
	shared_ptr<CachedEntry> PickVictim() const;
	//! Evict until `entry` fits the budget; false when the cache holds more valuable results (caller holds the lock)
	bool MakeRoom(const CachedEntry &entry, idx_t budget_bytes);
	//! Adds a stored entry to `dependents` (caller holds the lock)
	void AddDependents(const CachedEntry &entry);
	//! Removes the entry from the maps, `dependents` and the accounting; the maps may hold the last reference to it
	void Erase(const CachedEntry &entry);
	//! The history of `hash`, created and moved to the front of the LRU order (caller holds the lock)
	SignatureHistory &History(const SigHash &hash);
	using VersionMap = std::unordered_map<idx_t, CatalogVersions>;
	//! The version of the written table or catalog, created on first use (caller holds the lock)
	TableVersion &Version(const PendingWrite &write);
	//! The version of an announced write in its catalog's versions, which exist since the announcement (caller holds
	//! the lock)
	static TableVersion &AnnouncedVersion(CatalogVersions &catalog, const PendingWrite &write);
	//! Erases the versions of the catalog without a writer and whose last commit is 0 or below
	//! `lowest_active_start`, and the catalog's versions once none remains (caller holds the lock)
	void Forget(VersionMap::iterator catalog, transaction_t lowest_active_start);
	//! No uncommitted writer of the table or its catalog and no commit of either the snapshot predates (caller holds
	//! the lock)
	bool Fresh(const TableDependency &dependency) const;
	bool Fresh(const vector<TableDependency> &dependencies) const;

	using HistoryOrder = std::list<SigHash>;
	struct HistoryNode {
		SignatureHistory history;
		HistoryOrder::iterator position;
	};

	mutable std::mutex lock;
	EntryMap whole;
	EntryMap subtrees;
	BlockMap blocks;
	std::unordered_map<SigHash, HistoryNode, SigHashHasher> history;
	//! Most recently noted first
	HistoryOrder history_order;
	//! By TableDependency::catalog; kept across Clear(), since a reader with an old snapshot may store after it. A
	//! version lives while a writer is announced and, after its commit, while a transaction of its catalog may
	//! predate the commit: a commit of the catalog forgets the others (ForgetCommits). The versions of a detached
	//! database stay, since none of its commits follows.
	VersionMap versions;
	//! The stored entries over each table, by TableDependency::catalog and TableDependency::table: a commit evicts
	//! through it without visiting the entries over other tables
	std::unordered_map<idx_t, std::unordered_map<string, std::unordered_set<const CachedEntry *>>> dependents;
	idx_t total_bytes = 0;
	idx_t total_used_bytes = 0;
	std::atomic<idx_t> sequence {0};
};

} // namespace duckdb
