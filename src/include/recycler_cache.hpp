#pragma once
//===----------------------------------------------------------------------===//
// recycler_cache.hpp - the shared, bounded cache of materialized results
//===----------------------------------------------------------------------===//
#include "recycler_signature.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <atomic>
#include <list>
#include <mutex>
#include <unordered_map>

namespace duckdb {

enum class RecyclerEntryKind : uint8_t {
	//! The complete result of a query (keyed on the pre-optimization plan)
	WHOLE_RESULT = 0,
	//! The output of a subtree of an optimized plan
	SUBTREE = 1,
	//! The output of an SPJ block keyed on its shape; reusable by narrower blocks (predicate/column subsumption)
	BLOCK = 2
};

const char *RecyclerEntryKindName(RecyclerEntryKind kind);

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
	std::atomic<uint64_t> pre_ns {0};
	std::atomic<uint64_t> post_ns {0};
	//! why the pre hook declined to take part (diagnostics)
	std::atomic<uint64_t> skip_not_query {0};
	std::atomic<uint64_t> skip_modifying {0};
	std::atomic<uint64_t> skip_not_read {0};
	std::atomic<uint64_t> skip_disabled {0};
	std::atomic<uint64_t> skip_modified_databases {0};
	std::atomic<uint64_t> skip_write_transaction {0};
	std::atomic<uint64_t> skip_analyze_failed {0};
	std::atomic<uint64_t> root_not_cacheable {0};
	//! root_not_cacheable by reason
	std::atomic<uint64_t> refused[static_cast<idx_t>(RefusalReason::COUNT)] {};
};

class RecyclerCache {
public:
	//! Look up the entry for the subtree rooted at `op`; a hash match is confirmed by rendering the subtree and
	//! comparing the canonical byte string
	shared_ptr<CachedEntry> Lookup(RecyclerEntryKind kind, const SigHash &hash, const PlanSignatures &sig,
	                               const LogicalOperator &op);
	//! Insert an entry (never overwrites); evicts lower-benefit entries to respect the budget. Returns success.
	bool Store(shared_ptr<CachedEntry> entry, idx_t budget_bytes);
	//! Insert a BLOCK entry under its shape hash (several entries per shape may coexist)
	bool StoreBlock(shared_ptr<CachedEntry> entry, idx_t budget_bytes);
	//! All BLOCK entries with this shape hash
	vector<shared_ptr<CachedEntry>> LookupBlocks(const SigHash &hash) const;
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
	void Erase(const CachedEntry &entry);
	//! The history of `hash`, created and moved to the front of the LRU order (caller holds the lock)
	SignatureHistory &History(const SigHash &hash);

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
	idx_t total_bytes = 0;
	idx_t total_used_bytes = 0;
	std::atomic<idx_t> sequence {0};
};

} // namespace duckdb
