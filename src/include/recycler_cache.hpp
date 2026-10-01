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
#include <mutex>
#include <unordered_map>
#include <unordered_set>

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
	std::atomic<idx_t> last_used {0};
	std::atomic<idx_t> hits {0};

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

	//! Bookkeeping about candidate subtrees that were seen but not (yet) cached
	idx_t NoteSeen(const SigHash &hash);
	idx_t Seen(const SigHash &hash) const;
	void NoteTooBig(const SigHash &hash);
	bool IsTooBig(const SigHash &hash) const;

	idx_t TotalBytes() const;
	idx_t EntryCount() const;
	vector<shared_ptr<CachedEntry>> Entries() const;

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

	mutable std::mutex lock;
	EntryMap whole;
	EntryMap subtrees;
	BlockMap blocks;
	std::unordered_map<SigHash, idx_t, SigHashHasher> seen;
	std::unordered_set<SigHash, SigHashHasher> too_big;
	idx_t total_bytes = 0;
	std::atomic<idx_t> sequence {0};
};

} // namespace duckdb
