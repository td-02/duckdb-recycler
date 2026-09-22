#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <string>

namespace duckdb {

//! A single cached join result.
struct CachedResult {
    shared_ptr<ColumnDataCollection> collection; //!< the cached rows
    vector<LogicalType>              types;       //!< column types
    idx_t                            row_count = 0;
};

//! Thread-safe in-memory cache: join-skeleton-key → CachedResult.
class RecyclerCache {
public:
    RecyclerCache() = default;
    RecyclerCache(const RecyclerCache &) = delete;
    RecyclerCache &operator=(const RecyclerCache &) = delete;

    //! Returns true and sets result_out when the key is in cache.
    bool Lookup(const string &key, shared_ptr<CachedResult> &result_out);

    //! Stores a result under key.  Overwrites any existing entry.
    void Store(const string &key, shared_ptr<CachedResult> result);

    //! Human-readable statistics (hits/misses/stores survive Clear()).
    string Stats() const;

    //! Evict all entries.  Counters are NOT reset (matches original).
    void Clear();

    //! Number of cached entries.
    idx_t Size() const;

private:
    mutable std::mutex                               cache_lock;
    std::unordered_map<string, shared_ptr<CachedResult>> entries;
    std::atomic<uint64_t> hits   {0};
    std::atomic<uint64_t> misses {0};
    std::atomic<uint64_t> stores {0};
};

} // namespace duckdb
