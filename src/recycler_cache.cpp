#include "recycler_cache.hpp"
#include <sstream>

namespace duckdb {

bool RecyclerCache::Lookup(const string &key,
                           shared_ptr<CachedResult> &result_out) {
    std::lock_guard<std::mutex> lk(cache_lock);
    auto it = entries.find(key);
    if (it == entries.end()) { ++misses; return false; }
    ++hits;
    result_out = it->second;
    return true;
}

void RecyclerCache::Store(const string &key,
                          shared_ptr<CachedResult> result) {
    std::lock_guard<std::mutex> lk(cache_lock);
    entries[key] = std::move(result);
    ++stores;
}

string RecyclerCache::Stats() const {
    std::lock_guard<std::mutex> lk(cache_lock);
    std::ostringstream os;
    os << "RecyclerCache entries=" << entries.size()
       << " hits="   << hits.load()
       << " misses=" << misses.load()
       << " stores=" << stores.load();
    return os.str();
}

void RecyclerCache::Clear() {
    std::lock_guard<std::mutex> lk(cache_lock);
    entries.clear();
    // Counters intentionally NOT reset: recycler_stats remains meaningful
    // after recycler_clear (matches original behaviour).
}

idx_t RecyclerCache::Size() const {
    std::lock_guard<std::mutex> lk(cache_lock);
    return static_cast<idx_t>(entries.size());
}

} // namespace duckdb
