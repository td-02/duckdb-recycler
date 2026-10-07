#include "recycler_cache.hpp"

namespace duckdb {

const char *RecyclerEntryKindName(RecyclerEntryKind kind) {
	switch (kind) {
	case RecyclerEntryKind::WHOLE_RESULT:
		return "whole_result";
	case RecyclerEntryKind::SUBTREE:
		return "subtree";
	case RecyclerEntryKind::BLOCK:
		return "block";
	case RecyclerEntryKind::PAGINATION:
		return "pagination";
	default:
		return "unknown";
	}
}

double CachedEntry::Benefit() const {
	double saved = est_input_rows > rows ? double(est_input_rows - rows) : 1.0;
	double reuse = 1.0 + double(hits.load());
	return saved * reuse / double(MaxValue<idx_t>(bytes, 1));
}

RecyclerCache::EntryMap &RecyclerCache::Map(RecyclerEntryKind kind) {
	switch (kind) {
	case RecyclerEntryKind::WHOLE_RESULT:
		return whole;
	case RecyclerEntryKind::PAGINATION:
		return paginations;
	default:
		return subtrees;
	}
}

idx_t RecyclerCache::NextSequence() {
	return ++sequence;
}

shared_ptr<CachedEntry> RecyclerCache::Lookup(RecyclerEntryKind kind, const SigHash &hash,
                                              const std::function<string()> &render_canonical) {
	shared_ptr<CachedEntry> candidate;
	{
		std::lock_guard<std::mutex> guard(lock);
		auto &map = Map(kind);
		auto entry = map.find(hash);
		if (entry == map.end()) {
			stats.misses++;
			return nullptr;
		}
		candidate = entry->second;
	}
	// confirm the match outside of the lock (rendering walks the plan)
	auto canonical = render_canonical();
	if (canonical != candidate->canonical) {
		stats.hash_collisions++;
		stats.misses++;
		return nullptr;
	}
	NoteHit(*candidate);
	return candidate;
}

void RecyclerCache::NoteHit(CachedEntry &entry) {
	entry.hits++;
	entry.last_used = NextSequence();
}

shared_ptr<CachedEntry> RecyclerCache::PickVictim() const {
	shared_ptr<CachedEntry> victim;
	auto consider = [&](const shared_ptr<CachedEntry> &candidate) {
		if (!victim || candidate->Benefit() < victim->Benefit() ||
		    (candidate->Benefit() == victim->Benefit() && candidate->last_used < victim->last_used)) {
			victim = candidate;
		}
	};
	for (auto *map : {&whole, &subtrees, &paginations}) {
		for (auto &entry : *map) {
			consider(entry.second);
		}
	}
	for (auto &shape : blocks) {
		for (auto &entry : shape.second) {
			consider(entry);
		}
	}
	return victim;
}

void RecyclerCache::Erase(const CachedEntry &entry) {
	if (entry.kind == RecyclerEntryKind::BLOCK) {
		auto shape = blocks.find(entry.hash);
		if (shape != blocks.end()) {
			auto &list = shape->second;
			for (idx_t i = 0; i < list.size(); i++) {
				if (list[i].get() == &entry) {
					list.erase(list.begin() + static_cast<int64_t>(i));
					break;
				}
			}
			if (list.empty()) {
				blocks.erase(shape);
			}
		}
	} else {
		Map(entry.kind).erase(entry.hash);
	}
	total_bytes -= MinValue<idx_t>(total_bytes, entry.bytes);
	stats.evictions++;
}

bool RecyclerCache::MakeRoom(const CachedEntry &entry, idx_t budget_bytes) {
	if (entry.bytes > budget_bytes) {
		return false;
	}
	while (total_bytes + entry.bytes > budget_bytes) {
		auto victim = PickVictim();
		if (!victim) {
			break;
		}
		if (victim->Benefit() > entry.Benefit()) {
			// the cache is full of more valuable results
			return false;
		}
		Erase(*victim);
	}
	return true;
}

bool RecyclerCache::Store(shared_ptr<CachedEntry> entry, idx_t budget_bytes) {
	std::lock_guard<std::mutex> guard(lock);
	auto &map = Map(entry->kind);
	if (map.find(entry->hash) != map.end()) {
		// never overwrite: a running query may reference the existing collection through the plan
		stats.store_rejected++;
		return false;
	}
	if (!MakeRoom(*entry, budget_bytes)) {
		stats.store_rejected++;
		return false;
	}
	entry->last_used = NextSequence();
	total_bytes += entry->bytes;
	map[entry->hash] = entry;
	if (entry->kind == RecyclerEntryKind::WHOLE_RESULT) {
		stats.whole_stores++;
	} else {
		stats.subtree_stores++;
	}
	return true;
}

shared_ptr<CachedEntry> RecyclerCache::LookupPagination(const SigHash &hash,
                                                        const std::function<string()> &render_canonical,
                                                        idx_t needed_rows, idx_t &existing_rows) {
	existing_rows = 0;
	shared_ptr<CachedEntry> candidate;
	{
		std::lock_guard<std::mutex> guard(lock);
		auto entry = paginations.find(hash);
		if (entry == paginations.end()) {
			stats.misses++;
			return nullptr;
		}
		candidate = entry->second;
	}
	if (render_canonical() != candidate->canonical) {
		stats.hash_collisions++;
		stats.misses++;
		return nullptr;
	}
	if (!candidate->complete && candidate->rows < needed_rows) {
		existing_rows = candidate->rows;
		stats.misses++;
		return nullptr;
	}
	NoteHit(*candidate);
	return candidate;
}

bool RecyclerCache::StorePagination(shared_ptr<CachedEntry> entry, idx_t budget_bytes) {
	std::lock_guard<std::mutex> guard(lock);
	auto &map = paginations;
	auto existing = map.find(entry->hash);
	shared_ptr<CachedEntry> older;
	if (existing != map.end()) {
		older = existing->second;
		// only a longer prefix (or the completed result) replaces what is there; a concurrent writer may have won
		bool grows = older->canonical == entry->canonical &&
		             ((entry->complete && !older->complete) || entry->rows > older->rows);
		if (!grows) {
			stats.store_rejected++;
			return false;
		}
		// the family keeps its reuse history, so that growing does not make it a cold eviction candidate
		entry->hits = older->hits.load();
		// take the older entry out of the accounting while room is made; it is not an eviction
		total_bytes -= MinValue<idx_t>(total_bytes, older->bytes);
		map.erase(existing);
	}
	if (!MakeRoom(*entry, budget_bytes)) {
		if (older) {
			// nothing changed for readers: the older prefix is back, and so are its bytes
			total_bytes += older->bytes;
			map[older->hash] = older;
		}
		stats.store_rejected++;
		return false;
	}
	entry->last_used = NextSequence();
	total_bytes += entry->bytes;
	map[entry->hash] = entry;
	stats.subtree_stores++;
	return true;
}

bool RecyclerCache::StoreBlock(shared_ptr<CachedEntry> entry, idx_t budget_bytes) {
	std::lock_guard<std::mutex> guard(lock);
	auto &list = blocks[entry->hash];
	for (auto &existing : list) {
		if (existing->canonical == entry->canonical && existing->conjuncts == entry->conjuncts &&
		    existing->columns == entry->columns) {
			stats.store_rejected++;
			return false;
		}
	}
	if (!MakeRoom(*entry, budget_bytes)) {
		if (list.empty()) {
			blocks.erase(entry->hash);
		}
		stats.store_rejected++;
		return false;
	}
	entry->last_used = NextSequence();
	total_bytes += entry->bytes;
	blocks[entry->hash].push_back(entry);
	stats.block_stores++;
	return true;
}

vector<shared_ptr<CachedEntry>> RecyclerCache::LookupBlocks(const SigHash &hash) const {
	std::lock_guard<std::mutex> guard(lock);
	auto shape = blocks.find(hash);
	if (shape == blocks.end()) {
		return {};
	}
	return shape->second;
}

void RecyclerCache::Clear() {
	std::lock_guard<std::mutex> guard(lock);
	whole.clear();
	subtrees.clear();
	paginations.clear();
	blocks.clear();
	seen.clear();
	too_big.clear();
	total_bytes = 0;
	stats.clears++;
}

idx_t RecyclerCache::NoteSeen(const SigHash &hash) {
	std::lock_guard<std::mutex> guard(lock);
	return ++seen[hash];
}

idx_t RecyclerCache::Seen(const SigHash &hash) const {
	std::lock_guard<std::mutex> guard(lock);
	auto entry = seen.find(hash);
	return entry == seen.end() ? 0 : entry->second;
}

void RecyclerCache::NoteTooBig(const SigHash &hash) {
	std::lock_guard<std::mutex> guard(lock);
	too_big.insert(hash);
	stats.too_big++;
}

bool RecyclerCache::IsTooBig(const SigHash &hash) const {
	std::lock_guard<std::mutex> guard(lock);
	return too_big.find(hash) != too_big.end();
}

idx_t RecyclerCache::TotalBytes() const {
	std::lock_guard<std::mutex> guard(lock);
	return total_bytes;
}

idx_t RecyclerCache::EntryCount() const {
	std::lock_guard<std::mutex> guard(lock);
	idx_t count = whole.size() + subtrees.size() + paginations.size();
	for (auto &shape : blocks) {
		count += shape.second.size();
	}
	return count;
}

vector<shared_ptr<CachedEntry>> RecyclerCache::Entries() const {
	std::lock_guard<std::mutex> guard(lock);
	vector<shared_ptr<CachedEntry>> result;
	for (auto *map : {&whole, &subtrees, &paginations}) {
		for (auto &entry : *map) {
			result.push_back(entry.second);
		}
	}
	for (auto &shape : blocks) {
		for (auto &entry : shape.second) {
			result.push_back(entry);
		}
	}
	return result;
}

} // namespace duckdb
