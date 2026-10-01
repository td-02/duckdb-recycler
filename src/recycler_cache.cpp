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
	return kind == RecyclerEntryKind::WHOLE_RESULT ? whole : subtrees;
}

idx_t RecyclerCache::NextSequence() {
	return ++sequence;
}

shared_ptr<CachedEntry> RecyclerCache::Lookup(RecyclerEntryKind kind, const SigHash &hash, const PlanSignatures &sig,
                                              const LogicalOperator &op) {
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
	auto canonical = sig.Render(op);
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
	for (auto *map : {&whole, &subtrees}) {
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
	D_ASSERT(total_bytes >= entry.bytes && total_used_bytes >= entry.used_bytes);
	total_bytes -= entry.bytes;
	total_used_bytes -= entry.used_bytes;
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
	entry->stored_at = NextSequence();
	entry->last_used = entry->stored_at;
	total_bytes += entry->bytes;
	total_used_bytes += entry->used_bytes;
	map[entry->hash] = entry;
	if (entry->kind == RecyclerEntryKind::WHOLE_RESULT) {
		stats.whole_stores++;
	} else {
		stats.subtree_stores++;
	}
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
	entry->stored_at = NextSequence();
	entry->last_used = entry->stored_at;
	total_bytes += entry->bytes;
	total_used_bytes += entry->used_bytes;
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
	blocks.clear();
	history.clear();
	history_order.clear();
	total_bytes = 0;
	total_used_bytes = 0;
	stats.clears++;
}

SignatureHistory &RecyclerCache::History(const SigHash &hash) {
	auto node = history.find(hash);
	if (node == history.end()) {
		if (history.size() == HISTORY_BOUND) {
			history.erase(history_order.back());
			history_order.pop_back();
		}
		history_order.push_front(hash);
		node = history.emplace(hash, HistoryNode {SignatureHistory(), history_order.begin()}).first;
	} else {
		history_order.splice(history_order.begin(), history_order, node->second.position);
	}
	node->second.history.last_seen = NextSequence();
	return node->second.history;
}

idx_t RecyclerCache::NoteSeen(const SigHash &hash) {
	std::lock_guard<std::mutex> guard(lock);
	return ++History(hash).seen;
}

idx_t RecyclerCache::Seen(const SigHash &hash) const {
	std::lock_guard<std::mutex> guard(lock);
	auto node = history.find(hash);
	return node == history.end() ? 0 : node->second.history.seen;
}

void RecyclerCache::NoteMaterialized(const SigHash &hash, idx_t rows, idx_t bytes, uint64_t measured_cost_ns,
                                     bool too_big) {
	if (too_big) {
		stats.too_big++;
	}
	std::lock_guard<std::mutex> guard(lock);
	auto &entry = History(hash);
	entry.rows = rows;
	entry.bytes = bytes;
	entry.measured_cost_ns = measured_cost_ns;
}

bool RecyclerCache::TooBig(const SigHash &hash, idx_t cap_rows, idx_t cap_bytes) const {
	std::lock_guard<std::mutex> guard(lock);
	auto node = history.find(hash);
	if (node == history.end()) {
		return false;
	}
	auto &entry = node->second.history;
	return entry.rows > cap_rows || entry.bytes > cap_bytes;
}

void RecyclerCache::NoteMaterializeTime(const SigHash &hash, uint64_t measured_cost_ns, uint64_t materialize_ns) {
	std::lock_guard<std::mutex> guard(lock);
	auto &entry = History(hash);
	entry.measured_cost_ns = measured_cost_ns;
	entry.materialize_ns = materialize_ns;
}

void RecyclerCache::NoteHitTime(const SigHash &hash, uint64_t hit_ns) {
	std::lock_guard<std::mutex> guard(lock);
	History(hash).hit_ns = hit_ns;
}

idx_t RecyclerCache::HistorySize() const {
	std::lock_guard<std::mutex> guard(lock);
	return history.size();
}

idx_t RecyclerCache::TotalBytes() const {
	std::lock_guard<std::mutex> guard(lock);
	return total_bytes;
}

idx_t RecyclerCache::UsedBytes() const {
	std::lock_guard<std::mutex> guard(lock);
	return total_used_bytes;
}

idx_t RecyclerCache::EntryCount() const {
	std::lock_guard<std::mutex> guard(lock);
	idx_t count = whole.size() + subtrees.size();
	for (auto &shape : blocks) {
		count += shape.second.size();
	}
	return count;
}

vector<shared_ptr<CachedEntry>> RecyclerCache::Entries() const {
	std::lock_guard<std::mutex> guard(lock);
	vector<shared_ptr<CachedEntry>> result;
	for (auto *map : {&whole, &subtrees}) {
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
