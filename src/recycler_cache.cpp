#include "recycler_cache.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <algorithm>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Table dependencies
//===----------------------------------------------------------------------===//
bool IsTrackedDatabase(AttachedDatabase &db) {
	return !db.IsSystem() && !db.IsTemporary() && db.GetTransactionManager().IsDuckTransactionManager();
}

string TableKey(const TableCatalogEntry &table) {
	// the schema name's length keeps names that contain dots apart: schema "a.b" with table "c" and schema "a" with
	// table "b.c" would otherwise share a key
	auto &schema = table.schema.name;
	return to_string(table.catalog.GetOid()) + "." + to_string(schema.size()) + ":" + schema + "." + table.name;
}

TableSnapshot TakeTableSnapshot(ClientContext &context, const vector<reference<const TableCatalogEntry>> &tables) {
	TableSnapshot snapshot;
	for (auto &table_ref : tables) {
		auto &table = table_ref.get();
		TableDependency dependency;
		dependency.table = TableKey(table);
		dependency.catalog = table.catalog.GetOid();
		if (table.catalog.IsDuckCatalog()) {
			// every catalog counts its own transactions: the values are only comparable within one catalog, which
			// is all the per-table checks do
			auto &transaction = DuckTransaction::Get(context, table.catalog);
			dependency.start_time = transaction.start_time;
			dependency.catalog_version = transaction.catalog_version;
		}
		snapshot.emplace(table, std::move(dependency));
	}
	return snapshot;
}

vector<TableDependency> TableDependencies(const vector<reference<const TableCatalogEntry>> &tables,
                                          const TableSnapshot &snapshot) {
	vector<TableDependency> result;
	result.reserve(tables.size());
	for (auto &table : tables) {
		result.push_back(snapshot.at(table));
	}
	return result;
}

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

shared_ptr<CachedEntry> RecyclerCache::Lookup(RecyclerEntryKind kind, const NodeInfo &info, const PlanSignatures &sig,
                                              const LogicalOperator &op, const TableSnapshot &snapshot) {
	shared_ptr<CachedEntry> candidate;
	{
		std::lock_guard<std::mutex> guard(lock);
		auto &map = Map(kind);
		auto entry = map.find(info.hash);
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
	if (!IsCurrent(*candidate, info.tables, snapshot)) {
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

void RecyclerCache::AddDependents(const CachedEntry &entry) {
	for (auto &dependency : entry.tables) {
		auto &listed = dependents[dependency.catalog][dependency.table];
		// Erase removes one listing per dependency, so no two dependencies of an entry share a table
		D_ASSERT(listed.find(&entry) == listed.end());
		listed.insert(&entry);
	}
}

void RecyclerCache::Erase(const CachedEntry &entry) {
	for (auto &dependency : entry.tables) {
		auto catalog = dependents.find(dependency.catalog);
		D_ASSERT(catalog != dependents.end());
		auto table = catalog->second.find(dependency.table);
		D_ASSERT(table != catalog->second.end());
		table->second.erase(&entry);
		if (table->second.empty()) {
			catalog->second.erase(table);
			if (catalog->second.empty()) {
				dependents.erase(catalog);
			}
		}
	}
	D_ASSERT(total_bytes >= entry.bytes && total_used_bytes >= entry.used_bytes);
	total_bytes -= entry.bytes;
	total_used_bytes -= entry.used_bytes;
	if (entry.kind == RecyclerEntryKind::BLOCK) {
		auto shape = blocks.find(entry.hash);
		D_ASSERT(shape != blocks.end());
		auto &list = shape->second;
		auto stored = std::find_if(list.begin(), list.end(), [&](const shared_ptr<CachedEntry> &candidate) {
			return candidate.get() == &entry;
		});
		D_ASSERT(stored != list.end());
		list.erase(stored);
		if (list.empty()) {
			blocks.erase(shape);
		}
	} else {
		auto &map = Map(entry.kind);
		auto stored = map.find(entry.hash);
		D_ASSERT(stored != map.end() && stored->second.get() == &entry);
		map.erase(stored);
	}
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
		stats.evictions++;
	}
	return true;
}

TableVersion &RecyclerCache::Version(const PendingWrite &write) {
	auto &catalog = versions[write.catalog];
	return write.table.empty() ? catalog.whole : catalog.tables[write.table];
}

TableVersion &RecyclerCache::AnnouncedVersion(CatalogVersions &catalog, const PendingWrite &write) {
	if (write.table.empty()) {
		D_ASSERT(catalog.whole.writers > 0);
		return catalog.whole;
	}
	auto table = catalog.tables.find(write.table);
	D_ASSERT(table != catalog.tables.end() && table->second.writers > 0);
	return table->second;
}

static bool FreshFor(const TableVersion &version, transaction_t start_time) {
	// a commit id never equals a start time; the stamp of CommitWriter is an upper bound of the commit id, which
	// `<=` treats as a commit too
	return version.writers == 0 && start_time > version.last_commit;
}

bool RecyclerCache::Fresh(const TableDependency &dependency) const {
	auto catalog = versions.find(dependency.catalog);
	if (catalog == versions.end()) {
		return true;
	}
	if (!FreshFor(catalog->second.whole, dependency.start_time)) {
		return false;
	}
	auto &tables = catalog->second.tables;
	auto table = tables.find(dependency.table);
	return table == tables.end() || FreshFor(table->second, dependency.start_time);
}

bool RecyclerCache::Fresh(const vector<TableDependency> &dependencies) const {
	for (auto &dependency : dependencies) {
		if (!Fresh(dependency)) {
			return false;
		}
	}
	return true;
}

bool RecyclerCache::IsCurrent(const CachedEntry &entry, const vector<reference<const TableCatalogEntry>> &tables,
                              const TableSnapshot &snapshot) {
	// a confirmed match pairs the entry's tables with the reader's by position
	D_ASSERT(entry.tables.size() == tables.size());
	std::lock_guard<std::mutex> guard(lock);
	for (idx_t i = 0; i < tables.size(); i++) {
		auto &stored = entry.tables[i];
		auto &reader = snapshot.at(tables[i]);
		// the canonical rendering names the catalog, not the attached database: one detached and attached again
		// under the same name renders alike
		D_ASSERT(stored.catalog != reader.catalog || stored.table == reader.table);
		if (stored.catalog != reader.catalog || stored.catalog_version != reader.catalog_version || !Fresh(stored) ||
		    !Fresh(reader)) {
			stats.blocked_by_version++;
			return false;
		}
	}
	return true;
}

bool RecyclerCache::Admissible(const vector<reference<const TableCatalogEntry>> &tables,
                               const TableSnapshot &snapshot) const {
	std::lock_guard<std::mutex> guard(lock);
	for (auto &table : tables) {
		if (!Fresh(snapshot.at(table))) {
			return false;
		}
	}
	return true;
}

void RecyclerCache::AnnounceWriter(const PendingWrite &write) {
	std::lock_guard<std::mutex> guard(lock);
	Version(write).writers++;
}

//! An entry over `table` whose snapshot does not exceed `commit`, or nullptr
static const CachedEntry *StaleEntry(const string &table, const std::unordered_set<const CachedEntry *> &entries,
                                     transaction_t commit) {
	for (auto entry : entries) {
		for (auto &dependency : entry->tables) {
			if (dependency.table == table && dependency.start_time <= commit) {
				return entry;
			}
		}
	}
	return nullptr;
}

const CachedEntry *RecyclerCache::FirstStale(const PendingWrite &write, transaction_t commit) const {
	auto catalog = dependents.find(write.catalog);
	if (catalog == dependents.end()) {
		return nullptr;
	}
	if (!write.table.empty()) {
		auto table = catalog->second.find(write.table);
		return table == catalog->second.end() ? nullptr : StaleEntry(table->first, table->second, commit);
	}
	for (auto &table : catalog->second) {
		auto stale = StaleEntry(table.first, table.second, commit);
		if (stale) {
			return stale;
		}
	}
	return nullptr;
}

void RecyclerCache::CommitWriter(const PendingWrite &write, transaction_t commit) {
	std::lock_guard<std::mutex> guard(lock);
	auto catalog_versions = versions.find(write.catalog);
	D_ASSERT(catalog_versions != versions.end());
	auto &version = AnnouncedVersion(catalog_versions->second, write);
	version.writers--;
	// two writers of one table read their stamps before taking this lock, so the later caller may hold the
	// smaller one
	version.last_commit = MaxValue(version.last_commit, commit);
	// one entry at a time: collecting them would allocate, and Erase may remove the lists they are found in
	for (auto stale = FirstStale(write, commit); stale; stale = FirstStale(write, commit)) {
		Erase(*stale);
		stats.invalidated_entries++;
	}
}

void RecyclerCache::RetractWriter(const PendingWrite &write) {
	std::lock_guard<std::mutex> guard(lock);
	auto catalog = versions.find(write.catalog);
	D_ASSERT(catalog != versions.end());
	AnnouncedVersion(catalog->second, write).writers--;
	// a version no commit stamped holds nothing; a stamped one is forgotten by a later commit of its catalog
	Forget(catalog, 0);
}

static bool Forgettable(const TableVersion &version, transaction_t lowest_active_start) {
	return version.writers == 0 && (version.last_commit == 0 || version.last_commit < lowest_active_start);
}

void RecyclerCache::Forget(VersionMap::iterator catalog, transaction_t lowest_active_start) {
	auto &tables = catalog->second.tables;
	for (auto table = tables.begin(); table != tables.end();) {
		if (Forgettable(table->second, lowest_active_start)) {
			table = tables.erase(table);
		} else {
			++table;
		}
	}
	if (tables.empty() && Forgettable(catalog->second.whole, lowest_active_start)) {
		versions.erase(catalog);
	}
}

void RecyclerCache::ForgetCommits(idx_t catalog, const DuckTransactionManager &manager) {
	std::lock_guard<std::mutex> guard(lock);
	auto catalog_versions = versions.find(catalog);
	if (catalog_versions == versions.end()) {
		return;
	}
	// read under this lock, after the stamps it is compared with: a transaction that began before one of their
	// commits and is still active holds the lowest start down, and one that begins later starts after them all. A
	// commit no active transaction predates is in every snapshot from now on, and its commit evicted the entries
	// that predate it.
	Forget(catalog_versions, manager.LowestActiveStart());
}

bool RecyclerCache::Store(shared_ptr<CachedEntry> entry, idx_t budget_bytes) {
	std::lock_guard<std::mutex> guard(lock);
	auto &map = Map(entry->kind);
	if (map.find(entry->hash) != map.end()) {
		// never overwrite: a running query may reference the existing collection through the plan
		stats.store_rejected++;
		return false;
	}
	if (!Fresh(entry->tables)) {
		stats.blocked_by_version++;
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
	AddDependents(*entry);
	if (entry->kind == RecyclerEntryKind::WHOLE_RESULT) {
		stats.whole_stores++;
	} else {
		stats.subtree_stores++;
	}
	return true;
}

bool RecyclerCache::StoreBlock(shared_ptr<CachedEntry> entry, idx_t budget_bytes) {
	std::lock_guard<std::mutex> guard(lock);
	auto shape = blocks.find(entry->hash);
	if (shape != blocks.end()) {
		for (auto &existing : shape->second) {
			if (existing->canonical == entry->canonical && existing->conjuncts == entry->conjuncts &&
			    existing->columns == entry->columns) {
				stats.store_rejected++;
				return false;
			}
		}
	}
	if (!Fresh(entry->tables)) {
		stats.blocked_by_version++;
		stats.store_rejected++;
		return false;
	}
	// MakeRoom may evict the other blocks of this shape and erase their list
	if (!MakeRoom(*entry, budget_bytes)) {
		stats.store_rejected++;
		return false;
	}
	entry->stored_at = NextSequence();
	entry->last_used = entry->stored_at;
	total_bytes += entry->bytes;
	total_used_bytes += entry->used_bytes;
	AddDependents(*entry);
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
	dependents.clear();
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

vector<std::pair<SigHash, SignatureHistory>> RecyclerCache::HistoryEntries() const {
	std::lock_guard<std::mutex> guard(lock);
	vector<std::pair<SigHash, SignatureHistory>> result;
	result.reserve(history.size());
	for (auto &hash : history_order) {
		result.emplace_back(hash, history.find(hash)->second.history);
	}
	return result;
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

idx_t RecyclerCache::VersionCount() const {
	std::lock_guard<std::mutex> guard(lock);
	idx_t count = 0;
	for (auto &catalog : versions) {
		count += 1 + catalog.second.tables.size();
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
