//===----------------------------------------------------------------------===//
// recycler_functions.cpp - registration, settings, introspection functions and the connection callbacks
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"
#include "recycler_operators.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

optional_ptr<RecyclerState> GetRecyclerState(ClientContext &context) {
	for (auto &ext : OptimizerExtension::Iterate(context)) {
		if (!ext.optimizer_info) {
			continue;
		}
		auto state = dynamic_cast<RecyclerState *>(ext.optimizer_info.get());
		if (state) {
			return state;
		}
	}
	return nullptr;
}

//===----------------------------------------------------------------------===//
// Connection state and the end-of-query callback
//===----------------------------------------------------------------------===//
RecyclerClientState::RecyclerClientState(RecyclerState &state) : predates_load(true) {
	state.predating++;
}

RecyclerClientState &RecyclerClientState::Get(ClientContext &context) {
	return *context.registered_state->GetOrCreate<RecyclerClientState>("recycler");
}

void RecyclerClientState::NoteTouched(idx_t operator_id, shared_ptr<CachedEntry> entry) {
	if (!profiled) {
		return;
	}
	std::lock_guard<std::mutex> guard(touched_lock);
	touched.push_back(TouchedEntry {operator_id, std::move(entry)});
}

//! A RECYCLER_MATERIALIZE or RECYCLER_SCAN node of the profiler tree
struct ProfiledOperator {
	bool materialize;
	idx_t id;
	//! Operator thread time of the node and of the operators below it, in nanoseconds
	uint64_t own_ns;
	uint64_t below_ns;
};

static uint64_t ToNs(double seconds) {
	return uint64_t(seconds * 1e9);
}

//! The "Id" parameter of a recycler operator from the EXTRA_INFO map of its profiler node
static bool ProfiledId(const ProfilingInfo &info, idx_t &id) {
	auto extra = info.metrics.find(MetricType::EXTRA_INFO);
	if (extra == info.metrics.end()) {
		return false;
	}
	for (auto &pair : MapValue::GetChildren(extra->second)) {
		auto &fields = StructValue::GetChildren(pair);
		if (StringValue::Get(fields[0]) == "Id") {
			id = std::stoull(StringValue::Get(fields[1]));
			return true;
		}
	}
	return false;
}

//! Collects the recycler operators of the subtree; returns the subtree's summed operator thread time in seconds
static double CollectProfiledOperators(const ProfilingNode &node, vector<ProfiledOperator> &out) {
	double below = 0;
	for (auto &child : node.children) {
		below += CollectProfiledOperators(*child, out);
	}
	auto &info = node.GetProfilingInfo();
	auto timing = info.metrics.find(MetricType::OPERATOR_TIMING);
	double own = timing == info.metrics.end() ? 0 : timing->second.GetValue<double>();
	auto name = info.metrics.find(MetricType::OPERATOR_NAME);
	if (name != info.metrics.end()) {
		auto &op_name = StringValue::Get(name->second);
		bool materialize = op_name == "RECYCLER_MATERIALIZE";
		if (materialize || op_name == "RECYCLER_SCAN") {
			ProfiledOperator op;
			op.materialize = materialize;
			if (ProfiledId(info, op.id)) {
				op.own_ns = ToNs(own);
				op.below_ns = ToNs(below);
				out.push_back(op);
			}
		}
	}
	return below + own;
}

void RecyclerClientState::QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
	// the post hook resets the state, but never runs when an optimizer between the hooks throws: the snapshot's
	// catalog-entry references must not outlive the statement either way
	Reset();
	if (!profiled) {
		return;
	}
	profiled = false;
	vector<TouchedEntry> entries;
	{
		std::lock_guard<std::mutex> guard(touched_lock);
		entries.swap(touched);
	}
	if (error || entries.empty()) {
		// a failed query has no complete tree; without a touched entry the tree holds nothing to pair
		return;
	}
	// runs after the query's latency was fixed (section 6.5 of the plan); the profiler's tree of this query stays
	// readable until the next profiled query starts
	try {
		auto root = QueryProfiler::Get(context).GetRoot();
		D_ASSERT(root);
		vector<ProfiledOperator> operators;
		CollectProfiledOperators(*root, operators);
		auto &cache = *GetRecyclerState(context)->cache;
		for (auto &op : operators) {
			for (auto &touched : entries) {
				if (touched.operator_id != op.id) {
					continue;
				}
				auto &entry = *touched.entry;
				if (op.materialize) {
					entry.measured_cost_ns = op.below_ns;
					entry.materialize_ns = op.own_ns;
					cache.NoteMaterializeTime(entry.hash, op.below_ns, op.own_ns);
				} else {
					entry.hit_ns = op.own_ns;
					cache.NoteHitTime(entry.hash, op.own_ns);
				}
				break;
			}
		}
	} catch (std::exception &) {
		// a profiler tree without the metrics we read; the measurement is a by-product, never a failure
	}
}

//===----------------------------------------------------------------------===//
// Write announcements and the transaction callbacks
//===----------------------------------------------------------------------===//
// Every write is announced before it runs, so that no reader can see a commit before the cache knows about it: the
// pre hook names the target table or the modified databases, QueryBegin and OnExecutePrepared announce the
// statements that run without the pre hook, and the transaction callbacks settle the announcements. A physical
// append through an InternalAppender plans no statement of its own and marks no database as modified, so it is seen
// only through the statement that runs it: dbgen and dsdgen register the database they fill at bind, which the pre
// hook announces as a whole. An append whose statement registers no modification is not seen at all, and the
// entries over its table are still served after it commits.
void RecyclerClientState::AnnounceWrite(RecyclerCache &cache, PendingWrite write) {
	for (auto &pending : pending_writes) {
		if (pending.catalog == write.catalog && pending.table == write.table) {
			return;
		}
	}
	pending_writes.push_back(std::move(write));
	cache.AnnounceWriter(pending_writes.back());
}

bool RecyclerClientState::MayWriteUnannounced(idx_t catalog) const {
	bool unplanned = false;
	bool announced = false;
	for (auto &write : pending_writes) {
		if (write.catalog == catalog) {
			if (write.table.empty()) {
				return false;
			}
			announced = true;
		}
		unplanned = unplanned || write.IsUnplanned();
	}
	return unplanned || !announced;
}

//! The database the transaction modified, when the cache can stamp its commits (a DuckDB database): writes to the
//! temporary catalog do not set it, and no entry reads a table of a catalog other than DuckDB's
static optional_ptr<AttachedDatabase> StampableDatabase(MetaTransaction &transaction) {
	auto db = transaction.ModifiedDatabase();
	if (db && !db->GetTransactionManager().IsDuckTransactionManager()) {
		return nullptr;
	}
	return db;
}

void RecyclerClientState::QueryBegin(ClientContext &context) {
	// a statement the extension optimizer does not see runs without the pre hook, so which tables it writes is
	// unknown: until its transaction ends no connection is served or stores, and the whole database the transaction
	// modified is stamped with its commit (see TransactionCommit for a statement that commits together with it)
	bool unplanned = !ClientConfig::GetConfig(context).enable_optimizer ||
	                 Optimizer::OptimizerDisabled(context, OptimizerType::EXTENSION);
	// a database this transaction may have written without an announcement (by an unplanned statement, or by one
	// that ran before LOAD registered this state) is announced as a whole before the transaction can commit
	auto db = StampableDatabase(MetaTransaction::Get(context));
	if (!unplanned && (!db || !MayWriteUnannounced(db->oid))) {
		return;
	}
	auto &cache = *GetRecyclerState(context)->cache;
	if (unplanned) {
		AnnounceWrite(cache, PendingWrite::Unplanned());
	}
	if (db && MayWriteUnannounced(db->oid)) {
		AnnounceWrite(cache, PendingWrite {db->oid, string()});
	}
}

static bool HasRecyclerOperator(const PhysicalOperator &op) {
	if (AsRecyclerOperator(op)) {
		return true;
	}
	for (auto &child : op.GetChildren()) {
		if (HasRecyclerOperator(child.get())) {
			return true;
		}
	}
	return false;
}

RebindQueryInfo RecyclerClientState::OnExecutePrepared(ClientContext &context, PreparedStatementCallbackInfo &info,
                                                       RebindQueryInfo current_rebind) {
	// the physical plan names no logical write target, so every database the statement modifies is announced as a
	// whole; when DuckDB rebinds the statement, the pre hook announces again (AnnounceWrite deduplicates)
	auto &modified = info.prepared_statement.properties.modified_databases;
	if (!modified.empty()) {
		auto &cache = *GetRecyclerState(context)->cache;
		for (auto &database : modified) {
			AnnounceWrite(cache, PendingWrite {database.second.identity.catalog_oid, string()});
		}
	}
	// the hooks rewrote the plan under the snapshot of the transaction that prepared it: a scan would keep serving
	// its entry after a commit evicted it, and a write would record that snapshot. Planned again, the hooks decide
	// under the executing transaction's. A statement prepared before all its parameters were bound, or whose plan
	// DuckDB refuses to cache, has no physical plan: RequireRebind already asked to plan it again.
	auto &plan = info.prepared_statement.physical_plan;
	D_ASSERT(plan || current_rebind == RebindQueryInfo::ATTEMPT_TO_REBIND);
	if (plan && HasRecyclerOperator(plan->Root())) {
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
	return current_rebind;
}

void RecyclerClientState::StampUnstampedCommit(RecyclerCache &cache) {
	if (unstamped_catalog == DConstants::INVALID_INDEX) {
		return;
	}
	PendingWrite write {unstamped_catalog, string()};
	cache.AnnounceWriter(write);
	cache.CommitWriter(write, unstamped_commit);
	cache.RetractWriter(PendingWrite::Unplanned());
	unstamped_catalog = DConstants::INVALID_INDEX;
}

void RecyclerClientState::EndPredating(RecyclerState &state) {
	if (!predates_load) {
		return;
	}
	predates_load = false;
	state.predating--;
}

void RecyclerClientState::TransactionBegin(MetaTransaction &transaction, ClientContext &context) {
	auto &state = *GetRecyclerState(context);
	StampUnstampedCommit(*state.cache);
	// a connection idle at LOAD ended the transaction it was in before this one
	EndPredating(state);
	transaction_tracked = state.predating == 0;
}

void RecyclerClientState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	// the announcements of writes to a database the cache cannot stamp are only retracted
	auto db = StampableDatabase(transaction);
	if (!db && pending_writes.empty() && !predates_load) {
		return;
	}
	// this runs after DuckDB made the commit durable: nothing here allocates, since a throw would report a failed
	// commit for a transaction that committed, and leave its writes announced. Every version it settles exists since
	// the write's announcement.
	auto &state = *GetRecyclerState(context);
	auto &cache = *state.cache;
	transaction_t commit = 0;
	if (db) {
		// the catalog's DuckTransaction is gone by now (the manager removed it before this callback), so the commit
		// id is bounded from above by the catalog's last commit, which the manager assigned right after drawing the
		// id under the same lock
		commit = DuckTransactionManager::Get(*db).GetLastCommit();
		// a write that ran before LOAD registered this state needs no stamp: no transaction that began before this
		// commit is tracked
		if (MayWriteUnannounced(db->oid) && !predates_load) {
			// a write no hook announced, by an unplanned statement that committed together with it (QueryBegin
			// announced the database for every later statement): the tables it wrote are unknown and stamping its
			// whole database may allocate the database's version, so an unplanned writer (whose version always
			// exists) withholds every entry until this connection's next transaction stamps the database
			D_ASSERT(unstamped_catalog == DConstants::INVALID_INDEX);
			cache.AnnounceWriter(PendingWrite::Unplanned());
			unstamped_catalog = db->oid;
			unstamped_commit = commit;
		}
	}
	for (auto &write : pending_writes) {
		if (db && write.catalog == db->oid) {
			cache.CommitWriter(write, commit);
		} else {
			cache.RetractWriter(write);
		}
	}
	pending_writes.clear();
	EndPredating(state);
}

void RecyclerClientState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	if (pending_writes.empty() && !predates_load) {
		return;
	}
	auto &state = *GetRecyclerState(context);
	for (auto &write : pending_writes) {
		state.cache->RetractWriter(write);
	}
	pending_writes.clear();
	EndPredating(state);
}

//===----------------------------------------------------------------------===//
// Connection lifetime
//===----------------------------------------------------------------------===//
class RecyclerConnectionCallback : public ExtensionCallback {
public:
	explicit RecyclerConnectionCallback(shared_ptr<RecyclerState> state_p) : state(std::move(state_p)) {
	}

	void OnConnectionOpened(ClientContext &context) override {
		// QueryBegin and the transaction callbacks reach registered states only: the state must see the first
		// TransactionBegin, and the pre hook that registers it otherwise never runs on a connection whose
		// optimizer is disabled from its start, yet its writes must be announced
		RecyclerClientState::Get(context);
	}

	void OnConnectionClosed(ClientContext &context) override {
		// a connection that closes without another transaction settles what its last one left. LOAD may register a
		// closing connection: whichever of the two creates its state, the predating count is released.
		auto &client = RecyclerClientState::Get(context);
		client.StampUnstampedCommit(*state->cache);
		client.EndPredating(*state);
		// cached ColumnDataCollections are buffer-managed. DatabaseInstance destroys the buffer manager before the
		// DBConfig that (transitively) owns this cache, so the collections must be released while the last
		// connection closes.
		auto &db = DatabaseInstance::GetDatabase(context);
		if (db.GetConnectionManager().GetConnectionCount() <= 1) {
			state->cache->Clear();
		}
	}

private:
	shared_ptr<RecyclerState> state;
};

//===----------------------------------------------------------------------===//
// recycler_stats()
//===----------------------------------------------------------------------===//
struct RecyclerStatsGlobalState : public GlobalTableFunctionState {
	vector<std::pair<string, int64_t>> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> RecyclerStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::BIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> RecyclerStatsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<RecyclerStatsGlobalState>();
	auto state = GetRecyclerState(context);
	if (state) {
		auto &cache = *state->cache;
		auto &s = cache.stats;
		auto add = [&](const char *name, uint64_t value) {
			result->rows.emplace_back(name, int64_t(value));
		};
		add("entries", cache.EntryCount());
		add("bytes", cache.TotalBytes());
		add("bytes_used", cache.UsedBytes());
		add("pre_calls", s.pre_calls);
		add("post_calls", s.post_calls);
		add("whole_hits", s.whole_hits);
		add("block_hits", s.block_hits);
		add("subtree_hits", s.subtree_hits);
		add("misses", s.misses);
		add("whole_stores", s.whole_stores);
		add("subtree_stores", s.subtree_stores);
		add("writes_planned", s.writes_planned);
		add("blocks_planted", s.blocks_planted);
		add("block_stores", s.block_stores);
		add("subsumption_hits", s.subsumption_hits);
		add("store_rejected", s.store_rejected);
		add("too_big", s.too_big);
		add("evictions", s.evictions);
		add("hash_collisions", s.hash_collisions);
		add("clears", s.clears);
		add("invalidated_entries", s.invalidated_entries);
		add("blocked_by_version", s.blocked_by_version);
		add("announced_table_writes", s.announced_table_writes);
		add("pre_ns", s.pre_ns);
		add("post_ns", s.post_ns);
		add("skip_not_query", s.skip_not_query);
		add("skip_modifying", s.skip_modifying);
		add("skip_not_read", s.skip_not_read);
		add("skip_disabled", s.skip_disabled);
		add("skip_modified_databases", s.skip_modified_databases);
		add("skip_write_transaction", s.skip_write_transaction);
		add("skip_untracked_transaction", s.skip_untracked_transaction);
		add("skip_analyze_failed", s.skip_analyze_failed);
		add("root_not_cacheable", s.root_not_cacheable);
		add("swap_bytes", BufferManager::GetBufferManager(context).GetUsedSwap());
		add("history_entries", cache.HistorySize());
		for (idx_t reason = 1; reason < static_cast<idx_t>(RefusalReason::COUNT); reason++) {
			result->rows.emplace_back(string("refused_") + RefusalReasonName(static_cast<RefusalReason>(reason)),
			                          int64_t(s.refused[reason].load()));
		}
	}
	return std::move(result);
}

static void RecyclerStatsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<RecyclerStatsGlobalState>();
	idx_t count = 0;
	while (data.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = data.rows[data.offset++];
		output.SetValue(0, count, Value(row.first));
		output.SetValue(1, count, Value::BIGINT(row.second));
		count++;
	}
	output.SetCardinality(count);
}

//===----------------------------------------------------------------------===//
// recycler_entries()
//===----------------------------------------------------------------------===//
struct RecyclerEntriesGlobalState : public GlobalTableFunctionState {
	vector<shared_ptr<CachedEntry>> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> RecyclerEntriesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("hits");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("ordered");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("est_input_rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("used_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("measured_cost_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("materialize_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("hit_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("sink_calls");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("stored_at");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("last_used");
	return_types.emplace_back(LogicalType::BIGINT);
	return nullptr;
}

static Value Milliseconds(uint64_t ns) {
	return Value::DOUBLE(double(ns) / 1e6);
}

static unique_ptr<GlobalTableFunctionState> RecyclerEntriesInit(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto result = make_uniq<RecyclerEntriesGlobalState>();
	auto state = GetRecyclerState(context);
	if (state) {
		result->entries = state->cache->Entries();
	}
	return std::move(result);
}

static void RecyclerEntriesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<RecyclerEntriesGlobalState>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = *data.entries[data.offset++];
		output.SetValue(0, count, Value(RecyclerEntryKindName(entry.kind)));
		output.SetValue(1, count, Value(entry.hash.ToString()));
		output.SetValue(2, count, Value::BIGINT(int64_t(entry.rows)));
		output.SetValue(3, count, Value::BIGINT(int64_t(entry.bytes)));
		output.SetValue(4, count, Value::BIGINT(int64_t(entry.hits.load())));
		output.SetValue(5, count, Value::BOOLEAN(entry.ordered));
		output.SetValue(6, count, Value::BIGINT(int64_t(entry.est_input_rows)));
		output.SetValue(7, count, Value::BIGINT(int64_t(entry.used_bytes)));
		output.SetValue(8, count, Milliseconds(entry.measured_cost_ns));
		output.SetValue(9, count, Milliseconds(entry.materialize_ns));
		output.SetValue(10, count, Milliseconds(entry.hit_ns));
		output.SetValue(11, count, Value::BIGINT(int64_t(entry.sink_calls)));
		output.SetValue(12, count, Value::BIGINT(int64_t(entry.stored_at)));
		output.SetValue(13, count, Value::BIGINT(int64_t(entry.last_used.load())));
		count++;
	}
	output.SetCardinality(count);
}

//===----------------------------------------------------------------------===//
// recycler_history()
//===----------------------------------------------------------------------===//
struct RecyclerHistoryGlobalState : public GlobalTableFunctionState {
	vector<std::pair<SigHash, SignatureHistory>> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> RecyclerHistoryBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("seen");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("last_seen");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("measured_cost_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("materialize_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("hit_ms");
	return_types.emplace_back(LogicalType::DOUBLE);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> RecyclerHistoryInit(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto result = make_uniq<RecyclerHistoryGlobalState>();
	auto state = GetRecyclerState(context);
	if (state) {
		result->entries = state->cache->HistoryEntries();
	}
	return std::move(result);
}

static void RecyclerHistoryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<RecyclerHistoryGlobalState>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		auto &history = entry.second;
		output.SetValue(0, count, Value(entry.first.ToString()));
		output.SetValue(1, count, Value::BIGINT(int64_t(history.seen)));
		output.SetValue(2, count, Value::BIGINT(int64_t(history.last_seen)));
		output.SetValue(3, count, Value::BIGINT(int64_t(history.rows)));
		output.SetValue(4, count, Value::BIGINT(int64_t(history.bytes)));
		output.SetValue(5, count, Milliseconds(history.measured_cost_ns));
		output.SetValue(6, count, Milliseconds(history.materialize_ns));
		output.SetValue(7, count, Milliseconds(history.hit_ns));
		count++;
	}
	output.SetCardinality(count);
}

//===----------------------------------------------------------------------===//
// PRAGMA recycler_clear
//===----------------------------------------------------------------------===//
static void RecyclerClearPragma(ClientContext &context, const FunctionParameters &parameters) {
	auto state = GetRecyclerState(context);
	if (state) {
		state->cache->Clear();
	}
}

//===----------------------------------------------------------------------===//
// Registration
//===----------------------------------------------------------------------===//
//! Whether a transaction (or a checkpoint) is active on a database whose commits the cache stamps
static bool HasOpenTransaction(DatabaseInstance &db) {
	for (auto &database : DatabaseManager::Get(db).GetDatabases()) {
		if (database->IsSystem() || database->IsTemporary() ||
		    !database->GetTransactionManager().IsDuckTransactionManager()) {
			continue;
		}
		// the manager sets the lowest active start to a start time while a transaction is active, and to at least
		// TRANSACTION_ID_START when none is
		if (DuckTransactionManager::Get(*database).LowestActiveStart() < TRANSACTION_ID_START) {
			return true;
		}
	}
	return false;
}

void LoadRecycler(ExtensionLoader &loader) {
	auto state = make_shared_ptr<RecyclerState>();

	OptimizerExtension optimizer_extension;
	optimizer_extension.pre_optimize_function = RecyclerPreOptimize;
	optimizer_extension.optimize_function = RecyclerOptimize;
	optimizer_extension.optimizer_info = state;

	auto &db = loader.GetDatabaseInstance();
	ExtensionCallbackManager::Get(db).Register(std::move(optimizer_extension));
	ExtensionCallbackManager::Get(db).Register(make_shared_ptr<RecyclerConnectionCallback>(state));
	// the connections opened before LOAD: from now on their writes are announced. A transaction open on a database
	// now may already have written, unannounced, and commit before its connection's next callback: then until every
	// connection open now has ended the transaction it is in (an idle one: until it runs a statement or closes), no
	// transaction that begins is tracked. A statement planned before the hooks were registered has bound, and so
	// started its transaction on every database it writes, before this check.
	bool predating = HasOpenTransaction(db);
	for (auto &connection : db.GetConnectionManager().GetConnectionList()) {
		if (predating) {
			connection->registered_state->GetOrCreate<RecyclerClientState>("recycler", *state);
		} else {
			RecyclerClientState::Get(*connection);
		}
	}
	state->predating--;

	auto &config = DBConfig::GetConfig(db);
	RecyclerSettings defaults;
	config.AddExtensionOption("recycler_enabled", "Enable query recycling", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(defaults.enabled));
	config.AddExtensionOption("recycler_whole_result", "Cache and serve complete query results",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(defaults.whole_result));
	config.AddExtensionOption("recycler_blocks", "Serve nested query blocks from cached query results",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(defaults.blocks));
	config.AddExtensionOption("recycler_subtrees", "Cache and reuse intermediate results of optimized plans",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(defaults.subtrees));
	config.AddExtensionOption("recycler_subsumption",
	                          "Reuse cached SPJ blocks for narrower predicates and column subsets (predicate subsumption)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(defaults.subsumption));
	config.AddExtensionOption("recycler_budget_mb", "Memory budget of the recycler cache in MB", LogicalType::BIGINT,
	                          Value::BIGINT(int64_t(defaults.budget_bytes / (1024 * 1024))));
	config.AddExtensionOption("recycler_entry_cap_mb", "Largest cache entry in MB", LogicalType::BIGINT,
	                          Value::BIGINT(int64_t(defaults.entry_cap_bytes / (1024 * 1024))));
	config.AddExtensionOption("recycler_entry_cap_rows", "Largest cache entry in rows", LogicalType::BIGINT,
	                          Value::BIGINT(int64_t(defaults.entry_cap_rows)));
	config.AddExtensionOption("recycler_min_input_rows",
	                          "Minimum estimated rows scanned by a subtree before it is worth caching",
	                          LogicalType::BIGINT, Value::BIGINT(int64_t(defaults.min_input_rows)));
	config.AddExtensionOption("recycler_speculate_mb",
	                          "Largest estimated result (MB) materialized on first sight, before a repeat is seen",
	                          LogicalType::BIGINT, Value::BIGINT(int64_t(defaults.speculate_bytes / (1024 * 1024))));

	loader.RegisterFunction(TableFunction("recycler_stats", {}, RecyclerStatsFunction, RecyclerStatsBind,
	                                      RecyclerStatsInit));
	loader.RegisterFunction(TableFunction("recycler_entries", {}, RecyclerEntriesFunction, RecyclerEntriesBind,
	                                      RecyclerEntriesInit));
	loader.RegisterFunction(TableFunction("recycler_history", {}, RecyclerHistoryFunction, RecyclerHistoryBind,
	                                      RecyclerHistoryInit));
	loader.RegisterFunction(PragmaFunction::PragmaStatement("recycler_clear", RecyclerClearPragma));
}

} // namespace duckdb
