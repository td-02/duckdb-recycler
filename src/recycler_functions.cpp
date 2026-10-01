//===----------------------------------------------------------------------===//
// recycler_functions.cpp - registration, settings, introspection functions and the connection callbacks
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/storage/buffer_manager.hpp"

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
RecyclerClientState &RecyclerClientState::Get(ClientContext &context) {
	return *context.registered_state->GetOrCreate<RecyclerClientState>("recycler");
}

void RecyclerClientState::NoteTouched(shared_ptr<CachedEntry> entry) {
	if (!profiled) {
		return;
	}
	std::lock_guard<std::mutex> guard(touched_lock);
	touched.push_back(std::move(entry));
}

//! A RECYCLER_MATERIALIZE or RECYCLER_SCAN node of the profiler tree
struct ProfiledOperator {
	bool materialize;
	SigHash hash;
	//! Operator thread time of the node and of the operators below it, in nanoseconds
	uint64_t own_ns;
	uint64_t below_ns;
};

static uint64_t ToNs(double seconds) {
	return uint64_t(seconds * 1e9);
}

//! The "Key" parameter of a recycler operator from the EXTRA_INFO map of its profiler node
static bool ProfiledKey(const ProfilingInfo &info, SigHash &hash) {
	auto extra = info.metrics.find(MetricType::EXTRA_INFO);
	if (extra == info.metrics.end()) {
		return false;
	}
	for (auto &pair : MapValue::GetChildren(extra->second)) {
		auto &fields = StructValue::GetChildren(pair);
		if (StringValue::Get(fields[0]) == "Key") {
			return SigHash::TryParse(StringValue::Get(fields[1]), hash);
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
			if (ProfiledKey(info, op.hash)) {
				op.own_ns = ToNs(own);
				op.below_ns = ToNs(below);
				out.push_back(op);
			}
		}
	}
	return below + own;
}

void RecyclerClientState::QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) {
	if (!profiled) {
		return;
	}
	profiled = false;
	vector<shared_ptr<CachedEntry>> entries;
	{
		std::lock_guard<std::mutex> guard(touched_lock);
		entries.swap(touched);
	}
	if (error) {
		return;
	}
	// runs after the query's latency was fixed (section 6.5 of the plan); the profiler's tree of this query stays
	// readable until the next profiled query starts
	try {
		auto root = QueryProfiler::Get(context).GetRoot();
		if (!root) {
			// the plan had no operator the profiler covers
			return;
		}
		vector<ProfiledOperator> operators;
		CollectProfiledOperators(*root, operators);
		auto &cache = *GetRecyclerState(context)->cache;
		for (auto &op : operators) {
			for (auto &entry : entries) {
				if (entry->hash == op.hash) {
					if (op.materialize) {
						entry->measured_cost_ns = op.below_ns;
						entry->materialize_ns = op.own_ns;
					} else {
						entry->hit_ns = op.own_ns;
					}
				}
			}
			if (op.materialize) {
				cache.NoteMaterializeTime(op.hash, op.below_ns, op.own_ns);
			} else {
				cache.NoteHitTime(op.hash, op.own_ns);
			}
		}
	} catch (std::exception &) {
		// a profiler tree without the metrics we read; the measurement is a by-product, never a failure
	}
}

//===----------------------------------------------------------------------===//
// Shutdown ordering
//===----------------------------------------------------------------------===//
// Cached ColumnDataCollections are buffer-managed. DatabaseInstance destroys the buffer manager before the
// DBConfig that (transitively) owns this cache, so the collections must be released while the last connection
// closes.
class RecyclerShutdownCallback : public ExtensionCallback {
public:
	explicit RecyclerShutdownCallback(shared_ptr<RecyclerCache> cache_p) : cache(std::move(cache_p)) {
	}

	void OnConnectionClosed(ClientContext &context) override {
		auto &db = DatabaseInstance::GetDatabase(context);
		if (db.GetConnectionManager().GetConnectionCount() <= 1) {
			cache->Clear();
		}
	}

private:
	shared_ptr<RecyclerCache> cache;
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
		add("pre_ns", s.pre_ns);
		add("post_ns", s.post_ns);
		add("skip_not_query", s.skip_not_query);
		add("skip_modifying", s.skip_modifying);
		add("skip_not_read", s.skip_not_read);
		add("skip_disabled", s.skip_disabled);
		add("skip_modified_databases", s.skip_modified_databases);
		add("skip_write_transaction", s.skip_write_transaction);
		add("skip_analyze_failed", s.skip_analyze_failed);
		add("root_not_cacheable", s.root_not_cacheable);
		add("bytes_used", cache.UsedBytes());
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

static Value Milliseconds(const std::atomic<uint64_t> &ns) {
	return Value::DOUBLE(double(ns.load()) / 1e6);
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
void LoadRecycler(ExtensionLoader &loader) {
	auto state = make_shared_ptr<RecyclerState>();

	OptimizerExtension optimizer_extension;
	optimizer_extension.pre_optimize_function = RecyclerPreOptimize;
	optimizer_extension.optimize_function = RecyclerOptimize;
	optimizer_extension.optimizer_info = state;

	auto &db = loader.GetDatabaseInstance();
	ExtensionCallbackManager::Get(db).Register(std::move(optimizer_extension));
	ExtensionCallbackManager::Get(db).Register(make_shared_ptr<RecyclerShutdownCallback>(state->cache));

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
	loader.RegisterFunction(PragmaFunction::PragmaStatement("recycler_clear", RecyclerClearPragma));
}

} // namespace duckdb
