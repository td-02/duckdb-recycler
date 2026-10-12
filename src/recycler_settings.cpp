//===----------------------------------------------------------------------===//
// recycler_settings.cpp - the connection's copy of the recycler's settings
//===----------------------------------------------------------------------===//
#include "recycler_state.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

static bool GetBoolSetting(ClientContext &context, const char *name, bool default_value) {
	Value value;
	if (context.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
	}
	return default_value;
}

static idx_t GetIntSetting(ClientContext &context, const char *name, idx_t default_value) {
	Value value;
	if (context.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		auto v = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		return v < 0 ? 0 : idx_t(v);
	}
	return default_value;
}

//! A setting given in MB, as bytes: clamped so that scaling it cannot wrap (a wrapped cap of 0 would silently
//! disable every store)
static idx_t GetByteSetting(ClientContext &context, const char *name, idx_t default_bytes) {
	static constexpr idx_t MB = 1024 * 1024;
	auto mb = GetIntSetting(context, name, default_bytes / MB);
	return MinValue<idx_t>(mb, NumericLimits<idx_t>::Maximum() / MB) * MB;
}

RecyclerSettings LoadRecyclerSettings(ClientContext &context) {
	RecyclerSettings s;
	s.enabled = GetBoolSetting(context, "recycler_enabled", s.enabled);
	s.whole_result = GetBoolSetting(context, "recycler_whole_result", s.whole_result);
	s.blocks = GetBoolSetting(context, "recycler_blocks", s.blocks);
	s.subtrees = GetBoolSetting(context, "recycler_subtrees", s.subtrees);
	s.subsumption = GetBoolSetting(context, "recycler_subsumption", s.subsumption);
	s.pagination = GetBoolSetting(context, "recycler_pagination", s.pagination);
	s.exists_rewrite = GetBoolSetting(context, "recycler_exists_rewrite", s.exists_rewrite);
	// cached results compete with the queries for memory: whatever the setting says, at most a quarter of the limit
	s.budget_bytes = MinValue<idx_t>(GetByteSetting(context, "recycler_budget_mb", s.budget_bytes),
	                                 BufferManager::GetBufferManager(context).GetMaxMemory() / 4);
	s.entry_cap_bytes = GetByteSetting(context, "recycler_entry_cap_mb", s.entry_cap_bytes);
	s.entry_cap_rows = GetIntSetting(context, "recycler_entry_cap_rows", s.entry_cap_rows);
	s.min_input_rows = GetIntSetting(context, "recycler_min_input_rows", s.min_input_rows);
	s.speculate_bytes = GetByteSetting(context, "recycler_speculate_mb", s.speculate_bytes);
	s.compact_rows = GetIntSetting(context, "recycler_compact_rows", s.compact_rows);
	return s;
}

const RecyclerSettings &RecyclerClientState::GetSettings(ClientContext &context) {
	if (!settings_loaded) {
		settings = LoadRecyclerSettings(context);
		settings_loaded = true;
	}
	return settings;
}

} // namespace duckdb
