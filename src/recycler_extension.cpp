#define DUCKDB_EXTENSION_MAIN

//==-------------------------------------------------------------------==//
// recycler_extension.cpp  – entry point registered with DuckDB
//==-------------------------------------------------------------------==//
#include "recycler_extension.hpp"
#include "recycler_optimizer.hpp"   // RecyclerState, RecyclerOptimize

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// ── RecyclerShutdownCallback ───────────────────────────────────────
// A cached CachedResult owns a ColumnDataCollection, whose blocks are
// released through the BufferManager. RecyclerCache is only destroyed
// when DBConfig is (as part of the OptimizerExtension holding it), and
// DatabaseInstance::~DatabaseInstance() resets buffer_manager *before*
// its config member is destroyed -- so any entry still cached at that
// point segfaults on shutdown (BlockHandle outliving its BufferManager).
// Clearing the cache once the last connection closes drops every
// ColumnDataCollection while the buffer manager is still alive.
class RecyclerShutdownCallback : public ExtensionCallback {
public:
    explicit RecyclerShutdownCallback(shared_ptr<RecyclerCache> cache_p)
        : cache(std::move(cache_p)) {}

    void OnConnectionClosed(ClientContext &context) override {
        auto &db = DatabaseInstance::GetDatabase(context);
        if (db.GetConnectionManager().GetConnectionCount() <= 1) {
            cache->Clear();
        }
    }

private:
    shared_ptr<RecyclerCache> cache;
};

// ── PRAGMA recycler_stats ─────────────────────────────────────────
static void RecyclerStatsPragma(ClientContext &context,
                                 const FunctionParameters & /*params*/) {
    for (auto &ext : OptimizerExtension::Iterate(context)) {
        if (!ext.optimizer_info) { continue; }
        auto *state = dynamic_cast<RecyclerState *>(ext.optimizer_info.get());
        if (state) {
            Printer::Print(state->cache->Stats());
            return;
        }
    }
    Printer::Print("recycler: not loaded");
}

// ── PRAGMA recycler_clear ─────────────────────────────────────────
static void RecyclerClearPragma(ClientContext &context,
                                 const FunctionParameters & /*params*/) {
    for (auto &ext : OptimizerExtension::Iterate(context)) {
        if (!ext.optimizer_info) { continue; }
        auto *state = dynamic_cast<RecyclerState *>(ext.optimizer_info.get());
        if (state) {
            state->cache->Clear();
            Printer::Print("recycler: cache cleared");
            return;
        }
    }
    Printer::Print("recycler: not loaded");
}

// ── LoadInternal ──────────────────────────────────────────────────
static void LoadInternal(ExtensionLoader &loader) {
    auto state = make_shared_ptr<RecyclerState>();

    OptimizerExtension opt_ext;
    opt_ext.optimize_function = RecyclerOptimize;
    opt_ext.optimizer_info    = state;

    auto &db = loader.GetDatabaseInstance();
    ExtensionCallbackManager::Get(db).Register(std::move(opt_ext));
    ExtensionCallbackManager::Get(db).Register(make_shared_ptr<RecyclerShutdownCallback>(state->cache));

    loader.RegisterFunction(
        PragmaFunction::PragmaStatement("recycler_stats", RecyclerStatsPragma));
    loader.RegisterFunction(
        PragmaFunction::PragmaStatement("recycler_clear", RecyclerClearPragma));
}

void RecyclerExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

} // namespace duckdb

// Fix #6: DUCKDB_CPP_EXTENSION_ENTRY already emits extern "C" linkage
// internally – do NOT wrap it in another extern "C" { } block.
DUCKDB_CPP_EXTENSION_ENTRY(recycler, loader) {
    duckdb::LoadInternal(loader);
}
