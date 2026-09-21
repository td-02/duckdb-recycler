#define DUCKDB_EXTENSION_MAIN

//===----------------------------------------------------------------------===//
// RecyclerExtension — entry point registered with DuckDB
//===----------------------------------------------------------------------===//
#include "recycler_extension.hpp"
#include "recycler_optimizer.hpp"   // RecyclerState, RecyclerOptimize

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// PRAGMA recycler_stats() — print cache statistics
// ---------------------------------------------------------------------------
static void RecyclerStatsPragma(ClientContext &context, const FunctionParameters & /*params*/) {
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

// ---------------------------------------------------------------------------
// PRAGMA recycler_clear() — flush the cache
// ---------------------------------------------------------------------------
static void RecyclerClearPragma(ClientContext &context, const FunctionParameters & /*params*/) {
    for (auto &ext : OptimizerExtension::Iterate(context)) {
        if (!ext.optimizer_info) { continue; }
        auto *state = dynamic_cast<RecyclerState *>(ext.optimizer_info.get());
        if (state) {
            state->cache->Clear();
            Printer::Print("recycler: cache cleared");
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// LoadInternal — called by DuckDB when the extension is loaded
// ---------------------------------------------------------------------------
static void LoadInternal(ExtensionLoader &loader) {
    // 1. Create the shared cache and attach it to an optimizer extension.
    auto state = make_shared_ptr<RecyclerState>();

    OptimizerExtension opt_ext;
    opt_ext.optimize_function = RecyclerOptimize;
    opt_ext.optimizer_info    = state;

    auto &db = loader.GetDatabaseInstance();
    ExtensionCallbackManager::Get(db).Register(std::move(opt_ext));

    // 2. Register PRAGMA recycler_stats
    PragmaFunction stats_pragma =
        PragmaFunction::PragmaStatement("recycler_stats", RecyclerStatsPragma);
    loader.RegisterFunction(std::move(stats_pragma));

    // 3. Register PRAGMA recycler_clear
    PragmaFunction clear_pragma =
        PragmaFunction::PragmaStatement("recycler_clear", RecyclerClearPragma);
    loader.RegisterFunction(std::move(clear_pragma));
}

// ---------------------------------------------------------------------------
// Extension entry points
// ---------------------------------------------------------------------------
void RecyclerExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

} // namespace duckdb

DUCKDB_CPP_EXTENSION_ENTRY(recycler, loader) {
    duckdb::LoadInternal(loader);
}
