//===----------------------------------------------------------------------===//
// DSA Assignment 2 — Query Recycler Extension
// Intercepts query plans at the optimizer level and recycles (caches) results
// of subplans that have been seen before, identified by their structural
// join skeleton (table set + join conditions), NOT by SQL string.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class RecyclerExtension {
public:
	static void Load(ExtensionLoader &loader);
	static std::string Name() { return "recycler"; }
	std::string Version() const {
#ifdef EXT_VERSION_RECYCLER
		return EXT_VERSION_RECYCLER;
#else
		return "v0.1.0";
#endif
	}
};

} // namespace duckdb
