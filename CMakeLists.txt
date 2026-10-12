cmake_minimum_required(VERSION 3.5)
set(TARGET_NAME waddle)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include_directories(src/include)

set(EXTENSION_SOURCES
    src/waddle_extension.cpp       # extension entry point (template scalar function + LoadRecycler)
    src/recycler_signature.cpp     # canonical structural signatures of plan subtrees
    src/recycler_cache.cpp         # bounded, benefit-ordered cache of materialized results
    src/recycler_operators.cpp     # RecyclerScan (hit) and RecyclerMaterialize (miss) operators
    src/recycler_policy.cpp        # serving and admission decisions
    src/recycler_rewrite.cpp       # plan rewrites: scan substitution, materialize wrapping, block finalization
    src/recycler_exists.cpp        # correlated EXISTS subqueries as plain joins the join order optimizer places
    src/recycler_hooks.cpp         # pre-/post-optimizer hooks: statement classification, whole results, ordering
    src/recycler_settings.cpp      # the connection's copy of the recycler's settings
    src/recycler_subsumption.cpp   # predicate / column subsumption on SPJ blocks
    src/recycler_pagination.cpp    # LIMIT/OFFSET pages over one sorted result
    src/recycler_subtrees.cpp      # the post hook's subtree cache
    src/recycler_functions.cpp     # setting registration, recycler_stats()/recycler_entries(), PRAGMA recycler_clear
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
