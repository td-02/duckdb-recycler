# Query Recycler — Solution Plan (DuckDB v1.5.5 extension `waddle`)

Status: plan as of 2026-09-24, branch `dev_tapesh` (identical content to `origin/main`, which the grader currently builds).
Measurements below were taken on the local machine (6 threads, hot page cache, `scripts/benchmark.py`).
DuckDB source citations are relative to `duckdb/` (the pinned v1.5.5 submodule).

## Implementation status (2026-09-24)

Milestones M0–M4 of the roadmap below are implemented on `dev_tapesh` (canonical signatures, whole-result cache at the
pre-optimize hook, nested-block matching, post-optimize subtree cache with the delim join as atomic unit, admission gate,
budget/eviction, invalidation, settings, `recycler_stats()`/`recycler_entries()`, SQL tests, differential harness).
Measured on the local machine (6 threads, hot cache, `make benchmark`): **geomean 14.9 ms vs 30.65 ms vanilla**
(2.1x; 14.9–15.8 ms across runs), total 15.6 s vs 20.2 s; `IMDB_DATA=1 make test` passes and `scripts/recycler_diff.py` matches vanilla for all
149 queries in filename, reverse and shuffled order.

M5 (predicate subsumption, 2026-09-25): implemented as pre-optimize *block shapes* (structure minus range predicates
on base columns, columns by physical id), planted `RECYCLER_MATERIALIZE` markers whose constrained columns are kept
alive through column pruning, post-optimize resolution of the stored columns' identities (seeing through DuckDB's
compress/decompress projections), and consumer rewrites to a column-subset `RECYCLER_SCAN` plus a residual filter.
Measured: **geomean 9.24 ms** (vs 14.9 ms after M0–M4, 30.65 ms vanilla), total 11.9 s; all 149 answers match vanilla in
three query orders (21–38 subsumption hits per run depending on order). Deferred: union-projection widening (M7),
batch-index sink mode.

M6 (ordered pagination reuse, 2026-10-07): repeated `ORDER BY … LIMIT n OFFSET m` pages over the same expensive input.
M5 already skipped the joins of later pages (block hits) but each page still re-sorted the whole input. M6 caches the
*leading rows of the sorted result* once, so later pages only skip/scan their own slice. Details below.

- **Matching.** A `LIMIT` with constant limit and constant (or no) offset, `limit + offset > 5000` (above that DuckDB keeps
  `ORDER BY + LIMIT` instead of a Top-N, `src/optimizer/topn_optimizer.cpp:23-52`), reached from an `ORDER BY` through
  order-preserving projections/filters only. The bound `ORDER BY` subtree must be deterministic and cacheable, and its
  ORDER BY must be *provably a total order* (fail-closed proof: a base-table primary key, carried through filters, plain
  projections, aggregate group keys and equality joins that cannot multiply the rows of the side the keys come from;
  UNIQUE columns do not count, they may tie on NULL). Ties make vanilla itself nondeterministic, so tie-bearing
  pagination is left alone. M6 acts only when the bound plan and the optimized plan each hold exactly one such pagination.
- **Stable key.** Taken from the pre-optimize bound plan, because the optimized plan below the sort differs between pages
  (a planted M5 block marker on the first page, a block scan on later ones, and compressed-materialization types that
  need column statistics a block scan does not have). The key covers the bound `ORDER BY` subtree (canonical signature),
  the ORDER BY output positions the query's output actually *demands* (propagated down through the projections above the
  LIMIT; a mere reference is not enough), and the layout of the cached unit: the top of the projection/filter chain under
  the LIMIT, above the sort's decompression, so it emits logical types in every plan (ORDER BY directions/NULL orders,
  output column count and types). Hash and canonical string both cover all of it; `LIMIT`/`OFFSET` are excluded, so
  every page of one pagination shares one entry. The same sort with different demanded columns (even of the same SQL
  type) gets a different key. The session's result-affecting settings (the salt, `PlanSignatures::SettingsSalt`, that
  every other key carries) are part of it, so a page is never served rows computed under other values.
- **Prefix materialization.** Entries have their own kind, `PAGINATION`, in a separate cache map, so no generic
  `SUBTREE` lookup can see them; only `TryPagination` (post hook) looks them up. The writer stores the first
  `4 × (offset + limit)` sorted rows and stops the sort's output there (`SinkResultType::FINISHED`; whole chunks are
  appended, so a prefix may overshoot by one chunk). `CachedEntry::complete` says whether `rows` is the whole result (the
  input ended first) or a prefix. A page hits only when `complete || offset + limit ≤ rows`. A page that needs more than
  a stored prefix misses and writes a longer one, `max(4 × need, 2 × existing rows)`, which replaces the shorter entry.
  The target is clamped to `entry_cap_rows` (less one chunk) and `entry_cap_bytes` per estimated row width; if the
  clamped prefix cannot even cover the current page, M6 declines and the query runs as M5 plans it. Admission is
  `AdmitPaginationWrite` in the policy module: `min_input_rows` and the history-based `TooBig(hash, caps)` check, plus
  `Admissible` for the table versions. A first sight is admitted: the prefix is small, so a never-reused write is
  cheap. The M5 writes below the ordered unit (the join and block writes) stay active, because the pagination writer
  does not reduce the write policy.
- **Safety.** A prefix is never a `SUBTREE` result (separate map; `Lookup(SUBTREE)` cannot reach it). All sizing arithmetic
  saturates. The exact type-equality check of `SubstituteScan` still runs on every hit.
- **Freshness and invalidation.** Entries carry the table dependencies (`CachedEntry::tables`) of the bound ORDER BY
  subtree, read at the pre hook from the statement's snapshot (the optimized plan may reach the tables through a block
  scan, so its own tables are no stand-in). `StorePagination` refuses a store over a table with an announced writer or
  a commit the snapshot predates, exactly like `Store`, and lists the entry in the cache's per-table `dependents`;
  `LookupPagination` accepts an entry only when it is current for the reader (`IsCurrentFor`, the check `IsCurrent`
  uses). A table mutation therefore evicts the pagination entries over it when the writer commits (`invalidated_entries`),
  as it does for every other kind, and the first page after the write is recomputed.
- **Replacement and accounting.** Replacing a prefix is atomic under the cache mutex and is not an eviction: the shorter
  entry leaves the maps, `dependents` and the accounting (`total_bytes`, `total_used_bytes`), room is made, and the new
  entry inherits its hit history; if room cannot be made the older entry is published again with its bytes and
  dependents, so a failed growth leaves the old prefix fully usable. Eviction (`Benefit()`), `recycler_clear` and the
  accounting cover the new map; running queries hold entries by `shared_ptr`, so replacement or clearing never frees
  data under a plan.
- **Per-statement state.** `PaginationSite` lives in `RecyclerClientState` and follows its `Reset()` lifecycle: cleared
  at the start of every pre hook and by the post hook on every exit, so nothing leaks into the next statement.
  `subtree_hits`/`subtree_stores` also count `PAGINATION` activity; `recycler_entries()` shows the kind.
- **Setting.** `recycler_pagination` (default `true`; needs `recycler_subtrees`) is the independent ablation switch. With
  it off no pagination is detected, nothing is written or looked up, and plans are exactly the current M0–M5 plans.
- **Validation.** `make release`; all recycler tests pass (14,157 assertions in 21 test cases) and `IMDB_DATA=1 make
  test` passes (15,843 assertions in 23 test cases, including all 149 benchmark answers vs vanilla).
  `test/sql/recycler/recycler_pagination.test` covers: reuse across pages (with `preserve_insertion_order` true and
  false), the grader configuration (`preserve_insertion_order=false`, ≥ 2 joins so block planting and block
  subsumption fire, later-page plan without `ORDER_BY`), prefix coverage, growth with hit-history carry-over, a complete
  short result (deep page and page past the end), failed growth under a tight budget, eviction between two pagination
  families, `recycler_clear`, byte-accounting equality (`recycler_stats` vs `recycler_entries`), the clamp/decline paths,
  the ablation switch, a prefix not being usable as a subtree (no-LIMIT query and materialized CTE), ties and
  row-multiplying joins (no entry), the same-type/different-column layout collision regression, two pagination sites in
  one statement (no M6), no `ORDER BY`, a window between LIMIT and ORDER BY, and below-threshold requests (Top-N).
- **Performance** (6 threads, hot cache; the machine drifted by about ±25 % between runs, so treat these as indicative).
  Pages that hit the cached prefix drop from roughly 80–100 ms to about 1–2 ms (q0093/q0094). In the targeted
  q0092 → q0093 → q0094 run on the integrated build, q0092 took about 145 ms and q0093/q0094 about 1 ms each: about
  0.15 s in total and a best geomean of 5.51 ms for the three queries, against ≈ 365 ms and 111.03 ms for the M5 baseline.
  The q0092 family stores about 40,960 rows (≈ 3.1 MB) instead of the ≈ 983K-row, ≈ 65 MB entry of a full
  materialization (which made the first page ≈ 200 ms slower). An A/B with `recycler_pagination` on/off over the six
  public pagination-family queries showed a large, consistent benefit with it on (six-query geomean roughly 8–15 ms vs
  52–57 ms). Whole-run timings varied too much on the measurement machine to quote a full-suite gain.

EXISTS rewrite (2026-10-07, §3.9): correlated `EXISTS` subqueries become plain joins the join order optimizer places;
the first change aimed at the *total* time (the private leaderboard scores it as much as the geomean, and there it
was 25.7% against 81.5% for the geomean). Gate against the previous `main` build: **geomean 8.40 -> 6.13 ms, total
11.99 -> 4.54 s**, same 47 queries served, all answers equal to vanilla.

## 0. Decision in one paragraph

Rebuild the extension around three ideas, in this order: (1) a **whole-result cache** matched at the *pre-optimize* hook and served by our own order-preserving `EXTENSION` scan operator; (2) a **structural subtree cache** at the *post-optimize* hook whose cache unit for the dominant query family is the whole `LOGICAL_DELIM_JOIN` node (never anything below it), with aggregates and CTE bodies as further cache points, one materialization per root-to-leaf path, chosen top-down by a cost/benefit gate; (3) **predicate subsumption** on range filters (`production_year BETWEEN`, `IS NOT NULL`) over cached outputs that still carry the filter column. The current miss path must first be made free (parallel sink, parallel re-scan, no nested writers) and the unsound string key replaced by a canonical structural signature. Every rewrite must fail closed (fall back to the vanilla plan), because one wrong answer ranks the team below every all-correct team.

Projected geomean (vanilla 30.65 ms; hit model: recycled part costs 5% of itself, 1.5 ms floor, payer write cost not charged): whole-result 25.1 ms, + nested-block matching 22.0 ms, + subtree/delim/CTE reuse ~20.4 ms, + subsumption ~16 ms, + pagination ~15.5 ms, + union-projection widening 12–13 ms.

## 1. Current state assessment (why the code on main loses)

| Fact | Measurement / evidence |
|---|---|
| Vanilla DuckDB (extension optimizer disabled) | geomean **30.65 ms**, total 20.17 s |
| Current extension | geomean **35.03 ms**, total 22.67 s → **14% slower** |
| Exact repeats q0248/q0284/q0293 of q0240 | still 0.8–1.3 s each: **no hit** |
| Correctness test `IMDB_DATA=1 make test` | passes (all 149), but only because the workload avoids the broken cases below |

Root causes, all confirmed in source:

1. **Nothing in Family A (q0226–q0293, 15.4 s of the 20.2 s total) is ever cached.** The `EXISTS (...)` decorrelates into a `LOGICAL_DELIM_JOIN` (SEMI, `id IS NOT DISTINCT FROM id`) that the Deliminator keeps because both sides carry selections (`src/optimizer/deliminator.cpp:58-84,112-137`); it sits at the *top* of the join tree, and `WalkAndRecycle` refuses to act at or below any delim join (`src/recycler_optimizer.cpp`).
2. **The miss path is serialized.** `PhysicalCacheWriter::ParallelSink()` is false, so `Pipeline::ScheduleParallel` runs the whole producing pipeline on one thread (`src/parallel/pipeline.cpp:101-108`); `ParallelSource()` defaults to false and `GlobalSourceState::MaxThreads()` to 1, so everything above the writer is single-threaded too. Every join level is wrapped (up to 3 nested writers per query), each a pipeline breaker. Measured: q0119 0.104 s → 0.380 s, q0133 1.65x.
3. **The key is unsound (wrong answers reproduced on IMDB by the audit):** column identity is positional (`n.gender` returned garbage after `n.name` was cached); table-function parameters ignored (`range(6)` served `range(3)`'s rows); operators on the non-projected side of SEMI/ANTI joins ignored (`LIMIT 5` served `LIMIT 2`); join type and comparison operator not encoded; IN-list constants live in a `CHUNK_GET` collection the key never reads; `ToString()` keys leak per-query `#[t.c]` bindings; no invalidation on writes; volatile functions not detected.
4. **Two crashes:** `LOGICAL_ANY_JOIN` is `Cast<LogicalComparisonJoin>` (unchecked, UB → "Attempted to dereference unique_ptr that is NULL"); `PhysicalCacheWriter` keeps `children` empty, so `PhysicalOperator::GetSources()` throws for any LIMIT planned under `preserve_insertion_order=true` (`src/execution/physical_operator.cpp:246-262`) — q0016/q0122 fail under DuckDB defaults.
5. **Bottom-up poisoning:** a child hit becomes a `LogicalColumnDataGet` whose bindings are "untraceable", so the parent is never keyed; multi-level reuse is impossible by construction.
6. Unbounded memory (469 MB of COLUMN_DATA after 51 queries), non-owning collection pointers (use-after-free on `recycler_clear` from another connection), vestigial second `DUCKDB_CPP_EXTENSION_ENTRY(recycler)`, no serialization story for the extension operator.

Worth keeping: the registration skeleton, the `LogicalExtensionOperator::CreatePlan → planner.Make<>` pattern, the `ColumnBindingReplacer` hit-substitution mechanics, the shutdown-ordering insight (collections must die before the `BufferManager`), and the "decline when untraceable" instinct.

## 2. Scoring model and budgets

- The scored `latency` timer starts in `ClientContext::PendingStatementOrPreparedStatement` **before binding and optimization** and stops in `PhysicalExplainAnalyze::Finalize` (`src/main/client_context.cpp:958-965`, `src/execution/operator/helper/physical_explain_analyze.cpp:20-27`). Both optimizer hooks, physical planning and cache publication in `Finalize` are inside it.
- 29 of 149 queries run under 5 ms, 11 under 0.4 ms. A flat +0.1 ms per query costs +4.1% geomean; +0.3 ms costs +9.9%. **Budget: ≤ 25 µs for trivial plans, ≤ 50 µs for 30-node plans.** Hash, never build strings on the hot path; early-exit on non-SELECT roots and on plans without joins/aggregates (after a single cheap whole-result hash lookup).
- **Profiler trap (verified):** `QueryProfiler::OperatorRequiresProfiling` (`src/main/query_profiler.cpp:134-176`) does **not** list `COLUMN_DATA_SCAN`; a plan of only `EXPLAIN_ANALYZE` + `COLUMN_DATA_SCAN` disables profiling and the JSON has no `latency` → the grader counts the query as failed. Whole-result hits must therefore be served by our own `PhysicalOperatorType::EXTENSION` scan operator (listed at line 171), never by a bare `LogicalColumnDataGet`.
- Gains must come from saved CPU (joins/aggregates over the 36M-row `cast_info`), not I/O: the grader's warm-up leaves the OS page cache hot.

## 3. Architecture

### 3.1 Module layout (`src/`)

| File | Responsibility |
|---|---|
| `waddle_extension.cpp` | entry point (keep name `waddle`); calls `LoadRecycler(loader)`; registers settings, `recycler_stats()` table function, `PRAGMA recycler_clear` |
| `recycler_signature.{hpp,cpp}` | canonical renderer + 128-bit compositional hash over logical subtrees (§3.2) |
| `recycler_cache.{hpp,cpp}` | entries, statistics map, byte budget, eviction, invalidation epochs, pinning |
| `recycler_policy.{hpp,cpp}` | candidate enumeration, admissibility rules, cost/benefit gate (§3.5–3.7) |
| `recycler_rewrite.{hpp,cpp}` | hit substitution (`ColumnBindingReplacer`), residual filter/projection compensation, post-rewrite self-check |
| `logical_recycler_materialize.*` / `physical_recycler_materialize.*` | miss-path writer: sink+source, parallel / sequential / batch-index modes |
| `logical_recycler_scan.*` / `physical_recycler_scan.*` | hit-path scan over a cached collection, `EXTENSION` type, ordered/unordered |
| `recycler_hooks.cpp` | `pre_optimize_function` and `optimize_function` |
| `recycler_exists.{hpp,cpp}` | correlated `EXISTS` subqueries as plain joins the join order optimizer places (§3.9); runs at the end of the pre hook, independent of the cache |
| delete | `recycler_extension.cpp/.hpp` (second entry point), `join_utils.*`, `logical_cache_writer.*`, `physical_cache_writer.*` |

### 3.2 Canonical signature (the correctness core)

One pre-order pass assigns every index returned by `op.GetTableIndex()` a sequential canonical id (LogicalGet, Projection, Aggregate group/aggregate/groupings indexes, MARK `mark_index`, CTE/CTERef/ColumnDataGet/DelimGet/DummyScan/ExpressionGet). One post-order pass renders each node into a byte string and combines child hashes. Rendered fields:

- `LogicalGet`: `catalog.schema.table` (via `GetTable()`; **refuse** table functions without a catalog table), `column_ids` (primary indexes, incl. row-id), `projection_ids`, every `table_filters` entry by `TableFilterType` with constants (`optional:` filters included; `DYNAMIC_FILTER` → refuse or strip, §3.6), `extra_info.sample_options`. `dynamic_filters` non-null → see §3.6.
- `LogicalComparisonJoin` / delim join: `join_type`, each condition's `comparison` type and both sides, `left/right_projection_map`, `mark_index`, `duplicate_eliminated_columns`, `delim_flipped`.
- `LogicalAggregate`: groups, aggregate expressions (function name, `DISTINCT`, filter, order_bys), grouping sets/functions. `LogicalProjection`/`LogicalFilter` (+ `projection_map`), `LogicalOrder` (+ `projection_map`), `LogicalTopN` (orders, limit, offset), `LogicalLimit` (both `BoundLimitNode`s), `LogicalDistinct`, `LogicalMaterializedCTE`/`LogicalCTERef` (canonical cte id, must be inside the subtree), `LogicalColumnDataGet` (types + **hash of the collection contents** — this is where IN-list constants live, `src/optimizer/in_clause_rewriter.cpp:60-105`).
- Expressions: custom visitor emitting `ExpressionClass`/`ExpressionType`, `return_type`, function **name** (`BaseScalarFunction::Hash` omits it), constants via `Value::Hash`, cast targets, and `(canonical id, column_index)` for `BOUND_COLUMN_REF`. Never `Expression::Hash()` (absolute `table_index`) or `ToString()` (alias-based).
- Refusals (sentinel = not cacheable): any binding whose table_index is outside the subtree, any `LogicalDelimGet` or foreign `CTE_SCAN` inside, any expression with `!IsConsistent()` or `HasParameter()`, any operator type not on the allowlist. Refuse by default; extend the allowlist deliberately.
- Typed output list (`op->types` after `ResolveOperatorTypes()`) is part of the key; compressed-materialization `__internal_compress_*` projections are rendered like any projection.
- Entries store the full canonical byte string; a hash match is confirmed by byte comparison before substitution.

### 3.3 Cache store

- `CachedEntry { shared_ptr<ColumnDataCollection> data; vector<LogicalType> types; idx_t rows; idx_t bytes; bool ordered; string canonical; column identity list; catalog version; epoch; stats }` behind a mutex. Plans hold a `shared_ptr<CachedEntry>` (the scan operator owns it) so eviction/clear can never free data under a running query. `Store` never overwrites an existing key.
- A separate **statistics map** (seen-count, last-seen query number, measured rows/bytes, refusal reason) keyed by the same signature, retained after eviction (Nagel's history; Jindal's "seen once" filter).
- Budget: `recycler_budget_mb` (default 2000) accounted with `AllocationSize()`; per-entry cap `recycler_entry_cap_mb` (256) and 4M rows enforced on **actual** counts during `Sink` (estimates are 3–10x off here); eviction by benefit density `saved_rows × E / bytes` with LRU tie-break, never evicting pinned entries.
- Lifetime: collections are `BUFFER_MANAGER_ALLOCATOR` (memory-limit accounted, spillable). Keep the last-connection `OnConnectionClosed` clear (correct: `DatabaseInstance::~DatabaseInstance` resets `buffer_manager` before `DBConfig`, `src/main/database.cpp:80-104`); optionally switch to `ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES`.
- Invalidation: at the pre hook, if `optimizer.binder.GetStatementProperties().modified_databases` is non-empty or the root is a write/DDL/ATTACH/DETACH/SET-like type → clear everything and skip; entries record `Catalog::GetCatalogVersion` and are ignored on mismatch; no store/hit inside a non-read-only transaction. Tables in `AttachedDatabase::IsReadOnly()` catalogs (the graded `imdb`) need no per-hit revalidation.

### 3.4 Physical operators

**PhysicalRecyclerMaterialize** (sink + source, `EXTENSION` type): `children.push_back(child)` and inherit the default `BuildPipelines`/`GetSources`/`GetChildren` (`src/execution/physical_operator.cpp:214-262`) — this alone fixes the delim-scan and LIMIT crashes. Modes chosen in `CreatePlan`:
- `PAR`: `ParallelSink()=true`, per-thread collection + `ColumnDataAppendState` in the local state, `Combine` under a mutex (pattern `physical_materialized_collector.cpp:14-63`); source `ParallelSource()=true` with `ColumnDataParallelScanState`, `MaxThreads()=ChunkCount()` (pattern `physical_column_data_scan.cpp:25-62`).
- `SEQ` (ordered child, e.g. whole-result of an ORDER BY query): `ParallelSink()=false`, single collection, `SourceOrder()=FIXED_ORDER`, single-threaded scan. Cheap because the sort's output stage is small.
- `BATCH` (later, for the 983K-row pagination case): `RequiredPartitionInfo=BatchIndex` + `BatchedDataCollection`, only when `child.AllSourcesSupportBatchIndex()`.
- Pass the child's `estimated_cardinality` through (0 disables Bloom-filter pushdown in the parent hash join); `GetName()`="RECYCLER_MATERIALIZE", `ParamsToString()` with key hash and rows. Abort flag when the actual cap is exceeded: keep forwarding chunks, stop appending, don't publish in `Finalize`.

**PhysicalRecyclerScan** (childless source, `EXTENSION` type, owns `shared_ptr<CachedEntry>`): unordered mode = parallel scan; ordered mode = `SourceOrder()=FIXED_ORDER`, `ParallelSource()=false`, sequential scan. Optional column subset (`InitializeScan` accepts column ids) for subsumption/widening hits. `SetEstimatedCardinality(rows)` on the logical node.

Logical counterparts are `LogicalExtensionOperator`s that pass through the child's bindings/types, return `{}` from `GetTableIndex()`, override `SupportSerialization()` to false (so `Planner::VerifyPlan` skips the round-trip; `LogicalExtensionOperator::Deserialize` would throw), and `GetName()/ParamsToString()`.

### 3.5 Candidate selection and admission (post-optimize hook)

Walk top-down after computing all signatures bottom-up (so a child hit never poisons the parent). At each node, in order: (a) exact hit → substitute, stop descending; (b) subsumption hit (§3.7) → substitute with compensation, stop; (c) admissible write → wrap, stop descending on this path (one materialization per root-to-leaf path; hits below a writer are allowed); (d) recurse.

Candidate node types: `LOGICAL_DELIM_JOIN` (atomic unit), `LOGICAL_COMPARISON_JOIN`, `LOGICAL_AGGREGATE`, `LOGICAL_DISTINCT`, `LOGICAL_PROJECTION`/`LOGICAL_FILTER` directly above those, the definition child of `LOGICAL_MATERIALIZED_CTE`. Never `LOGICAL_ORDER_BY`/`TOP_N`/`LIMIT` themselves (cache *below* the sort; the exception is the pagination mode, §4 M6).

Admission gate (initial values, tune on the benchmark): `est_in` = Σ leaf estimated cardinalities + Σ join (left+right) cardinalities; `est_out` = `EstimateCardinality()`; `est_bytes` = est_out × Σ type widths (VARCHAR ≈ 24 B). Admit iff `est_in ≥ 200K` and `est_out ≤ 0.5 × est_in` and `est_bytes ≤ entry cap` and expected reuses `E = 1 + aged seen-count(bucket)` satisfy `E × (est_in − est_out) > 2 × est_out`, with speculation on first sight only when `est_bytes < 64 MB`. Zhou's containment rule: prefer the higher node unless a lower admissible node is < 0.5× its size with equal-or-higher seen-count.

Never admit: subtrees under a `LIMIT` without `ORDER BY` (q0016, q0122: arbitrary rows), subtrees containing a `LogicalDelimGet`, the delim side of a delim join, the non-delim child of a delim join when the delim join itself is the unit (its LHS is 12–36M rows), anything with volatile/parameterized expressions, table functions without a catalog table, `SAMPLE`.

### 3.6 Delim joins and dynamic filters (the two silent-wrong-answer traps)

- **Delim rule:** `PlanDelimJoin` gathers `DELIM_SCAN`s by walking `op.children` of the delim side only (`src/execution/physical_plan/plan_delim_join.cpp:14-24`); a `DELIM_SCAN` whose dependency was not registered dereferences a missing map entry (`physical_column_data_scan.cpp:67-84`). Hence: the whole `LOGICAL_DELIM_JOIN` is a safe atomic cache unit (all `DELIM_GET`s internal; on a hit the whole structure disappears); wrapping *above* it is safe; the non-delim child is wiring-safe once `children` is populated but is far too large in this workload; the delim side is never cacheable. `ColumnBindingReplacer` does rewrite `duplicate_eliminated_columns` and conditions (`src/planner/logical_operator_visitor.cpp:159-174`).
- **Dynamic-filter rule:** `JOIN_FILTER_PUSHDOWN` attaches a `DynamicTableFilterSet` to probe-side `LogicalGet`s that the build side fills at run time (`src/optimizer/join_filter_pushdown_optimizer.cpp:256-270`); a materialized subtree containing such a scan holds rows pruned by *that query's* partner (q0133 vs q0134: 837K vs 1.85M rows). If the owning join is inside the candidate (true for the delim-join unit), nothing to do; otherwise **refuse** (M4) or **strip** (`get.dynamic_filters = nullptr` and erase the owner's `probe_info` entry; also `OptionalFilter(DynamicFilter)` from TopN) before signing and wrapping (M5).

### 3.7 Subsumption with compensation (scope)

Only this narrow, provably sound form: an entry whose signature equals the query's except for range/`IS NOT NULL` table filters on **one base column that is projected in the cached output** (here `title.production_year`), where every cached conjunct is implied by the query's conjuncts (interval containment; `IS NOT NULL` is implied by any `BETWEEN`/comparison). Rewrite: `RecyclerScan(entry)` → `LogicalFilter(residual conjuncts rebound to the scan)` → `LogicalProjection` to the query's column order. Apply only if `rows(entry) × width + residual < est_in(query) × 0.7`. Anything outside the grammar (IN lists, LIKE, different country code, different EXISTS structure) is a miss. Behind `SET recycler_subsumption=true` until the differential harness is clean in both orders.

Union-projection widening (payer materializes extra columns so later sub-group members hit by column subsumption) is the biggest single lever (~+4 ms) but speculative: payer cost +5–15%, entries up to 340 MB. Last milestone, behind a setting.

### 3.8 Order preservation rules

`PreserveInsertionOrder` looks at the first `IsSource()` operator's `SourceOrder()` (`src/execution/physical_plan/plan_insert.cpp:15-52`). Rules: cached collections are unordered multisets unless written in `SEQ`/`BATCH` mode; never substitute a node whose output order is observable by its parent (a `LIMIT`, the result collector) unless the entry is ordered and the scan runs in ordered mode; whole-result entries of `ORDER BY` queries are always written and served ordered. The local test compares 143/149 queries with `rowsort`, but the grader's comparison may be order-sensitive, so treat order as part of correctness.

### 3.9 Correlated EXISTS as a plain join (added 2026-10-07)

The one change that lowers the *total* time rather than the geomean, and the first that makes a first-time query faster than vanilla. 63 of the 149 public queries carry 77 such subqueries and every one is rewritten. Measured 2026-10-07 with `scripts/score_gate.py` against the previous `main` build (fastest filename pass, grader's protocol): geomean 8.40 ms -> 6.13 ms, total 11.99 s -> 4.54 s, the same 47 queries served, 298/298 answers equal to vanilla in three orders; q0231 1285 -> 137 ms, q0240 957 -> 100 ms, the pages family (q0230/q0232/q0239/q0292) 570-750 -> 90-100 ms, the family-A payers 300-350 -> 57-73 ms. Ablation on one binary (fastest of two passes, a warmer machine): vanilla 35.7 ms / 24.4 s, rewrite alone 16.8 ms / 6.4 s, cache alone 9.5 ms / 12.9 s, both 6.4 ms / 4.4 s; no EXISTS query is slower with the rewrite than in vanilla, and no query without one moves by more than noise.

- **What DuckDB does.** `WHERE [NOT] EXISTS (SELECT ... FROM inner WHERE inner.k = outer.c ...)` is bound as a MARK dependent join over the whole FROM, flattened into a `LOGICAL_DELIM_JOIN` before the optimizer runs (`src/planner/planner.cpp:98`): the FROM's rows are materialized, their `c` values deduplicated and joined into the subquery, and the matches joined back. The Deliminator makes a plain join of it only when nothing in the whole delim join filters (`src/optimizer/deliminator.cpp`, `HasSelection`), and the join order optimizer never reorders a delim join nor any join tree containing one (`src/optimizer/join_order/relation_manager.cpp`, `LOGICAL_DELIM_JOIN` returns `can_reorder = false`). With one predicate anywhere, q0231 materializes 35.8 M joined rows to keep 2.8 M.
- **The rewrite (`recycler_exists.cpp`).** For the exact shape `DELIM_JOIN(MARK)[outer, PROJECTION[..., delim cols] -> FILTER[e(delim) = f(inner) ..., rest] -> CROSS_PRODUCT(inner, DELIM_GET)]` with every correlated conjunct an `=` or `IS NOT DISTINCT FROM` comparison between an expression over the delim columns and one over the subquery, and nothing else in the subquery referring to the outer row: replace the delim join by a plain `LOGICAL_COMPARISON_JOIN(MARK)` whose conditions are `e(c) IS NOT DISTINCT FROM p` with `p = f(inner)` appended to the subquery's projection, and `f(inner) IS NOT NULL` added to its filter for the `=` conjuncts (a plain `=` never matches a NULL; this keeps the mark two-valued, which is what lets filter pushdown turn `NOT EXISTS` into an ANTI join, `pushdown_mark_join.cpp:54-75`). Then move the new join below the operators the join order optimizer stops at: the preserved side of an outer join, filters, joins that keep every row of the side that produces the correlated columns. Filter pushdown turns the MARK join into SEMI/ANTI; the join order optimizer, which does reorder plain semi joins (`JoinIsReorderable`), places it by cost: on `title` when that shrinks the join, above a selective join when it does not (measured: an EXISTS correlated on `cast_info` under a `title.production_year = 2015` join stays above it, 0.023 s either way).
- **Left alone (fail closed):** correlated `IN`/`ANY` (extra `=` condition, NULL semantics), scalar subqueries (`SINGLE`), inequality correlation, a correlation whose sides are not bare columns (an inner-side expression would be evaluated for every inner row by the new projection, where the planner evaluates it only against outer values that exist: a failing cast would fail a query the planner answers), correlation in a join condition or an aggregate of the subquery, subqueries with `LIMIT` (planned with a window), nested correlated subqueries, a subquery that projects the outer column, a delim join with projection maps or `delim_flipped` (never the case before the optimizer), and a subquery holding one of the recycler's own operators. The last one was a wrong answer found by review before it shipped: with a block marker planted inside the subquery (the outer WHERE not cacheable, the subquery's FROM/ON block admissible), the rewrite put the subquery's conjunct directly above the marker, and the join order optimizer, which passes single-child operators it does not know through (`relation_manager.cpp`, `ExtractJoinRelations`), pushed it below the marker: the stored block missed rows its key promised and the next query with that block read them. The same pass-through can reach any marker that ends up directly below a filter; today's planting sites (a WHERE filter under a projection or aggregate) do not, but it is the thing to check for every new planting site.
- **Where it runs.** At the end of the pre hook, after every key was taken and every block marker planted: whole-result, nested-block and block keys are those of the plan as bound, and the rewrite is a pure function of the shape, so a payer and the queries it serves still render alike in the post hook. It runs for every statement (EXPLAIN, writes, untracked transactions) and whether or not recycling is enabled; only `SET disabled_optimizers = 'extension'` turns it off, which is what keeps the gate's vanilla checkpoints vanilla. Setting `recycler_exists_rewrite` (default true); counters `exists_rewrites`, `exists_pushdowns`, `exists_refused`, `exists_ns` (about 2 us per statement).
- **Verification.** `test/sql/recycler/recycler_exists.test` (generated by `scripts/gen-exists-test.py`): 50 shapes, each asserted with the rewrite on, off and with the extension's optimizers disabled, against values vanilla returns, plus plan and counter checks and a block/subsumption round trip; DuckDB's own `test/sql/subquery/*` (87 cases) under the release and the assertion-enabled build; `scripts/recycler_diff.py` in three orders; `scripts/score_gate.py` against the previous `main` build.
- **Pitfall found on the way.** The connection's cached settings are dropped by the gate when the statement is a `SET`/`RESET`/`PRAGMA` and re-read by the next statement; reading them during the `SET` statement itself caches the values from before it ran (every later query then used `recycler_min_input_rows = 200000`). Anything that reads settings outside the gate must skip those roots.

## 4. Roadmap

Effort is calendar days for a small student team working in parallel. Gains use the workload analyst's model (`scratchpad/workload_analysis.md`, `projection.py`).

| # | Milestone | Effort | Geomean after | Risk |
|---|---|---|---|---|
| M0 | **Stop the regression, fix crashes.** Populate `children`, default `BuildPipelines`; parallel sink + parallel source; pass cardinality; drop `ANY_JOIN`; `static_cast` for the info; `GetTableIndex()={}`, `SupportSerialization()=false`; delete second entry point; explicit includes; disable materialization by default until M1 keys land. Ship to `main` and dispatch CI. | 1–2 d | ~30.7 ms (parity) | low |
| M1 | **Canonical signature + infrastructure.** Renderer/hash of §3.2 with byte-string confirmation; statistics map; owning `CachedEntry` + pinning; budget/eviction; invalidation epochs; `recycler_stats()` table function (hits, misses, stores, refusals by reason, bytes, hook nanoseconds); settings via `DBConfig::AddExtensionOption`; `scripts/recycler_diff.py` differential harness (vanilla vs enabled; filename, reverse, shuffled order; 2× repeat; threads=1; tiny budget); `test/sql/recycler/*.test` for every audit repro. | 2–3 d | ~30.5 ms | low–medium |
| M2 | **Whole-result cache (mechanism i).** Pre hook computes the root key (child of `LOGICAL_EXPLAIN`, or root) and stashes it in a per-connection `ClientContextState`; hit → replace with ordered/unordered `RecyclerScan`; miss + admissible (≥ 2 joins or est_in ≥ 1M, no LIMIT-without-ORDER, no parameters) → wrap root in `RecyclerMaterialize` (`SEQ` if ordered), cap 1M rows / 64 MB. Verify the EXPLAIN ANALYZE JSON still has `latency` on a hit. 18 hits: q0248/q0284/q0293 (1.0–1.3 s → ~60 ms), q0274, q0084–q0090, lookups. | 2 d | **~25.1 ms** | low–medium |
| M3 | **Nested-block matching (iv).** In the pre hook, compare every subtree's key (outside CTE definitions) against whole-result entries; substitute `RecyclerScan` under the residual `FILTER`/`ORDER`/`PROJECTION`, rebind with `ColumnBindingReplacer` from the root. Premise to verify first with plain `EXPLAIN`: q0233's bound inner block is node-for-node q0231's bound plan. 7 hits: q0233, q0241, q0286, q0287, q0083, q0258, q0267. | 1.5–2 d | **~22.0 ms** | medium |
| M4 | **Post-optimize subtree cache (ii, iii-a, vii).** Signatures for all nodes, top-down selection, admission gate, `LOGICAL_DELIM_JOIN` as atomic unit, aggregates/distinct/CTE definitions as cache points, dynamic-filter refusal, self-check pass, unwrap-on-failure. Hits: q0242←q0239, q0117, q0132, q0134, q0121/q0125–q0128, q0058, opt_r users (q0082/q0089/q0092–q0094/q0255/q0257/q0264/q0273), q0298, q0054/q0055 CTEs. | 3–4 d | **~20.4 ms** | medium |
| M5 | **Subsumption (v)** per §3.7 (+ dynamic-filter stripping to widen coverage). 12 hits: q0234←q0231; q0244–q0246, q0280–q0282, q0289–q0291←q0240; q0249←q0247; q0266←q0259. | 3–5 d | **~16 ms** | medium–high |
| M6 | **Pagination (vi). Implemented** (see the status section above): a `PAGINATION` entry kind holding the first `4 × (offset + limit)` sorted rows under `LIMIT/OFFSET` when `limit+offset > 5000` (not TopN, `src/optimizer/topn_optimizer.cpp:23-52`) and the ORDER BY is a provable total order; key taken from the bound plan, ordered `SEQ` sink that stops after the prefix, ordered scan on hits, geometric growth, switch `recycler_pagination`. q0093/q0094 ← q0092. | 1–2 d | no full-suite figure (noisy machine) | medium |
| M7 | **Union-projection widening (iii-b)**, behind `recycler_widen`: per-signature history of needed columns; payer keeps up to 8 extra cheap columns alive (`LogicalGet::AddColumnId` + keep-alive refs on the marker); hits project a subset. 16 more Family A hits. | 2–3 d | **12–13 ms** | high |
| M8 | **Hardening/CI** (continuous): debug build (`D_ASSERT`, `ColumnBindingResolver::Verify`), ASAN/UBSAN once, GCC `-Wall -Wextra -Werror`, `PRAGMA enable_verification`, `preserve_insertion_order=true`, memory_limit=1 GB (eviction/spilling), 3× workload in one process, overhead gate ≤ 25 µs/plan. | 1–2 d | — | low |

Ship after every milestone: `IMDB_DATA=1 make test`, `make benchmark`, differential harness, then merge to `main` and **dispatch the CI build** (grading uses the last successful build). M0 alone removes the 14% penalty the graded binary currently carries.

## 5. Testing and verification strategy

- `test/sql/recycler/adversarial_pairs.test` (synthetic tables; every pair must return vanilla results with the cache warm): positional column identity (`n.name` vs `n.gender`), `range(3)`/`range(6)`, `LIMIT 2`/`LIMIT 5` inside `IN (subquery)`, INNER vs LEFT, `=` vs `<`, IN lists of 6 differing values, different projection maps, `random()`/`now()` (must never hit), prepared parameters.
- `order_limit.test`: whole-result hits of `ORDER BY` queries with total keys compared `nosort`; pagination with three offsets; `LIMIT` without `ORDER BY` never hits.
- `delim_cte.test`: EXISTS/NOT EXISTS/IN-subquery blocks, two EXISTS, nested `SELECT * FROM (<q>) d WHERE ...` with/without outer ORDER BY, CTE referenced once/twice.
- `invalidation.test`: INSERT/UPDATE/DELETE/DDL between repeats, `BEGIN ... ROLLBACK`, two connections, `ATTACH ... (READ_ONLY)` + `USE`, `DETACH`/re-`ATTACH`.
- `memory.test`: tiny `recycler_budget_mb`, eviction, abort on the actual-size cap, `recycler_stats()` counters.
- `explain.test`: `EXPLAIN (ANALYZE, FORMAT JSON)` on a hit contains `latency` and a non-empty operator tree.
- `scripts/recycler_diff.py`: runs all 149 queries vanilla and with the extension in filename, reverse and shuffled order, twice, and compares result multisets (and exact order for `ORDER BY` queries).
- Review gate: `anthropic-skills:duckdb-code-review` checklist before each merge (justify every addition, no copies, concurrency, deterministic tests).

## 6. CI / portability checklist

Only `linux_amd64`/`linux_arm64` are built; local development is macOS/clang. Before dispatching: GCC build (`manylinux` container or `brew gcc`) with `TREAT_WARNINGS_AS_ERRORS=1`; explicit includes (`duckdb/common/printer.hpp`, `to_string.hpp`, `<utility>`); no `reinterpret_cast`; one `DUCKDB_CPP_EXTENSION_ENTRY(waddle)`; no debug-only assumptions (`ColumnBindingResolver::Verify` is `#ifdef DEBUG`, so add our own self-check); keep `test/sql/benchmark.test` behind `require-env IMDB_DATA`.

## 7. Do not do

- Do not rename the extension (`waddle`), change `duckdb_version`/`uses` in the workflow, or touch anything under `duckdb/`.
- No cross-run disk persistence of the cache: the harness deliberately gives each run a fresh process, cwd and TMPDIR.
- No caching of `LIMIT`-without-`ORDER BY` results, delim sides, `DELIM_GET`-containing subtrees, table functions, volatile expressions, or anything whose bindings cannot be traced.
- No `Expression::Hash()`/`ToString()`-based keys; no bare `LogicalColumnDataGet` at the root under EXPLAIN ANALYZE (profiler trap).
- No re-running the optimizer, aggregate rewriting (cube caching), or partial/overlapping-result merging: the risk/benefit is wrong for a correctness-dominated score.
- No `PRAGMA` output via `Printer::Print` for stats (untestable); use a table function.

## 8. Open questions to settle by experiment in week 1

1. Actual cost of a wide (union-projection) collection write on the payers q0226/q0231/q0235/q0240 (decides M7).
2. Bound-plan identity of nested blocks (q0233 vs q0231) at the pre hook — the premise of M3.
3. Per-query hook overhead on the 11 sub-millisecond queries once M1 lands (`recycler_stats()` nanoseconds).
4. Whether the grader's answer comparison is order-sensitive for `ORDER BY` queries (assume yes).
5. Whether stripping dynamic filters (vs refusing) is needed for enough coverage to matter.
6. The unexplained 1.72× slowdown of q0231 in the current extension run although its plan has no writer (buffer-pool pressure from ~0.5 GB of cached data, or noise).

## Appendix: key DuckDB v1.5.5 references

`optimizer/optimizer.cpp:331-349` (hook order) · `optimizer/deliminator.cpp:58-84` (why delim joins survive) · `optimizer/in_clause_rewriter.cpp:60-105` (IN → CHUNK_GET) · `optimizer/join_filter_pushdown_optimizer.cpp:256-270` (dynamic filters) · `optimizer/column_binding_replacer.cpp:18-41` · `planner/logical_operator_visitor.cpp:159-174` · `execution/physical_operator.cpp:214-262` (default BuildPipelines/GetSources) · `execution/physical_plan/plan_delim_join.cpp:14-63` · `execution/operator/join/physical_left_delim_join.cpp:38-55,124-142` · `execution/operator/scan/physical_column_data_scan.cpp:25-84` · `execution/operator/helper/physical_materialized_collector.cpp:14-63` · `execution/physical_plan/plan_insert.cpp:15-69` (order preservation) · `execution/physical_plan/plan_limit.cpp:57-96` · `main/query_profiler.cpp:134-176,959-975` (profiler trap) · `main/client_context.cpp:958-965` (timer start) · `main/database.cpp:80-104` (shutdown order) · `common/types/column/column_data_collection.cpp:1054-1113,1214-1227` (parallel scan, Combine).
