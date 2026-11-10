# duckdb-recycler

A DuckDB extension that **recycles query results**. It notices when a query repeats all or part of an earlier one
and serves that part from cache instead of computing it again. Dashboards, BI tools and data agents that explore a
dataset step by step all send this kind of overlapping workload.

Built against **DuckDB v1.5.5**. The loadable binary is named `waddle` (a name kept from the course template it
started from); everything it adds is prefixed `recycler_`.

## Results

On the public workload (149 queries over the IMDB / JOB dataset, the set run twice; `threads = 6`,
`memory_limit = '8000MB'`, `preserve_insertion_order = false`). Every answer matches vanilla DuckDB.

| Configuration | Geomean per query | Total |
|---|---:|---:|
| Vanilla DuckDB 1.5.5 | 35.7 ms | 24.4 s |
| EXISTS rewrite only | 16.8 ms | 6.4 s |
| Result cache only | 9.5 ms | 12.9 s |
| **Both (default)** | **6.4 ms** | **4.4 s** |

All four rows are the same binary, measured on one machine in one session (2026-10-07); the middle rows turn a feature off.

## How it works

The extension adds two optimizer hooks (`src/recycler_hooks.cpp`) and two physical operators, `RECYCLER_SCAN`
(reads a cached result) and `RECYCLER_MATERIALIZE` (stores a result while passing it on).

- **Whole-result reuse.** Each bound plan gets a canonical structural signature (`src/recycler_signature.cpp`).
  A repeat of an earlier query, or a nested block equal to one, is answered straight from the cache.
- **Subtree reuse.** After DuckDB's own optimizers run, joins, aggregates, DISTINCTs and CTE definitions are
  candidates. Exact hits become a `RECYCLER_SCAN`. Misses worth keeping (large input, output much smaller,
  within size limits) are wrapped in a `RECYCLER_MATERIALIZE`.
- **Predicate subsumption.** A block whose range predicates (`<`, `BETWEEN`, `IS NOT NULL`, ...) are implied by a
  cached block is served from that block, with a residual filter on top.
- **Ordered pagination.** Repeated `ORDER BY ... LIMIT n OFFSET m` pages over the same input share one cached
  sorted prefix instead of sorting again for each page.
- **Correlated EXISTS rewrite** (`src/recycler_exists.cpp`). `[NOT] EXISTS` subqueries become plain MARK joins
  that DuckDB's join order optimizer can place by cost, instead of delim joins that lock the FROM order in place.

### Correctness

Results must equal vanilla DuckDB's, so the cache is conservative:

- Keys cover the exact bound plan, including types and the session settings that change results at run time
  (`TimeZone`, `Calendar`, `default_order`, ...).
- Nothing is cached that uses volatile or session-dependent functions, parameters, table functions, temporary
  tables, attached non-DuckDB catalogs, or `LIMIT` without `ORDER BY`.
- Any statement that changes data or the catalog clears the cache, and nothing is cached or served inside a
  transaction that has written.
- Memory is bounded: cached data lives in DuckDB's buffer manager, and benefit-density eviction keeps the cache
  under `min(recycler_budget_mb, memory_limit / 4)`.

The full design, with the reasoning behind each rule, is in [`docs/SOLUTION_PLAN.md`](docs/SOLUTION_PLAN.md).

## Build

```sh
git clone --recurse-submodules https://github.com/td-02/duckdb-recycler.git
cd duckdb-recycler
make release
```

This produces `build/release/duckdb` (a shell with the extension linked in) and
`build/release/extension/waddle/waddle.duckdb_extension` (the loadable binary).

To load it into a stock DuckDB 1.5.5:

```sql
-- start duckdb with -unsigned, or SET allow_unsigned_extensions = true
LOAD 'build/release/extension/waddle/waddle.duckdb_extension';
```

## Usage

The recycler is on as soon as the extension loads. To see what it is doing:

```sql
SELECT * FROM recycler_stats();     -- hits, misses, bytes, budget
SELECT * FROM recycler_entries();   -- what is cached right now
SELECT * FROM recycler_history();   -- what is known about each signature
PRAGMA recycler_clear;              -- empty the cache
```

Settings (all `SET`-able per connection):

| Setting | What it controls |
|---|---|
| `recycler_enabled` | master switch for caching |
| `recycler_whole_result`, `recycler_subtrees`, `recycler_blocks` | which kinds of results are cached |
| `recycler_subsumption`, `recycler_pagination` | predicate subsumption, pagination prefixes |
| `recycler_exists_rewrite` | the correlated EXISTS rewrite |
| `recycler_budget_mb`, `recycler_entry_cap_mb`, `recycler_entry_cap_rows` | memory budget and per-entry caps |
| `recycler_min_input_rows`, `recycler_speculate_mb`, `recycler_compact_rows` | when a miss is worth storing |

## Testing and benchmarking

```sh
make test_release                         # SQL tests, including test/sql/recycler/
python3 ./scripts/download-imdb.py        # fetch the IMDB dataset into data/
IMDB_DATA=1 make test_release             # also checks every benchmark query against vanilla
python3 ./scripts/recycler_diff.py        # differential check over the workload in several orders
make benchmark                            # time the workload in benchmark/
```

CI (`.github/workflows/tests.yml`, described in [`docs/CI.md`](docs/CI.md)) builds in release, debug, ASan/UBSan
and TSan, runs the recycler suites, loads the binary into the PyPI `duckdb==1.5.5` wheel, and checks formatting.

## Repository layout

| Path | Contents |
|---|---|
| `src/` | extension sources: hooks, signatures, cache, policy, rewrites, operators, table functions |
| `test/sql/recycler/` | sqllogictest suites (invalidation, settings, concurrency, memory, types, ...) |
| `benchmark/` | the public query workload, one statement per file, run in filename order |
| `scripts/` | dataset download, benchmark runner, differential checker, score gate |
| `docs/` | design plan, CI notes, upstream extension-template docs |

## Background

This project began as Assignment 2 of a Data System Architecture course: make repeated analytical workloads
faster in DuckDB without ever changing an answer. Background reading:

- Saxena et al., [*Why TPC Is Not Enough: An Analysis of the Amazon Redshift Fleet*](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf), PVLDB 2024
- Nagel, Boncz, Viglas, [*Recycling in Pipelined Query Evaluation*](https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=6544837), ICDE 2013
- Dursun et al., [*Revisiting Reuse in Main Memory Database Systems*](https://dl.acm.org/doi/pdf/10.1145/3035918.3035957), SIGMOD 2017

## License

MIT, see [`LICENSE`](LICENSE).
