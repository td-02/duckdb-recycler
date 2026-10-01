# CI: `.github/workflows/tests.yml`

Runs on every pull request to `main` and by hand (`Run workflow`; tick `full` to add the slow jobs). It never publishes a binary: `MainDistributionPipeline.yml` stays the only producer of what the grader benchmarks.

| Job | Build | Runs | What a green run proves |
|---|---|---|---|
| `build-test` | `make release` (GCC, ninja), ICU linked in | `test/sql/recycler/*`, `test/sql/waddle.test` | the extension builds like a developer's `make release` and the recycler returns vanilla answers on the module tests |
| `debug` (PR / `full`) | `make debug`, `DISABLE_SANITIZER=1`, ICU linked in | same suites | every `D_ASSERT` and DuckDB's debug-only plan verification (`ColumnBindingResolver::Verify`) hold on the same suites |
| `asan-ubsan` (PR / `full`) | `make relassert` (RelWithDebInfo + `FORCE_ASSERT`, which is how DuckDB's CMake enables ASAN and UBSAN; the makefile has no switch that adds them to `release`) | same suites | no memory error, no undefined behaviour, assertions on |
| `tsan` (PR / `full`) | `THREADSAN=1 make reldebug`, clang, DuckDB's suppression list | `test/sql/recycler/recycler_stress.test` | no data race between concurrent readers, writers and the cache |
| `stock-duckdb` | none; downloads `build-test`'s `waddle.duckdb_extension` | `scripts/ci_stock_duckdb.py` on the PyPI `duckdb==1.5.5` wheel | the loadable binary loads into stock DuckDB with `allow_unsigned_extensions` (exactly how the grader consumes it), repeated queries match a plain connection, and `recycler_stats()` shows the repeats were served |
| `format` (non-blocking) | none | `clang-format 11.0.1 --dry-run -Werror` over `src/` with `duckdb/.clang-format` | the sources follow DuckDB's style; blocking once `make format` has been run (32 violations on 2026-10-01) |

`build-test` and `debug` build with `EXT_FLAGS=-DBUILD_EXTENSIONS=icu` (the makefile's channel for extra CMake flags), which
links the ICU extension into the unittest binary so that `test/sql/recycler/recycler_settings_icu.test` runs; without it the
test's `require icu` skips it, as it does in a plain developer build. The waddle artifact does not change. The sanitizer jobs
build without ICU.

The four build jobs share `.github/actions/build-extension`: it checks that the `duckdb` submodule is at the pinned tag `v1.5.5` (fetching
that one tag so `git describe` stamps the extension with the version stock DuckDB expects), restores a `ccache` keyed on job name
and submodule commit, runs one make target, and saves the cache right after the build so a red test run still warms the next one.
Cost (Actions minutes on `ubuntu-latest`, 4 vCPU): the first run of each job compiles all of DuckDB, about 45 min for `build-test`
and `debug` and 55 min for each sanitizer job, roughly 200 min for the first pull request; with a warm cache only the extension's
sources recompile, about 12 min per build job, 60 min per pull request and 15 min for a plain dispatch (`build-test`, `stock-duckdb`,
`format`). Jobs run in parallel, so wall time is bounded by the slowest sanitizer job, and `cancel-in-progress` drops superseded runs.
