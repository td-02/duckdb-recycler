# Query Recycler — Production Plan (cost model, memory diet, ageing, disk tier, merge-readiness)

Status: written 2026-09-30 against the code on `dev_tapesh_m6` (commit 9fb66fce3b, open as td-02/dsa_asignment2#10).
`origin/main` carries M0–M5 and is what the grader builds; the local `main` checkout is 13 commits behind it.
This plan extends `docs/SOLUTION_PLAN.md` (design, M0–M6 done) and `docs/EXTENSION_REPORT.md` (measurements of
2026-09-26). Every number below is either quoted from the report or was measured today with `build/release/duckdb`
(v1.5.5 d8cdaa33fd, the M6 binary) on `data/imdb.duckdb`; every DuckDB fact cites the pinned v1.5.5 source under
`duckdb/`. "MiB" is used throughout; the report's "MB" are MiB.

The plan answers four requests in order: (1) a cost/performance model that decides which results are worth
caching and which queries are worth touching at all (§2, §3); (2) a memory diet (§4); (3) cache ageing policies
(§5); (4) whether the buffer manager can serve as a disk tier for the cache — the risky part (§6). §7 turns the
current code into merge-worthy code (review findings, refactor, tests, CI) and §8 sequences everything into
milestones with exit criteria.

## 0. Ground rules that do not change

- Extension name `waddle`, DuckDB pinned at v1.5.5, nothing under `duckdb/` is touched, `.github/workflows`
  keeps `duckdb_version` and `uses` as they are.
- Correctness before everything: any rewrite fails closed to the vanilla plan; every merge passes
  `IMDB_DATA=1 make test`, the recycler sqllogictests, `scripts/recycler_diff.py` in three orders, and the
  `anthropic-skills:duckdb-code-review` checklist (justify every addition, no copies, rigorous concurrency,
  deterministic tests, one PR = one change).
- The score is the geometric mean of per-query latency of the faster of two fresh-process runs, measured under
  `EXPLAIN (ANALYZE, FORMAT JSON)` with `threads=6`, `memory_limit='8000MB'`, `preserve_insertion_order=false`.

## 1. Facts measured for this plan (2026-09-30)

### 1.1 Where the payer overhead goes (per-operator profile of the two heaviest payers)

`EXPLAIN (ANALYZE, FORMAT JSON)` of q0240 and q0231 in fresh processes, four configurations, two repetitions
each, grader settings, database opened read-only. The profiler's per-operator time is *thread time* (summed over
the 6 threads).

| Query | Block entry | `RECYCLER_MATERIALIZE` (block) thread time | Thread time per MiB written and re-read | Join order vs vanilla |
|---|---|---:|---:|---|
| q0240 | 1,643,061 rows, 79 MiB | 736 ms | 9.3 ms/MiB | identical |
| q0231 | 2,781,804 rows, 130 MiB | 1,043 ms | 8.0 ms/MiB | identical |

Consequences: the payer overhead of the block writes is the *copy* (append into a buffer-managed collection plus
the re-scan by the parent pipeline), not a worse plan — the plan shape and the join order are the same with and
without the marker, so the estimate-distortion worry from the M5 notes is not what costs. At 6 threads the copy is
1.3–1.6 ms/MiB of wall time if it overlaps perfectly; the report's fastest-run deltas (q0240 +375 ms, q0231
+428 ms, i.e. 3.3–4.7 ms/MiB all-in) include the pipeline break and run-to-run noise. Today's absolute wall clocks
were 1.5–2× the report's for every configuration including vanilla (thermal/noise state of the laptop), so only
the thread-time ratios above are relied upon. Reproduction: Appendix A.

### 1.2 What the cache holds after the public workload (census, one process, filename order)

| | Entries | Rows | Bytes (allocated) | Hits |
|---|---:|---:|---:|---:|
| block | 52 | 21,882,896 | 1,061 MiB | 19 |
| subtree | 83 | 10,180,451 | 205 MiB | 11 |
| whole result | 123 | 3,016,907 | 207 MiB | 20 |
| **total** | **258** | | **1,473 MiB** | **50** |

- **228 of 258 entries were never read; they hold 1,035 MiB of the 1,473 MiB.** The 30 entries that were hit
  hold 438 MiB.
- **Duplicate block copies.** 14 shapes hold 2–7 copies each (one per distinct column set of the payer). The
  six largest: 209 MiB (5 copies), 194 MiB (4), 176 MiB (7 copies, 10 hits), 145 MiB (2, 0 hits), 125 MiB (2),
  64 MiB (2). Replacing each shape's copies by one wide entry (assumed 1.3× the average copy) would free
  **≈ 555 MiB** of the 1,061 MiB of block space.
- **Tiny entries pay for whole blocks.** A buffer-managed `ColumnDataCollection` allocates at least one 256 KiB
  block per writer thread (`ColumnDataAllocator::AllocateBlock`, `column_data_allocator.cpp:94-108`, allocates
  `max(size, block_size)`), and the parallel sink writes from up to 6 threads. The 166 entries with at most 1,000
  rows hold 9,005 rows in total but **71.5 MiB**; the median entry with ≤ 100 rows is exactly one block
  (262,136 bytes) and the largest is six (1.57 MiB).
- **Ordered whole results** (91 entries, 196 MiB) include the three 983K-row page sources (171 MiB).
- `duckdb_memory()` at the end: `COLUMN_DATA` 1,473 MiB (= the cache, to within 0.3 MiB), `BASE_TABLE`
  1,494 MiB (the IMDB pages), `ART_INDEX` 21.5 MiB, `temporary_storage_bytes` 0 everywhere: nothing spilled,
  3.0 GiB used of the 8,000 MB limit.
- Hook cost: pre hook 48 µs per call, post hook 26 µs per call (156 / 145 calls); the report measured 27/15 µs.

### 1.3 The score in log space (from the report's Appendix D, fastest run of three)

Because the score is a geometric mean, each query contributes `ln(vanilla / extension)` "nats" and the score is
`vanilla_geomean × exp(−Σ nats / 149)`. Decomposition of the measured 29.08 → 9.22 ms:

| Class | Queries | Σ nats | Effect on the geomean |
|---|---:|---:|---|
| served by at least one hit | 47 | +179.5 | × 0.300 |
| payers (materialize, never served) | 87 | −8.0 | × 1.055 |
| untouched | 15 | −0.4 | × 1.003 |
| total | 149 | +171.1 | × 0.317 (= 9.22 / 29.08) |

Value of one hit at the ~0.3 ms hit floor, by the query's vanilla latency, and cost of overheads:

| Vanilla latency of the served query | nats | share of the score |
|---:|---:|---:|
| 0.5 ms | 0.5 | 0.34 % |
| 2 ms | 1.9 | 1.27 % |
| 5 ms | 2.8 | 1.87 % |
| 20 ms | 4.2 | 2.78 % |
| 50 ms | 5.1 | 3.38 % |
| 200 ms | 6.5 | 4.27 % |
| 1,000 ms | 8.1 | 5.30 % |

| Overhead | nats | share of the score |
|---|---:|---:|
| payer +10 % (a typical block write) | 0.095 | 0.06 % |
| payer +40 % (q0231, q0240 in the report) | 0.336 | 0.23 % |
| +0.03 ms on a 0.2 ms query (the hook cost on a point lookup) | 0.140 | 0.09 % |

Three things follow and drive the whole plan:

1. **A hit on a 20 ms query is worth as much as 30 wasted 10 % writes.** Under a geomean, speculation is nearly
   free and hits on mid-size queries matter as much as hits on one-second queries. Never trade a plausible hit
   for a cheaper payer.
2. **Fixed overhead on sub-millisecond queries is the most expensive thing per byte of code.** The 12 queries
   vanilla answers under 1 ms cost −1.98 nats (1.3 % of the score) for zero possible gain: they must be skipped
   before any analysis runs.
3. **Payer overhead is worth reducing only where it is large relative to the query** (the page sources of
   q0080/q0082 at +30–40 %, the block writes of q0231/q0240), and never by giving up a store that a family
   member will use.

What-if arithmetic on the same table: payers at vanilla speed → 8.65 ms; the sub-2 ms queries at vanilla speed →
9.08 ms; whole-result and nested-block hits at a 0.15 ms floor → 8.33 ms; subsumption hits twice as fast →
8.44 ms.

## 2. Which queries are worth optimizing, and which are not

Per family (membership from the report §3.2/§6.5; nats from Appendix D):

| Tier | Family | n | payers | hits | Σ nats | share of score | Verdict |
|---|---|---:|---:|---:|---:|---:|---|
| A | EXISTS block over `cast_info`, year predicates (q0226–q0229, q0234–q0238, q0243–q0246, q0279–q0282, q0288–q0291) | 21 | 9 | 12 | +45.2 | 26.2 % | the score; 9 payers are candidates for widening hits (§4.1) |
| A | small exact repeats q0075–q0081 → q0084–q0090 | 12 | 6 | 6 | +28.3 | 17.3 % | 3–56 ms queries served at 0.13–0.21 ms; worth as much as the one-second repeats |
| A | q0240 aggregation and its repeats (q0231, q0240, q0248, q0284, q0293) | 5 | 2 | 3 | +22.8 | 14.2 % | keep; the two payers cost 0.7 nats together |
| A | q0265/q0274, q0259/q0266/q0268, q0256/q0258, q0267, q0269/q0270 | 10 | 5 | 5 | +22.0 | 13.7 % | keep |
| A | drill-downs into q0231/q0240 (q0233, q0241, q0286, q0287) | 4 | 0 | 4 | +20.3 | 12.7 % | keep |
| B | sorted `title` pages (q0080, q0082, q0089, q0092–q0094, q0255, q0257, q0264, q0273) | 10 | 6 | 4 | +13.4 | 8.6 % | worth it; the two page-source payers cost −0.30/−0.34 nats each: the most expensive writes in the set relative to their query |
| B | CTE / shared aggregates (q0053–q0055, q0057–q0058, q0125–q0134) | 15 | 6 | 8 | +9.5 | 6.2 % | worth it; q0133 is served but still 68 ms (residual work) |
| B | EXISTS with LIMIT/OFFSET pages (q0230, q0232, q0239, q0242, q0247, q0249, q0283, q0285, q0292) | 9 | 7 | 2 | +7.5 | 4.9 % | worth it; two hits pay for seven writes |
| C | sub-millisecond queries (12) | 12 | 0 | 0 | −2.0 | −1.3 % | **not worth touching**: bail out before analysis |
| C | everything else (singletons and small families) | 52 | 46 | 3 | +4.1 | 2.7 % | not worth engineering effort; their writes cost 0.02–0.09 nats each and are tiny after compaction, except the large never-hit subtrees (q0011: 2.2 M rows) that the memory diet handles |

Reading: tiers A and B are 45 % of the queries and 99 % of the gain; the cost model's job is not to find new
hits there (widening does that, §4.1) but to keep them while spending less memory and less payer time, and to
make the same decisions on the private set, whose families are "similar" but whose sizes and orders are unknown.

## 3. The cost/performance model

### 3.1 Quantities

For a candidate `e` (a signature at a plan node; kinds `WHOLE_RESULT`, `SUBTREE`, `BLOCK`):

| Symbol | Meaning | Source today | Source after P2 |
|---|---|---|---|
| `est_in` | rows scanned by the subtree | `NodeInfo::est_leaf_rows` | unchanged |
| `est_out`, `width`, `est_bytes` | output rows, bytes per row, bytes | `EstimateCardinality`, `RecyclerEstimateRowWidth` | unchanged; `bytes` measured once stored |
| `t_p` | time of the query that would pay | — | measured in `QueryEnd` (query latency) |
| `c_e` | time the subtree took to produce | — | measured: thread time of the subtree under the `RECYCLER_MATERIALIZE` node in the profiler tree (available under EXPLAIN ANALYZE, i.e. always on the grader) |
| `m_e` | cost of materializing | — | measured: the operator's own thread time; first sight: `bytes × w_write` |
| `h_e` | cost of a hit | — | measured on hits (`RECYCLER_SCAN` thread time + hit floor); first sight: `floor + bytes × w_scan` |
| `p_e` | probability the entry is reused before it is evicted | implicit (op-type rules) | per-kind ratio `(entries hit at least once + 1) / (entries stored + 2)`, updated online, and 0.7 once the signature has been seen twice |
| `w_write`, `w_scan`, `ns_per_est_row`, `hit_floor` | machine/workload calibration | fixed thresholds | online medians over the last 64 observations; defaults from §1.1: `w_write` 8.6 ns/B thread time, `w_scan` 4 ns/B, `ns_per_est_row` 20 ns, `hit_floor` 0.3 ms |
| `threads` | `TaskScheduler::NumberOfThreads()` | — | converts thread time to wall time for parallel sinks/scans; 1 for serial |

All measured quantities live in the signature-keyed history that already exists (`RecyclerCache::seen`), which
becomes a bounded `std::unordered_map<SigHash, SignatureHistory>` (P2): `seen`, `last_seen_sequence`, `bytes`,
`cost_ns`, `materialize_ns`, `hit_ns`, `refusal_reason`. It survives eviction (Nagel et al.'s recycler history)
and is bounded to 4,096 signatures by LRU so a long-running server cannot leak.

### 3.2 The admission rule (post-optimize hook, replaces `SubtreeWalker::Admit`'s magic factors)

```
gain  = p_e × ln( max(t_q, hit_floor) / h_e )        // nats a future hit is expected to earn
cost  = ln( 1 + m_e / t_p )                          // nats the payer loses now
rent  = lambda × bytes_e                             // nats per byte of cache the entry displaces
admit iff gain > cost + rent, and the hard caps hold (entry cap rows/bytes, budget, correctness rules of §0)
```

with `t_q ≈ t_p` (the consumer of a whole result or block runs about the payer's work), `t_p ≈ est_in ×
ns_per_est_row / threads` on first sight and the measured value afterwards, `m_e ≈ bytes_e × w_write / threads`
(`/ 1` for serial sinks), `h_e ≈ hit_floor + bytes_e × w_scan / threads`, and `lambda` = the eviction priority of
the entry that `MakeRoom` would evict for this one (0 while the cache is below its soft target, §4.4), expressed
in nats per byte so that the rule stays dimensionless.

Worked examples with the calibration defaults (6 threads):

| Candidate | bytes | t_p | p | gain | cost | decision |
|---|---:|---:|---:|---:|---:|---|
| q0240 block (first sight) | 79 MiB | 928 ms | 0.3 (BLOCK prior) | 0.3 × ln(928/25) = 1.08 | ln(1 + 119/928) = 0.12 | admit |
| q0080 page source (ordered, batch sink) | 57 MiB | 33 ms | 0.16 (WHOLE prior) | 0.16 × ln(33/13) = 0.15 | ln(1 + 86/33) = 1.3 | **refuse on first sight**; admit on the second sighting (p = 0.7 → 0.65 > cost? still no: the page source is only worth it because *three* later pages reuse it: the history's `seen` on the page-source signature reaches 2 at q0082, and with p = 0.7 and the measured `t_p` of the family (36 ms) the rule admits at q0082, so q0089 onwards hit; q0080 no longer pays) |
| q0011 subtree (2.2 M rows, singleton) | 60 MiB | 455 ms | 0.13 (SUBTREE prior) | 0.13 × ln(455/20) = 0.41 | ln(1 + 90/455) = 0.18 | admit (cheap in nats); memory handled by ageing |
| a 20-row whole result of a 4 ms query | 1 KiB after compaction | 4 ms | 0.16 | 0.16 × ln(4/0.3) = 0.41 | ≈ 0 | admit |
| point lookup, est_in 3 rows | — | 0.2 ms | — | — | — | never analyzed (bail-out, §3.3) |

The rule reproduces every decision that the report shows paying off, refuses the one class of write that costs
more than a third of its query (the page sources on first sight) until a repeat is seen, and keeps speculating on
everything that is cheap in log terms. It degrades gracefully: with an empty history it is the current gate with
the thresholds expressed as costs; with history it uses measurements.

### 3.3 Fast bail-out (pre-optimize hook, before `PlanSignatures::Analyze`)

A single early pass over the bound plan sums `EstimateCardinality` of `LOGICAL_GET` nodes and stops as soon as
the sum reaches `recycler_min_lookup_rows` (default 10,000; ≈ 0.2 ms of work at 20 ns/row). Plans below it —
the point lookups — skip both hooks entirely; nothing is looked up, stored, timed, or counted except one
`skipped_tiny` counter. Expected effect: recovers the −2.0 nats of §1.3 (≈ 1 % of the score) and removes the
only class of query the extension makes slower without any possible gain. The threshold must stay far below
the smallest served query (q0084 at 3.4 ms / ~200 K rows) — a test pins both sides.

### 3.4 The serving rule (hit path)

Serve a hit only when it is clearly cheaper than recomputation: `h_e < 0.5 × t_q`, where `h_e` uses the measured
scan cost of the entry when known, and — when the buffer pool reports swap in use (§6) and the entry is older
than the swap high-water mark — the disk read-back estimate `bytes / disk_bandwidth` (learned from the first
such hit; default 1 GiB/s). Whole-result hits of tiny entries are always cheaper; the rule only ever refuses
large, cold entries for cheap queries.

### 3.5 Where it plugs in, and what it replaces

- New `src/recycler_policy.{hpp,cpp}` (≈ 300 lines): `RecyclerPolicy` with `ShouldAnalyze(plan)`,
  `AdmitWrite(kind, NodeInfo, history, settings)`, `ShouldServe(entry, t_q)`, the calibration medians, and the
  per-kind priors. It is the only place that decides; the hooks only ask.
- Replaces `SpeculationAllowed` and the `×2 / ÷8` speculation factors in `recycler_hooks.cpp:844-921`
  (dev_tapesh_m6 line numbers), `SubsumptionPass::Admit` (`:425-429`), and the whole-result estimate guard
  (`:1065-1076`). `unreliable_estimate` stays: it zeroes `p_e` on first sight above cross products and
  inequality joins.
- Settings that survive: `recycler_budget_mb`, `recycler_entry_cap_mb`, `recycler_entry_cap_rows`,
  `recycler_min_input_rows` (now the floor for *any* write), the mechanism toggles. New:
  `recycler_min_lookup_rows`. Removed: `recycler_speculate_mb` (subsumed by the rule). Everything the rule
  computes is visible in `recycler_entries()` (`priority`, `measured_cost_ms`, `materialize_ms`) and
  `recycler_stats()` (`refused_by_cost`, `refused_by_rent`, `skipped_tiny`).

### 3.6 Validation and expected effect

Ablation on the public set in filename, reverse and shuffled order, interleaved with vanilla checkpoints
(the report's §6.7 protocol): current gate vs cost model. Exit criteria: no query served today is lost in any
order; payer overhead total (1.69 s) reduced by at least the page-source deferrals; sub-ms queries within 2 % of
vanilla; geomean within noise or better (expected 9.2 → 8.9–9.0 ms from the bail-out and the page sources).
The model's real value is on the private set: fewer expensive first-sight writes, no cost on tiny queries, and
memory bounded by usefulness rather than by the size of the workload.

## 4. Memory diet

Ranked by MiB saved per day of work, all against the census of §1.2 (1,473 MiB after the public workload).

### 4.1 One wide entry per block shape, and subsumption-aware replacement (≈ −555 MiB, and the largest remaining score lever)

This is M7 "union-projection widening" of `SOLUTION_PLAN.md` §3.7, now justified twice: the 14 shapes with
2–7 copies waste ≈ 555 MiB, and the 9 payers of the largest family (tier A) are payers *only* because each
projects a different column set of the same block. Design:

- The signature history records, per shape, the union of column identities any member needed (`ColumnIdentity`
  lists already exist on `CachedEntry` and `BlockShape`).
- The payer keeps that union alive through column pruning with the existing keep-alive mechanism
  (`WrapMaterialize`'s `keep_alive` expressions, `recycler_hooks.cpp:219-228`), capped at 8 extra columns and at
  the entry cap, and only columns of fixed width or `VARCHAR` under 64 bytes average (statistics from the
  catalog's `TableStatistics` where present, otherwise refuse the widening).
- `StoreBlock` becomes subsumption-aware: a new entry whose constraints are implied by an existing entry's *and*
  whose columns are a subset is not stored; an existing entry whose constraints are implied by the new one and
  whose columns are a subset is evicted when the new one lands (§5's index makes this O(copies)).
- Order dependence remains (the widest member must come first for predicate subsumption); widening removes the
  column-set dependence only.

Expected: in filename order, 8–12 more members of the 21-member family hit (each 140–330 ms → ~5 ms, 3.3–4.2
nats), i.e. +25–40 nats ≈ geomean 9.2 → 7.3–7.8 ms; block space 1,061 → ~500 MiB. Risk: medium (column
identity through the compress/decompress projections, `BlockFinalizer`, must hold for every kept column; the
differential harness in three orders is the gate). Effort 2–3 days.

### 4.2 Compaction of small entries (≈ −70 MiB, half a day)

In `PhysicalRecyclerMaterialize::Finalize`, an entry with at most `recycler_compact_rows` rows (default 8,192,
i.e. at most a handful of chunks) is re-appended into a fresh `ColumnDataCollection(Allocator::Get(context),
types)` — the in-memory allocator sizes its allocations to the data — and the buffer-managed collection is
dropped. Cost: one copy of at most a few chunks, microseconds, inside `Finalize`. Effect: the 166 tiny entries
go from 71.5 MiB to under 2 MiB, and 256 KiB blocks stop being the unit of a 20-row result. These entries are
not spillable, which is right for data that small. `bytes` is then the allocation size of the in-memory
collection, which `ColumnDataCollection::AllocationSize()` reports for both allocator types.

### 4.3 Accounting that tells the truth (part of P2)

`CollectionBytes` (`recycler_operators.cpp:275-287`) reports allocated bytes. Add used bytes
(`ColumnDataCollection::SizeInBytes()`, same de-duplication of shared allocators) so `recycler_entries()` shows
both, and `recycler_stats()` reports `bytes_used` next to `bytes`. The budget keeps charging allocated bytes,
because that is what the buffer pool charges.

### 4.4 A budget that follows the memory limit, and a soft target that makes ageing run (§5)

- `recycler_budget_mb` stays the hard cap but defaults to `min(2000 MiB, 25 % of BufferManager::GetMaxMemory())`
  (2,000 MiB at the grader's 8,000 MB; 250 MiB at a 1 GB limit), re-read when `memory_limit` changes (the
  setting is read at the pre hook like the others).
- A soft target `recycler_target_mb` (default half the budget) above which the ageing policy evicts
  speculative entries that have not been hit (§5.3). On the public workload the 30 hit entries hold 438 MiB, so a
  1,000 MiB target keeps every hit and drops the never-read ballast during the run rather than at the end.

### 4.5 Bounded history (part of P2)

`RecyclerCache::seen` and `too_big` (`recycler_cache.cpp:189-209`) grow without bound and `too_big` never
expires. Both merge into the bounded `SignatureHistory` map of §3.1; a "too big" verdict is an ordinary history
entry with a reason and is reconsidered when the caps change.

### 4.6 Considered and not done

- Skipping the `whole: N, subtree: N` pairs (34 queries store a whole result and a subtree with identical row
  counts): after compaction both are kilobytes, and the subtree entry is what serves "same aggregate, different
  ORDER BY" (a tested case). Not worth a rule.
- Compressing entries (dictionary/RLE) inside collections: the workload's blocks are 45–50 bytes per row of
  mostly integers; DuckDB's collections are uncompressed by design and a private format would cost the zero-copy
  scan. Widening and ageing save more for less.

Projection after §4.1–§4.4 on the public workload: cache ≈ 850 MiB peak, ≤ 1,000 MiB enforced, with the same
hits plus the widening hits.

## 5. Cache ageing and eviction policies

### 5.1 What exists

`RecyclerCache::PickVictim` (`recycler_cache.cpp:61-80`) scans every entry of every map for the lowest
`Benefit() = (est_in − rows) × (1 + hits) / bytes`, and `MakeRoom` loops it: O(n) per eviction, O(n·k) to free
k entries, on the timed path. There is no ageing at all: a hit once, ever, keeps an entry ahead of a never-hit
entry forever, and `est_in` is an estimate that can be 5× off (M5 notes). No eviction happened on the public
workload because the budget (2,000 MiB) was never reached, so the policy has never been exercised outside the
sqllogictests.

### 5.2 Candidates

| Policy | Priority (evict the lowest) | Behaviour on this workload | Cost per op | Verdict |
|---|---|---|---|---|
| LRU | `last_used` | evicts the big never-hit blocks only if they are the oldest; evicts q0240's 2.5 K-row whole result before a 130 MiB never-hit block that came later | O(1) | baseline in the ablation only |
| LFU | `hits` | no ageing; ties among the 228 never-hit entries broken arbitrarily | O(log n) | no |
| Benefit density (current) | `(est_in − rows)(1 + hits) / bytes` | size- and cost-aware, but estimated cost, no ageing, O(n) victim search | O(n) | replace |
| **GDSF** (GreedyDual-Size-Frequency) | `L + saved_ns × (1 + hits) / bytes`, `L` = priority of the last evicted entry | ages by inflation: everything inserted before the last eviction ranks below newer entries of equal value; size- and cost-aware with *measured* `saved_ns = c_e − h_e` | O(log n) | **recommended** |
| LRFU (exponential decay) | `hits` decayed by `2^(−Δ/τ)` per statement | one more parameter (`τ`); same information as GDSF's inflation | O(log n) | fallback if inflation misbehaves in the ablation |
| Speculation TTL (generational) | evict entries with `hits = 0` older than `N` statements once above the soft target | directly targets the 228 never-read entries (1,035 MiB) while the run is in progress | O(1) per statement | **add on top of GDSF** |

### 5.3 Specification

- Priority is computed at store time and recomputed at each hit (`NoteHit`), stored on the entry, and kept in
  an ordered index `std::set<std::pair<double, idx_t>>` (priority, entry id) under the cache lock; `MakeRoom`
  pops from the front; `Erase` removes by the stored key. All O(log n). `L` is a field of the cache.
- `saved_ns` is the measured `c_e − h_e` from §3.1 when known, the estimate otherwise; the measured value
  replaces the estimate at the entry's first hit.
- The ageing clock is the cache's statement sequence (`RecyclerCache::sequence`), never wall time, so every
  sqllogictest is deterministic.
- Speculation TTL: at `QueryEnd`, if `TotalBytes() > target`, evict entries with `hits == 0` and
  `sequence − stored_at > recycler_speculation_ttl` (default 40 statements; the public workload's longest
  first-to-repeat distance among served entries is q0240 → q0248, 8 statements, and q0265 → q0274, 9), in
  priority order, until below the target. Runs outside the timed region (§6.5).
- Entries referenced by a running query stay alive through their `shared_ptr` (existing design); eviction only
  removes them from the maps.
- Tests: tiny budgets and targets on synthetic tables, asserting through `recycler_entries()` which entries
  survive after a scripted sequence of hits; the same script under each policy in the ablation branch.

### 5.4 Ablation protocol

Budgets {256, 512, 1,000, 2,000} MiB × policies {LRU, current, GDSF, GDSF + TTL} × three orders; record hits
lost versus the 2,000 MiB run and peak `bytes`. The policy setting exists only on the ablation branch; the
merged code carries one policy (GDSF + TTL) unless the ablation shows a case for two.

## 6. The buffer manager as a disk tier (the risky part)

### 6.1 What DuckDB v1.5.5 already does with the cache (verified)

- Cached collections are `BUFFER_MANAGER_ALLOCATOR` collections. `ColumnDataAllocator::AllocateBlock`
  (`column_data_allocator.cpp:94-108`) calls `BufferManager::Allocate(MemoryTag::COLUMN_DATA, size, false)`;
  `can_destroy = false` maps to `DestroyBufferUpon::BLOCK` (`standard_buffer_manager.cpp:176`,
  `enums/destroy_buffer_upon.hpp`), i.e. **the block is evictable: under memory pressure the pool writes it to
  the temporary directory and a later `Pin` reads it back**. The scan path pins per chunk
  (`ColumnDataAllocator::InitializeChunkState`). Nothing in the extension needs to change for the cache to
  page out and in; today it simply never happened (3.0 GiB used of 8,000 MB).
- Temporary directory: for a file database it defaults to `<database>.tmp` (`config.cpp:555-557`), here
  `data/imdb.duckdb.tmp`; `scripts/benchmark.py` sets `temp_directory` to the run's own directory, and the
  grader is said to mirror it. `BufferManager::HasTemporaryDirectory()`, `GetUsedSwap()`, `GetMaxSwap()` are
  public; `duckdb_memory()` shows per-tag `temporary_storage_bytes`.
- **Eviction order** (`buffer_pool.cpp:15-27`, queue sizes `{1, 6, 1}` at `buffer_pool.hpp:115-117`,
  `EvictBlocks` iterates the queues in order): `BLOCK`/`EXTERNAL_FILE` buffers first ("cheap, just free" —
  these are the IMDB table pages), `MANAGED_BUFFER` second (our collections, "have to write to storage"),
  `TINY_BUFFER` last. **So a cache that outgrows the free memory pushes the base tables out of the buffer pool
  before a single cache block is written to disk.** On the grader the tables come back from the OS page cache,
  but it is the wrong order for the cache's purpose and it is what a large private workload would trigger first.
- Within the managed-buffer queues, `BlockMemory::eviction_queue_idx` orders eviction: index ≥ 6 goes to the
  front (evicted first), index 0 to the back (`GetEvictionQueueForBlockMemory`, `buffer_pool.cpp:290-300`).
  The only public way to set it for a collection is `ColumnDataCollection::SetPartitionIndex(idx)`
  (`column_data_collection.hpp:73`, requires an empty collection, `column_data_collection.cpp:124-128`), which
  every block of that collection inherits as `RadixBits(idx)` = the bit length of `idx`
  (`column_data_allocator.cpp:103-106`, `radix_partitioning.hpp:34-36`). **`SetPartitionIndex(32)` therefore
  marks a collection "evict me first among managed buffers".**
- What is *not* reachable from an extension: per-block residency (`BlockHandle` state sits behind
  `ColumnDataAllocator`'s private `blocks`), targeted eviction (`BufferPool::EvictBlocks` is `protected`,
  `buffer_pool.hpp:79-90`), the cross-type order, and `BatchedDataCollection`'s internal collections (the
  ordered page sources), whose blocks are allocated before we can set an index.

### 6.2 What the profiler tells about read-back cost

A spilled hit costs `bytes / disk bandwidth` plus the pool's write on the way out. At 1–3 GiB/s (local SSD)
the 130 MiB q0231 block reads back in 45–130 ms, still 7–20× cheaper than its 990 ms recomputation; the 57 MiB
page source reads back in 20–60 ms against a 33–55 ms recomputation — a loss. Hence the serving rule of §3.4.

### 6.3 Designs

| | Design | What it needs | Risk | Verdict |
|---|---|---|---|---|
| D0 | **Passive tier (exists).** Measure it: a test with `memory_limit='1GB'`, a temp directory and a workload that overflows; `recycler_stats()` reports `spilled_bytes` (from `GetUsedSwap()` / the `COLUMN_DATA` tag) and the hit path learns the read-back bandwidth. | tests, two counters | none | do (P7) |
| D1 | **Evict-first priority + swap-aware serving.** Every unordered write (parallel sink and the compacted small entries excluded, they are in-memory) creates its per-thread collections with `SetPartitionIndex(32)` before the first append, so cache blocks leave memory before any other managed buffer; the serving rule of §3.4 refuses cold large entries for cheap queries; the soft target of §4.4 also tightens when `GetUsedMemory() > 0.8 × GetMaxMemory()`. | ≈ 40 lines in the sink states + policy; a test that forces eviction and asserts hits still return vanilla answers | low: public API, no private state; the ordered page sources keep the default index (3 entries, 171 MiB, accepted and documented) | do (P7), behind `recycler_evict_first` for the ablation, default on |
| D2 | **Recycler-managed cold tier.** Entries demoted by the ageing policy are serialized to files in the temporary directory (`FileSystem` + `BufferedFileWriter`, `DataChunk` serialization) and their collections freed; a hit on a cold entry re-reads the file into a fresh collection. Full control over *what* is on disk and *when*; independent of the pool's heuristics. | ≈ 400 lines, file lifecycle (delete on evict, on clear, on shutdown, on error), a temp-dir-less fallback, tests | medium | only if the private set overflows the budget by a wide margin; decide after D1's measurements |
| D3 | **Persistence across processes** ("get the cache from disk" at startup): files outside the run's directory, or tables written into the database. | — | breaks the harness's isolation (fresh cwd and `TMPDIR` per run; the database is opened read-only by the grader) and the assignment's intent | **never** |

Recommendation: D0 + D1 now (one day), D2 only on evidence. The honest summary for the "risky endeavour": the
buffer manager *can* be the cache's disk tier and already is one, transparently; what the extension can add
with public APIs is priority (evict-first), awareness (swap counters, cold-entry serving rule) and tests. It
cannot pick which block to evict or ask whether a block is resident, and it cannot change the fact that table
pages are evicted before managed buffers.

### 6.4 Test matrix for the tier

`memory_limit` {8000 MB, 1 GB, 256 MB} × `temp_directory` {set, unset} × the recycler sqllogictests and a
synthetic overflow test; `preserve_insertion_order` both ways. Without a temp directory DuckDB throws on
eviction of managed buffers ("temporary buffer eviction set a temporary directory",
`standard_buffer_manager.cpp:456`) — the test asserts that the *query* still succeeds, which requires the
store to catch the out-of-memory path in `Finalize` and drop the entry (the miss path must never turn an
answerable query into an error).

### 6.5 A free by-product: work after the clock stops

`ClientContextState::QueryEnd(ClientContext &, optional_ptr<ErrorData>)` (`client_context_state.hpp:53-61`) is
called from `ClientContext::EndQueryInternal` *after* `profiler->EndQuery()` (`client_context.cpp:292-306`),
and the EXPLAIN ANALYZE `latency` is fixed even earlier, in `PhysicalExplainAnalyze::Finalize`
(`physical_explain_analyze.cpp:20-27`). Everything the recycler does in `QueryEnd` — reading the profiler tree
for measured costs, updating calibration, the speculation TTL sweep, evictions to reach the soft target — is
outside the scored latency. The `RecyclerClientState` already is a `ClientContextState`; it gets the override.
Publication itself (`Store`) stays in `Finalize` so that the next statement on any connection sees the entry;
only its bookkeeping moves.

## 7. Merge-worthiness: what the reviewer will say, and the answer

### 7.1 Findings from reading the m6 sources (`src/` at 9fb66fce3b)

| Where | Finding | Rule | Fix (milestone) |
|---|---|---|---|
| `recycler_cache.cpp:61-80, 104-120` | O(n) victim scan per eviction inside `MakeRoom`'s loop, on the timed path | 9, perf | ordered index (P6) |
| `recycler_cache.cpp:100` | `total_bytes -= MinValue(total_bytes, entry.bytes)` defends against an impossible state | 13 | `D_ASSERT(total_bytes >= entry.bytes)` (P1) |
| `recycler_cache.cpp:189-209` | `seen` / `too_big` unbounded; `too_big` never expires | 4, 9 | bounded history (P2) |
| `recycler_cache.hpp:107-108` | `Lookup` takes a `std::function` (heap allocation per lookup, on every candidate node) | 3 | pass `const PlanSignatures &` and the operator (P1) |
| `recycler_hooks.cpp:230-233` | `IsRecyclerOperator` = two `dynamic_cast`s per visited node in three walkers | 2, 7 | common `LogicalRecyclerOperator` base with a `kind` (P1) |
| `recycler_hooks.cpp:449-462, 942-952, 1050-1060, 1078-1090` | four hand-filled `RecyclerWriteSpec` blocks | 2 | one `RecyclerWriteSpec::Make(...)` (P1) |
| `recycler_hooks.cpp:321-328, 400-406` and `SubtreeWalker` | three walkers repeat the CTE-definition skipping rule | 2 | one `ForEachRewritableSlot` visitor (P1) |
| `recycler_hooks.cpp:699-825` | `RecyclerPreOptimize` does gate, hoisting, analysis and four mechanisms in one 125-line function | 17 | named steps, same order, no behaviour change (P1) |
| `recycler_hooks.cpp:844-921` | admission by op-type factors `×2`, `÷8`, `/2` tuned on the public set | 16 | cost model (P5) |
| `recycler_hooks.cpp:52-85` | settings cached per connection are refreshed only after this connection's `SET`; a `SET GLOBAL` elsewhere is missed | 4 | document the invariant, or key the cache on `DBConfig`'s option values (P1: document) |
| `recycler_operators.cpp:275-287` | only allocated bytes; no used bytes | 18 | §4.3 (P2) |
| `recycler_operators.cpp:353-386` | `Finalize` charges accounting and eviction to the query; no measured cost recorded | perf, 16 | §6.5 (P2) |
| `recycler_signature.cpp:133-135` | `Refuse(info, reason)` discards the reason: `recycler_stats()` cannot say why a plan was refused | 18 | reason enum + counters (P2) |
| `recycler_signature.cpp:326` | `return_type.ToString()` allocates a string per expression in the hash pass | 3 | `Sink::Type()` virtual: the hash sink hashes `LogicalType::Hash()`, the string sink still writes `ToString()` (P1; measure the hook time before and after) |
| `recycler_signature.cpp:692-725` | IN-list rendering builds a `Value` per element (100 of 149 queries have IN lists; bounded at 4,096 rows) | 3 | acceptable; note in the hook-time budget test (P8) |
| `recycler_functions.cpp:38-52` | shutdown by connection count is correct but subtle | 12 | comment says why (destroy order of `DatabaseInstance`), keep (P1) |
| tests | no test for: eviction under a tiny budget, `preserve_insertion_order = true` with every mechanism, two connections, `memory_limit` overflow, the `latency` field on a hit under `EXPLAIN (ANALYZE, FORMAT JSON)`, the hook-time budget, the bail-out threshold | 5 | P2–P8 each add theirs |

Concurrency, checked and kept: the cache lock covers every map mutation; `Lookup` renders outside the lock and
confirms by byte comparison; `hits`/`last_used` are atomics because `NoteHit` runs outside the lock (the
comment should say so); per-connection state is touched only by that connection's hooks and `QueryEnd`;
entries are `shared_ptr`-owned by plans. The ordered index of §5.3 lives under the same lock.

### 7.2 Structure after the refactor (P1, a pure move)

| File | Responsibility | Lines (est.) |
|---|---|---:|
| `recycler_signature.{hpp,cpp}` | unchanged: canonical renderer, hashes, shapes, constraints | 1,400 |
| `recycler_cache.{hpp,cpp}` | entries, maps, ordered index, history, budget/target, stats | 500 |
| `recycler_policy.{hpp,cpp}` | bail-out, admission, serving rule, calibration, priors (§3) | 300 |
| `recycler_rewrite.{hpp,cpp}` | `SubstituteScan`, `WrapMaterialize`, `HoistPaginationLimit`, `BlockFinalizer`, the shared slot visitor | 350 |
| `recycler_hooks.cpp` | the two hooks as orchestration of gate → analysis → mechanisms → writes | 400 |
| `recycler_operators.{hpp,cpp}` | operators, compaction, evict-first priority, measured timings | 500 |
| `recycler_functions.cpp` | registration, settings, `recycler_stats()`, `recycler_entries()`, pragma, `QueryEnd` state | 300 |

One PR per row change; the P1 PR moves code and adds nothing (diff readable as a transform).

### 7.3 Naming and comments

Names say when to use them: `ShouldAnalyze`, `AdmitWrite`, `ShouldServe`, `SignatureHistory`,
`recycler_min_lookup_rows`, `recycler_target_mb`, `recycler_speculation_ttl`, `recycler_evict_first`.
Comments only where the *why* is non-obvious: the profiler trap (why `EXTENSION` operators), the destroy order
at shutdown, the eviction-queue trick, the batch-index exception, the `NoteHit`-outside-the-lock atomics, the
LIMIT hoisting invariant. No narration of what the code does.

### 7.4 Tests and CI

- `test/sql/recycler/`: keep the three suites; add `recycler_memory.test` (budget, target, TTL, compaction,
  used vs allocated bytes), `recycler_policy.test` (bail-out threshold both sides, admission on first vs second
  sight with scripted sizes, serving rule), `recycler_order.test` (every mechanism under
  `preserve_insertion_order = true`), `recycler_spill.test` (`memory_limit` overflow with and without a temp
  directory), `recycler_explain.test` (`EXPLAIN (ANALYZE, FORMAT JSON)` of a hit contains `latency` and a
  non-empty tree), `recycler_connections.test` (two connections: invalidation, `PRAGMA recycler_clear` while
  the other scans).
- Determinism: no sleeps, no wall-clock; sizes drive every decision through settings.
- Local matrix before each merge: `make release` + tests; a debug build with `D_ASSERT`s; one ASAN/UBSAN run of
  the recycler suites; GCC `-Wall -Wextra -Werror` (the CI compiles with GCC on Linux; development is
  clang/macOS); `scripts/recycler_diff.py` in three orders; `make benchmark` with interleaved vanilla
  checkpoints; three passes of the workload in one process.
- CI: the distribution pipeline stays untouched. A second, manually dispatched workflow (`tests.yml`: Linux
  GCC release build + `test/sql/recycler` + `test/sql/waddle.test`) is added so the merge gate runs where the
  grader's binary is built; it does not touch `duckdb_version`/`uses` and is not run on push (Actions minutes).

## 8. Milestones

Effort in working days for one developer; each ends with the verification chain of §0 and, when merged to
`main`, a dispatched build (the grader uses the last successful build).

| # | Milestone | Content | Effort | Exit criteria |
|---|---|---|---:|---|
| P0 | Land M6 | review td-02/dsa_asignment2#10 with the checklist, merge, dispatch CI, fast-forward local `main` | 0.5 | CI green on `main`; benchmark within noise of 9.2 ms |
| P1 | Structure | §7.2 pure-move refactor, `RecyclerWriteSpec::Make`, common operator base, `Lookup` without `std::function`, `D_ASSERT`s, comments of §7.3 | 1 | identical results and stats on the public set; hook time not worse |
| P2 | Observability | `QueryEnd` state, measured costs from the profiler tree, bounded `SignatureHistory` with reasons, used/allocated bytes, spilled bytes, new `recycler_entries()`/`recycler_stats()` columns | 1–1.5 | every entry shows a measured cost after its payer; tests |
| P3 | Memory diet 1 | compaction (§4.2), budget/target from the memory limit (§4.4), bounded history (§4.5) | 0.5–1 | tiny entries ≤ 2 MiB in total; `recycler_memory.test` |
| P4 | Wide block entries | §4.1 widening + subsumption-aware replacement | 2–3 | ≥ 8 more served queries in filename order, all answers vanilla in three orders, block space ≤ 600 MiB |
| P5 | Cost model | `recycler_policy` (§3), bail-out, serving rule, removal of the speculation factors | 1.5–2 | ablation of §3.6 passes; sub-ms queries within 2 % of vanilla |
| P6 | Ageing | GDSF + TTL with the ordered index (§5.3), policy ablation (§5.4) | 1–1.5 | O(log n) eviction; hits preserved at 1,000 MiB target; peak bytes ≤ target + one entry |
| P7 | Disk tier | D0 tests and counters, D1 evict-first + swap-aware serving (§6.3) | 1 | `recycler_spill.test`; 1 GB run answers everything; decision on D2 recorded |
| P8 | Hardening & CI | §7.4 matrix, `tests.yml`, docs (README section, this plan's status), report addendum | 1–2 | all suites green on GCC and clang; `EXTENSION_REPORT.md` numbers refreshed |

Total ≈ 10–13 days. Order matters: P1 before any behaviour change (reviewable diffs), P2 before P5/P6 (the
model needs measurements), P4 before P7 (it changes what is worth spilling). P4 is the only milestone with a
large expected score effect (9.2 → ~7.5 ms); P3, P5, P6, P7 are what make the extension safe on a workload
that is not the public one.

## 9. Risks and open questions, each with the experiment that settles it

1. **Widening's keep-alive through compress/decompress projections** may leave a kept column unresolvable
   (`BlockFinalizer` refuses compressed identities). Experiment: plant the union on the 21-member family and
   count `usable` columns per marker before writing the hit path.
2. **Payer cost of wider blocks**: +N columns × 1.6 ms/MiB. Bound it with the 8-column / fixed-width rule and
   measure on q0226/q0231/q0235/q0240 (fresh processes, alternating binaries, min of 3).
3. **The priors of §3.1 come from the public set.** The online ratio per kind corrects them within a few dozen
   statements; the private set is "similar". Guard: the rule can never refuse an entry under 1 MiB (cost ≈ 0).
4. **Speculation TTL of 40 statements** could evict a block right before its family's consumers arrive on a
   differently ordered private set. Mitigation: the TTL runs only above the soft target, and blocks of shapes
   seen twice are exempt.
5. **Evict-first (D1) changes nothing on the grader** (no pressure at 8,000 MB) and could cost on a machine
   where the cache is the only thing worth keeping. Keep it a setting; the ablation reports both.
6. **Hit floor on large bound plans**: q0248/q0284/q0293 hit at 0.36–0.45 ms while q0084-q0090 hit at
   0.13–0.21 ms; the difference is binding a six-table query with a subquery, not our hooks. Measure the split
   (bind vs hooks vs execution) once with the profiler before spending anything on it; the ceiling is ≈ 0.5 ms
   of geomean.
7. **A streaming "tee" materializer** (append while forwarding, no pipeline break) was evaluated and rejected:
   `PipelineExecutor` skips a whole pipeline whose sink already knows no output is possible and runs
   `FinalExecute` only from the operator that finished early (`pipeline_executor.cpp:42-46, 51-57`), so a tee
   below a LIMIT or on the probe side of a join with an empty build sees a partial input and cannot tell. The
   saving would have been at most the re-scan half of §1.1 on five queries (< 0.3 % of the score) against a
   silent-wrong-answer risk.

## 10. Do not

- Persist anything across processes (files outside the run's directory, tables in the database).
- Rename the extension, change the pinned DuckDB, touch `duckdb/`, or the distribution workflow's keys.
- Serve `LIMIT` without `ORDER BY`, `SAMPLE`, volatile or parameterized expressions, table functions, delim
  sides, subtrees with foreign `DELIM_GET`s or runtime filters, or anything whose bindings cannot be traced.
- Add a setting, field, atomic, or fallback "just in case": every one above has a test or a measurement that
  needs it.
- Quote a wall-clock effect below the machine's noise (±5 % on the geomean, more on one-second queries) without
  interleaved vanilla checkpoints.

## Appendix A. Reproducing today's measurements

```bash
make release                                   # build/release/duckdb with the extension
python3 ./scripts/download-imdb.py             # once
# payer profile (four configurations, two repetitions, thread time per operator):
#   fresh process per run, SET threads=6; SET memory_limit='8000MB'; SET preserve_insertion_order=false;
#   plus SET disabled_optimizers='extension' | nothing | SET recycler_subsumption=false |
#   SET recycler_subsumption=false; SET recycler_subtrees=false; SET recycler_blocks=false
#   EXPLAIN (ANALYZE, FORMAT JSON) <benchmark/q0240.sql>; read operator_timing of RECYCLER_MATERIALIZE
# census: all 149 queries under EXPLAIN (ANALYZE, FORMAT JSON) in one -readonly CLI process, then
SELECT kind, count(*), sum(rows), sum(bytes), sum(hits) FROM recycler_entries() GROUP BY kind;
SELECT key, count(*) AS copies, sum(bytes) FROM recycler_entries() WHERE kind='block' GROUP BY key HAVING count(*) > 1;
SELECT count(*), sum(bytes) FROM recycler_entries() WHERE hits = 0;
SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory();
```

## Appendix B. DuckDB v1.5.5 references verified for this plan (paths under `duckdb/src`)

`common/types/column/column_data_allocator.cpp:94-108` (block allocation, `can_destroy=false`, partition index →
eviction queue index) · `storage/standard_buffer_manager.cpp:176` (`can_destroy` → `DestroyBufferUpon`) ·
`include/duckdb/common/enums/destroy_buffer_upon.hpp` · `storage/buffer/buffer_pool.cpp:15-27, 244, 290-300`
and `include/duckdb/storage/buffer/buffer_pool.hpp:79-90, 115-117` (eviction order, queue sizes, `EvictBlocks`
protected) · `include/duckdb/common/radix_partitioning.hpp:34-36` (`RadixBits`) ·
`include/duckdb/common/types/column/column_data_collection.hpp:73` and `common/types/column/column_data_collection.cpp:124-128`
(`SetPartitionIndex`) · `include/duckdb/main/client_context_state.hpp:53-61` and `main/client_context.cpp:292-306`
(`QueryEnd` after `profiler->EndQuery()`) · `execution/operator/helper/physical_explain_analyze.cpp:20-27`
(latency fixed in `Finalize`) · `parallel/pipeline_executor.cpp:42-46, 51-57` (skipped pipelines, partial
flush) · `main/config.cpp:555-557` (temporary directory default) · `storage/standard_buffer_manager.cpp:456`
(eviction without a temporary directory throws) · `function/table/system/duckdb_memory.cpp` (`tag`,
`memory_usage_bytes`, `temporary_storage_bytes`) · `include/duckdb/storage/storage_info.hpp:29` (256 KiB blocks).

Literature the model follows: Nagel, Boncz, Viglas (ICDE 2013) for measured-cost recycling with a persistent
history; Dursun et al. (SIGMOD 2017) for the observation that in-memory materialization is not free; Zhou et al.
(SIGMOD 2007) for covering subexpressions; Jindal et al. (VLDB 2018) for utility-based selection at scale;
Cherkasova (1998) for GreedyDual-Size-Frequency.
