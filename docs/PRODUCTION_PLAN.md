# Query Recycler — Production Plan: a general-purpose recycler for DuckDB that keeps the leaderboard score

Status: revision 2, 2026-09-30, on branch `dev_tapesh_plan` (revision 1 is commit e2e60aafe2). Written against
the code on `dev_tapesh_m6` (commit 9fb66fce3b, open as td-02/dsa_asignment2#10). `origin/main` carries M0–M5 and
is what the grader builds. This plan extends `docs/SOLUTION_PLAN.md` (design, M0–M6 done) and
`docs/EXTENSION_REPORT.md` (measurements of 2026-09-26). Every number below is either quoted from the report or
was measured on 2026-09-30 with `build/release/duckdb` (v1.5.5 d8cdaa33fd, the M6 binary) on `data/imdb.duckdb`;
every DuckDB fact cites the pinned v1.5.5 source under `duckdb/`. "MiB" is used throughout; the report's "MB"
are MiB.

Direction of this revision: the extension stops being a solution tuned to the IMDB workload and becomes a
**general query recycler that any DuckDB user could load** — correct under DuckDB's default settings, with
writes, transactions, several connections, prepared statements, temporary tables, external files — with CI and
stress testing to match, **under the hard rule that the leaderboard score never drops** (§10). The score levers
and the safety work of revision 1 (cost model, memory diet, ageing, disk tier) are kept and re-ordered around
that rule.

Sections: §1 measured facts · §2 which queries are worth it · §3 cost/performance model · §4 memory diet ·
§5 ageing policies · §6 the buffer manager as a disk tier · §7 generalization · §8 faster without losing score ·
§9 verification: tests, stress, CI · §10 score protection · §11 merge-worthiness · §12 milestones · §13 risks ·
§14 do not.

## 0. Ground rules that do not change

- Extension name `waddle`, DuckDB pinned at v1.5.5, nothing under `duckdb/` is touched, the distribution
  workflow keeps `duckdb_version` and `uses` as they are. Additional workflows may be added; the distribution
  pipeline is never edited before grading ends.
- Correctness before everything: any rewrite fails closed to the vanilla plan; a served result is always what
  vanilla returns under the same snapshot and settings.
- The score is the geometric mean of per-query latency of the faster of two fresh-process runs, measured under
  `EXPLAIN (ANALYZE, FORMAT JSON)` with `threads=6`, `memory_limit='8000MB'`, `preserve_insertion_order=false`.
  **Nothing merges to `main` and no CI build is dispatched without the score gate of §10.**
- Every merge passes `IMDB_DATA=1 make test`, the recycler sqllogictests, the differential harness in three
  orders, the stress suite of §9, and the `anthropic-skills:duckdb-code-review` checklist (justify every
  addition, no copies, rigorous concurrency, deterministic tests, one PR = one change).

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
the thread-time ratios above are relied upon. A copy at 9 ms/MiB of thread time is slow for a memory copy; §8.3
says what to measure next. Reproduction: Appendix A.

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
The same model is what makes the extension general: every threshold that was tuned on this workload becomes a
quantity calibrated online (§3.1).

## 3. The cost/performance model

### 3.1 Quantities

For a candidate `e` (a signature at a plan node; kinds `WHOLE_RESULT`, `SUBTREE`, `BLOCK`):

| Symbol | Meaning | Source today | Source after P2 |
|---|---|---|---|
| `est_in` | rows scanned by the subtree | `NodeInfo::est_leaf_rows` | unchanged |
| `est_out`, `width`, `est_bytes` | output rows, bytes per row, bytes | `EstimateCardinality`, `RecyclerEstimateRowWidth` | unchanged; `bytes` measured once stored |
| `t_p` | time of the query that would pay | — | measured in `QueryEnd` (query latency) |
| `c_e` | time the subtree took to produce | — | measured: thread time of the subtree under the `RECYCLER_MATERIALIZE` node in the profiler tree (available under EXPLAIN ANALYZE, i.e. always on the grader; otherwise the sink's own timer) |
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
| q0080 page source (ordered, batch sink) | 57 MiB | 33 ms | 0.16 (WHOLE prior) | 0.16 × ln(33/13) = 0.15 | ln(1 + 86/33) = 1.3 | **refuse on first sight**; the page-source signature is seen again at q0082, and with p = 0.7 and the family's measured `t_p` the rule admits there, so q0089 onwards hit and q0080 no longer pays |
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
The model's real value is on the private set and on any other database: fewer expensive first-sight writes, no
cost on tiny queries, and memory bounded by usefulness rather than by the size of the workload.

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
| D2 | **Recycler-managed cold tier.** Entries demoted by the ageing policy are serialized to files in the temporary directory (`FileSystem` + `BufferedFileWriter`, `DataChunk` serialization) and their collections freed; a hit on a cold entry re-reads the file into a fresh collection. Full control over *what* is on disk and *when*; independent of the pool's heuristics. | ≈ 400 lines, file lifecycle (delete on evict, on clear, on shutdown, on error), a temp-dir-less fallback, tests | medium | only if a workload overflows the budget by a wide margin; decide after D1's measurements |
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

## 7. Generalization: from the IMDB workload to a recycler any DuckDB user can load

### 7.1 What is workload-specific today

| Aspect | Today (dev_tapesh_m6) | General form |
|---|---|---|
| Admission thresholds | 200 K input rows, 64 MiB speculation with `×2`/`÷8` factors, 1 M / 4 M row caps, 256 MiB entry cap, tuned on the public set | the calibrated cost model of §3 (constants learned online, caps derived from the memory limit) |
| Row order | unordered writes and subsumption are disabled when `preserve_insertion_order = true`, which is **DuckDB's default**: most users would get the whole-result cache of `ORDER BY` queries and nothing else | mirror vanilla's own order decision (§7.3) |
| Invalidation | any modifying statement clears the entire cache; a write in flight on another connection is not seen until it is bound | per-table versions driven by the transaction callbacks, race-free by construction (§7.2) |
| Temporary tables | keyed by `temp.main.t` like any table, although a temp table is private to its connection: two connections with a same-named, same-shaped temp table would share results (**wrong answers** outside the grader) | refuse temporary entries (`CatalogEntry::temporary`, `catalog_entry.hpp:52`) |
| Non-DuckDB catalogs | any `TableCatalogEntry` is accepted; a table in an attached foreign catalog (SQLite, Postgres, DuckLake through their extensions) can change outside DuckDB's transactions | cache tables of DuckDB catalogs only (`Catalog::GetCatalogType() == "duckdb"`), everything else refused |
| Session settings | not part of the key; `TimeZone`/`Calendar` (ICU) and `integer_division`/`ieee_floating_point_ops` change results of the same plan at execution time | hash the result-affecting settings into every key (§7.4) |
| Prepared statements | any `BOUND_PARAMETER` refuses the plan | rebind on demand and key on the parameter values (§7.5) |
| Table functions | refused ("table function scan"): `read_parquet`, `read_csv`, `read_json`, `range` never take part, although repeated scans of files are the case the assignment's introduction motivates | allow-listed pure and file-backed functions, keyed on their parameters, with file freshness checks (§7.6) |
| Pagination | `limit + offset > 5000` (DuckDB's Top-N threshold) | already DuckDB-general (`TopN::CanOptimize`) |
| Delim join as atomic unit, `EXTENSION` operators for the profiler, fail-closed renderer | DuckDB-general | unchanged |

### 7.2 Fine-grained, race-free invalidation

Verified facts: `TransactionContext::Commit()` runs the registered states' `TransactionCommit(MetaTransaction &,
ClientContext &)` **after** `transaction->Commit()` succeeded and `TransactionRollback(...)` on failure or
rollback (`transaction/transaction_context.cpp:50-72, 107`); `DuckTransaction` exposes `start_time`,
`transaction_id` and `commit_id` (`duck_transaction.hpp:43-47`), drawn from one monotonic counter, and a
transaction sees exactly the commits with `commit_id < start_time`; `Catalog::GetCatalogVersion(context)`
(`catalog.hpp:127`) changes on DDL.

Design (`RecyclerCache` gains a `TableVersions` map keyed by `(catalog, schema, table)`):

1. **Dependencies.** Every entry records the tables its scans read (the signature already renders them) and,
   per table, the `start_time` of the transaction of that table's catalog at the pre hook (`DuckTransaction::Get
   (context, catalog)`; entries spanning several catalogs record one `start_time` per catalog).
2. **Writers announce themselves before they write.** At the pre hook of `INSERT`/`UPDATE`/`DELETE`/`MERGE`/
   `TRUNCATE`/`COPY ... TO table` the target table (`LogicalInsert::table`, `LogicalUpdate::table`,
   `LogicalDelete::table`, …) is added to the connection's pending-write set and `TableVersions[t].writers++`
   under the cache lock. While `writers > 0`, entries depending on `t` are neither served nor stored for any
   connection (readers miss and run vanilla; correct by construction).
3. **Commit bumps the version.** `TransactionCommit`: for every pending table `TableVersions[t].last_commit =
   transaction.commit_id`, `writers--`, and every entry with `snapshot_start_time(t) < last_commit` is evicted.
   `TransactionRollback`: `writers--` only. Auto-commit statements go through the same callbacks.
4. **Store and serve check the same predicate.** `Store` refuses an entry whose recorded `start_time` for any
   table is `< TableVersions[t].last_commit` (the writer committed after the reader's snapshot: the race of a
   reader that started before, and stored after, a concurrent commit) or whose table has `writers > 0`.
   `Lookup` does the same before returning.
5. **DDL** stays coarse: `ALTER`/`DROP`/`CREATE OR REPLACE`/`ATTACH`/`DETACH` at the pre hook clear the
   cache, and every entry records `GetCatalogVersion` at store time and is ignored on mismatch (belt and
   braces for DDL committed by another connection between its pre hook and its commit).
6. **Reads inside a writing transaction** keep today's rule (`MetaTransaction::ModifiedDatabase()` → no
   store, no serve), so uncommitted writes stay visible to their own connection.

Cost: two map lookups per table per statement under the existing lock; no effect on a read-only workload except
the recorded `start_time`s. Tests: two connections with interleaved `BEGIN`/write/read/`COMMIT`/`ROLLBACK`
sequences and an oracle (§9.2); a deterministic reproduction of the reader-stores-after-writer-commits race
using explicit transactions.

### 7.3 Row order: do exactly what vanilla does

Verified: `PhysicalResultCollector::GetResultCollector` (`physical_result_collector.cpp:25-52`) chooses the
result collector with two public static functions: not `PhysicalPlanGenerator::PreserveInsertionOrder(context,
root)` → parallel materialized collector; preserving but not `UseBatchIndex(context, root)` → single-threaded
collector; both → batch collector (`physical_plan_generator.hpp:91-93`, `plan_insert.cpp:36-69`).
`OrderPreservationRecursive` (`plan_insert.cpp:15-34`) returns the first non-`INSERTION_ORDER` source order it
finds, so a hash aggregate anywhere below makes the plan `NO_ORDER` (parallel) and a sort makes it
`FIXED_ORDER`.

Rule: `LogicalRecyclerMaterialize::CreatePlan` calls the same two functions on its child and picks the sink
mode exactly as vanilla picks its collector: `NO_ORDER` → parallel sink (today's `PAR`); order-preserving with
batch index → batch-index sink (today's M6 sink, now used for every entry, not only page sources);
order-preserving without batch index → **no write** for subtrees and blocks (a serial sink would run the
payer's pipeline on one thread) and the serial sink only for whole results of `ORDER BY` queries (the sort's
output stage is small, today's behaviour). `PhysicalRecyclerScan` hands out its stored chunk position as the
batch index (it must report batch-index support to the planner) so a parent batch collector reproduces the
stored order. Under the grader's `preserve_insertion_order = false` every decision is unchanged, so this is a
pure generalization; under DuckDB's default it turns on subtree, block and subsumption reuse for every plan
whose sources support batch indexes (table scans do), with the same rows in the same order vanilla returns.
Verify before implementing: `PhysicalColumnDataScan`'s batch-index support in v1.5.5 as the model for the
scan's `SupportsPartitioning`.

### 7.4 Session settings in the key

A short explicit list is hashed into every signature at the pre hook (values from
`ClientContext::TryGetCurrentSetting`, ≈ 1 µs): `TimeZone`, `Calendar`, `integer_division`,
`ieee_floating_point_ops`. Everything bound into the plan (`default_null_order`, `default_order`, collations,
`search_path`, `old_implicit_casting`, `pivot_limit`) is already in the signature; `preserve_insertion_order` is
§7.3; `threads`/`memory_limit` change speed, not results. One sqllogictest per listed setting (same query, two
settings, both answers vanilla). Adding a setting to the list is a one-line change with a test; the list is
documented in the README as the invariant "settings that change a result at execution time".

### 7.5 Prepared statements

Verified: `ClientContextState::CanRequestRebind()` and `OnExecutePrepared(context, info, current_rebind)`
(`client_context_state.hpp:73-85`) let a registered state return `RebindQueryInfo::ATTEMPT_TO_REBIND` at
execution time (`client_context.cpp:615-623`), after which DuckDB re-binds and re-optimizes the statement for
this execution with the parameter values known (`PreparedStatementCallbackInfo::parameters`), and both hooks
run again. Design: `OnFinalizePrepare` records the statement's cost class from the parameter-free plan;
`OnExecutePrepared` requests a rebind only when the class is above the bail-out floor (a rebind costs a bind and
an optimize, 0.1–0.5 ms, which a point lookup must not pay); during that rebind the renderer emits each
`BOUND_PARAMETER`'s current value as a constant instead of refusing, so repeated executions with equal
parameters hit the whole-result cache and different parameters key different entries. Tests: `PREPARE`/`EXECUTE`
with equal, different and NULL parameters, and mixed parameter types; the answer is always vanilla's. No effect
on the grader (plain SQL).

### 7.6 Table functions and external files

`LogicalGet` carries `function`, `parameters`, `named_parameters`, `input_table_types` and `bind_data`
(`logical_get.hpp:32-50`), and `TableFunction::to_string` renders the bind data (`table_function.hpp:334, 454`;
the Parquet and CSV readers list their files there). Design: an allow-list of functions whose output is a pure
function of their inputs — `range`, `generate_series`, `read_parquet`/`parquet_scan`, `read_csv`/
`read_csv_auto`, `read_json`/`read_json_auto`, `read_text`, `read_blob` — is keyed on function name, parameters,
named parameters, input table types and the `to_string` rendering; every other table function stays refused
(`duckdb_*` system functions, `glob`, `query_table`, functions of other extensions). File-backed functions
record `(path, last modified, size)` per file at store time (`FileSystem::GetLastModifiedTime`/`GetFileSize`)
and re-check on hit; a mismatch evicts the entry (DuckDB's own external-file cache validates the same way).
Remote files (`http`/`s3`) are excluded by default (`recycler_remote_files = false`: a `HEAD` per file per hit
is not free and object stores are eventually consistent). No effect on the grader (native tables), large
effect for the file-scanning workloads DuckDB is mostly used for.

### 7.7 Scale and operations

- **Many connections.** One mutex guards the maps; every critical section is a hash-map operation. The stress
  suite (§9.3) runs 64 connections × 8 threads; sharding the lock by signature hash is done only if that run
  shows contention (reviewer rule: no "just in case").
- **Memory.** Budget and target follow the memory limit (§4.4); the cache is per `DatabaseInstance`, so several
  databases in one process have separate budgets, each bounded by its own limit.
- **Observability.** `recycler_stats()`, `recycler_entries()` (with the §3.5 columns), `PRAGMA recycler_clear`,
  the operators' names and parameters in `EXPLAIN`; admission and eviction decisions go to DuckDB's structured
  logger at debug level so `duckdb_logs()` explains a decision without a rebuild.
- **Packaging after grading.** The distribution pipeline already builds the loadable extension; restoring the
  excluded platforms (`exclude_archs`) and a community-extension descriptor are one commit each, done after the
  deadline so that the graded workflow is never touched. The name `waddle` stays until then.

## 8. Faster, without losing score

| Lever | Expected effect on the public score | Risk | Milestone |
|---|---|---|---|
| Wide block entries (§4.1) | 8–12 more hits, +25–40 nats, geomean 9.2 → ~7.5 ms | medium | P4 |
| Fast bail-out (§3.3) | +2.0 nats, ≈ 1 % | none | P5 |
| Cost model on the page sources (§3.2) | ≈ +0.5 nats (q0080/q0082 stop paying) | low | P5 |
| Cheaper payer copy (§8.3) | up to −40 % of the 1.69 s payer cost, ≈ +3 nats, ≈ 2 % | low | P3 |
| Hit floor on large bound plans (§8.4) | ≤ +0.5 ms of geomean; measure first | none | P2 |
| Subsumption hits 2× faster (§8.5) | +0.8 ms of geomean if achieved; measure first | medium | after P4 |

### 8.1 Rules for every speed change

Every change is measured with the score gate of §10 in filename order (the grader's order) and in two more
orders, with interleaved vanilla checkpoints; a change that loses any served query, or costs more than the
checkpoints' noise band, does not merge, whatever it wins elsewhere.

### 8.2 Hook budget

The pre hook is ≤ 25 µs on plans under 10 nodes and ≤ 50 µs on 30-node plans (SOLUTION_PLAN §2); today it is
27–48 µs on average. A sqllogictest reads `pre_ns / pre_calls` after a synthetic 30-node plan and fails above
the budget, so the generalizations of §7 (settings hash, table versions, temp-table and catalog checks) cannot
creep. Two known savings if needed: the hash sink hashes `LogicalType::Hash()` instead of allocating
`ToString()` per expression (`recycler_signature.cpp:326`), and the IN-list rendering (`:692-725`) hashes
values without constructing `Value`s.

### 8.3 The payer copy

9 ms of thread time per MiB (§1.1) is far from a memory copy. The likely cause is many small `Sink` calls: the
semi-join output above the delim join arrives in sparse chunks, and `ColumnDataCollection::Append` has a fixed
per-call, per-column cost (vector format resolution, chunk state, pins). Measure `Sink` calls and rows per call
in P2; if the average chunk is well under `STANDARD_VECTOR_SIZE`, the local sink state buffers rows into a full
chunk before appending, as DuckDB's `CachingPhysicalOperator` does for outputs under `CACHE_THRESHOLD = 64` rows
(`physical_operator.hpp:268-296`). Target: ≤ 4 ms/MiB, i.e. the block writes of q0231/q0240 cost 60–80 ms
less each. Also measured in P2: the share of the re-scan (source side) in the operator's time; the scan is
zero-copy over pinned blocks and should be a small share.

### 8.4 The hit floor

q0084–q0090 hit at 0.13–0.21 ms, q0248/q0284/q0293 at 0.36–0.45 ms; the difference is binding a six-table query
with a subquery, not the hooks (48 µs). Profile the split (bind / hooks / execution) once; if the hooks' render
for the byte comparison is a measurable share on hits, cache the rendered string of the last miss per
connection (a hit renders the same plan the miss rendered). Ceiling ≈ 0.5 ms of geomean.

### 8.5 Subsumption hits

A subsumption hit scans the whole stored block and filters (2.7–11.5 ms for 1.6 M rows). Per-chunk min/max of
the constrained columns, recorded at materialization, would let the scan skip chunks whose range misses the
residual predicate. Because the parallel sink stores rows in arrival order the ranges per chunk are wide;
worth it only if the payer's rows arrive clustered by the constrained column (they do when `title` is the probe
side and is scanned in insertion order). Measure the skip ratio on the q0240 block before building it.

## 9. Verification: tests, stress, CI

### 9.1 The pyramid

| Layer | What | Where it runs |
|---|---|---|
| 1. Module tests | sqllogictests per module and per generalization: keys (existing `recycler_basic`), subsumption, pagination, memory/budget/TTL/compaction, policy (bail-out both sides, admission first vs second sight, serving rule), order under `preserve_insertion_order = true`, spill (§6.4), invalidation and transactions (§7.2), temp tables, settings (§7.4), prepared statements (§7.5), table functions and file freshness (§7.6), all DuckDB types round-tripped through an entry (nested, decimal, enum, UUID, hugeint, timestamps with time zone, NULL-only columns, zero rows, 1,000 columns), `EXPLAIN (ANALYZE, FORMAT JSON)` of a hit carries `latency` | every build, CI |
| 2. Differential | `scripts/recycler_diff.py` (IMDB, three orders, twice) and the in-process oracle of §9.2 on synthetic data | local (IMDB) / CI (synthetic) |
| 3. Randomized | seeded query generator over synthetic schemas: SPJ, aggregates, `EXISTS`, CTEs, `IN` lists, `BETWEEN`, `LIMIT/OFFSET` with total-order tiebreakers, `DISTINCT`, set operations, window functions (must never be served), volatile functions (never served); every generated query checked against the oracle | CI, 10 minutes, fixed seeds; nightly with rotating seeds |
| 4. Stress | §9.3 | CI (TSAN job) and nightly |
| 5. Resource | `memory_limit` {128 MB, 256 MB, 1 GB, 8 GB} × `threads` {1, 2, 6, 16} × temp directory {set, unset}; 10 passes of the workload in one process with RSS and `duckdb_memory()` sampled (no growth after pass 2) | CI (small), nightly (full) |
| 6. Sanitizers | ASAN + UBSAN on layers 1–3; TSAN on layer 4 | CI |
| 7. Score gate | §10, IMDB, local only (the data is not in CI) | before every merge to `main` |
| 8. Stock DuckDB | the built `waddle.duckdb_extension` loaded into the PyPI `duckdb==1.5.5` wheel with `allow_unsigned_extensions`, running layers 1–3 through Python — exactly how the grader consumes the binary | CI |

### 9.2 The oracle

Every checked query runs twice inside one transaction on the same connection: once as-is, once after
`SET disabled_optimizers = 'extension'` (a session setting; the hooks do not run, as the report's vanilla
baseline relies on). Same snapshot, same settings, same thread count; results must be identical as multisets,
and identical as sequences when the query has an `ORDER BY` with a total key or when `preserve_insertion_order
= true` makes the order observable. This is the only oracle that is valid under concurrent writers.

### 9.3 Stress "to the max" (`scripts/recycler_stress.py`, seeded, deterministic in single-thread mode)

Six synthetic tables (integers, strings, decimals, timestamps, nested types, NULL-heavy columns, one table with
stale statistics after bulk deletes so estimates lie), 24 query templates with random constants, and the
following actors on one database:

- 8–64 reader connections on their own threads, each running random templates and checking every result with
  the oracle of §9.2;
- 1–4 writer connections doing `INSERT`/`UPDATE`/`DELETE`/`MERGE` in auto-commit and in explicit transactions
  that commit or roll back at random, plus occasional DDL (`ALTER TABLE ADD COLUMN`, `CREATE OR REPLACE VIEW`,
  `DROP`/`CREATE` of a table with the same name and a different content);
- a chaos connection issuing `PRAGMA recycler_clear`, random `SET recycler_*` toggles, `SET memory_limit`
  changes, `SET preserve_insertion_order`, `SET TimeZone`, and closing/reopening its connection;
- temp tables of the same name and shape but different content on two connections;
- prepared statements executed with rotating parameters;
- a Parquet/CSV file rewritten in place between reads.

Assertions: every result equals the oracle's; no query errors except the expected DDL races; `recycler_stats()`
counters are consistent (stores + rejects = writes planned, hits ≤ lookups); the process ends with the cache
cleared and no leaked collections (`duckdb_memory()` `COLUMN_DATA` back to zero after `recycler_clear`).
Runs: CI 5 minutes under TSAN with 8 readers; nightly 60 minutes with 64 readers and rotating seeds; any failure
is reproducible from its seed in single-thread mode.

### 9.4 CI

A new workflow `tests.yml` (manual dispatch and on pull requests to `main`; the distribution pipeline is not
touched) with jobs: `build-test` (Linux GCC release, layers 1–3), `debug` (debug build with `D_ASSERT` and
`ColumnBindingResolver::Verify`, layers 1–3), `asan-ubsan` (layers 1–3), `tsan-stress` (layer 4, 5 minutes),
`resource` (layer 5 small), `stock-duckdb` (layer 8), `format` (clang-format with DuckDB's configuration).
`ccache` keyed on the submodule commit keeps a job at 10–15 minutes after the first run; the full matrix costs
about 90 Actions minutes per pull request, within the monthly budget at the team's merge rate, and the manual
dispatch runs a subset for branches. A nightly schedule runs layers 3–5 in full.

## 10. Score protection: the gate that every change passes

`scripts/score_gate.py` (P2) runs, with the grader's protocol (`scripts/benchmark.py`'s fresh processes, warm-up,
`EXPLAIN (ANALYZE, FORMAT JSON)`, grader settings):

1. a vanilla checkpoint (`SET disabled_optimizers = 'extension'`) before and after, which defines the session's
   noise band;
2. the baseline binary (the last `main` build, kept in `build/baseline/`) and the candidate binary, each twice,
   in filename order, and once each in reverse and shuffled order;
3. per query: served or not (a `RECYCLER_SCAN` in the profile), latency, and the answer through
   `scripts/recycler_diff.py`.

The gate **fails** if any answer differs from vanilla in any order; if any query served by the baseline in
filename order is not served by the candidate; if the candidate's filename-order geomean is worse than the
baseline's by more than the noise band; or if any single payer is more than 25 % slower than in the baseline
without a served query that explains it. It **prints** the per-family table of §2 for both binaries so a gain
elsewhere never hides a loss. Merges to `main` and CI dispatches happen only after a passing gate, and the
passing gate's output is attached to the pull request.

Rules for defaults that follow from the gate:

- A general feature that cannot touch the IMDB workload (temp tables, foreign catalogs, prepared statements,
  table functions) ships default-on; the gate proves it is a no-op there.
- A feature on the hot path (settings hash, table versions, order decision) ships only within the hook budget
  of §8.2 and after the gate.
- A feature that changes admission (cost model, ageing, evict-first) ships only after the gate in three orders
  and the ablation of §3.6/§5.4; if the private set's order could turn a gain into a loss, the conservative
  default wins.

## 11. Merge-worthiness: what the reviewer will say, and the answer

### 11.1 Findings from reading the m6 sources (`src/` at 9fb66fce3b)

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
| `recycler_hooks.cpp:113-140, 721-724` | any write clears everything; temp tables and foreign catalogs are not distinguished | 15 | §7.2, §7.1 (G1) |
| `recycler_operators.cpp:275-287` | only allocated bytes; no used bytes | 18 | §4.3 (P2) |
| `recycler_operators.cpp:353-386` | `Finalize` charges accounting and eviction to the query; no measured cost recorded | perf, 16 | §6.5 (P2) |
| `recycler_operators.cpp:183-187` | the sink mode is chosen from `spec.ordered` and a batch-index check, not from vanilla's own order decision | 7 | §7.3 (G2) |
| `recycler_signature.cpp:133-135` | `Refuse(info, reason)` discards the reason: `recycler_stats()` cannot say why a plan was refused | 18 | reason enum + counters (P2) |
| `recycler_signature.cpp:326` | `return_type.ToString()` allocates a string per expression in the hash pass | 3 | `Sink::Type()` virtual: the hash sink hashes `LogicalType::Hash()`, the string sink still writes `ToString()` (P1; measure the hook time before and after) |
| `recycler_signature.cpp:493-499` | every `LogicalGet` without a catalog table is refused, every one with a table accepted | 15 | §7.1, §7.6 (G1, G4) |
| `recycler_signature.cpp:692-725` | IN-list rendering builds a `Value` per element (100 of 149 queries have IN lists; bounded at 4,096 rows) | 3 | acceptable; covered by the hook-budget test (P8) |
| `recycler_functions.cpp:38-52` | shutdown by connection count is correct but subtle | 12 | comment says why (destroy order of `DatabaseInstance`), keep (P1) |
| tests | no test for: eviction under a tiny budget, `preserve_insertion_order = true` with every mechanism, two connections, `memory_limit` overflow, the `latency` field on a hit, the hook-time budget, the bail-out threshold, and none of §7 | 5 | §9 |

Concurrency, checked and kept: the cache lock covers every map mutation; `Lookup` renders outside the lock and
confirms by byte comparison; `hits`/`last_used` are atomics because `NoteHit` runs outside the lock (the
comment should say so); per-connection state is touched only by that connection's hooks and callbacks; entries
are `shared_ptr`-owned by plans. The ordered index of §5.3 and the table versions of §7.2 live under the same
lock.

### 11.2 Structure after the refactor (P1, a pure move)

| File | Responsibility | Lines (est.) |
|---|---|---:|
| `recycler_signature.{hpp,cpp}` | canonical renderer, hashes, shapes, constraints; §7.4 settings hash, §7.5 parameter values, §7.6 table-function keys | 1,600 |
| `recycler_cache.{hpp,cpp}` | entries, maps, ordered index, history, table versions, budget/target, stats | 600 |
| `recycler_policy.{hpp,cpp}` | bail-out, admission, serving rule, calibration, priors (§3) | 300 |
| `recycler_rewrite.{hpp,cpp}` | `SubstituteScan`, `WrapMaterialize`, `HoistPaginationLimit`, `BlockFinalizer`, the shared slot visitor | 350 |
| `recycler_hooks.cpp` | the two hooks as orchestration of gate → analysis → mechanisms → writes | 400 |
| `recycler_operators.{hpp,cpp}` | operators, sink mode from vanilla's order decision, compaction, evict-first priority, measured timings, batch-index scan | 600 |
| `recycler_functions.cpp` | registration, settings, `recycler_stats()`, `recycler_entries()`, pragma, the `ClientContextState` callbacks (query end, transactions, prepared statements) | 400 |

One PR per row change; the P1 PR moves code and adds nothing (diff readable as a transform).

### 11.3 Naming and comments

Names say when to use them: `ShouldAnalyze`, `AdmitWrite`, `ShouldServe`, `SignatureHistory`,
`TableVersions`, `recycler_min_lookup_rows`, `recycler_target_mb`, `recycler_speculation_ttl`,
`recycler_evict_first`, `recycler_remote_files`. Comments only where the *why* is non-obvious: the profiler
trap (why `EXTENSION` operators), the destroy order at shutdown, the eviction-queue trick, the batch-index
exception, the `NoteHit`-outside-the-lock atomics, the LIMIT hoisting invariant, the writer-announce-before-write
invariant of §7.2, the list of result-affecting settings. No narration of what the code does.

## 12. Milestones

Effort in working days for one developer; each ends with the verification chain of §0 and the score gate of
§10 and, when merged to `main`, a dispatched build. Three tracks, one order.

| # | Milestone | Content | Effort | Exit criteria |
|---|---|---|---:|---|
| P0 | Land M6 | review td-02/dsa_asignment2#10 with the checklist, merge, dispatch CI, fast-forward local `main` | 0.5 | CI green on `main`; benchmark within noise of 9.2 ms |
| P1 | Structure | §11.2 pure-move refactor, `RecyclerWriteSpec::Make`, common operator base, `Lookup` without `std::function`, `D_ASSERT`s, comments of §11.3 | 1 | identical results and stats on the public set; hook time not worse |
| P2 | Observability and the gate | `QueryEnd` state, measured costs from the profiler tree, `Sink` call statistics, bounded `SignatureHistory` with reasons, used/allocated bytes, spilled bytes, new `recycler_entries()`/`recycler_stats()` columns, **`scripts/score_gate.py`**, the oracle harness skeleton of §9.2 | 2 | every entry shows a measured cost after its payer; the gate runs end to end and passes on the current binary against itself |
| G1 | Correctness on general databases | temp tables refused, DuckDB catalogs only, settings hash (§7.4), fine-grained race-free invalidation (§7.2); two-connection and transaction tests | 2–3 | stress layer 4 with writers passes under TSAN; hook budget test passes; gate passes |
| P3 | Memory diet 1 and the payer copy | compaction (§4.2), budget/target from the memory limit (§4.4), chunk buffering in the sink if §8.3's measurement says so | 1–1.5 | tiny entries ≤ 2 MiB in total; payer copy ≤ 4 ms/MiB; gate passes |
| G2 | Row order like vanilla | §7.3: sink mode from `PreserveInsertionOrder`/`UseBatchIndex`, batch-index scan; `recycler_order.test` | 1.5–2 | every mechanism active and correct under `preserve_insertion_order = true`; gate unchanged (the grader's setting is `false`) |
| P4 | Wide block entries | §4.1 widening + subsumption-aware replacement | 2–3 | ≥ 8 more served queries in filename order, all answers vanilla in three orders, block space ≤ 600 MiB; gate passes |
| P5 | Cost model | `recycler_policy` (§3), bail-out, serving rule, removal of the speculation factors | 1.5–2 | ablation of §3.6 passes; sub-ms queries within 2 % of vanilla; gate passes |
| P6 | Ageing | GDSF + TTL with the ordered index (§5.3), policy ablation (§5.4) | 1–1.5 | O(log n) eviction; hits preserved at a 1,000 MiB target; gate passes |
| V1 | Stress and CI to the max | §9.3 harness complete (readers, writers, chaos, temp tables, files), `tests.yml` with all jobs, nightly schedule, layer 5 and 8 | 2–3 | CI green on a pull request; a seeded nightly run reproduces any failure |
| P7 | Disk tier | D0 tests and counters, D1 evict-first + swap-aware serving (§6.3) | 1 | `recycler_spill.test`; 1 GB run answers everything; decision on D2 recorded |
| G3 | Prepared statements | §7.5 rebind on demand, parameter values in the key | 1–1.5 | `PREPARE`/`EXECUTE` tests; no rebind below the bail-out floor |
| G4 | Table functions and files | §7.6 allow-list, keys, freshness checks | 2–3 | file rewrite between reads never serves stale rows; remote files off by default |
| G5 | Release readiness | portability builds after the deadline, README for users, community-extension descriptor, report addendum with refreshed numbers | 1 | loads into stock DuckDB 1.5.5 on Linux/macOS/Windows |

Total ≈ 20–26 days. Order rationale: the gate and the measurements come first because everything after them is
judged by them; G1 comes before any performance work because a general recycler that can return a stale or
foreign row is worthless whatever its score; P4 is the only milestone with a large expected score effect
(9.2 → ~7.5 ms); G2–G4 are pure generalizations that the gate proves to be no-ops on the grader.

## 13. Risks and open questions, each with the experiment that settles it

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
6. **Hit floor on large bound plans**: the difference between 0.13 and 0.45 ms hits is binding, not the hooks.
   Measure the split once; the ceiling is ≈ 0.5 ms of geomean.
7. **A streaming "tee" materializer** (append while forwarding, no pipeline break) was evaluated and rejected:
   `PipelineExecutor` skips a whole pipeline whose sink already knows no output is possible and runs
   `FinalExecute` only from the operator that finished early (`pipeline_executor.cpp:42-46, 51-57`), so a tee
   below a LIMIT or on the probe side of a join with an empty build sees a partial input and cannot tell. The
   saving would have been at most the re-scan half of §1.1 on five queries (< 0.3 % of the score) against a
   silent-wrong-answer risk.
8. **Batch-index support of the scan (§7.3)**: if `PhysicalColumnDataScan` in v1.5.5 does not model it, the
   scan implements `SupportsPartitioning(BatchIndex)` itself with the chunk index; a test under
   `preserve_insertion_order = true` with a total-order comparison settles it.
9. **Table versions and DuckDB's visibility rule (§7.2)**: the design assumes a transaction sees commits with
   `commit_id < start_time`. Verify against `DuckTransaction`/`UndoBuffer` before relying on `<` versus `≤`;
   the explicit-transaction race test fails loudly if the comparison is off by one.
10. **Rebind cost for prepared statements (§7.5)** could exceed the gain for mid-size statements. The cost
    class threshold is measured, and a statement whose rebinds never hit stops requesting them (history).
11. **The oracle's `disabled_optimizers` trick (§9.2)** must be re-verified whenever the hooks change: a test
    asserts that `recycler_stats()` counters do not move on the oracle connection.

## 14. Do not

- Persist anything across processes (files outside the run's directory, tables in the database).
- Rename the extension, change the pinned DuckDB, touch `duckdb/`, or the distribution workflow before grading
  ends.
- Serve `LIMIT` without `ORDER BY`, `SAMPLE`, volatile or parameterized (unbound) expressions, non-allow-listed
  table functions, temporary or foreign-catalog tables, delim sides, subtrees with foreign `DELIM_GET`s or
  runtime filters, or anything whose bindings cannot be traced.
- Serve or store while a writer of a dependency is in flight, inside a writing transaction, or under a
  different snapshot than the one the entry was computed under.
- Add a setting, field, atomic, or fallback "just in case": every one above has a test or a measurement that
  needs it.
- Merge to `main` or dispatch a build without a passing score gate; quote a wall-clock effect below the
  machine's noise band without interleaved vanilla checkpoints.

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

Memory and eviction: `common/types/column/column_data_allocator.cpp:94-108` (block allocation,
`can_destroy=false`, partition index → eviction queue index) · `storage/standard_buffer_manager.cpp:176`
(`can_destroy` → `DestroyBufferUpon`) · `include/duckdb/common/enums/destroy_buffer_upon.hpp` ·
`storage/buffer/buffer_pool.cpp:15-27, 244, 290-300` and `include/duckdb/storage/buffer/buffer_pool.hpp:79-90,
115-117` (eviction order, queue sizes, `EvictBlocks` protected) · `include/duckdb/common/radix_partitioning.hpp:34-36`
(`RadixBits`) · `include/duckdb/common/types/column/column_data_collection.hpp:73` and
`common/types/column/column_data_collection.cpp:124-128` (`SetPartitionIndex`) ·
`include/duckdb/common/types/column/column_data_scan_states.hpp:18-27` (allocator types) ·
`storage/standard_buffer_manager.cpp:456` (eviction without a temporary directory throws) ·
`function/table/system/duckdb_memory.cpp` (`tag`, `memory_usage_bytes`, `temporary_storage_bytes`) ·
`include/duckdb/storage/storage_info.hpp:29` (256 KiB blocks) · `main/config.cpp:555-557` (temporary directory
default).

Lifecycle and callbacks: `include/duckdb/main/client_context_state.hpp:20-47, 53-92` (`QueryBegin`/`QueryEnd`,
transaction callbacks, `CanRequestRebind`, `OnFinalizePrepare`, `OnExecutePrepared`,
`OnRebindPreparedStatement`, `RebindQueryInfo`) · `main/client_context.cpp:292-306` (`QueryEnd` after
`profiler->EndQuery()`) and `:615-623` (rebind request honoured at execution) ·
`transaction/transaction_context.cpp:50-72, 107` (commit and rollback callbacks after the commit) ·
`include/duckdb/transaction/duck_transaction.hpp:43-47` (`start_time`, `transaction_id`, `commit_id`) ·
`include/duckdb/catalog/catalog.hpp:127` (`GetCatalogVersion`) · `include/duckdb/catalog/catalog_entry.hpp:51-54`
(`temporary`, `internal`) · `execution/operator/helper/physical_explain_analyze.cpp:20-27` (latency fixed in
`Finalize`) · `parallel/pipeline_executor.cpp:42-46, 51-57` (skipped pipelines, partial flush).

Order and operators: `execution/operator/helper/physical_result_collector.cpp:25-52` (vanilla's collector
choice) · `include/duckdb/execution/physical_plan_generator.hpp:91-93` and `execution/physical_plan/plan_insert.cpp:15-69`
(`OrderPreservationRecursive`, `PreserveInsertionOrder`, `UseBatchIndex`) ·
`include/duckdb/execution/physical_operator.hpp:227, 268-296` (`AllSourcesSupportBatchIndex`,
`CachingPhysicalOperator`, `CACHE_THRESHOLD`) · `include/duckdb/planner/operator/logical_get.hpp:32-50, 69`
(`function`, `bind_data`, `parameters`, `named_parameters`, `input_table_types`, `GetTable`) ·
`include/duckdb/function/table_function.hpp:334, 454` (`to_string`) · `include/duckdb/function/function.hpp:61-62`
(`FunctionData::Copy`/`Equals`).

Literature the model follows: Nagel, Boncz, Viglas (ICDE 2013) for measured-cost recycling with a persistent
history; Dursun et al. (SIGMOD 2017) for the observation that in-memory materialization is not free; Zhou et al.
(SIGMOD 2007) for covering subexpressions; Jindal et al. (VLDB 2018) for utility-based selection at scale;
Cherkasova (1998) for GreedyDual-Size-Frequency.
