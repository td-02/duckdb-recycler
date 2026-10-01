#!/usr/bin/env python3
"""The gate every change to the recycler passes before it reaches main.

    python3 ./scripts/score_gate.py --baseline build/baseline/duckdb --candidate build/release/duckdb
    python3 ./scripts/score_gate.py --baseline ... --candidate ... --runs 1 --orders filename --json gate.json

Two duckdb CLIs with the extension linked in - the last main build (the
baseline) and the change under review (the candidate) - are measured with the
grader's protocol, which scripts/benchmark.py implements and this script
imports: a fresh process per pass, a throwaway warm-up pass first, the grader's
settings, every query wrapped in EXPLAIN (ANALYZE, FORMAT JSON) so its timing
comes from DuckDB's own profile. Around the measured passes the candidate runs
twice with the extension's optimizer switched off: those two vanilla
checkpoints are the reference for the per-family score and, by how much they
disagree, the session's noise band. The candidate's answers are then compared
against vanilla in every order with scripts/recycler_diff.py's comparison.

The gate FAILS (exit code 1) when any of these holds:

  1. an answer of the candidate differs from vanilla, in any order;
  2. a query the baseline serves from its cache in filename order is not
     served by the candidate;
  3. the candidate's filename-order geomean (fastest run) is worse than the
     baseline's by more than the noise band: the relative spread between the
     two vanilla checkpoints, never below 3 %;
  4. a query is at least 25 % slower in the candidate than in the baseline
     without being served by the candidate, which is what would explain a
     slower query that pays for other ones. A binary's time for a query is
     the fastest of every pass in which the query was not served, in any
     order: what the query pays does not depend on the order, so every pass
     is evidence. Single queries are noisier than the set: the 25 % is
     raised to the largest disagreement of the two vanilla checkpoints on
     one query, so that on a machine where vanilla itself varies that much
     a slowdown within it is not called a regression; and queries below the
     timer's millisecond are left to the geomean rule. A slowdown found this
     way is re-measured before it counts: both binaries run filename order
     again, up to twice, and the fastest times are compared anew, because a
     regression reproduces and a stall of the machine during one pass does
     not.

--noise-margin replaces both measured bands with a fixed one (the per-query
band keeps its 25 % floor), for a runner whose noise is known.

A query that fails in any pass fails the gate as well: it would be a wrong
answer on the leaderboard.

Printed: per binary, every pass (geomean, total, served and paying queries);
the per-family table of docs/PRODUCTION_PLAN.md section 2 with the score
contribution sum(ln(vanilla / binary)) of each family for both binaries, so a
gain in one family never hides a loss in another; the served queries the
candidate lost and gained; the noise band; the verdicts.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark as bench  # noqa: E402
import recycler_diff as diff  # noqa: E402

ORDERS = ("filename", "reverse", "shuffled")
# recycler_diff's seed, so the shuffled timing pass and its answer check see
# the same permutation.
SEED = 42
# A second pass over the set in the answer check is where every query is a
# hit, whole-result or subtree, so it is where a wrong cache entry shows.
DIFF_REPEAT = 2

NOISE_FLOOR = 0.03
SLOWER = 0.25
SUB_MS_S = 1e-3
# Extra filename-order passes per binary a single-query slowdown may ask for.
CONFIRMATIONS = 2

VANILLA = "SET disabled_optimizers = 'extension';"
SCAN = "RECYCLER_SCAN"
MATERIALIZE = "RECYCLER_MATERIALIZE"

# docs/PRODUCTION_PLAN.md section 2, with the two sorted title pages (q0080,
# q0089) left out of the exact-repeat family so that every query has one
# family. The sub-millisecond family is defined by the vanilla checkpoint,
# see families_of(); everything unlisted is the last family.
FAMILIES: list[tuple[str, str, str]] = [
    ("A", "EXISTS block over cast_info, year predicates",
     "q0226-q0229 q0234-q0238 q0243-q0246 q0279-q0282 q0288-q0291"),
    ("A", "small exact repeats q0075-q0081 -> q0084-q0090",
     "q0075-q0079 q0081 q0084-q0088 q0090"),
    ("A", "q0240 aggregation and its repeats", "q0231 q0240 q0248 q0284 q0293"),
    ("A", "q0265/q0274, q0259/q0266/q0268, q0256/q0258, q0267, q0269/q0270",
     "q0256 q0258 q0259 q0265 q0266 q0267 q0268 q0269 q0270 q0274"),
    ("A", "drill-downs into q0231/q0240", "q0233 q0241 q0286 q0287"),
    ("B", "sorted title pages",
     "q0080 q0082 q0089 q0092-q0094 q0255 q0257 q0264 q0273"),
    ("B", "CTE / shared aggregates", "q0053-q0055 q0057-q0058 q0125-q0134"),
    ("B", "EXISTS with LIMIT/OFFSET pages",
     "q0230 q0232 q0239 q0242 q0247 q0249 q0283 q0285 q0292"),
]
SUB_MS_FAMILY = ("C", "sub-millisecond queries")
REST_FAMILY = ("C", "everything else")


class Result(NamedTuple):
    """One query in one pass."""
    latency: float
    served: bool        # a RECYCLER_SCAN anywhere in the plan
    materialized: bool  # a RECYCLER_MATERIALIZE anywhere in the plan

    @property
    def payer(self) -> bool:
        """Wrote into the cache and got nothing back from it in this query."""
        return self.materialized and not self.served


Pass = dict[str, Result]


class Family(NamedTuple):
    tier: str
    name: str
    members: list[str]


def expand(spec: str) -> list[str]:
    """'q0075-q0079 q0081' -> ['q0075.sql', ..., 'q0079.sql', 'q0081.sql']."""
    names = []
    for item in spec.split():
        lo, _, hi = item.partition("-")
        for n in range(int(lo[1:]), int(hi[1:] if hi else lo[1:]) + 1):
            names.append("q%04d.sql" % n)
    return names


def ordered(queries: list[tuple[str, str]], order: str) -> list[tuple[str, str]]:
    out = list(queries)
    if order == "reverse":
        out.reverse()
    elif order == "shuffled":
        random.Random(SEED).shuffle(out)
    return out


def operator_names(path: Path) -> set[str]:
    """Every operator name in a profile, wrappers included."""
    text = path.read_text()
    profile = json.JSONDecoder().raw_decode(text[text.index("{"):])[0]
    names: set[str] = set()
    todo = [profile]
    while todo:
        node = todo.pop()
        name = node.get("operator_name") or node.get("operator_type")
        if name:
            names.add(name)
        todo += node.get("children", [])
    return names


def build_script(queries: list[tuple[str, str]], profiles: Path, tmp_dir: Path,
                 args, vanilla: bool) -> str:
    """benchmark.py's script; a vanilla pass switches the extension's optimizer off in its header."""
    script = bench.build_script(queries, profiles, tmp_dir, args)
    if not vanilla:
        return script
    head, sep, tail = script.partition("\n.once ")
    return head + "\n" + VANILLA + sep + tail


def one_pass(pass_dir: Path, queries: list[tuple[str, str]], binary: str,
             db: Path, args, vanilla: bool) -> tuple[Pass, list[tuple[str, str]]]:
    """benchmark.py's one_run, plus what the recycler did to each query.

    A process of its own with an empty cache, cwd and temp directory, so
    nothing but the OS page cache carries over from the pass before.
    """
    profiles = pass_dir / "profiles"
    tmp_dir = pass_dir / "tmp"
    for d in (profiles, tmp_dir):
        d.mkdir(parents=True, exist_ok=True)
    script = pass_dir / "script.sql"
    script.write_text(build_script(queries, profiles.resolve(), tmp_dir.resolve(),
                                   args, vanilla))
    bench.run_cli(binary, db.resolve(), script, pass_dir, profiles, len(queries))
    times, failed = bench.measure(queries, profiles,
                                  (pass_dir / "stderr.log").read_text())
    results: Pass = {}
    for seq, (name, _) in enumerate(queries):
        if name not in times:
            continue
        names = operator_names(profiles / ("s%04d.json" % seq))
        results[name] = Result(times[name], SCAN in names, MATERIALIZE in names)
        if vanilla and (results[name].served or results[name].materialized):
            bench.die(f"{name} ran through the recycler in a vanilla pass",
                      f"{VANILLA} did not switch the extension off in {binary}")
    shutil.rmtree(str(tmp_dir), ignore_errors=True)
    return results, failed


def geomean(results: Pass, scored: list[str]) -> float:
    return bench.geomean([results[q].latency for q in scored])


def unserved_time(runs: list[Pass], q: str) -> float:
    """The fastest a binary ran `q` without the cache answering it: what the query pays.

    A query served in every pass has only served times, and they are the
    fastest it has.
    """
    times = [r[q].latency for r in runs if not r[q].served]
    return min(times or [r[q].latency for r in runs])


def check_binary(path: Path, role: str) -> str:
    """The CLI's version string; dies if `path` is not a duckdb CLI."""
    if not path.is_file():
        bench.die(f"the {role} binary {path} does not exist")
    try:
        version = subprocess.run([str(path), "--version"], capture_output=True,
                                 text=True).stdout.strip()
    except OSError as e:
        bench.die(f"the {role} binary {path} cannot be run: {e}")
    if not version:
        bench.die(f"the {role} binary {path} is not a duckdb CLI")
    return version


def parse_margin(text: str) -> float | None:
    """'auto' -> None (measure it); '5%' or '0.05' -> 0.05."""
    if text == "auto":
        return None
    try:
        value = float(text.rstrip("%"))
    except ValueError:
        bench.die(f"--noise-margin has to be 'auto', a fraction or a percentage, not {text!r}")
    return value / 100 if text.endswith("%") or value >= 1 else value


def families_of(scored: list[str], vanilla: Pass) -> list[Family]:
    """Section 2's families over the queries of this workload, each query in one."""
    present = set(scored)
    families, taken = [], set()
    for tier, name, spec in FAMILIES:
        members = [q for q in expand(spec) if q in present]
        families.append(Family(tier, name, members))
        taken.update(members)
    rest = [q for q in scored if q not in taken]
    sub_ms = [q for q in rest if vanilla[q].latency < SUB_MS_S]
    families.append(Family(*SUB_MS_FAMILY, sub_ms))
    families.append(Family(*REST_FAMILY, [q for q in rest if vanilla[q].latency >= SUB_MS_S]))
    return families


def nats(vanilla: Pass, run: Pass, members: list[str]) -> float:
    """The family's contribution to ln(vanilla geomean / geomean), summed, not averaged."""
    return sum(math.log(max(vanilla[q].latency, bench.FLOOR_S) /
                        max(run[q].latency, bench.FLOOR_S)) for q in members)


def check_answers(candidate: Path, db: Path, queries: list[tuple[str, str]],
                  orders: list[str], work: Path, threads: int) -> list[str]:
    """recycler_diff.py's comparison of the candidate against vanilla; the mismatches."""
    reference, _ = diff.run(candidate, db, work / "vanilla", queries, threads, True, 1)
    mismatches = []
    for order in orders:
        got, _ = diff.run(candidate, db, work / order, ordered(queries, order), threads,
                          False, DIFF_REPEAT)
        bad = 0
        for pass_no in range(DIFF_REPEAT):
            for name, sql in queries:
                expected, actual = reference[f"0:{name}"], got[f"{pass_no}:{name}"]
                if diff.same_result(expected, actual, diff.has_limit(sql)):
                    continue
                bad += 1
                if "<no output>" in actual or "<no output>" in expected:
                    why = "the query produced no output"
                else:
                    why = f"vanilla returned {len(expected) - 1} rows, the candidate {len(actual) - 1}"
                mismatches.append(f"{name} ({order} order, pass {pass_no}): {why}")
        print(f"  {order:9} {len(queries) * DIFF_REPEAT - bad}/{len(queries) * DIFF_REPEAT} "
              f"results match vanilla")
    return mismatches


def print_passes(label: str, binary: str, version: str,
                 passes: dict[str, list[Pass]], scored: list[str], best: int) -> None:
    bench.section(f"{label}: {binary} ({version})")
    print(f"  {'order':9} {'run':>3} {'geomean':>11} {'total':>10} {'served':>7} {'payers':>7}")
    for order, runs in passes.items():
        for i, run in enumerate(runs):
            print(f"  {order:9} {i:>3} {1000 * geomean(run, scored):8.2f} ms "
                  f"{sum(run[q].latency for q in scored):8.2f} s "
                  f"{sum(run[q].served for q in scored):7} "
                  f"{sum(run[q].payer for q in scored):7}"
                  + ("   <- fastest" if order == "filename" and i == best else ""))


def print_families(families: list[Family], vanilla: Pass, baseline: Pass,
                   candidate: Pass) -> None:
    bench.section("Score by family (filename order, fastest run; nats = sum ln(vanilla / binary))")
    print(f"  {'':4} {'family':52} {'n':>3} | {'baseline':^24} | {'candidate':^24} | {'delta':>7}")
    print(f"  {'':4} {'':52} {'':3} | {'served':>6} {'payers':>6} {'nats':>9} | "
          f"{'served':>6} {'payers':>6} {'nats':>9} | {'nats':>7}")
    total_b = total_c = 0.0
    for tier, name, members in families:
        b, c = nats(vanilla, baseline, members), nats(vanilla, candidate, members)
        total_b += b
        total_c += c
        print(f"  {tier:4} {name[:52]:52} {len(members):3} | "
              f"{sum(baseline[q].served for q in members):6} "
              f"{sum(baseline[q].payer for q in members):6} {b:+9.2f} | "
              f"{sum(candidate[q].served for q in members):6} "
              f"{sum(candidate[q].payer for q in members):6} {c:+9.2f} | {c - b:+7.2f}")
    n = sum(len(f.members) for f in families)
    print(f"  {'':4} {'all':52} {n:3} | {'':6} {'':6} {total_b:+9.2f} | "
          f"{'':6} {'':6} {total_c:+9.2f} | {total_c - total_b:+7.2f}")
    print(f"\n  a family's nats / {n} is its share of ln(vanilla geomean / geomean)")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="\n".join(__doc__.splitlines()[1:]),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", type=Path, required=True,
                    help="duckdb CLI of the last main build, extension linked in")
    ap.add_argument("--candidate", type=Path, required=True,
                    help="duckdb CLI of the change under review, extension linked in")
    ap.add_argument("queries", nargs="*", default=[str(bench.QUERIES)],
                    help=f".sql files, or directories of them (default: {bench.QUERIES})")
    ap.add_argument("--db", type=Path, default=bench.DB,
                    help=f"database (default: {bench.DB})")
    ap.add_argument("--orders", default=",".join(ORDERS),
                    help=f"comma separated subset of {','.join(ORDERS)} (default: all)")
    ap.add_argument("--runs", type=int, default=bench.RUNS,
                    help=f"filename-order passes per binary (default: {bench.RUNS})")
    ap.add_argument("--noise-margin", default="auto",
                    help="the geomean margin the candidate may lose: 'auto' (the vanilla "
                         "checkpoints' spread, at least 3 %%), a fraction or a percentage")
    ap.add_argument("--threads", type=int, default=bench.THREADS,
                    help=f"SET threads (default: {bench.THREADS})")
    ap.add_argument("--max-memory-mb", type=int, default=bench.MEMORY_MB,
                    help=f"SET memory_limit, in MB (default: {bench.MEMORY_MB})")
    ap.add_argument("--keep", action="store_true",
                    help="keep the profiles, logs and query outputs instead of deleting them")
    ap.add_argument("--json", type=Path, metavar="OUT",
                    help="also write every measurement and verdict to this file")
    args = ap.parse_args()

    orders = [o.strip() for o in args.orders.split(",") if o.strip()]
    orders = ["shuffled" if o == "shuffle" else o for o in orders]
    for order in orders:
        if order not in ORDERS:
            bench.die(f"unknown order {order!r}", f"choose from {', '.join(ORDERS)}")
    if "filename" not in orders:
        bench.die("--orders has to include filename: it is the order the leaderboard scores")
    if args.runs < 1:
        bench.die("--runs has to be at least 1")
    margin = parse_margin(args.noise_margin)
    binaries = {"baseline": str(args.baseline.resolve()),
                "candidate": str(args.candidate.resolve())}
    versions = {role: check_binary(Path(path), role) for role, path in binaries.items()}
    if not args.db.exists():
        bench.die(f"{args.db} does not exist", "python3 ./scripts/download-imdb.py")
    queries = bench.collect(args.queries)

    started = time.perf_counter()
    out = Path(tempfile.mkdtemp(prefix="score-gate-"))
    bench.section("Score gate")
    for role, path in binaries.items():
        print(f"  {role:10}{path} ({versions[role]})")
    print(f"  {'database':10}{args.db}")
    print(f"  {'queries':10}{len(queries)} x {args.runs} run(s) in filename order, "
          f"1 in each of {', '.join(o for o in orders if o != 'filename') or 'no other order'}")
    print(f"  {'budget':10}{args.threads} threads, {args.max_memory_mb} MB memory")
    print()

    verdicts: list[str] = []
    failed: dict[str, list[tuple[str, str]]] = {}

    def run(label: str, binary: str, order: str, vanilla: bool = False) -> Pass:
        print(f"  {label} ...")
        results, bad = one_pass(out / label.replace(" ", "_"), ordered(queries, order),
                                binary, db, args, vanilla)
        if bad:
            failed[label] = bad
        return results

    try:
        db = bench.fresh_db(args.db, out)
        # The warm-up pulls the database into the OS page cache, as the grader's
        # correctness pass does before its clock starts; its timings are thrown away.
        run("warm-up", binaries["candidate"], "filename")
        checkpoints = [run("vanilla before", binaries["candidate"], "filename", vanilla=True)]
        # Interleaved, so that a drift of the machine over the session (thermal,
        # a background job) hits both binaries alike instead of one of them.
        passes: dict[str, dict[str, list[Pass]]] = {role: {} for role in binaries}
        for order in orders:
            for i in range(args.runs if order == "filename" else 1):
                for role, binary in binaries.items():
                    passes[role].setdefault(order, []).append(
                        run(f"{role} {order} {i}", binary, order))
        checkpoints.append(run("vanilla after", binaries["candidate"], "filename", vanilla=True))

        def scored_queries() -> list[str]:
            """The queries every pass timed, so the passes are comparable."""
            every_pass = checkpoints + [p for by_order in passes.values()
                                        for runs in by_order.values() for p in runs]
            scored = sorted(set.intersection(*(set(p) for p in every_pass)))
            if not scored:
                bench.die("no query produced a timing in every pass",
                          "re-run with --keep to read the CLIs' stderr.log")
            return scored

        scored = scored_queries()
        # Vanilla: the faster checkpoint as a whole, the same rule as the score's.
        vanilla = min(checkpoints, key=lambda p: geomean(p, scored))
        spread = [geomean(p, scored) for p in checkpoints]
        noise = abs(spread[0] - spread[1]) / min(spread)
        band = max(noise, NOISE_FLOOR) if margin is None else margin
        # Below a millisecond the timer's own jitter dominates, so those
        # queries would set a band that says nothing about the others.
        query_noise = {q: abs(checkpoints[0][q].latency - checkpoints[1][q].latency) /
                       min(c[q].latency for c in checkpoints)
                       for q in scored if min(c[q].latency for c in checkpoints) >= SUB_MS_S}
        noisiest = max(query_noise, key=query_noise.get, default=None)
        if margin is not None:
            query_band = max(SLOWER, margin)
        else:
            query_band = max(SLOWER, query_noise[noisiest] if noisiest else 0.0)

        for attempt in range(CONFIRMATIONS + 1):
            scored = scored_queries()
            best = {role: min(range(len(passes[role]["filename"])),
                              key=lambda i: geomean(passes[role]["filename"][i], scored))
                    for role in binaries}
            fastest = {role: passes[role]["filename"][best[role]] for role in binaries}
            per_query = {role: {q: unserved_time([p for runs in passes[role].values()
                                                  for p in runs], q)
                                for q in scored} for role in binaries}
            served = {role: {q for q in scored if fastest[role][q].served} for role in binaries}
            slower = [q for q in scored
                      if per_query["baseline"][q] >= SUB_MS_S
                      and per_query["candidate"][q] >= per_query["baseline"][q] * (1 + query_band)
                      and q not in served["candidate"]]
            if not slower or attempt == CONFIRMATIONS:
                break
            print(f"  {len(slower)} unserved quer{'y is' if len(slower) == 1 else 'ies are'} "
                  f"slower in the candidate: re-measuring both binaries to confirm ...")
            for role, binary in binaries.items():
                passes[role]["filename"].append(
                    run(f"{role} filename {len(passes[role]['filename'])}", binary, "filename"))

        for label, bad in failed.items():
            for name, why in bad:
                verdicts.append(f"{name} failed in the {label} pass: {why}")
        lost = sorted(served["baseline"] - served["candidate"])
        gained = sorted(served["candidate"] - served["baseline"])
        score = {role: geomean(fastest[role], scored) for role in binaries}

        print("  checking the candidate's answers against vanilla ...")
        mismatches = check_answers(Path(binaries["candidate"]), db, queries, orders,
                                   out / "answers", args.threads)
        verdicts += [f"answer differs from vanilla: {m}" for m in mismatches]
        verdicts += [f"served by the baseline but not by the candidate: {q}" for q in lost]
        if score["candidate"] > score["baseline"] * (1 + band):
            verdicts.append(f"candidate geomean {1000 * score['candidate']:.2f} ms is "
                            f"{100 * (score['candidate'] / score['baseline'] - 1):.1f} % worse "
                            f"than the baseline's {1000 * score['baseline']:.2f} ms, beyond "
                            f"the {100 * band:.1f} % noise band")
        for q in slower:
            b, c = per_query["baseline"][q], per_query["candidate"][q]
            verdicts.append(f"{q} is {100 * (c / b - 1):.0f} % slower in the candidate "
                            f"({1000 * c:.2f} ms vs {1000 * b:.2f} ms, fastest of "
                            f"{len(passes['candidate']['filename'])} filename-order runs) "
                            f"and not served")

        for role, binary in binaries.items():
            print_passes(role, binary, versions[role], passes[role], scored, best[role])
        bench.section("Vanilla checkpoints (candidate binary, extension optimizer off)")
        for label, s in zip(("before", "after"), spread):
            print(f"  {label:8} geomean {1000 * s:8.2f} ms")
        source = (f"measured, floored at {100 * NOISE_FLOOR:.0f} %" if margin is None
                  else "from --noise-margin")
        print(f"  spread {100 * noise:.1f} %; noise band {100 * band:.1f} % ({source})")
        if noisiest:
            print(f"  largest spread on one query above 1 ms: {100 * query_noise[noisiest]:.0f} % "
                  f"({noisiest.removesuffix('.sql')}); per-query band {100 * query_band:.0f} %")
        else:
            print(f"  per-query band {100 * query_band:.0f} %")
        families = families_of(scored, vanilla)
        print_families(families, vanilla, fastest["baseline"], fastest["candidate"])
        bench.section("Served queries (filename order, fastest run)")
        print(f"  baseline {len(served['baseline'])}, candidate {len(served['candidate'])}")
        print(f"  lost     {', '.join(q.removesuffix('.sql') for q in lost) or '-'}")
        print(f"  gained   {', '.join(q.removesuffix('.sql') for q in gained) or '-'}")

        bench.section("Verdict")
        for v in verdicts:
            print(f"  FAIL  {v}")
        print(f"  {'GATE FAILED' if verdicts else 'GATE PASSED'}: candidate "
              f"{1000 * score['candidate']:.2f} ms vs baseline {1000 * score['baseline']:.2f} ms "
              f"(vanilla {1000 * geomean(vanilla, scored):.2f} ms), "
              f"{len(verdicts)} failure(s), {time.perf_counter() - started:.0f} s")

        if args.json:
            args.json.write_text(json.dumps({
                "binaries": binaries, "versions": versions, "queries": scored,
                "noise_band": band, "noise_spread": noise, "query_band": query_band,
                "checkpoints": [{q: r._asdict() for q, r in p.items()} for p in checkpoints],
                "passes": {role: {order: [{q: r._asdict() for q, r in p.items()} for p in runs]
                                  for order, runs in by_order.items()}
                           for role, by_order in passes.items()},
                "geomean": score, "served_lost": lost, "served_gained": gained,
                "families": [{"tier": f.tier, "name": f.name, "members": f.members,
                              "baseline_nats": nats(vanilla, fastest["baseline"], f.members),
                              "candidate_nats": nats(vanilla, fastest["candidate"], f.members)}
                             for f in families],
                "failed": failed, "verdicts": verdicts}, indent=1))
            print(f"\n  written to {args.json}")
    finally:
        if args.keep:
            print(f"\n  profiles, logs and outputs kept in {out}")
        else:
            shutil.rmtree(str(out), ignore_errors=True)
    if verdicts:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit("\naborted")
