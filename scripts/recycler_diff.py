#!/usr/bin/env python3
"""Differential correctness check of the recycler against vanilla DuckDB.

    python3 ./scripts/recycler_diff.py                      # benchmark/ in filename, reverse and shuffled order
    python3 ./scripts/recycler_diff.py --orders filename --repeat 3
    python3 ./scripts/recycler_diff.py benchmark/q0231.sql benchmark/q0233.sql

Every query is run once with the extension's optimizer disabled (the reference) and then, in one CLI process
per order so that the cache warms up across queries, `--repeat` times with the recycler enabled. Results are
compared as multisets of rows (ORDER BY ties make exact order comparisons meaningless, and the grader's own
test uses rowsort), except that queries whose top-level ORDER BY ends in a unique `<table>.id` tiebreaker have a
total order and are compared row by row in output order (this is what checks LIMIT/OFFSET pages); floating-point
columns are compared with a relative tolerance of 1e-9 because parallel aggregation makes the last digits of
avg()/corr()/stddev() vary between runs of vanilla DuckDB itself; other queries with a LIMIT are compared by row
count only (which rows survive a LIMIT with ties is arbitrary). A mismatch or a query error is a failure.
"""

from __future__ import annotations

import argparse
import random
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

BINARY = Path("build") / "release" / "duckdb"
DB = Path("data") / "imdb.duckdb"


FLOAT_RE = re.compile(r"^-?\d+\.\d+(e[+-]?\d+)?$", re.IGNORECASE)


def compare_mode(sql: str) -> str:
    """'ordered' (row by row) when the last ORDER BY ends in a `<table>.id` tiebreaker, 'count' for other
    LIMIT queries, 'sorted' otherwise."""
    pos = sql.upper().rfind("ORDER BY")
    if pos >= 0 and re.match(r"ORDER BY[^;()]*\b\w+\.id\s*(LIMIT|OFFSET|$)", sql[pos:], re.IGNORECASE | re.DOTALL):
        return "ordered"
    if re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        return "count"
    return "sorted"


def same_value(expected: str, actual: str) -> bool:
    if expected == actual:
        return True
    if FLOAT_RE.match(expected) and FLOAT_RE.match(actual):
        e, a = float(expected), float(actual)
        return abs(e - a) <= 1e-9 * max(1.0, abs(e), abs(a))
    return False


def same_row(expected: str, actual: str) -> bool:
    if expected == actual:
        return True
    e_cols, a_cols = expected.split(","), actual.split(",")
    return len(e_cols) == len(a_cols) and all(same_value(e, a) for e, a in zip(e_cols, a_cols))


def same_result(expected: list[str], actual: list[str], mode: str) -> bool:
    """expected/actual are CSV lines in output order (header included)."""
    if "<no output>" in actual or "<no output>" in expected:
        return False
    if len(expected) != len(actual):
        return False
    if mode == "count":
        return True
    if mode == "sorted":
        expected, actual = sorted(expected), sorted(actual)
    return all(same_row(e, a) for e, a in zip(expected, actual))


def collect(paths: list[str]) -> list[tuple[str, str]]:
    files: list[Path] = []
    for arg in paths:
        path = Path(arg)
        files += sorted(path.glob("**/*.sql")) if path.is_dir() else [path]
    if not files:
        sys.exit(f"no .sql files in {', '.join(paths)}")
    return [(f.name, re.sub(r"[;\s]+$", "", f.read_text())) for f in files]


def run(binary: Path, db: Path, out_dir: Path, queries: list[tuple[str, str]], threads: int,
        vanilla: bool, repeat: int) -> tuple[dict[str, list[str]], str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = [".bail off", ".mode csv", ".headers on", f"SET threads = {threads};",
             "SET preserve_insertion_order = false;"]
    if vanilla:
        lines.append("SET disabled_optimizers = 'extension';")
    for pass_no in range(repeat):
        for name, sql in queries:
            lines.append(f".once {out_dir / f'{pass_no}_{name}.csv'}")
            lines.append(sql + ";")
    lines.append(f".once {out_dir / 'stats.csv'}")
    lines.append("SELECT * FROM recycler_stats();")
    script = "\n".join(lines) + "\n"
    proc = subprocess.run([str(binary), "-readonly", "-batch", str(db)], input=script, capture_output=True,
                          text=True)
    results: dict[str, list[str]] = {}
    for pass_no in range(repeat):
        for name, _ in queries:
            path = out_dir / f"{pass_no}_{name}.csv"
            key = f"{pass_no}:{name}"
            if not path.exists():
                results[key] = ["<no output>"]
                continue
            results[key] = path.read_text().splitlines()
    stats = (out_dir / "stats.csv").read_text() if (out_dir / "stats.csv").exists() else ""
    if proc.stderr.strip():
        sys.stderr.write(proc.stderr)
    return results, stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("queries", nargs="*", default=["benchmark"])
    ap.add_argument("--db", type=Path, default=DB)
    ap.add_argument("--binary", type=Path, default=BINARY)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--repeat", type=int, default=2, help="passes over the set per order (default 2)")
    ap.add_argument("--orders", default="filename,reverse,shuffle",
                    help="comma separated: filename, reverse, shuffle (default all three)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    if not args.binary.exists():
        sys.exit(f"{args.binary} does not exist, run `make` first")
    if not args.db.exists():
        sys.exit(f"{args.db} does not exist")
    queries = collect(args.queries)
    work = Path(tempfile.mkdtemp(prefix="recycler-diff-"))
    failures = 0
    try:
        print(f"reference run ({len(queries)} queries, vanilla) ...")
        reference, _ = run(args.binary.resolve(), args.db.resolve(), work / "vanilla", queries, args.threads,
                           True, 1)
        for order in [o.strip() for o in args.orders.split(",") if o.strip()]:
            ordered = list(queries)
            if order == "reverse":
                ordered.reverse()
            elif order == "shuffle":
                random.Random(args.seed).shuffle(ordered)
            elif order != "filename":
                sys.exit(f"unknown order {order}")
            print(f"recycler run: order={order} repeat={args.repeat} ...")
            got, stats = run(args.binary.resolve(), args.db.resolve(), work / order, ordered, args.threads,
                             False, args.repeat)
            bad = 0
            for pass_no in range(args.repeat):
                for name, sql in ordered:
                    expected = reference[f"0:{name}"]
                    actual = got[f"{pass_no}:{name}"]
                    mode = compare_mode(sql)
                    if not same_result(expected, actual, mode):
                        bad += 1
                        failures += 1
                        print(f"  MISMATCH pass {pass_no} {name} ({mode}): expected {len(expected)} lines, "
                              f"got {len(actual)}")
                        if mode == "sorted":
                            expected, actual = sorted(expected), sorted(actual)
                        for e, a in zip(expected[:3], actual[:3]):
                            if e != a:
                                print(f"    expected: {e[:120]}")
                                print(f"    actual:   {a[:120]}")
                                break
            print(f"  {len(ordered) * args.repeat - bad}/{len(ordered) * args.repeat} results match")
            if stats:
                compact = ", ".join(line.replace(",", "=") for line in stats.splitlines()[1:])
                print(f"  stats: {compact}")
    finally:
        if args.keep:
            print(f"outputs kept in {work}")
        else:
            shutil.rmtree(str(work), ignore_errors=True)
    if failures:
        sys.exit(f"{failures} mismatching results")
    print("all results match vanilla")


if __name__ == "__main__":
    main()
