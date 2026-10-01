#!/usr/bin/env python3
"""Differential check of the loadable extension inside the PyPI duckdb wheel.

    python3 scripts/ci_stock_duckdb.py build/release/extension/waddle/waddle.duckdb_extension

The grader consumes the extension exactly like this: a stock `duckdb` wheel, `allow_unsigned_extensions`,
`LOAD`. Every query runs REPEATS times on a connection with the extension loaded and once on a plain
connection holding the same deterministic tables; the rows must be identical as sequences, which the
queries make meaningful by ordering on a key. The recycler must also report at least one whole-result
hit, otherwise a silently inactive extension would pass.
"""

import sys

import duckdb

REPEATS = 3

TABLES = [
    "CREATE TABLE t AS SELECT range AS id, range % 7 AS g, range % 13 AS h, 'name' || range AS name FROM range(200000)",
    "CREATE TABLE d AS SELECT range AS id, 'role' || range AS role FROM range(12)",
]

QUERIES = [
    "SELECT g, count(*) AS c FROM t GROUP BY g ORDER BY g DESC",
    "SELECT g, min(id), max(h) FROM t GROUP BY g ORDER BY g",
    "SELECT d.role, count(*) FROM t JOIN d ON t.g = d.id WHERE t.h < 5 GROUP BY d.role ORDER BY d.role",
    "SELECT count(*) FROM t WHERE g IN (0, 1, 2, 3, 4, 5)",
    "SELECT id FROM t WHERE g = 5 ORDER BY id LIMIT 3",
    "SELECT * FROM (SELECT g, count(*) AS c FROM t GROUP BY g) sub WHERE sub.g IN (0, 6) ORDER BY sub.g",
]


def connect(extension_path):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET threads = 4")
    if extension_path is not None:
        con.load_extension(extension_path)
        # the synthetic tables are far below the default admission floor
        con.execute("SET recycler_min_input_rows = 1000")
    for statement in TABLES:
        con.execute(statement)
    return con


def main(extension_path):
    plain = connect(None)
    recycled = connect(extension_path)
    print("duckdb", plain.execute("PRAGMA version").fetchone())
    print("extension", recycled.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'waddle'").fetchone())

    failures = 0
    for sql in QUERIES:
        expected = plain.execute(sql).fetchall()
        for repeat in range(REPEATS):
            actual = recycled.execute(sql).fetchall()
            if actual != expected:
                failures += 1
                print("MISMATCH (run %d): %s\n  expected %r\n  got      %r" % (repeat + 1, sql, expected, actual))

    stats = dict(recycled.execute("SELECT name, value FROM recycler_stats()").fetchall())
    print("recycler_stats", {k: stats[k] for k in ("whole_hits", "subtree_hits", "misses", "whole_stores")})
    if stats["whole_hits"] < 1:
        failures += 1
        print("FAIL: the recycler served no repeated query")
    print("%d queries x %d runs, %d failures" % (len(QUERIES), REPEATS, failures))
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
