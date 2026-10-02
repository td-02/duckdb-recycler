#!/usr/bin/env python3
"""Differential check of the loadable extension inside the PyPI duckdb wheel.

    python3 scripts/ci_stock_duckdb.py build/release/extension/waddle/waddle.duckdb_extension

The grader consumes the extension exactly like this: a stock `duckdb` wheel, `allow_unsigned_extensions`,
`LOAD`. Every query runs REPEATS times on a connection with the extension loaded and once on a plain
connection holding the same deterministic tables; the rows must be identical as sequences, which the
queries make meaningful by ordering on a key. The recycler must also report at least one whole-result
hit, otherwise a silently inactive extension would pass. The parameterized queries run through
executemany, which prepares once and executes the prepared statement: the client-API path (also taken by
the C API and JDBC) that the sqllogictest runner, which sends SQL text, cannot reach. So does LOAD inside an
explicit transaction next to an idle connection, after which the cache must serve once that transaction ends, and
the parameterized inserts at the end: one into a table no query reads must keep the cached results, and one into t
must outdate them.
"""

import sys

import duckdb

REPEATS = 3

TABLES = [
    "CREATE TABLE t AS SELECT range AS id, range % 7 AS g, range % 13 AS h, 'name' || range AS name FROM range(200000)",
    "CREATE TABLE d AS SELECT range AS id, 'role' || range AS role FROM range(12)",
    "CREATE TABLE w (id BIGINT)",
]

QUERIES = [
    "SELECT g, count(*) AS c FROM t GROUP BY g ORDER BY g DESC",
    "SELECT g, min(id), max(h) FROM t GROUP BY g ORDER BY g",
    "SELECT d.role, count(*) FROM t JOIN d ON t.g = d.id WHERE t.h < 5 GROUP BY d.role ORDER BY d.role",
    "SELECT count(*) FROM t WHERE g IN (0, 1, 2, 3, 4, 5)",
    "SELECT id FROM t WHERE g = 5 ORDER BY id LIMIT 3",
    "SELECT * FROM (SELECT g, count(*) AS c FROM t GROUP BY g) sub WHERE sub.g IN (0, 6) ORDER BY sub.g",
]

# prepared with an unbound parameter, so DuckDB keeps no physical plan and plans each execution again; and prepared
# without one once QUERIES has cached the result, so the prepared plan scans the cache entry and the extension asks
# DuckDB to plan each execution again
PARAMETERIZED = [
    ("SELECT g, count(*) AS c FROM t WHERE h < ? GROUP BY g ORDER BY g", [5]),
    ("SELECT d.role, count(*) FROM t JOIN d ON t.g = d.id WHERE t.h < ? GROUP BY d.role ORDER BY d.role", [9]),
    (QUERIES[0], []),
    (QUERIES[2], []),
]


def connect(extension_path):
    """Returns the connection, and the idle one opened before LOAD"""
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET threads = 4")
    for statement in TABLES:
        con.execute(statement)
    if extension_path is None:
        return con, None
    # LOAD runs inside a transaction, next to an idle connection: neither may keep the cache off once the
    # transaction ends
    idle = con.cursor()
    con.execute("BEGIN TRANSACTION")
    con.execute("SELECT count(*) FROM t").fetchall()
    con.load_extension(extension_path)
    con.execute("COMMIT")
    # the synthetic tables are far below the default admission floor
    con.execute("SET recycler_min_input_rows = 1000")
    return con, idle


def whole_hits(con):
    return con.execute("SELECT value FROM recycler_stats() WHERE name = 'whole_hits'").fetchone()[0]


def mismatches(sql, plain, recycled, run):
    """Runs `run(connection)` once on the plain connection and REPEATS times on the recycled one"""
    expected = run(plain)
    failures = 0
    for repeat in range(REPEATS):
        actual = run(recycled)
        if actual != expected:
            failures += 1
            print("MISMATCH (run %d): %s\n  expected %r\n  got      %r" % (repeat + 1, sql, expected, actual))
    return failures


def main(extension_path):
    plain, _ = connect(None)
    recycled, idle = connect(extension_path)
    print("duckdb", plain.execute("PRAGMA version").fetchone())
    print("extension", recycled.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'waddle'").fetchone())

    failures = 0
    for sql in QUERIES:
        failures += mismatches(sql, plain, recycled, lambda con: con.execute(sql).fetchall())
    for sql, parameters in PARAMETERIZED:
        failures += mismatches(sql, plain, recycled, lambda con: con.executemany(sql, [parameters]).fetchall())

    # QUERIES[0] is cached: a parameterized insert runs its prepared plan, and only an insert into a table the query
    # reads may outdate the entry
    hits = whole_hits(recycled)
    for con in (plain, recycled):
        con.executemany("INSERT INTO w VALUES (?)", [[1], [2]])
    failures += mismatches(QUERIES[0], plain, recycled, lambda con: con.execute(QUERIES[0]).fetchall())
    if whole_hits(recycled) < hits + REPEATS:
        failures += 1
        print("FAIL: a parameterized insert into a table no query reads evicted the cached results")
    for con in (plain, recycled):
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", [[200000, 0, 0, "x"], [200001, 6, 1, "y"]])
    failures += mismatches(QUERIES[0], plain, recycled, lambda con: con.execute(QUERIES[0]).fetchall())

    stats = dict(recycled.execute("SELECT name, value FROM recycler_stats()").fetchall())
    print("recycler_stats", {k: stats[k] for k in ("whole_hits", "subtree_hits", "misses", "whole_stores")})
    if stats["whole_hits"] < 1:
        failures += 1
        print("FAIL: the recycler served no repeated query")
    print("%d queries x %d runs, %d failures" % (len(QUERIES) + len(PARAMETERIZED) + 2, REPEATS, failures))
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
