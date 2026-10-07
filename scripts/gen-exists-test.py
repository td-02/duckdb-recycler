#!/usr/bin/env python3
"""Generates test/sql/recycler/recycler_exists.test: every shape the EXISTS rewrite meets, each asserted with the
rewrite on, with it off, and with the extension's optimizer passes disabled, against the values vanilla DuckDB
returns (captured from the baseline binary with recycling off)."""
import subprocess, sys
from pathlib import Path

BASELINE = "build/baseline/duckdb"
OUT = Path("test/sql/recycler/recycler_exists.test")

SETUP = """
CREATE TABLE t AS
SELECT range AS id, range % 7 AS g, 1990 + range % 30 AS y,
       CASE WHEN range % 11 = 0 THEN NULL ELSE range % 13 END AS h
FROM range(3000);
CREATE TABLE f AS
SELECT range AS id, range % 3000 AS tid, range % 5 AS k,
       CASE WHEN range % 17 = 0 THEN NULL ELSE range % 3000 END AS ntid,
       CASE WHEN range % 19 = 0 THEN NULL ELSE range % 13 END AS nh
FROM range(9000);
CREATE TABLE p AS SELECT range AS id, range % 3000 AS tid, range % 9 AS v FROM range(4000);
CREATE TABLE d AS SELECT range AS id, 'role' || range AS role FROM range(5);
CREATE TABLE s AS SELECT range AS id, CAST(range % 7 AS VARCHAR) AS gs FROM range(100);
"""

# (label, sql) - every query has a deterministic result (ORDER BY or a single row)
QUERIES = [
    ("EXISTS over a join, grouped",
     "SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g"),
    ("NOT EXISTS over a join, grouped",
     "SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g"),
    ("EXISTS with NULLs on both sides of the correlation",
     "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.nh = t.h AND f.k < 3)"),
    ("NOT EXISTS with NULLs on both sides: NULL outer keys are kept",
     "SELECT count(*), count(h) FROM t WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.nh = t.h AND f.k < 3)"),
    ("EXISTS with IS NOT DISTINCT FROM: NULL matches NULL",
     "SELECT count(*), count(h) FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.nh IS NOT DISTINCT FROM t.h AND f.k < 3)"),
    ("NOT EXISTS with IS NOT DISTINCT FROM",
     "SELECT count(*), count(h) FROM t WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.nh IS NOT DISTINCT FROM t.h AND f.k < 3)"),
    ("two EXISTS on the same table",
     "SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) AND EXISTS (SELECT 1 FROM p WHERE p.tid = t.id AND p.v = 2) GROUP BY g ORDER BY g"),
    ("EXISTS and NOT EXISTS together",
     "SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) AND NOT EXISTS (SELECT 1 FROM p WHERE p.tid = t.id AND p.v = 2) GROUP BY g ORDER BY g"),
    ("EXISTS on the preserved side of a LEFT JOIN",
     "SELECT t.g, count(*) AS c, count(p.id) AS cp FROM t LEFT JOIN p ON p.tid = t.id AND p.v = 1 JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY t.g ORDER BY t.g"),
    ("EXISTS on the null-supplying side of a LEFT JOIN",
     "SELECT d.id, count(*) AS c, count(t.id) AS ct FROM d LEFT JOIN t ON t.g = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY d.id ORDER BY d.id"),
    ("NOT EXISTS on the null-supplying side of a LEFT JOIN keeps the unmatched rows",
     "SELECT d.id, count(*) AS c, count(t.id) AS ct FROM d LEFT JOIN t ON t.g = d.id AND t.id < 10 WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY d.id ORDER BY d.id"),
    ("EXISTS on the preserved side of a RIGHT JOIN",
     "SELECT t.g, count(*) AS c, count(p.id) AS cp FROM p RIGHT JOIN t ON p.tid = t.id AND p.v = 1 WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY t.g ORDER BY t.g"),
    ("EXISTS above a FULL OUTER JOIN",
     "SELECT count(*), count(t.id), count(p.id) FROM p FULL JOIN t ON p.tid = t.id AND p.v = 1 WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1)"),
    ("EXISTS over a comma join",
     "SELECT g, count(*) AS c FROM t, d WHERE t.g % 5 = d.id AND EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g"),
    ("EXISTS over a SEMI JOIN",
     "SELECT g, count(*) AS c FROM t SEMI JOIN d ON t.g = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g"),
    ("EXISTS in the select list is a plain boolean",
     "SELECT t.id, EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 2) AS e FROM t WHERE t.id < 12 ORDER BY t.id"),
    ("EXISTS in the select list with a NULL outer key",
     "SELECT t.id, t.h, EXISTS (SELECT 1 FROM f WHERE f.nh = t.h AND f.k = 2) AS e FROM t WHERE t.id BETWEEN 9 AND 14 ORDER BY t.id"),
    ("EXISTS under OR stays a filter over the mark",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) OR t.g = 6 GROUP BY g ORDER BY g"),
    ("NOT EXISTS under OR",
     "SELECT g, count(*) AS c FROM t WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.nh = t.h AND f.k = 1) OR t.g = 6 GROUP BY g ORDER BY g"),
    ("correlated IN is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE t.g IN (SELECT f.k FROM f WHERE f.tid = t.id) GROUP BY g ORDER BY g"),
    ("correlated NOT IN with NULLs is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE t.h NOT IN (SELECT f.nh FROM f WHERE f.tid = t.id) GROUP BY g ORDER BY g"),
    ("correlated ANY is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE t.g < ANY (SELECT f.k FROM f WHERE f.tid = t.id) GROUP BY g ORDER BY g"),
    ("inequality correlation is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid > t.id + 2990 AND f.k = 0) GROUP BY g ORDER BY g"),
    ("correlation inside an aggregate is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id GROUP BY f.k HAVING count(*) > 1) GROUP BY g ORDER BY g"),
    ("correlated scalar subquery is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE t.g = (SELECT max(f.k) FROM f WHERE f.tid = t.id) GROUP BY g ORDER BY g"),
    ("correlation in a join condition of the subquery is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f JOIN p ON p.tid = f.tid AND f.tid = t.id WHERE f.k = 1 AND p.v = 1) GROUP BY g ORDER BY g"),
    ("subquery with LIMIT is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1 LIMIT 1) GROUP BY g ORDER BY g"),
    ("subquery that projects the outer column is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT t.id FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g"),
    ("conjunct over the outer row alone",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE t.g = 3 AND f.tid = t.id) GROUP BY g ORDER BY g"),
    ("correlation through a CAST",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.gs = CAST(t.g AS VARCHAR) AND s.id < 50) GROUP BY g ORDER BY g"),
    ("two correlated columns",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM p WHERE p.tid = t.id AND p.v = t.g) GROUP BY g ORDER BY g"),
    ("expression over two correlated columns",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM p WHERE p.tid + p.v = t.id + t.g AND p.v < 4) GROUP BY g ORDER BY g"),
    ("subquery with an uncorrelated IN of its own",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k IN (SELECT id FROM d WHERE id < 2)) GROUP BY g ORDER BY g"),
    ("nested correlated EXISTS is left to the planner",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND EXISTS (SELECT 1 FROM p WHERE p.tid = f.tid AND p.v = t.g)) GROUP BY g ORDER BY g"),
    ("EXISTS over a CTE",
     "WITH c AS (SELECT tid, count(*) AS n FROM f WHERE k < 2 GROUP BY tid) SELECT g, count(*) AS cnt FROM t WHERE EXISTS (SELECT 1 FROM c WHERE c.tid = t.id AND c.n > 1) GROUP BY g ORDER BY g"),
    ("EXISTS inside a derived table under a join",
     "SELECT x.g, count(*) AS c FROM (SELECT * FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1)) x JOIN d ON x.g % 5 = d.id GROUP BY x.g ORDER BY x.g"),
    ("correlation on a derived table's expression",
     "SELECT x.g, count(*) AS c FROM (SELECT id * 2 AS id2, g FROM t) x WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = x.id2 AND f.k = 1) GROUP BY x.g ORDER BY x.g"),
    ("correlation through the subquery's own derived table",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM (SELECT * FROM f WHERE f.tid = t.id) z WHERE z.k = 1) GROUP BY g ORDER BY g"),
    ("EXISTS in both arms of a UNION ALL",
     "SELECT * FROM (SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g UNION ALL SELECT g, -count(*) FROM t WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g) u ORDER BY 1, 2"),
    ("DISTINCT over an EXISTS",
     "SELECT DISTINCT g FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1 AND f.id < 30) ORDER BY g"),
    ("EXISTS with GROUP BY and HAVING",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g HAVING count(*) > 400 ORDER BY g"),
    ("EXISTS under a window function",
     "SELECT g, c, rank() OVER (ORDER BY c DESC, g) AS r FROM (SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g) q ORDER BY g"),
    ("EXISTS with the outer table joined twice",
     "SELECT a.g, count(*) AS c FROM t a JOIN t b ON b.id = a.id + 1 WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = b.id AND f.k = 1) AND NOT EXISTS (SELECT 1 FROM p WHERE p.tid = a.id AND p.v = 0) GROUP BY a.g ORDER BY a.g"),
    ("EXISTS whose subquery has no other predicate",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM p WHERE p.tid = t.id) GROUP BY g ORDER BY g"),
    ("EXISTS whose subquery is a join",
     "SELECT g, count(*) AS c FROM t WHERE EXISTS (SELECT 1 FROM f JOIN d ON d.id = f.k WHERE f.tid = t.id AND d.role = 'role1') GROUP BY g ORDER BY g"),
    ("NOT EXISTS whose subquery is a join",
     "SELECT g, count(*) AS c FROM t WHERE NOT EXISTS (SELECT 1 FROM f JOIN d ON d.id = f.k WHERE f.tid = t.id AND d.role = 'role1') GROUP BY g ORDER BY g"),
    ("EXISTS with a LIMIT on the outer query",
     "SELECT id, g FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) ORDER BY id LIMIT 7 OFFSET 3"),
]

# statements that change data: run on a fresh copy each time, the check is the query after them
MUTATIONS = [
    ("DELETE with EXISTS",
     ["CREATE TABLE t2 AS SELECT * FROM t",
      "DELETE FROM t2 WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t2.id AND f.k = 4)"],
     "SELECT count(*), sum(id) FROM t2",
     ["DROP TABLE t2"]),
    ("UPDATE with NOT EXISTS",
     ["CREATE TABLE t2 AS SELECT * FROM t",
      "UPDATE t2 SET g = -1 WHERE NOT EXISTS (SELECT 1 FROM f WHERE f.tid = t2.id AND f.k = 4)"],
     "SELECT g, count(*) AS c FROM t2 GROUP BY g ORDER BY g",
     ["DROP TABLE t2"]),
    ("CREATE TABLE AS with EXISTS",
     ["CREATE TABLE t2 AS SELECT id, g FROM t WHERE EXISTS (SELECT 1 FROM p WHERE p.tid = t.id AND p.v = 3)"],
     "SELECT count(*), sum(id), sum(g) FROM t2",
     ["DROP TABLE t2"]),
]

TYPE_LETTER = {"BIGINT": "I", "INTEGER": "I", "HUGEINT": "I", "SMALLINT": "I", "TINYINT": "I", "UBIGINT": "I",
               "BOOLEAN": "I", "VARCHAR": "T", "DOUBLE": "R", "FLOAT": "R"}


def run_baseline(sql_lines):
    # the oracle is vanilla DuckDB whatever the baseline binary carries: its extension passes are switched off
    script = "\n".join([".mode tabs", ".headers off", ".nullvalue NULL",
                        "SET threads = 4;", "SET disabled_optimizers = 'extension';", SETUP] + sql_lines) + "\n"
    proc = subprocess.run([BASELINE, "-init", "/dev/null", "-batch"], input=script, capture_output=True, text=True)
    if proc.returncode != 0 or proc.stderr.strip():
        sys.exit(f"baseline failed:\n{proc.stderr}")
    return proc.stdout


def types_of(sql, statements_before=(), statements_after=()):
    """The sqllogictest type letters of the query's columns, from DESCRIBE after `statements_before` ran."""
    out = run_baseline([s + ";" for s in statements_before] + [f"DESCRIBE {sql};"] +
                       [s + ";" for s in statements_after])
    letters = []
    for line in out.strip().splitlines():
        cols = line.split("\t")
        letters.append("R" if cols[1].startswith("DECIMAL") else TYPE_LETTER[cols[1]])
    return "".join(letters)


def result_of(statements_before, sql, statements_after):
    out = run_baseline([s + ";" for s in statements_before] + [sql + ";"] + [s + ";" for s in statements_after])
    return out.rstrip("\n")


def block(label, before, sql, after, types, result):
    lines = [f"# {label}"]
    for s in before:
        lines += ["statement ok", s, ""]
    lines += [f"query {types}", sql, "----", result, ""]
    for s in after:
        lines += ["statement ok", s, ""]
    return "\n".join(lines)


def main():
    cases = []
    for label, sql in QUERIES:
        cases.append((label, [], sql, [], types_of(sql), result_of([], sql, [])))
    for label, before, sql, after in MUTATIONS:
        cases.append((label, before, sql, after, types_of(sql, before, after), result_of(before, sql, after)))

    modes = [
        ("the rewrite on", ["SET recycler_exists_rewrite = true"]),
        ("the rewrite off", ["SET recycler_exists_rewrite = false"]),
        ("the extension's optimizer passes disabled (vanilla DuckDB)",
         ["SET recycler_exists_rewrite = true", "SET disabled_optimizers = 'extension'"]),
    ]
    out = []
    out.append("""# name: test/sql/recycler/recycler_exists.test
# description: correlated EXISTS subqueries planned as plain joins (recycler_exists.cpp): every shape the rule
# meets, asserted with the rewrite on, with it off and with the extension's optimizer passes disabled, against the
# values vanilla DuckDB returns. Generated by scripts/gen-exists-test.py; edit the generator, not this file.
# group: [recycler]

require waddle

statement ok
SET threads = 4;

# no caching: every query below must be planned and executed afresh in every mode
statement ok
SET recycler_enabled = false;
""")
    for stmt in SETUP.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            out.append(f"statement ok\n{stmt};\n")
    for mode_label, settings in modes:
        out.append(f"\n# ---------------------------------------------------------------------------\n# with {mode_label}\n# ---------------------------------------------------------------------------")
        for s in settings:
            out.append(f"statement ok\n{s};\n")
        for label, before, sql, after, types, result in cases:
            out.append(block(label, before, sql, after, types, result))
    # the tail's own values, from the baseline as well
    count_exists = "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1)"
    count_in = "SELECT count(*) FROM t WHERE t.g IN (SELECT f.k FROM f WHERE f.tid = t.id)"
    wide = ("SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id JOIN s ON s.id = t.g WHERE y IS NOT NULL AND "
            "EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g")
    narrow = ("SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id JOIN s ON s.id = t.g WHERE y BETWEEN 2000 AND "
              "2009 AND EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g")
    p2 = "CREATE TABLE p2 AS SELECT range AS id, range % 3000 AS tid, range % 9 AS v FROM range(4000)"
    marker_payer = ("SELECT count(*) FROM t WHERE random() < 2 AND EXISTS (SELECT 1 FROM f JOIN p2 ON p2.tid = f.tid "
                    "JOIN d ON d.id = f.k AND d.id + p2.v > 3 AND p2.v = 1 WHERE f.tid = t.id AND f.k = 3)")
    marker_hitter = ("SELECT count(*) FROM t WHERE random() < 2 AND EXISTS (SELECT 1 FROM f JOIN p2 ON p2.tid = f.tid "
                     "JOIN d ON d.id = f.k AND d.id + p2.v > 3 AND p2.v = 1 WHERE f.tid = t.id)")
    marker_hitter_not = ("SELECT count(*) FROM t WHERE random() < 2 AND NOT EXISTS (SELECT 1 FROM f JOIN p2 ON "
                         "p2.tid = f.tid JOIN d ON d.id = f.k AND d.id + p2.v > 3 AND p2.v = 1 WHERE f.tid = t.id)")
    tail = {name: result_of([], sql, []) for name, sql in
            [("count_exists", count_exists), ("count_in", count_in), ("wide", wide), ("narrow", narrow)]}
    for name, sql in [("marker_payer", marker_payer), ("marker_hitter", marker_hitter),
                      ("marker_hitter_not", marker_hitter_not)]:
        tail[name] = result_of([p2], sql, ["DROP TABLE p2"])
    out.append("""statement ok
SET disabled_optimizers = '';

# ---------------------------------------------------------------------------
# the plan: with the rewrite, no delim join is left; without it, the planner's delim join runs
# ---------------------------------------------------------------------------
statement ok
SET recycler_exists_rewrite = true;

query II
EXPLAIN SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g
----
physical_plan	<!REGEX>:.*DELIM.*

query II
EXPLAIN SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g
----
physical_plan	<REGEX>:.*SEMI.*

statement ok
SET recycler_exists_rewrite = false;

query II
EXPLAIN SELECT g, count(*) AS c FROM t JOIN d ON t.g % 5 = d.id WHERE EXISTS (SELECT 1 FROM f WHERE f.tid = t.id AND f.k = 1) GROUP BY g ORDER BY g
----
physical_plan	<REGEX>:.*DELIM_JOIN.*

statement ok
SET recycler_exists_rewrite = true;

# the counters: a rewritten shape, one the rule leaves alone
statement ok
CREATE TABLE counters AS SELECT name, value FROM recycler_stats() WHERE name LIKE 'exists_%';

query I
%(count_exists_sql)s
----
%(count_exists)s

query I
%(count_in_sql)s
----
%(count_in)s

query TI
SELECT s.name, s.value - c.value FROM recycler_stats() s JOIN counters c USING (name) WHERE s.name IN ('exists_rewrites', 'exists_refused') ORDER BY s.name
----
exists_refused	1
exists_rewrites	1

# ---------------------------------------------------------------------------
# with the cache on, a rewritten block is still stored and still serves a narrower query
# ---------------------------------------------------------------------------
statement ok
SET recycler_enabled = true;

statement ok
SET preserve_insertion_order = false;

statement ok
SET recycler_min_input_rows = 1000;

query II
%(wide_sql)s
----
%(wide)s

query I
SELECT value >= 1 FROM recycler_stats() WHERE name = 'block_stores'
----
true

query II
%(narrow_sql)s
----
%(narrow)s

query I
SELECT value >= 1 FROM recycler_stats() WHERE name = 'subsumption_hits'
----
true

# ---------------------------------------------------------------------------
# a block marker planted inside the subquery (the outer WHERE is not cacheable, the subquery's FROM/ON block is):
# the subquery is left to the planner. Rewritten, the subquery's own conjunct would sit directly above the marker,
# and the join order optimizer, which passes operators it does not know through, would push it below the marker:
# the stored block would then miss rows its key promises, and the second query here would read it.
# ---------------------------------------------------------------------------
statement ok
CREATE TABLE p2 AS SELECT range AS id, range % 3000 AS tid, range % 9 AS v FROM range(4000);

query I
%(marker_payer_sql)s
----
%(marker_payer)s

query I
%(marker_hitter_sql)s
----
%(marker_hitter)s

query I
%(marker_hitter_not_sql)s
----
%(marker_hitter_not)s
""".replace("%(marker_payer_sql)s", marker_payer).replace("%(marker_payer)s", tail["marker_payer"])
               .replace("%(marker_hitter_sql)s", marker_hitter).replace("%(marker_hitter)s", tail["marker_hitter"])
               .replace("%(marker_hitter_not_sql)s", marker_hitter_not)
               .replace("%(marker_hitter_not)s", tail["marker_hitter_not"])
               .replace("%(count_exists_sql)s", count_exists).replace("%(count_exists)s", tail["count_exists"])
               .replace("%(count_in_sql)s", count_in).replace("%(count_in)s", tail["count_in"])
               .replace("%(wide_sql)s", wide).replace("%(wide)s", tail["wide"])
               .replace("%(narrow_sql)s", narrow).replace("%(narrow)s", tail["narrow"]))
    OUT.write_text("\n".join(out))
    print(f"wrote {OUT} with {len(cases)} cases x {len(modes)} modes")


if __name__ == "__main__":
    main()
