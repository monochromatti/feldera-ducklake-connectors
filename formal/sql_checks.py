#!/usr/bin/env python3
"""SQL witnesses for a proposed protocol, not an end-to-end Feldera connector test.

Requires DuckDB; installs official DuckLake into a workspace-local directory.
Uses several connections sharing one DuckDB instance and a local DuckDB catalog.
PostgreSQL catalog, network partitions, process death and power loss are not tested.
"""
from __future__ import annotations

import collections
import json
import pathlib
import tempfile

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
results: dict = {"duckdb": duckdb.__version__, "tests": []}


def passed(name, **details):
    results["tests"].append({"name": name, "passed": True, **details})


def nonzero(c):
    return {k: v for k, v in c.items() if v}


with tempfile.TemporaryDirectory(prefix="sql-check-", dir=HERE) as temp:
    root = duckdb.connect()
    extension_dir = HERE.parent / ".tools" / "duckdb_extensions"
    root.execute("SET extension_directory = ?", [str(extension_dir)])
    root.execute("INSTALL ducklake; LOAD ducklake")
    results["extensions"] = root.execute(
        "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded"
    ).fetchall()
    metadata = pathlib.Path(temp) / "metadata.ducklake"
    root.execute(f"ATTACH 'ducklake:{metadata}' AS lake (DATA_INLINING_ROW_LIMIT 5)")
    root.execute("CREATE TABLE lake.src(id BIGINT, v BIGINT)")
    root.execute("INSERT INTO lake.src SELECT i, i*10 FROM range(20) t(i)")
    root.execute("INSERT INTO lake.src VALUES (7,70)")

    def head(c=root):
        return c.execute("FROM lake.current_snapshot()").fetchone()[0]

    def snapshot(s):
        return collections.Counter(root.execute(
            f"SELECT id,v FROM lake.src AT (VERSION => {int(s)})"
        ).fetchall())

    def feed(a, b):
        return root.execute(
            "SELECT snapshot_id,rowid,change_type,id,v "
            "FROM ducklake_table_changes('lake','main','src',?,?) "
            "ORDER BY snapshot_id,rowid,change_type,id,v", [a, b]
        ).fetchall()

    def normalized(rows):
        out = collections.Counter()
        for _, _, op, key, value in rows:
            assert op in {"insert", "delete", "update_preimage", "update_postimage"}
            out[(key, value)] += 1 if op in {"insert", "update_postimage"} else -1
        return out

    start = head()
    boundaries = [start]
    operations = [
        "UPDATE lake.src SET v=v+1 WHERE id=5",
        "UPDATE lake.src SET v=v+2 WHERE id=7",
        "DELETE FROM lake.src WHERE id=3",
        "INSERT INTO lake.src VALUES (21,210),(22,220)",
        "DELETE FROM lake.src WHERE id%2=0",
    ]
    for sql in operations:
        root.execute(sql)
        boundaries.append(head())
    stop = head()
    before = {(a, b): feed(a + 1, b) for a, b in zip(boundaries, boundaries[1:])}
    for (a, b), rows in before.items():
        reconstructed = snapshot(a)
        reconstructed.update(normalized(rows))
        assert nonzero(reconstructed) == nonzero(snapshot(b)), (a, b, rows)
    initial = snapshot(start)
    initial.update(normalized(feed(start + 1, stop)))
    assert nonzero(initial) == nonzero(snapshot(stop))
    passed("backfill_plus_follow_and_per_snapshot_signed_bag_deltas", windows=len(before))

    root.execute("CALL ducklake_flush_inlined_data('lake', table_name => 'src')")
    root.execute("CALL ducklake_rewrite_data_files('lake','src',delete_threshold=>0.0)")
    changed_raw_windows = []
    for (a, b), old_rows in before.items():
        new_rows = feed(a + 1, b)
        assert nonzero(normalized(old_rows)) == nonzero(normalized(new_rows))
        if old_rows != new_rows:
            changed_raw_windows.append([a, b])
    after_maintenance = head()
    reconstructed = snapshot(stop)
    reconstructed.update(normalized(feed(stop + 1, after_maintenance)))
    assert nonzero(reconstructed) == nonzero(snapshot(after_maintenance))
    passed("flush_and_rewrite_preserve_normalized_historic_deltas",
           raw_windows_that_changed=changed_raw_windows)

    # Administrative provisioning creates exactly one guard row. All writers
    # must preserve this invariant; DuckLake does not enforce it with UNIQUE.
    root.execute("CREATE TABLE lake.guard(generation BIGINT,epoch BIGINT,last_step BIGINT)")
    root.execute("INSERT INTO lake.guard VALUES (1,1,0)")
    root.execute("CREATE TABLE lake.out_a(k BIGINT,v BIGINT)")
    root.execute("CREATE TABLE lake.out_b(k BIGINT,v BIGINT)")

    def state(c=root):
        return {
            "guard": c.execute("FROM lake.guard").fetchall(),
            "a": c.execute("FROM lake.out_a ORDER BY ALL").fetchall(),
            "b": c.execute("FROM lake.out_b ORDER BY ALL").fetchall(),
        }

    def begin_publish(c, step, epoch=1, generation=1, amount=10,
                      begin_transaction=True, base=None):
        if begin_transaction:
            c.execute("BEGIN")
        guards = c.execute("FROM lake.guard").fetchall()
        assert len(guards) == 1
        g, e, previous = guards[0]
        if (g, e) != (generation, epoch):
            c.execute("ROLLBACK")
            return "fenced"
        if step <= previous:
            c.execute("ROLLBACK")
            return "duplicate"
        if base is None:
            base = step - 1
        if base != previous:
            c.execute("ROLLBACK")
            return "gap"
        c.execute(
            "UPDATE lake.guard SET last_step=? "
            "WHERE generation=? AND epoch=? AND last_step=?",
            [step, generation, epoch, previous]
        )
        assert c.execute("FROM lake.guard").fetchall() == [(generation, epoch, step)]
        # Keyed replacement preserves a one-row-per-key invariant initially true.
        c.execute("DELETE FROM lake.out_a WHERE k=1")
        c.execute("INSERT INTO lake.out_a VALUES (1,?)", [amount])
        c.execute("DELETE FROM lake.out_b WHERE k=1")
        c.execute("INSERT INTO lake.out_b VALUES (1,?)", [amount * 2])
        return "staged"

    writer = root.cursor()
    observer = root.cursor()
    original = state(observer)
    assert begin_publish(writer, 1) == "staged"
    assert state(observer) == original
    writer.execute("ROLLBACK")
    assert state(observer) == original
    passed("staging_is_invisible_and_explicit_rollback_discards_guard_and_outputs")

    assert begin_publish(writer, 1) == "staged"
    writer.execute("COMMIT")
    committed = state(observer)
    assert committed == {"guard": [(1, 1, 1)], "a": [(1, 10)], "b": [(1, 20)]}
    # Ignore the first COMMIT result, then retry with the same identity.
    assert begin_publish(writer, 1) == "duplicate"
    assert state(observer) == committed
    passed("lost_ack_retry_is_deduplicated_with_atomic_publication")

    contender1 = root.cursor()
    contender2 = root.cursor()
    assert begin_publish(contender1, 2, amount=30) == "staged"
    conflict = None
    first_committed = False
    try:
        outcome = begin_publish(contender2, 2, amount=30)
        if outcome == "staged":
            contender1.execute("COMMIT")
            first_committed = True
            contender2.execute("COMMIT")
            raise AssertionError("both transactions committed a shared guard update")
    except duckdb.Error as exc:
        conflict = str(exc)
        try:
            contender2.execute("ROLLBACK")
        except duckdb.Error:
            pass
    assert conflict, "expected a guard write conflict"
    if not first_committed:
        contender1.execute("COMMIT")
    assert state(observer)["guard"] == [(1, 1, 2)]
    assert begin_publish(contender2, 2, amount=30) == "duplicate"
    passed("concurrent_publishers_conflict_on_shared_guard", error=conflict)

    # A committed ownership change conflicts with an old-snapshot publisher.
    stale = root.cursor()
    new_owner = root.cursor()
    stale.execute("BEGIN")
    assert stale.execute("FROM lake.guard").fetchall() == [(1, 1, 2)]
    new_owner.execute("BEGIN")
    new_owner.execute("UPDATE lake.guard SET epoch=2")
    new_owner.execute("COMMIT")
    fence_conflict = None
    try:
        begin_publish(stale, 3, epoch=1, amount=50, begin_transaction=False)
        stale.execute("COMMIT")
    except duckdb.Error as exc:
        fence_conflict = str(exc)
        try:
            stale.execute("ROLLBACK")
        except duckdb.Error:
            pass
    assert fence_conflict
    assert begin_publish(stale, 3, epoch=1, amount=50) == "fenced"
    assert begin_publish(new_owner, 3, epoch=2, amount=50) == "staged"
    new_owner.execute("COMMIT")
    assert state(observer) == {"guard": [(1, 2, 3)], "a": [(1, 50)], "b": [(1, 100)]}
    passed("ownership_change_fences_old_publisher", error=fence_conflict)

    assert begin_publish(new_owner, 5, epoch=2) == "gap"
    assert begin_publish(new_owner, 4, epoch=2, generation=2) == "fenced"
    passed("sequence_gaps_and_wrong_generation_are_rejected")

    # Real Feldera transaction IDs need not be consecutive. An interval payload
    # can span empty steps when it declares the correct predecessor boundary.
    assert begin_publish(new_owner, 7, epoch=2, base=3, amount=90) == "staged"
    new_owner.execute("COMMIT")
    assert state(observer) == {"guard": [(1, 2, 7)], "a": [(1, 90)], "b": [(1, 180)]}
    passed("nonconsecutive_ids_accept_an_explicit_matching_base")

    root.execute("CREATE TABLE lake.naive_receipt(step BIGINT)")
    root.execute("CREATE TABLE lake.naive_events(v BIGINT)")
    a, b = root.cursor(), root.cursor()
    for c in [a, b]:
        c.execute("BEGIN")
        assert c.execute("SELECT count(*) FROM lake.naive_receipt WHERE step=1").fetchone() == (0,)
        c.execute("INSERT INTO lake.naive_events VALUES (10)")
        c.execute("INSERT INTO lake.naive_receipt VALUES (1)")
    a.execute("COMMIT")
    b.execute("COMMIT")
    assert root.execute("SELECT count(*) FROM lake.naive_events").fetchone() == (2,)
    passed("counterexample_concurrent_append_only_receipts_allow_duplicates")

    results["limitations"] = [
        "One DuckDB instance with multiple connections and local DuckDB catalog.",
        "No Feldera pipeline, Rust connector, SQL/Arrow codec, or original DBSP proof executed.",
        "Explicit ROLLBACK and ignored COMMIT acknowledgement are not process/power-loss tests.",
        "PostgreSQL/SQLite catalogs, network partitions, and cleanup races remain untested.",
        "SQL behavior is empirical evidence, not a Lean refinement proof.",
    ]

(HERE / "sql-results.json").write_text(json.dumps(results, indent=2) + "\n")
print(json.dumps(results, indent=2))
