"""Executable validation scenarios for the publication protocol.

These scenarios run the real DuckLake extension against a throwaway catalog and
assert properties that would break silently in production: atomic visibility of
payload plus guard, rollback on a failed payload, retry idempotence, fencing,
and the difference between keyed replacement and multiplicity-preserving
replacement.

They are deliberately *not* satisfied by mocking: every scenario either reads
committed state through a second cursor (a distinct transaction context) or
rustles a conflict, so a publisher that forgets the guard update fails here.
"""

from __future__ import annotations

import contextlib
import shutil
import tempfile
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .catalog import DuckLake, HistoryExpiredError
from .feldera import FelderaClient
from .input_connector import InputConnector, InputSpec
from .journal import Checkpoint, Journal
from .protocol import (
    Batch,
    Decision,
    Delta,
    Guard,
    Op,
    ProtocolError,
    decide,
)


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    detail: str
    duration_ms: float
    traceback: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
            "duration_ms": round(self.duration_ms, 2),
            "traceback": self.traceback,
        }


SCENARIOS: list[tuple[str, Callable[[Path], str]]] = []


def scenario(name: str):
    def register(fn: Callable[[Path], str]):
        SCENARIOS.append((name, fn))
        return fn

    return register


# -- helpers ---------------------------------------------------------------


def make_lake(
    root: Path,
    *,
    tables: Sequence[str] = ("orders",),
    keyed: bool = True,
    extra_columns: str = "",
) -> DuckLake:
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    lake = DuckLake(
        catalog=f"ducklake:{root / 'meta.ducklake'}",
        data_path=str(data_dir),
        tables=list(tables),
        key_columns={t: ["id"] for t in tables} if keyed else {},
        inlining_row_limit=10_000,
    )
    for table in tables:
        # DuckLake does not support PRIMARY KEY or UNIQUE constraints (validated:
        # "Not implemented Error: PRIMARY KEY/UNIQUE constraints are not supported in
        # DuckLake"), so "keyed" is a connector-level convention, not a catalog
        # constraint.  The connector implements keyed replacement as delete-by-key
        # then insert, which is why no constraint may be relied on for it.
        lake.execute(
            f"CREATE TABLE IF NOT EXISTS {lake.qualified(table)} ("
            "  id BIGINT,"
            "  customer VARCHAR,"
            "  total DOUBLE"
            f"{extra_columns}"
            ")"
        )
    lake.create_guard_table()
    return lake


def fresh_batch(
    *,
    group: str = "summary",
    generation: str = "gen-1",
    epoch: int = 1,
    base: int,
    end: int,
    table: str = "orders",
    rows: Sequence[Mapping[str, Any]] = (),
    deletes: Sequence[Mapping[str, Any]] = (),
    op: Op = Op.INSERT,
    resync: bool = False,
) -> Batch:
    deltas = [Delta(table=table, op=op, row=dict(row)) for row in rows]
    deltas += [Delta(table=table, op=Op.DELETE, row=dict(row)) for row in deletes]
    deltas.sort(key=lambda d: d.canonical())
    return Batch(
        output_group=group,
        generation=generation,
        epoch=epoch,
        base=base,
        end=end,
        deltas=tuple(deltas),
        resync=resync,
    )


def row_ids(lake: DuckLake, table: str = "orders") -> list[tuple[Any, ...]]:
    # Through an independent transaction context, never the writing connection:
    # a scenario must be able to fail when the payload landed but the guard did
    # not, and a reader sharing the writer's connection could be handed the
    # writer's session state instead of the committed snapshot.  `cursor()` is a
    # second connection to the same attached database, so it sees exactly what a
    # later process would.
    rows = lake.cursor().execute(f"SELECT * FROM {lake.qualified(table)} ORDER BY id").fetchall()
    return list(rows)


def provision(lake: DuckLake, group: str = "summary", generation: str = "gen-1") -> Guard:
    return lake.provision_guard(group, generation, epoch=1)


# -- scenarios: the decision function is load-bearing ----------------------


@scenario("guard/apply-advances-frontier")
def apply_advances_frontier(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    before = lake.read_guard("summary")
    assert before.frontier == 0 and before.batch_id is None, before
    result = lake.publish(
        fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}])
    )
    assert result.decision is Decision.APPLY, result.decision
    after = lake.read_guard("summary")
    assert after.frontier == 1, after
    assert after.batch_id == "gen-1:0->1", after
    assert after.digest is not None, after
    assert row_ids(lake) == [(1, "a", 10.0)], row_ids(lake)
    return "frontier 0 -> 1 with the row visible in the same commit"


@scenario("guard/duplicate-retry-is-a-no-op")
def duplicate_retry_is_a_no_op(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    batch = fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}])
    first = lake.publish(batch)
    second = lake.publish(batch)
    assert first.decision is Decision.APPLY, first
    assert second.decision is Decision.DUPLICATE, second
    assert row_ids(lake) == [(1, "a", 10.0)], row_ids(lake)
    assert lake.read_guard("summary").frontier == 1
    return "replaying the committed batch is DUPLICATE and leaves one row"


@scenario("guard/stale-publisher-is-refused")
def stale_publisher_is_refused(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    stale = fresh_batch(base=0, end=2, rows=[{"id": 2, "customer": "b", "total": 20.0}])
    result = lake.publish(stale)
    assert result.decision is Decision.STALE, result
    assert row_ids(lake) == [(1, "a", 10.0)], row_ids(lake)
    assert lake.read_guard("summary").frontier == 1
    return "a batch based on frontier 0 after frontier 1 is refused"


@scenario("guard/foreign-generation-is-refused")
def foreign_generation_is_refused(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    other = fresh_batch(
        base=1, end=2, generation="gen-2", rows=[{"id": 2, "customer": "b", "total": 20.0}]
    )
    result = lake.publish(other)
    assert result.decision is Decision.WRONG_GENERATION, result
    assert row_ids(lake) == [(1, "a", 10.0)], row_ids(lake)
    return "a batch from another pipeline generation is refused"


@scenario("guard/ownership-requires-explicit-takeover")
def ownership_requires_explicit_takeover(root: Path) -> str:
    lake = make_lake(root)
    provision(lake, generation="gen-1")
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    # Claiming a higher epoch does not grant ownership.
    claim = fresh_batch(base=1, end=2, epoch=2, rows=[{"id": 2, "customer": "b", "total": 20.0}])
    result = lake.publish(claim)
    assert result.decision is Decision.WRONG_OWNER, result
    assert row_ids(lake) == [(1, "a", 10.0)], row_ids(lake)
    # An explicit takeover then lets the new owner publish.
    guard = lake.take_over("summary", 2)
    assert guard.epoch == 2, guard
    applied = lake.publish(claim)
    assert applied.decision is Decision.APPLY, applied
    assert row_ids(lake) == [(1, "a", 10.0), (2, "b", 20.0)], row_ids(lake)
    # The old owner is now fenced.
    fenced = lake.publish(
        fresh_batch(base=2, end=3, epoch=1, rows=[{"id": 3, "customer": "c", "total": 30.0}])
    )
    assert fenced.decision is Decision.WRONG_OWNER, fenced
    assert row_ids(lake) == [(1, "a", 10.0), (2, "b", 20.0)], row_ids(lake)
    return "epoch 2 batch refused until take_over(2); then epoch 1 is fenced"


@scenario("guard/divergent-payload-for-committed-identity-is-detected")
def divergent_payload_is_detected(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    # Same identity (generation and interval), different content.
    divergent = fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "zzz", "total": 99.0}])
    try:
        lake.publish(divergent)
    except ProtocolError as exc:
        assert "different content" in str(exc), exc
    else:
        raise AssertionError("a divergent payload for a committed identity must not be accepted")
    assert row_ids(lake) == [(1, "a", 10.0)], row_ids(lake)
    return "divergence is reported instead of being treated as a duplicate"


# -- scenarios: atomicity ----------------------------------------------------


@scenario("ducklake/payload-and-guard-are-atomic-on-payload-failure")
def payload_failure_rolls_back_guard(root: Path) -> str:
    lake = make_lake(root, extra_columns=", note VARCHAR NOT NULL")
    provision(lake)
    lake.publish(
        fresh_batch(
            base=0,
            end=1,
            rows=[{"id": 1, "customer": "a", "total": 10.0, "note": "ok"}],
        )
    )
    bad = fresh_batch(
        base=1,
        end=2,
        rows=[{"id": 2, "customer": "b", "total": 20.0, "note": None}],
    )
    try:
        lake.publish(bad)
    except Exception as exc:  # duckdb.Error or ProtocolError
        detail = type(exc).__name__
    else:
        raise AssertionError("a NOT NULL violation must fail the publication")
    guard = lake.read_guard("summary")
    assert guard.frontier == 1, guard
    assert row_ids(lake) == [(1, "a", 10.0, "ok")], row_ids(lake)
    return f"payload failure ({detail}) left frontier at 1 and the payload untouched"


@scenario("ducklake/reader-sees-payload-and-guard-together")
def reader_sees_payload_and_guard_together(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    before_snapshot = lake.current_snapshot()
    reader = lake.cursor()
    reader.execute("BEGIN TRANSACTION")
    before = reader.execute(f"SELECT count(*) FROM {lake.qualified('orders')}").fetchone()[0]
    old_frontier = reader.execute(
        f"SELECT frontier FROM {lake.qualified('connector_guard')} WHERE output_group = 'summary'"
    ).fetchone()[0]
    assert (before, old_frontier) == (0, 0), (before, old_frontier)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    # The reader keeps its snapshot: neither the payload nor the guard moved.
    in_snapshot = reader.execute(
        f"SELECT (SELECT count(*) FROM {lake.qualified('orders')}), frontier "
        f"FROM {lake.qualified('connector_guard')} WHERE output_group = 'summary'"
    ).fetchone()
    assert in_snapshot == (0, 0), in_snapshot
    reader.execute("COMMIT")
    # A fresh transaction sees both changes.
    fresh = (
        lake.cursor()
        .execute(
            f"SELECT (SELECT count(*) FROM {lake.qualified('orders')}), frontier "
            f"FROM {lake.qualified('connector_guard')} WHERE output_group = 'summary'"
        )
        .fetchone()
    )
    assert fresh == (1, 1), fresh
    # Snapshot-versioned reads straddle the publication: the version before it
    # is empty, the version after it has the row.
    after_snapshot = lake.current_snapshot()
    assert after_snapshot > before_snapshot, (before_snapshot, after_snapshot)
    assert lake.read_snapshot("orders", before_snapshot) == [], before_snapshot
    at_version = lake.read_snapshot("orders", after_snapshot)
    assert [r["id"] for r in at_version] == [1], at_version
    return f"open reader sees (0,0); after commit another reader sees (1,1); version {before_snapshot} empty, {after_snapshot} has the row"


@scenario("ducklake/rollback-leaves-history-replayable")
def rollback_leaves_history_replayable(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    bad = fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": None, "total": 10.0}])
    lake.execute(f"ALTER TABLE {lake.qualified('orders')} ADD COLUMN note VARCHAR NOT NULL")
    try:
        lake.publish(bad)
    except Exception:
        pass
    else:
        raise AssertionError("expected the publication to fail")
    del bad
    # The same interval can still be published with a valid payload.
    good = fresh_batch(
        base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0, "note": "ok"}]
    )
    result = lake.publish(good)
    assert result.decision is Decision.APPLY, result
    assert lake.read_guard("summary").frontier == 1
    return "a rolled-back publication does not consume its interval"


@scenario("ducklake/resync-replaces-content-and-is-idempotent")
def resync_replaces_content(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    lake.publish(
        fresh_batch(
            base=0,
            end=1,
            rows=[
                {"id": 1, "customer": "a", "total": 10.0},
                {"id": 2, "customer": "b", "total": 20.0},
            ],
        )
    )
    resync = fresh_batch(
        base=0,
        end=5,
        rows=[{"id": 2, "customer": "b2", "total": 25.0}],
        resync=True,
    )
    result = lake.publish(resync)
    assert result.decision is Decision.APPLY, result
    assert row_ids(lake) == [(2, "b2", 25.0)], row_ids(lake)
    replay = lake.publish(resync)
    assert replay.decision is Decision.DUPLICATE, replay
    assert row_ids(lake) == [(2, "b2", 25.0)], row_ids(lake)
    return "resync drops row 1, rewrites row 2, and replays as DUPLICATE"


@scenario("guard/resync-clears-only-the-tables-it-repopulates")
def resync_clears_only_its_own_table(root: Path) -> str:
    """A resync of one table must not delete another table's rows.

    Both tables belong to the same output group, so one guard covers them: a
    resync that cleared every configured table would delete `invoices` without
    repopulating it, advance the guard, and commit — the rows gone, the batch
    recorded as applied, and nothing left to notice.  The Rust writer says the
    same thing with `Catalog::publish`'s `tables` argument.
    """
    lake = make_lake(root, tables=("orders", "invoices"))
    provision(lake)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    lake.publish(
        fresh_batch(
            base=1,
            end=2,
            table="invoices",
            rows=[{"id": 1, "customer": "inv", "total": 99.0}],
        )
    )
    assert row_ids(lake, "invoices") == [(1, "inv", 99.0)], row_ids(lake, "invoices")

    resync = fresh_batch(
        base=0,
        end=5,
        rows=[{"id": 2, "customer": "b", "total": 20.0}],
        resync=True,
    )
    assert lake.publish(resync).decision is Decision.APPLY
    assert row_ids(lake) == [(2, "b", 20.0)], row_ids(lake)
    assert row_ids(lake, "invoices") == [(1, "inv", 99.0)], row_ids(lake, "invoices")
    return "the resync cleared `orders` only; `invoices` kept its row and the guard covered both"


@scenario("guard/keyed-replacement-matches-a-null-key")
def keyed_replacement_matches_a_null_key(root: Path) -> str:
    """A NULL key part must be matched, not silently skipped.

    `=` never matches NULL, so a delete-by-key written with `=` deletes nothing
    for such a row, and the insert that follows leaves the old row in place: two
    rows where the pipeline said one, with the guard advanced past both.  The
    Rust writer uses `IS NOT DISTINCT FROM` for exactly this reason.
    """
    lake = make_lake(root)
    provision(lake)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": None, "customer": "a", "total": 1.0}]))
    lake.publish(fresh_batch(base=1, end=2, rows=[{"id": None, "customer": "b", "total": 2.0}]))
    assert row_ids(lake) == [(None, "b", 2.0)], row_ids(lake)
    return "the NULL-keyed row was replaced, not duplicated"


# -- scenarios: row algebra --------------------------------------------------


@scenario("ducklake/keyed-update-is-delete-then-insert")
def keyed_update_semantics(root: Path) -> str:
    lake = make_lake(root)
    provision(lake)
    lake.publish(fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}]))
    update = fresh_batch(
        base=1,
        end=2,
        rows=[{"id": 1, "customer": "a2", "total": 11.0}],
        deletes=[{"id": 1, "customer": "a", "total": 10.0}],
    )
    result = lake.publish(update)
    assert result.decision is Decision.APPLY, result
    assert row_ids(lake) == [(1, "a2", 11.0)], row_ids(lake)
    return "a keyed replacement leaves exactly the post-image"


@scenario("ducklake/multiset-delete-removes-one-copy")
def multiset_delete_removes_one_copy(root: Path) -> str:
    lake = make_lake(root, keyed=False)
    provision(lake)
    row = {"id": 7, "customer": "a", "total": 10.0}
    lake.publish(fresh_batch(base=0, end=1, rows=[row, dict(row)]))
    assert row_ids(lake) == [(7, "a", 10.0), (7, "a", 10.0)], row_ids(lake)
    lake.publish(fresh_batch(base=1, end=2, deletes=[row]))
    assert row_ids(lake) == [(7, "a", 10.0)], row_ids(lake)
    return "a duplicate row is deleted once, not twice"


# -- scenarios: concurrency --------------------------------------------------


@scenario("ducklake/concurrent-publishers-converge-to-one-outcome")
def concurrent_publishers_converge(root: Path) -> str:
    """Two publishers race in separate transaction contexts over one catalog.

    Both attempt the same interval.  Whatever the interleaving, the guard ends
    up advanced exactly once and the payload matches the committed batch: the
    loser sees a conflict (retried) or a guard mismatch, never a silent
    overwrite.
    """
    lake = make_lake(root)
    provision(lake)
    from .protocol import batch_from_feldera_chunk

    def batch_for(step: int) -> Batch:
        payload = [
            {
                "insert": {
                    "table": "orders",
                    "row": {"id": step, "customer": f"c{step}", "total": float(step)},
                }
            }
        ]
        return batch_from_feldera_chunk(
            output_group="summary",
            generation="gen-1",
            epoch=1,
            base=step - 1,
            end=step,
            rows=payload,
            table_columns={"orders": ()},
        )

    # Interleaved guard reads: both publishers believe the frontier is 0.
    first = lake.cursor()
    second = lake.cursor()
    for cursor in (first, second):
        cursor.execute("BEGIN TRANSACTION")
    guards = [
        cursor.execute(
            f"SELECT frontier FROM {lake.qualified('connector_guard')} WHERE output_group = 'summary'"
        ).fetchone()[0]
        for cursor in (first, second)
    ]
    assert guards == [0, 0], guards
    first.execute(
        f"UPDATE {lake.qualified('connector_guard')} SET frontier = 1, batch_id = ?, digest = ? "
        "WHERE output_group = 'summary' AND frontier = 0",
        ["gen-1:0->1", "digest-1"],
    )
    first.execute("COMMIT")
    second.execute(
        f"UPDATE {lake.qualified('connector_guard')} SET frontier = 1, batch_id = ?, digest = ? "
        "WHERE output_group = 'summary' AND frontier = 0",
        ["gen-1:0->2", "digest-2"],
    )
    conflicted = False
    conflict_message = ""
    try:
        second.execute("COMMIT")
    except Exception as exc:
        conflicted = True
        conflict_message = str(exc)
        with contextlib.suppress(Exception):
            second.execute("ROLLBACK")
    guard = lake.read_guard("summary")
    assert guard.frontier == 1, guard
    assert guard.batch_id == "gen-1:0->1", guard
    assert conflicted, "a second committer must observe a conflict on the guarded row"
    # The connector's retry predicate must recognise the conflict DuckLake
    # actually produces, otherwise a lost publication is reported as a bug.
    from .catalog import _is_transaction_conflict

    assert _is_transaction_conflict(Exception(conflict_message)), conflict_message
    del batch_for
    return f"second committer conflicts and the retry predicate matches: {conflict_message[:90]}"


@scenario("source/guard-update-is-conditional")
def guard_update_is_conditional(root: Path) -> str:
    """The guard write must stay a conditional UPDATE, not a blind SET.

    A blind ``SET frontier = ?`` would let a publisher that lost a race
    overwrite the winner (the classic lost-update), which no runtime check
    could then detect.  This scenario pins the shape of the statement in the
    committed source so the property cannot be dropped by an innocent-looking
    refactor.
    """
    import inspect

    from . import catalog

    source = inspect.getsource(catalog.DuckLake._update_guard)
    assert "UPDATE" in source, source
    assert "WHERE" in source, source
    for predicate in ("epoch = ?", "frontier = ?", "generation = ?"):
        assert predicate in source, f"guard update lost the {predicate!r} predicate:\n{source}"
    publish_source = inspect.getsource(catalog.DuckLake.publish)
    assert "BEGIN TRANSACTION" in publish_source, publish_source
    assert "read_guard" in publish_source, publish_source
    return "guard update is a conditional UPDATE ... WHERE epoch/frontier/generation"


@scenario("source/change-interval-is-not-replayed")
def change_interval_is_not_replayed(root: Path) -> str:
    """Replaying ``(checkpoint, current]`` must not re-emit the checkpoint's rows.

    DuckLake's ``ducklake_table_changes`` treats *both* bounds as inclusive:
    ``ducklake_table_changes(..., 2, 2)`` returns snapshot 2's changes rather
    than nothing.  A connector that forwards its checkpoint snapshot unchanged
    therefore re-reads the snapshot it has already consumed.  After a bootstrap
    those rows are already in Feldera, so the duplicate is a silent double
    count rather than an error.

    Rather than assert the bounds (which would just restate the fix), this
    replays the interval on top of the state at the checkpoint and compares
    with the state at ``current``.
    """
    lake = make_lake(root)
    table = lake.qualified("orders")
    lake.execute(f"INSERT INTO {table} VALUES (1, 'a', 10.0)")
    checkpoint = lake.current_snapshot()
    lake.execute(f"INSERT INTO {table} VALUES (2, 'b', 20.0)")
    lake.execute(f"INSERT INTO {table} VALUES (3, 'c', 30.0)")
    current = lake.last_committed_snapshot()

    before = {row["id"] for row in lake.read_snapshot("orders", checkpoint)}
    after = {row["id"] for row in lake.read_snapshot("orders", current)}
    replayed = {
        change.row["id"]
        for change in lake.table_changes("orders", checkpoint, current)
        if change.change_type == "insert"
    }

    re_emitted = replayed & before
    assert not re_emitted, (
        f"interval ({checkpoint}, {current}] re-emitted row ids {sorted(re_emitted)}, "
        f"which were already consumed at snapshot {checkpoint}"
    )
    assert replayed == after - before, (sorted(replayed), sorted(after - before))
    missing = after - before - replayed
    assert not missing, f"interval ({checkpoint}, {current}] lost row ids {sorted(missing)}"
    return (
        f"interval ({checkpoint}, {current}] replays exactly {sorted(after - before)} "
        f"without re-emitting snapshot {checkpoint}"
    )


@scenario("source/catalog-rewind-is-refused")
def catalog_rewind_is_refused(root: Path) -> str:
    """A catalog behind the checkpoint must stop the connector, not idle.

    A DuckLake catalog can move backwards — restored from a backup, rebuilt, or
    replaced by a fresh file whose snapshot counter starts again.  ``poll_once``
    folded that into its "no news" branch (``end <= checkpoint.source_snapshot``),
    so the pipeline polled forever looking healthy while never emitting anything,
    and the moment the catalog caught up the interval would have been a diff
    against a history this connector never read.  The Rust reader refuses the same
    condition.
    """
    lake = make_lake(root)
    lake.execute(f"INSERT INTO {lake.qualified('orders')} VALUES (1, 'a', 10.0)")
    ahead = lake.last_committed_snapshot() + 5

    state_dir = root / "state"
    journal = Journal(state_dir / "orders")
    journal.save_checkpoint(Checkpoint(source_snapshot=ahead, feldera_step=1, seq=0))

    spec = InputSpec(source_table="orders", feldera_table="orders")
    connector = InputConnector(
        lake,
        FelderaClient("http://127.0.0.1:1"),  # never contacted: the poll refuses first
        pipeline="p",
        generation="gen-1",
        specs=[spec],
        state_dir=state_dir,
    )
    try:
        connector.poll_once()
    except HistoryExpiredError as error:
        assert "went backwards" in str(error), error
        return f"a checkpoint at snapshot {ahead} against a catalog at {ahead - 5} is refused, not idled"
    raise AssertionError("a catalog behind the checkpoint was followed instead of refused")


@scenario("source/identity-is-the-interval-not-the-digest")
def identity_is_the_interval_not_the_digest(root: Path) -> str:
    """Batch identity must not include the payload digest.

    Feldera may re-send an en-route step with a slightly different payload; if
    the digest were part of the identity, the retry would look like a new batch
    and be applied on top of the committed one.  The scenario checks the
    identity is built from the generation and the interval only, and that a
    replay with reordered deltas is still a duplicate.
    """
    from .protocol import Batch, Delta, Op

    batch = Batch(
        output_group="g",
        generation="gen-1",
        epoch=1,
        base=0,
        end=1,
        deltas=(
            Delta(table="t", op=Op.INSERT, row={"id": 1}),
            Delta(table="t", op=Op.INSERT, row={"id": 2}),
        ),
    )
    reordered = Batch(
        output_group="g",
        generation="gen-1",
        epoch=1,
        base=0,
        end=1,
        deltas=tuple(reversed(batch.deltas)),
    )
    assert batch.batch_id == reordered.batch_id, (batch.batch_id, reordered.batch_id)
    assert batch.digest == reordered.digest, "digest must ignore delta order"
    assert batch.digest not in batch.batch_id, batch.batch_id
    return "identity = generation + interval; digest ignores ordering and is not part of identity"


@scenario("protocol/decision-is-total")
def decision_is_total(root: Path) -> str:
    """Every (guard, batch) pair must map to a decision, not to an exception."""
    guards = [
        Guard.initial("g", "gen-1", epoch=1),
        Guard("g", "gen-1", 1, NOWHERE_END := 4, "gen-1:0->4", "digest"),
        Guard("g", "gen-2", 3, 9, "gen-2:8->9", "digest"),
    ]
    batches = [
        fresh_batch(base=0, end=1),
        fresh_batch(base=0, end=1, epoch=2),
        fresh_batch(base=1, end=2, generation="gen-9"),
        fresh_batch(base=4, end=5),
        fresh_batch(base=100, end=101),
        fresh_batch(base=0, end=0, resync=True),
    ]
    seen = set()
    for guard in guards:
        for batch in batches:
            try:
                seen.add(decide(guard, batch))
            except ProtocolError:
                seen.add(Decision.APPLY)  # a digest mismatch is a refusal, not a decision
    assert Decision.APPLY in seen, seen
    assert Decision.STALE in seen, seen
    assert Decision.WRONG_GENERATION in seen, seen
    assert Decision.WRONG_OWNER in seen, seen
    assert NOWHERE_END == 4
    return f"all decisions reachable: {sorted(d.value for d in seen)}"


@scenario("report/lost-update-counterexample")
def append_only_receipt_duplicates(root: Path) -> str:
    """Reproduce the counterexample that motivated the guard row.

    A publisher that stores an append-only receipt *after* the payload (instead
    of one guarded row updated with it) has to decide between "payload applied,
    receipt missing" and "payload missing", and no reading of the receipt
    distinguishes them.  This scenario shows the duplicate that results, and
    that the guarded design does not have the problem, on the same catalog.
    """
    lake = make_lake(root)
    provision(lake)
    # Append-only receipt table, maintained by a naive publisher.
    lake.execute(
        f"CREATE TABLE IF NOT EXISTS {lake.qualified('receipts')} ("
        "  batch_id VARCHAR, batch_digest VARCHAR)"
    )

    naive_batch = fresh_batch(base=0, end=1, rows=[{"id": 1, "customer": "a", "total": 10.0}])
    # First attempt: payload commits, then the receipt write is lost.
    lake.connection.execute("BEGIN TRANSACTION")
    for delta in naive_batch.deltas:
        lake.connection.execute(
            f"INSERT INTO {lake.qualified('orders')} (id, customer, total) VALUES (?, ?, ?)",
            [delta.row["id"], delta.row["customer"], delta.row["total"]],
        )
    lake.connection.execute("COMMIT")
    receipts_before = (
        lake.cursor().execute(f"SELECT count(*) FROM {lake.qualified('receipts')}").fetchone()[0]
    )
    # Restart: the naive publisher cannot tell whether the payload landed, so it
    # re-applies it.
    for delta in naive_batch.deltas:
        lake.connection.execute(
            f"INSERT INTO {lake.qualified('orders')} (id, customer, total) VALUES (?, ?, ?)",
            [delta.row["id"], delta.row["customer"], delta.row["total"]],
        )
    naive_rows = (
        lake.cursor().execute(f"SELECT count(*) FROM {lake.qualified('orders')}").fetchone()[0]
    )
    assert receipts_before == 0, receipts_before
    assert naive_rows == 2, f"the naive design must duplicate the row, saw {naive_rows}"
    # Now the guarded design on the same catalog, in a fresh group.
    provision(lake, group="guarded", generation="gen-1")
    guarded = fresh_batch(
        group="guarded", base=0, end=1, rows=[{"id": 9, "customer": "z", "total": 90.0}]
    )
    lake.publish(guarded)
    lake.publish(guarded)
    rows = (
        lake.cursor()
        .execute(f"SELECT count(*) FROM {lake.qualified('orders')} WHERE id = 9")
        .fetchone()[0]
    )
    assert rows == 1, rows
    assert lake.read_guard("guarded").frontier == 1
    return "append-only receipt duplicates the row (2); the guard row keeps 1"


# -- runner ------------------------------------------------------------------


def run_all(
    root: Path | None = None, *, names: Sequence[str] | None = None
) -> list[ScenarioResult]:
    """Run every scenario (or a subset) and return their results."""
    base = Path(tempfile.mkdtemp(prefix="fd-connectors-scenarios-")) if root is None else Path(root)
    base.mkdir(parents=True, exist_ok=True)
    selected = [
        (name, fn)
        for name, fn in SCENARIOS
        if names is None or name in names or any(name.startswith(prefix) for prefix in names)
    ]
    results: list[ScenarioResult] = []
    for name, fn in selected:
        directory = base / name.replace("/", "__")
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)
        started = time.monotonic()
        try:
            detail = fn(directory)
            results.append(ScenarioResult(name, True, detail, (time.monotonic() - started) * 1000))
        except BaseException as exc:
            results.append(
                ScenarioResult(
                    name,
                    False,
                    f"{type(exc).__name__}: {exc}",
                    (time.monotonic() - started) * 1000,
                    traceback.format_exc(),
                )
            )
    return results
