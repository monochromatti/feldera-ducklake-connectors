"""The publication protocol: batch identity, delta algebra, guard rows.

The protocol is the bridge between two systems with different transactional
abilities:

* Feldera computes step by step and *cannot roll back*.  Its connectors can
  observe a step's outputs several times, so publication must be idempotent.
* DuckLake can commit a set of changes atomically.  We exploit exactly that:
  the guard row and the payload tables are updated in **one** transaction.

Definitions used throughout:

``frontier``
    The batch id that the group has committed.  ``0`` means "nothing yet".

``generation``
    An identifier for the Feldera pipeline and SQL program.  A new generation
    makes all older batches non-comparable: any live-epoch batch from another
    generation is refused, because applying it would mix two runs' results.
    Unlike the ownership epoch, a generation is *not* an integer that can be
    bumped by a competitor, so generation conflicts are a hard error.

``epoch``
    Ownership of the group.  A new owner takes over by *increasing* it; an
    ``ADOPT`` from a stale owner has a smaller epoch and is refused, which is
    the fencing mechanism.

``resync``
    A batch that replaces the whole group content: it carries a full snapshot
    and sets ``frontier`` to ``0`` so that a replay of the agreed history or
    the same process's own retry is still recognised as duplicate.

The decision function is deliberately total: every (guard, batch) pair maps to
one of APPLY / DUPLICATE / STALE / WRONG_GENERATION / WRONG_OWNER, and the two
order properties asserted in ``tests/test_protocol_properties.py`` are stated
in over ``decide`` rather than derived from an executable model.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

NOWHERE = 0
"""Frontier value of a group that has not published anything."""


class ProtocolError(Exception):
    """A batch or a guard violates the protocol."""


class Op(StrEnum):
    """Feldera's insert/delete update format."""

    INSERT = "insert"
    DELETE = "delete"

    def to_json(self) -> dict[str, Any]:
        return {self.value: None}


class Decision(StrEnum):
    """What a publisher should do with a batch, given a guard."""

    APPLY = "apply"
    DUPLICATE = "duplicate"
    STALE = "stale"
    WRONG_GENERATION = "wrong_generation"
    WRONG_OWNER = "wrong_owner"

    def conflicts(self) -> bool:
        return self in (Decision.STALE, Decision.WRONG_GENERATION, Decision.WRONG_OWNER)


def change_weight(change_type: str) -> int:
    """Feldera weight of a DuckLake change-feed event type."""
    if change_type in ("insert", "update_postimage"):
        return 1
    if change_type in ("delete", "update_preimage"):
        return -1
    raise ProtocolError(f"unknown DuckLake change type {change_type!r}")


@dataclass(frozen=True)
class Delta:
    """One row event, in Feldera's insert/delete representation."""

    table: str
    op: Op
    row: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {"table": self.table, "op": self.op.value, "row": dict(self.row)}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Delta:
        return cls(
            table=str(obj["table"]),
            op=Op(str(obj["op"])),
            row=dict(obj["row"]),
        )

    def to_feldera(self) -> dict[str, Any]:
        """The payload Feldera's JSON ingress expects for this row event."""
        return {self.op.value: dict(self.row)}

    def canonical(self) -> str:
        """Stable rendering used for deterministic ordering and digests."""
        return json.dumps(
            {"table": self.table, "op": self.op.value, "row": dict(self.row)},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )


def delta_from_json(obj: Mapping[str, Any]) -> Delta:
    return Delta.from_json(obj)


@dataclass(frozen=True)
class Batch:
    """A unit of publication output, tagged with its identity.

    A batch is the publisher's *claim*: "when ``frontier`` is ``base``, the
    group's new state is the old state plus these deltas, and the group's
    frontier becomes ``end``".
    """

    output_group: str
    generation: str
    epoch: int
    base: int
    end: int
    deltas: tuple[Delta, ...] = ()
    resync: bool = False
    declared_digest: str | None = None
    """Digest of the batch, as it travelled over the wire."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "deltas", tuple(self.deltas))

    @property
    def batch_id(self) -> str:
        """Identity of the batch: everything except who submits it and when."""
        return f"{self.generation}:{self.base}->{self.end}"

    @property
    def digest(self) -> str:
        return payload_digest(self)

    def canonical(self) -> str:
        return json.dumps(
            {
                "output_group": self.output_group,
                "generation": self.generation,
                "epoch": self.epoch,
                "base": self.base,
                "end": self.end,
                "resync": self.resync,
                "deltas": sorted(d.canonical() for d in self.deltas),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def with_epoch(self, epoch: int) -> Batch:
        return replace(self, epoch=epoch)


def payload_digest(
    batch: Batch, *, include_identity: bool = False, include_tables: bool = True
) -> str:
    """Digest over a batch payload.

    Deltas are sorted before hashing so that two batches with the same content
    in a different order produce the same digest: Feldera's output chunk
    order is not meaningful, and treating it as meaningful would make replay
    look like a payload change.
    """
    parts: list[str] = []
    for delta in sorted(batch.deltas, key=lambda d: d.canonical()):
        row = dict(delta.row)
        rendered = json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        prefix = f"{delta.table}:{delta.op.value}:" if include_tables else f"{delta.op.value}:"
        parts.append(prefix + rendered)
    if include_identity:
        parts.append(f"{batch.output_group}|{batch.generation}|{batch.base}|{batch.end}")
    if batch.resync:
        parts.append("resync")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


# The published DuckLake primitive this models is a single guarded row per
# output group, updated together with the payload tables.  These are the only
# guard columns the model can observe.
GUARD_COLUMNS = ("output_group", "generation", "epoch", "frontier", "batch_id", "digest")


@dataclass(frozen=True)
class Guard:
    """The protected singleton row of an output group."""

    output_group: str
    generation: str
    epoch: int
    frontier: int
    batch_id: str | None = None
    digest: str | None = None
    """Payload digest of the batch that set ``frontier``.

    It is *not* part of the batch identity (identity is ``generation`` and the
    interval), because Feldera may re-send an en-route step with different
    content and a digest in the identity would then refuse an idempotent
    retry.  It is used as a divergence detector: if a batch claims the same
    identity as the committed one but carries different content, that is a real
    anomaly and the connector stops instead of guessing.
    """

    @classmethod
    def initial(cls, output_group: str, generation: str, *, epoch: int = 1) -> Guard:
        return cls(
            output_group=output_group,
            generation=generation,
            epoch=epoch,
            frontier=NOWHERE,
            batch_id=None,
            digest=None,
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "output_group": self.output_group,
            "generation": self.generation,
            "epoch": self.epoch,
            "frontier": self.frontier,
            "batch_id": self.batch_id,
            "digest": self.digest,
        }

    @classmethod
    def from_row(cls, row: Sequence[Any]) -> Guard:
        if len(row) != len(GUARD_COLUMNS):
            raise ProtocolError(f"guard row has {len(row)} columns, expected {len(GUARD_COLUMNS)}")
        return cls(
            output_group=str(row[0]),
            generation=str(row[1]),
            epoch=int(row[2]),
            frontier=int(row[3]),
            batch_id=None if row[4] is None else str(row[4]),
            digest=None if row[5] is None else str(row[5]),
        )


def guard_from_row(row: Sequence[Any]) -> Guard:
    """Adapter used by the DuckLake layer, which selects the columns in order."""
    return Guard.from_row(row)


def advance(guard: Guard, batch: Batch) -> Guard:
    """The guard state after applying ``batch``."""
    return replace(guard, frontier=batch.end, batch_id=batch.batch_id, digest=batch.digest)


def decide(guard: Guard, batch: Batch) -> Decision:
    """The protocol's total decision function.

    Order of checks matters.  Identity is checked before ``base``: a group that
    has moved on will see a batch whose ``base`` is older than the frontier,
    and that batch is *stale* only if it did not already commit; "already
    committed" is decided by the batch identity, which is the same on a retry.

    Ownership is all-or-nothing: epochs must match exactly, so taking over is
    an explicit, committed act (:func:`acquire_ownership`) rather than a side
    effect of a batch claiming a higher number.
    """
    if batch.generation != guard.generation:
        return Decision.WRONG_GENERATION
    if batch.epoch != guard.epoch:
        return Decision.WRONG_OWNER
    if guard.frontier == batch.end and guard.batch_id == batch.batch_id:
        if guard.digest is not None and batch.digest != guard.digest:
            raise ProtocolError(
                f"batch {batch.batch_id} has the same identity as the committed batch of "
                f"{guard.output_group!r} but different content "
                f"(committed {guard.digest[:12]}, received {batch.digest[:12]}); refusing to "
                "treat a divergence as a duplicate"
            )
        return Decision.DUPLICATE
    if batch.base == batch.end or batch.resync:
        if batch.declared_digest is not None and batch.declared_digest != batch.digest:
            raise ProtocolError(
                f"batch {batch.batch_id} carries a digest that does not match its payload"
            )
        return Decision.APPLY
    if guard.frontier != batch.base:
        return Decision.STALE
    return Decision.APPLY


def acquire_ownership(guard: Guard, epoch: int) -> Guard:
    """Take ownership of an output group.

    ``epoch`` must strictly exceed the current epoch.  Bumping is the only
    operation that changes ownership, and it never lowers the epoch, so a
    publisher that has not observed the bump yet is fenced by the conditional
    guard update rather than by a lock.
    """
    if epoch <= guard.epoch:
        raise ProtocolError(
            f"epoch {epoch} does not acquire ownership of {guard.output_group!r} "
            f"(current epoch {guard.epoch})"
        )
    return replace(guard, epoch=epoch)


def validate(batch: Batch, *, required_tables: Sequence[str] | None = None) -> None:
    """Reject batches that are meaningless before they reach the catalog."""
    if not batch.output_group:
        raise ProtocolError("batch has no output group")
    if not batch.generation:
        raise ProtocolError("batch has no generation")
    if batch.epoch < 1:
        raise ProtocolError(f"batch epoch {batch.epoch} is not a positive integer")
    if batch.base < 0 or batch.end < 0:
        raise ProtocolError("batch bounds must be non-negative")
    if batch.end < batch.base:
        raise ProtocolError(f"batch {batch.batch_id} has end before base")
    for delta in batch.deltas:
        if not delta.table:
            raise ProtocolError("delta has no table")
        if not delta.row:
            raise ProtocolError(f"delta on {delta.table!r} has an empty row")
    if batch.resync and batch.base == batch.end:
        raise ProtocolError(f"resync batch {batch.batch_id} makes no progress")
    if required_tables:
        unknown = sorted({d.table for d in batch.deltas} - set(required_tables))
        if unknown:
            raise ProtocolError(f"batch writes unknown tables {unknown}")


def batch_from_feldera_chunk(
    *,
    output_group: str,
    generation: str,
    epoch: int,
    base: int,
    end: int,
    rows: Iterable[Mapping[str, Any]],
    table_columns: Mapping[str, Sequence[str]],
    resync: bool = False,
    declared_digest: str | None = None,
) -> Batch:
    """Turn one Feldera output chunk into a batch.

    ``rows`` are the objects of Feldera's ``insert``/``delete`` arrays.  Array
    order is not meaningful, so the batch is canonicalised here rather than at
    comparison time.
    """
    default_table = next(iter(table_columns)) if len(table_columns) == 1 else None
    deltas: list[Delta] = []
    for op_name in ("delete", "insert"):
        for entry in _collect(rows, op_name):
            table, row = _split_row(entry, default_table)
            deltas.append(Delta(table=table, op=Op(op_name), row=row))
    deltas.sort(key=lambda d: d.canonical())
    return Batch(
        output_group=output_group,
        generation=generation,
        epoch=epoch,
        base=base,
        end=end,
        deltas=tuple(deltas),
        resync=resync,
        declared_digest=declared_digest,
    )


def _collect(rows: Iterable[Mapping[str, Any]], op_name: str) -> Iterable[Mapping[str, Any]]:
    """Yield the row of every update object that carries ``op_name``.

    Feldera sends a step's updates as a JSON array of one-key objects::

        [{"insert": {...}}, {"delete": {...}}]

    so the op key is unwrapped here and the caller receives the row itself.
    """
    for row in rows:
        if op_name in row:
            value = row[op_name]
            if isinstance(value, list):
                yield from value
            else:
                yield value


def _split_row(payload: Mapping[str, Any], default_table: str | None) -> tuple[str, dict[str, Any]]:
    """Split one update payload into a (table, row) pair.

    The op key has already been stripped, so this receives the row.  A bridge
    that multiplexes several views into one stream wraps it further as
    ``{"table": ..., "row": {...}}``; that wrapper is recognised only when it
    is exactly those two keys, so a row that happens to own a ``table`` column
    is not mistaken for one.
    """
    if not isinstance(payload, Mapping):
        raise ProtocolError(f"update payload is not an object: {payload!r}")
    if set(payload) == {"table", "row"} and isinstance(payload["row"], Mapping):
        return str(payload["table"]), dict(payload["row"])
    if default_table is None:
        raise ProtocolError(
            "update payload is a bare row but several output tables are in scope; "
            "emit {'table': ..., 'row': {...}} instead"
        )
    return default_table, dict(payload)


def rows_to_batch(
    *,
    output_group: str,
    generation: str,
    epoch: int,
    table: str,
    previous_frontier: int,
    current: Sequence[Mapping[str, Any]],
    previous: Sequence[Mapping[str, Any]],
    key_columns: Sequence[str] | None = None,
    resync: bool = False,
    end: int | None = None,
) -> Batch:
    """Diff two materialised results into a batch.

    Used on the publisher side when the connector is given the full result of
    a step rather than Feldera's change stream.  A row that is present in both
    is emitted as no delta at all, so re-publishing an unchanged step is a
    no-op payload; that is what makes "same result, same batch id" hold.
    """
    if key_columns:
        keys = tuple(key_columns)
        old = {tuple(_key(r, keys)): r for r in previous}
        new = {tuple(_key(r, keys)): r for r in current}
        deltas: list[Delta] = []
        for key in sorted(old, key=repr):
            if key not in new:
                deltas.append(Delta(table=table, op=Op.DELETE, row=old[key]))
        for key in sorted(new, key=repr):
            if key not in old:
                deltas.append(Delta(table=table, op=Op.INSERT, row=new[key]))
            elif dict(old[key]) != dict(new[key]):
                deltas.append(Delta(table=table, op=Op.DELETE, row=old[key]))
                deltas.append(Delta(table=table, op=Op.INSERT, row=new[key]))
    else:
        old_rows = [dict(r) for r in previous]
        new_rows = [dict(r) for r in current]
        remaining = list(new_rows)
        deletes: list[Delta] = []
        for row in old_rows:
            if row in remaining:
                remaining.remove(row)
            else:
                deletes.append(Delta(table=table, op=Op.DELETE, row=row))
        inserts = [Delta(table=table, op=Op.INSERT, row=row) for row in remaining]
        deltas = deletes + inserts
    deltas.sort(key=lambda d: d.canonical())
    return Batch(
        output_group=output_group,
        generation=generation,
        epoch=epoch,
        base=previous_frontier,
        end=previous_frontier + 1 if end is None else end,
        deltas=tuple(deltas),
        resync=resync,
    )


def _key(row: Mapping[str, Any], keys: Sequence[str]) -> list[Any]:
    return [row.get(k) for k in keys]
