"""DuckLake access layer: snapshots, change feed, and guarded publication.

One :class:`DuckLake` instance owns one DuckDB connection, which is the unit of
concurrency DuckLake gives us: cursors created from the same connection behave
as independent connections over the same catalog, so transaction conflicts are
observable in-process.  For conflicts *between processes* the catalog must be
PostgreSQL-backed (a DuckDB-file catalog is single-writer).
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import duckdb

from .protocol import (
    Batch,
    Decision,
    Delta,
    Guard,
    Op,
    ProtocolError,
    acquire_ownership,
    advance,
    decide,
    guard_from_row,
    validate,
)

DEFAULT_GUARD_TABLE = "connector_guard"


class GuardInvariantError(ProtocolError):
    """The protected singleton invariant of an output group does not hold."""


class HistoryExpiredError(ProtocolError):
    """The change feed can no longer replay the interval we need."""


def quote_ident(ident: str) -> str:
    """Quote an identifier, refusing anything that is not a plain name."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", ident):
        raise ProtocolError(f"unsafe SQL identifier {ident!r}")
    return f'"{ident}"'


def _is_transaction_conflict(exc: Exception) -> bool:
    """True when a DuckDB error is a write-write conflict we may retry."""
    message = str(exc).lower()
    return any(
        needle in message
        for needle in (
            "conflict",
            "transactioncontext",
            "failed to commit",
            "optimistic",
            "write-write",
        )
    )


@dataclass(frozen=True)
class TableChange:
    """One row event from ``ducklake_table_changes``."""

    snapshot_id: int
    rowid: int
    change_type: str
    row: dict[str, Any]

    @property
    def weight(self) -> int:
        from .protocol import change_weight

        return change_weight(self.change_type)

    def delta(self, table: str) -> Delta:
        return Delta(table=table, op=Op.INSERT if self.weight > 0 else Op.DELETE, row=self.row)


@dataclass(frozen=True)
class PublicationResult:
    """Outcome of one publication attempt (possibly after retries)."""

    decision: Decision
    guard: Guard
    attempts: int = 1
    conflict: str | None = None

    @property
    def applied(self) -> bool:
        return self.decision is Decision.APPLY


@dataclass
class DuckLake:
    """A DuckLake catalog plus the publication protocol over it."""

    catalog: str
    """ATTACH target, e.g. ``ducklake:/path/meta.ducklake`` or
    ``ducklake:postgres:dbname=ducklake host=127.0.0.1``."""

    tables: Sequence[str] = ()
    """Output tables owned by this connector.  Required for publication."""

    schema: str = "main"
    alias: str = "dl"
    guard_table: str = DEFAULT_GUARD_TABLE
    key_columns: Mapping[str, Sequence[str]] = field(default_factory=dict)
    """Per-table primary key columns.  Configured tables get keyed replacement
    (delete-by-key then insert); other tables get multiplicity-preserving row
    replacement (delete one matching row, then insert)."""

    data_path: str | None = None
    extension_directory: str | None = None
    inlining_row_limit: int | None = None
    _connection: duckdb.DuckDBPyConnection | None = None
    _extension_version: str | None = None

    def __post_init__(self) -> None:
        self.tables = tuple(self.tables)
        self.key_columns = {k: list(v) for k, v in self.key_columns.items()}
        for table in self.tables:
            quote_ident(table)
        quote_ident(self.guard_table)
        self._connection = duckdb.connect()
        self._attach()

    # -- connection --------------------------------------------------------

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        assert self._connection is not None
        return self._connection

    @property
    def extension_version(self) -> str | None:
        """Build id of the loaded ducklake extension."""
        rows = self.connection.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
        ).fetchall()
        return rows[0][0] if rows else None

    def _attach(self) -> None:
        conn = self.connection
        if self.extension_directory:
            conn.execute("SET extension_directory = ?", [self.extension_directory])
        try:
            conn.execute("LOAD ducklake")
        except duckdb.Error:
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
        options = []
        if self.data_path:
            options.append(f"DATA_PATH '{self.data_path}'")
        if self.inlining_row_limit is not None:
            options.append(f"DATA_INLINING_ROW_LIMIT {int(self.inlining_row_limit)}")
        suffix = f" ({', '.join(options)})" if options else ""
        conn.execute(f"ATTACH '{self.catalog}' AS {quote_ident(self.alias)}{suffix}")

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """An independent cursor, i.e. a separate transaction context."""
        return self.connection.cursor()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> DuckLake:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def qualified(self, table: str) -> str:
        return f"{quote_ident(self.alias)}.{quote_ident(self.schema)}.{quote_ident(table)}"

    # -- snapshots and the change feed -------------------------------------

    def current_snapshot(self, cur: duckdb.DuckDBPyConnection | None = None) -> int:
        cur = cur or self.connection
        return int(
            cur.execute(f"SELECT * FROM ducklake_current_snapshot('{self.alias}')").fetchone()[0]
        )

    def last_committed_snapshot(self) -> int:
        return int(
            self.connection.execute(
                f"SELECT * FROM ducklake_last_committed_snapshot('{self.alias}')"
            ).fetchone()[0]
        )

    def snapshots(self, cur: duckdb.DuckDBPyConnection | None = None) -> list[tuple[int, Any, Any]]:
        cur = cur or self.connection
        return [
            (int(r[0]), r[1], r[2])
            for r in cur.execute(
                f"SELECT snapshot_id, snapshot_time, changes FROM ducklake_snapshots('{self.alias}') "
                "ORDER BY snapshot_id"
            ).fetchall()
        ]

    def table_changes(
        self,
        table: str,
        start_snapshot: int,
        end_snapshot: int,
        *,
        schema: str | None = None,
        cur: duckdb.DuckDBPyConnection | None = None,
    ) -> list[TableChange]:
        """Row events in the half-open interval ``(start_snapshot, end_snapshot]``.

        The start bound is exclusive and the end bound is inclusive, so
        ``(S, current]`` is exactly the interval that has not been consumed yet
        after a checkpoint at ``S``.

        DuckLake's ``ducklake_table_changes`` is *inclusive on both* bounds:
        ``ducklake_table_changes(..., 2, 2)`` returns snapshot 2's changes, not
        an empty set.  The start is therefore advanced by one here.  Forwarding
        a checkpoint unchanged would re-read the snapshot it was taken at and
        push its rows into Feldera a second time, which is a silent double count
        rather than an error.
        """
        if end_snapshot < start_snapshot:
            raise ProtocolError(
                f"end snapshot {end_snapshot} precedes start snapshot {start_snapshot}"
            )
        if end_snapshot == start_snapshot:
            return []
        cur = cur or self.connection
        schema_name = schema or self.schema
        rows = cur.execute(
            f"SELECT * FROM ducklake_table_changes('{self.alias}', ?, ?, ?, ?)",
            [schema_name, table, int(start_snapshot) + 1, int(end_snapshot)],
        ).fetchall()
        columns = [d[0] for d in cur.description]
        payload_columns = columns[3:]
        return [
            TableChange(
                snapshot_id=int(row[0]),
                rowid=int(row[1]),
                change_type=str(row[2]),
                row=dict(zip(payload_columns, row[3:], strict=True)),
            )
            for row in rows
        ]

    def read_snapshot(
        self,
        table: str,
        version: int,
        *,
        cur: duckdb.DuckDBPyConnection | None = None,
    ) -> list[dict[str, Any]]:
        """The full contents of ``table`` at a fixed DuckLake snapshot."""
        cur = cur or self.connection
        version = int(version)
        result = cur.execute(f"SELECT * FROM {self.qualified(table)} AT (VERSION => {version})")
        columns = [d[0] for d in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def table_columns(self, table: str) -> list[str]:
        result = self.connection.execute(f"SELECT * FROM {self.qualified(table)} LIMIT 0")
        return [d[0] for d in result.description]

    def table_exists(self, table: str) -> bool:
        rows = self.connection.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? "
            "AND table_name = ?",
            [self.alias, self.schema, table],
        ).fetchall()
        return bool(rows)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self.connection.execute(sql, params or [])

    # -- guard rows --------------------------------------------------------

    def read_guard(
        self, output_group: str, *, cur: duckdb.DuckDBPyConnection | None = None
    ) -> Guard:
        """Read the protected singleton row, enforcing the singleton invariant."""
        cur = cur or self.connection
        rows = cur.execute(
            f"SELECT output_group, generation, epoch, frontier, batch_id, digest "
            f"FROM {self.qualified(self.guard_table)} WHERE output_group = ?",
            [output_group],
        ).fetchall()
        if len(rows) != 1:
            raise GuardInvariantError(
                f"output group {output_group!r} must have exactly one guard row, found {len(rows)}"
            )
        return guard_from_row(rows[0])

    def provision_guard(self, output_group: str, generation: str, *, epoch: int = 1) -> Guard:
        """Create the guard row for a group if it does not exist yet."""
        guard = Guard.initial(output_group, generation, epoch=epoch)
        self.connection.execute(
            f"INSERT INTO {self.qualified(self.guard_table)} "
            "(output_group, generation, epoch, frontier, batch_id, digest, updated_at) "
            "SELECT ?, ?, ?, ?, ?, ?, now() WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {self.qualified(self.guard_table)} WHERE output_group = ?)",
            [
                guard.output_group,
                guard.generation,
                guard.epoch,
                guard.frontier,
                guard.batch_id,
                guard.digest,
                output_group,
            ],
        )
        return self.read_guard(output_group)

    def take_over(self, output_group: str, epoch: int) -> Guard:
        """Bump the ownership epoch in a transaction of its own.

        Old-snapshot publishers that later try to update the same guard row
        conflict, and publishers that start after the commit observe the new
        epoch and are fenced.
        """
        cur = self.cursor()
        try:
            cur.execute("BEGIN TRANSACTION")
            guard = self.read_guard(output_group, cur=cur)
            new_guard = acquire_ownership(guard, epoch)
            cur.execute(
                f"UPDATE {self.qualified(self.guard_table)} SET epoch = ?, updated_at = now() "
                "WHERE output_group = ? AND epoch = ?",
                [new_guard.epoch, output_group, guard.epoch],
            )
            current = self.read_guard(output_group, cur=cur)
            if current.epoch != new_guard.epoch:
                cur.execute("ROLLBACK")
                raise ProtocolError(
                    f"takeover of {output_group!r} to epoch {epoch} did not take effect"
                )
            cur.execute("COMMIT")
            return current
        except BaseException:
            with contextlib.suppress(duckdb.Error):
                cur.execute("ROLLBACK")
            raise

    def reset_group(self, output_group: str, generation: str, epoch: int = 1) -> Guard:
        """Point an output group at a new pipeline generation and clear its tables.

        Administratively used when a Feldera pipeline is recreated: the outputs
        of the old generation are no longer authoritative, so the guard is
        rebased on the new generation and the target tables are emptied in the
        same transaction.
        """
        cur = self.cursor()
        cur.execute("BEGIN TRANSACTION")
        try:
            cur.execute(
                f"UPDATE {self.qualified(self.guard_table)} "
                "SET generation = ?, epoch = ?, frontier = 0, batch_id = NULL, digest = NULL, "
                "updated_at = now() "
                "WHERE output_group = ?",
                [generation, epoch, output_group],
            )
            for table in self.tables:
                cur.execute(f"DELETE FROM {self.qualified(table)}")
            cur.execute("COMMIT")
        except BaseException:
            cur.execute("ROLLBACK")
            raise
        return self.read_guard(output_group)

    # -- publication -------------------------------------------------------

    def publish(self, batch: Batch, *, max_attempts: int = 8) -> PublicationResult:
        """Apply ``batch`` atomically with the guard update, or explain why not.

        The guard read, the guard update and the whole payload live in one
        DuckLake transaction, so a reader that shares a snapshot with a
        committed publication sees either all of it or none of it.  A
        transaction that loses a write-write race is retried with the *same*
        batch identity, which is what makes a retry after a lost acknowledgement
        safe.
        """
        validate(batch)
        if not self.tables:
            raise ProtocolError("the catalog has no output tables configured")
        known = set(self.tables)
        unknown = sorted({delta.table for delta in batch.deltas} - known)
        if unknown:
            raise ProtocolError(f"batch writes tables outside this connector: {unknown}")

        conflict: str | None = None
        # The tables a resync may clear, as in the Rust writer: what the batch
        # repopulates, or everything this connector owns when it names nothing.
        resync_tables = tuple(sorted({delta.table for delta in batch.deltas})) or self.tables
        for attempt in range(1, max_attempts + 1):
            cur = self.cursor()
            try:
                cur.execute("BEGIN TRANSACTION")
                guard = self.read_guard(batch.output_group, cur=cur)
                decision = decide(guard, batch)
                if decision is not Decision.APPLY:
                    cur.execute("ROLLBACK")
                    return PublicationResult(decision, guard, attempt, conflict)
                self._update_guard(cur, guard, batch)
                self._apply_payload(cur, batch, resync_tables)
                cur.execute("COMMIT")
                return PublicationResult(Decision.APPLY, advance(guard, batch), attempt, conflict)
            except duckdb.Error as exc:
                self._rollback(cur)
                if not _is_transaction_conflict(exc) or attempt == max_attempts:
                    raise
                conflict = str(exc)
        raise ProtocolError(  # pragma: no cover - loop either returns or raises
            f"publication of batch {batch.batch_id} did not converge: {conflict}"
        )

    @staticmethod
    def _rollback(cur: duckdb.DuckDBPyConnection) -> None:
        with contextlib.suppress(duckdb.Error):
            cur.execute("ROLLBACK")

    def _update_guard(self, cur: duckdb.DuckDBPyConnection, guard: Guard, batch: Batch) -> None:
        """Conditional guarded update; writes nothing if another writer won."""
        cur.execute(
            f"UPDATE {self.qualified(self.guard_table)} "
            "SET frontier = ?, batch_id = ?, digest = ?, updated_at = now() "
            "WHERE output_group = ? AND generation = ? AND epoch = ? AND frontier = ?",
            [
                batch.end,
                batch.batch_id,
                batch.digest,
                guard.output_group,
                guard.generation,
                guard.epoch,
                guard.frontier,
            ],
        )
        current = self.read_guard(guard.output_group, cur=cur)
        if current.frontier != batch.end or current.batch_id != batch.batch_id:
            raise ProtocolError(
                f"guard for {guard.output_group!r} did not advance to {batch.end}; "
                "a concurrent publisher changed it"
            )

    def _apply_payload(
        self, cur: duckdb.DuckDBPyConnection, batch: Batch, tables: Sequence[str]
    ) -> None:
        if batch.resync:
            # Only the tables this batch repopulates.  Clearing every configured
            # table would let a resync of one view delete the rows another view
            # owns, and then advance the guard over the hole: the rows are gone,
            # the batch is committed, and nothing reports a problem.  The Rust
            # writer was fixed to the same rule (`Catalog::publish`'s `tables`
            # argument); a batch that names no table at all still means "all of
            # them", which is why the caller falls back to `self.tables`.
            for table in tables:
                cur.execute(f"DELETE FROM {self.qualified(table)}")
        # Deletions first: in row mode a delete must match the state the batch
        # starts from, which still contains rows that this batch also inserts.
        ordered = sorted(batch.deltas, key=lambda d: (d.op is Op.INSERT, d.canonical()))
        for delta in ordered:
            self._apply_delta(cur, delta)

    def _apply_delta(self, cur: duckdb.DuckDBPyConnection, delta: Delta) -> None:
        table = delta.table
        target = self.qualified(table)
        row = {str(k): v for k, v in delta.row.items()}
        columns = sorted(row)
        keys = self.key_columns.get(table)

        if keys:
            # `IS NOT DISTINCT FROM`, not `=`: a NULL key part never equals
            # anything, so `= ?` would match no row, delete nothing, and still
            # advance the guard — leaving the previous row in place next to the
            # one this delta inserts.
            predicate = " AND ".join(f"{quote_ident(k)} IS NOT DISTINCT FROM ?" for k in keys)
            cur.execute(f"DELETE FROM {target} WHERE {predicate}", [row[k] for k in keys])
        elif delta.op is Op.DELETE:
            # Multiplicity-preserving removal: exactly one matching row.
            predicate = " AND ".join(f"{quote_ident(c)} IS NOT DISTINCT FROM ?" for c in columns)
            cur.execute(
                f"DELETE FROM {target} WHERE rowid IN ("
                f"  SELECT rowid FROM {target} WHERE {predicate} LIMIT 1)",
                [row[c] for c in columns],
            )
        if delta.op is Op.INSERT:
            placeholders = ", ".join("?" for _ in columns)
            cur.execute(
                f"INSERT INTO {target} ({', '.join(quote_ident(c) for c in columns)}) "
                f"VALUES ({placeholders})",
                [row[c] for c in columns],
            )

    # -- provisioning ------------------------------------------------------

    def create_guard_table(self) -> None:
        self.connection.execute(
            f"CREATE TABLE IF NOT EXISTS {self.qualified(self.guard_table)} ("
            "  output_group VARCHAR NOT NULL,"
            "  generation VARCHAR NOT NULL,"
            "  epoch BIGINT NOT NULL,"
            "  frontier BIGINT NOT NULL,"
            "  batch_id VARCHAR,"
            "  digest VARCHAR,"
            "  updated_at TIMESTAMP"
            ")"
        )
