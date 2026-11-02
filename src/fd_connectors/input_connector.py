"""Input connector: DuckLake change feed -> Feldera input tables.

Data flow (pull)::

    read checkpoint -> table_changes((checkpoint, last_committed]) -> journal
      -> push JSON to /ingress/{table} -> wait for the completion token
      -> journal "committed" -> save checkpoint

The three durability steps are ordered so that a crash is always detectable:
the journal entry exists before Feldera can see the data, and the checkpoint is
only moved after Feldera reports the data processed.  A batch that is journaled
but not acknowledged is *uncertain*; the connector never guesses whether it was
processed.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import codec
from .catalog import DuckLake, HistoryExpiredError, TableChange
from .feldera import FelderaClient, FelderaError
from .journal import Checkpoint, Journal, JournalError
from .protocol import Delta, Op, ProtocolError, change_weight


@dataclass(frozen=True)
class InputSpec:
    """One source table and the Feldera input table it feeds."""

    source_table: str
    feldera_table: str
    schema: str = "main"
    key_columns: tuple[str, ...] = ()


@dataclass
class InputProgress:
    source_table: str
    feldera_table: str
    checkpoint: Checkpoint | None
    batches: int = 0
    rows: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "feldera_table": self.feldera_table,
            "source_snapshot": None if self.checkpoint is None else self.checkpoint.source_snapshot,
            "feldera_step": None if self.checkpoint is None else self.checkpoint.feldera_step,
            "batches": self.batches,
            "rows": self.rows,
        }


def changes_to_deltas(table: str, changes: Iterable[TableChange]) -> list[Delta]:
    """Convert change-feed events into insert/delete deltas.

    Weight, not the event name, decides the operation: ``update_preimage`` is a
    retraction and ``update_postimage`` is an insertion.  Modelling a row update
    as "the update changed" would compute a difference between two facts rather
    than between two states.
    """
    deltas: list[Delta] = []
    for change in changes:
        weight = change_weight(change.change_type)
        deltas.append(
            Delta(
                table=table,
                op=Op.INSERT if weight > 0 else Op.DELETE,
                row=dict(change.row),
            )
        )
    return deltas


def _row_text(row: Mapping[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


class InputConnector:
    """Feeds one or more DuckLake tables into Feldera input tables."""

    def __init__(
        self,
        lake: DuckLake,
        client: FelderaClient,
        *,
        pipeline: str,
        generation: str,
        specs: Sequence[InputSpec],
        state_dir: str | Path,
        batch_size: int = 1000,
        completion_timeout: float = 120.0,
        require_completion_token: bool = True,
    ) -> None:
        if not specs:
            raise ProtocolError("the input connector needs at least one table")
        self.lake = lake
        self.client = client
        self.pipeline = pipeline
        self.generation = generation
        self.specs = tuple(specs)
        self.state_dir = Path(state_dir)
        self.batch_size = batch_size
        self.completion_timeout = completion_timeout
        self.require_completion_token = require_completion_token
        self.journals: dict[str, Journal] = {
            spec.feldera_table: Journal(self.state_dir / spec.feldera_table) for spec in self.specs
        }
        self.progress: dict[str, InputProgress] = {
            spec.feldera_table: InputProgress(
                source_table=spec.source_table,
                feldera_table=spec.feldera_table,
                checkpoint=self.journals[spec.feldera_table].load_checkpoint(),
            )
            for spec in self.specs
        }

    # -- pushing -----------------------------------------------------------

    def _push(
        self,
        spec: InputSpec,
        *,
        source_start: int,
        source_end: int,
        updates: Sequence[Mapping[str, Any]],
        batch_id: str,
    ) -> Checkpoint:
        """Journal, push and checkpoint one interval of one table."""
        journal = self.journals[spec.feldera_table]
        record = journal.append_batch(
            source_table=spec.source_table,
            source_start=source_start,
            source_end=source_end,
            deltas=_updates_to_deltas(spec.source_table, updates),
            batch_id=batch_id,
        )
        step: int | None = None
        for start in range(0, len(updates), self.batch_size) or [0]:
            chunk = list(updates[start : start + self.batch_size])
            token = self.client.push_updates(self.pipeline, spec.feldera_table, chunk)
            if token is None and self.require_completion_token:
                raise FelderaError(
                    f"ingress to {self.pipeline}/{spec.feldera_table} returned no completion "
                    "token; the processed-input frontier for this batch cannot be established"
                )
            status = self.client.wait_for_completion(
                self.pipeline, token, timeout=self.completion_timeout
            )
            if status.step is not None:
                step = status.step
        if step is None:
            raise FelderaError(
                f"batch {batch_id} on {spec.feldera_table} has no processed step, so the input "
                "frontier cannot be advanced"
            )
        journal.append_committed(record.seq, step)
        checkpoint = Checkpoint(source_snapshot=source_end, feldera_step=step, seq=record.seq)
        journal.save_checkpoint(checkpoint)
        progress = self.progress[spec.feldera_table]
        progress.checkpoint = checkpoint
        progress.batches += 1
        progress.rows += len(updates)
        return checkpoint

    # -- history checks ----------------------------------------------------

    def check_history(self, spec: InputSpec) -> None:
        checkpoint = self.journals[spec.feldera_table].load_checkpoint()
        if checkpoint is None:
            return
        available = [snapshot for snapshot, _time, _changes in self.lake.snapshots()]
        if not available:
            raise HistoryExpiredError(
                f"{spec.source_table}: a checkpoint at snapshot {checkpoint.source_snapshot} exists "
                "but the catalog reports no retained snapshots"
            )
        oldest = min(available)
        if checkpoint.source_snapshot < oldest:
            raise HistoryExpiredError(
                f"{spec.source_table}: checkpoint at snapshot {checkpoint.source_snapshot} is older "
                f"than the oldest retained snapshot {oldest}, so the interval cannot be replayed. "
                "Recreate the pipeline with a new generation and bootstrap again."
            )

    def uncertain(self) -> dict[str, list[int]]:
        """Journal sequences that may or may not have been processed."""
        result: dict[str, list[int]] = {}
        for spec in self.specs:
            journal = self.journals[spec.feldera_table]
            pending = journal.uncertain_batches(journal.load_checkpoint())
            if pending:
                result[spec.feldera_table] = [record.seq for record in pending]
        return result

    def recover(self, policy: str = "fail") -> None:
        """Act on batches whose fate is unknown after a crash.

        ``fail`` (default) refuses to continue and explains the options.
        ``rebootstrap`` abandons the history; it is only correct together with a
        *recreated* pipeline, because rows already pushed to a Feldera input
        table cannot be removed.
        """
        pending = self.uncertain()
        if not pending:
            return
        if policy == "fail":
            raise JournalError(
                "unacknowledged batches exist, so the input frontier cannot be advanced safely: "
                + ", ".join(f"{table} seq {seqs}" for table, seqs in sorted(pending.items()))
                + ". Either recreate the Feldera pipeline (new generation) and bootstrap again "
                "with --recover rebootstrap, or restore the pipeline from a checkpoint at or after "
                "these batches."
            )
        if policy == "rebootstrap":
            for spec in self.specs:
                _archive(self.journals[spec.feldera_table])
                self.progress[spec.feldera_table].checkpoint = None
            return
        raise JournalError(f"unknown recovery policy {policy!r}")

    # -- operations --------------------------------------------------------

    def bootstrap(self, spec: InputSpec) -> Checkpoint:
        """Push the full contents of ``spec.source_table`` at one snapshot."""
        snapshot = self.lake.current_snapshot()
        rows = self.lake.read_snapshot(spec.source_table, snapshot)
        updates = [{"insert": codec.to_json_row(row)} for row in rows]
        return self._push(
            spec,
            source_start=snapshot,
            source_end=snapshot,
            updates=updates,
            batch_id=f"{self.generation}:bootstrap@{snapshot}",
        )

    def poll_once(self) -> list[InputProgress]:
        """Consume every change committed since the last checkpoint."""
        for spec in self.specs:
            self.check_history(spec)
            journal = self.journals[spec.feldera_table]
            checkpoint = journal.load_checkpoint()
            if checkpoint is None:
                self.bootstrap(spec)
                continue
            end = self.lake.last_committed_snapshot()
            if end <= checkpoint.source_snapshot:
                continue
            changes = self.lake.table_changes(
                spec.source_table,
                checkpoint.source_snapshot,
                end,
                schema=spec.schema,
            )
            updates = [
                codec.encode_delta(delta) for delta in changes_to_deltas(spec.source_table, changes)
            ]
            # An interval with no row events still needs a checkpoint, otherwise
            # the same interval is re-read forever.
            self._push(
                spec,
                source_start=checkpoint.source_snapshot,
                source_end=end,
                updates=updates,
                batch_id=f"{self.generation}:{checkpoint.source_snapshot}->{end}",
            )
        return [self.progress[spec.feldera_table] for spec in self.specs]

    def run(
        self,
        *,
        iterations: int | None = None,
        interval: float = 1.0,
        idle_interval: float | None = None,
    ):
        """Poll repeatedly, yielding the progress of each round."""
        count = 0
        while iterations is None or count < iterations:
            before = {
                spec.feldera_table: self.progress[spec.feldera_table].rows for spec in self.specs
            }
            progress = self.poll_once()
            moved = any(
                self.progress[spec.feldera_table].rows != before[spec.feldera_table]
                for spec in self.specs
            )
            yield progress
            count += 1
            if iterations is not None and count >= iterations:
                break
            time.sleep(interval if moved else (idle_interval or interval))


def _updates_to_deltas(table: str, updates: Sequence[Mapping[str, Any]]) -> list[Delta]:
    """Rebuild journal deltas from Feldera-shaped update objects."""
    deltas: list[Delta] = []
    for update in updates:
        ((op_name, row),) = update.items()
        deltas.append(Delta(table=table, op=Op(str(op_name)), row=dict(row)))
    return deltas


def _archive(journal: Journal) -> None:
    """Move a journal and checkpoint aside so a fresh history can start."""
    stamp = time.strftime("%Y%m%dT%H%M%S")
    for path in (journal.path, journal.checkpoint_path):
        if path.exists():
            path.rename(path.with_name(f"{path.name}.{stamp}.superseded"))
