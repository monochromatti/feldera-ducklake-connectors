"""Durable journal and checkpoint for the input connector.

The journal is written *before* a batch is handed to Feldera, so a batch that
the pipeline may have processed is always recoverable from disk.  The
checkpoint is written *after* Feldera reports the batch's completion token as
``complete`` plus its step number, so the checkpoint always names a step whose
inputs are both journaled and known to be processed.

Records (one JSON object per line)::

    {"kind": "batch",     "seq": 7, "source_start": 3, "source_end": 4,
                          "source_table": "product", "deltas": [...],
                          "batch_id": "..."}
    {"kind": "committed", "seq": 7, "feldera_step": 12}

A ``batch`` record without a matching ``committed`` record is *uncertain*: the
process died somewhere between journaling and acknowledging, and this bridge
cannot tell whether Feldera consumed it.  Uncertainty is reported rather than
silently resolved, because the two possible resolutions have different
consequences (see ``docs/PROTOCOL.md``).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .protocol import Batch, Delta, delta_from_json

JOURNAL_NAME = "input-journal.jsonl"
CHECKPOINT_NAME = "input-checkpoint.json"


class JournalError(Exception):
    """The journal or checkpoint on disk cannot be used."""


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_atomic(path: Path, data: str) -> None:
    """Write ``data`` to ``path`` so that a reader sees old or new, never torn."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_directory(path.parent)


@dataclass(frozen=True)
class Checkpoint:
    """Where the input connector resumes from."""

    source_snapshot: int
    """Highest DuckLake snapshot whose changes have been consumed and processed."""

    feldera_step: int
    """Feldera step in which those changes became visible."""

    seq: int
    """Journal sequence number this checkpoint corresponds to."""

    def to_json(self) -> dict[str, Any]:
        return {
            "source_snapshot": self.source_snapshot,
            "feldera_step": self.feldera_step,
            "seq": self.seq,
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Checkpoint:
        return cls(
            source_snapshot=int(obj["source_snapshot"]),
            feldera_step=int(obj["feldera_step"]),
            seq=int(obj["seq"]),
        )


@dataclass(frozen=True)
class JournalRecord:
    kind: str
    seq: int
    payload: Mapping[str, Any]

    @property
    def batch(self) -> Batch | None:
        if self.kind != "batch":
            return None
        obj = self.payload
        return Batch(
            output_group="input",
            generation="input",
            epoch=1,
            base=int(obj["source_start"]),
            end=int(obj["source_end"]),
            deltas=tuple(delta_from_json(d) for d in obj["deltas"]),
        )


class Journal:
    """Append-only JSONL journal plus an atomically replaced checkpoint."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / JOURNAL_NAME
        self.checkpoint_path = self.directory / CHECKPOINT_NAME
        self._last_seq = self._scan_last_seq()

    # -- writing -----------------------------------------------------------

    def _scan_last_seq(self) -> int:
        if not self.path.exists():
            return 0
        last = 0
        for record in self.records():
            last = max(last, record.seq)
        return last

    @property
    def next_seq(self) -> int:
        return self._last_seq + 1

    def _append(self, kind: str, seq: int, payload: Mapping[str, Any]) -> JournalRecord:
        record = {"kind": kind, "seq": seq, **payload}
        line = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self._last_seq = max(self._last_seq, seq)
        return JournalRecord(kind=kind, seq=seq, payload=payload)

    def append_batch(
        self,
        *,
        source_table: str,
        source_start: int,
        source_end: int,
        deltas: Sequence[Delta],
        batch_id: str,
    ) -> JournalRecord:
        """Record a batch that is about to be pushed to Feldera."""
        return self._append(
            "batch",
            self.next_seq,
            {
                "source_table": source_table,
                "source_start": int(source_start),
                "source_end": int(source_end),
                "batch_id": batch_id,
                "deltas": [d.to_json() for d in deltas],
            },
        )

    def append_committed(self, seq: int, feldera_step: int) -> None:
        """Record that Feldera processed the batch with journal sequence ``seq``."""
        self._append("committed", seq, {"feldera_step": int(feldera_step)})

    # -- reading -----------------------------------------------------------

    def records(self) -> Iterator[JournalRecord]:
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as handle:
            for lineno, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise JournalError(f"{self.path}:{lineno}: {exc}") from exc
                kind = str(obj.pop("kind"))
                seq = int(obj.pop("seq"))
                yield JournalRecord(kind=kind, seq=seq, payload=obj)

    def load_checkpoint(self) -> Checkpoint | None:
        if not self.checkpoint_path.exists():
            return None
        try:
            obj = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise JournalError(f"{self.checkpoint_path}: {exc}") from exc
        return Checkpoint.from_json(obj)

    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        write_atomic(self.checkpoint_path, json.dumps(checkpoint.to_json(), indent=2) + "\n")

    def uncertain_batches(self, checkpoint: Checkpoint | None) -> list[JournalRecord]:
        """Batch records that are not known to have been processed.

        A batch is confirmed when a ``committed`` record with the same sequence
        number exists.  Batches at or below the checkpoint's sequence are
        confirmed implicitly.
        """
        committed: set[int] = set()
        batches: list[JournalRecord] = []
        for record in self.records():
            if record.kind == "batch":
                batches.append(record)
            elif record.kind == "committed":
                committed.add(record.seq)
        floor = checkpoint.seq if checkpoint else 0
        return [b for b in batches if b.seq > floor and b.seq not in committed]
