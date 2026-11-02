"""Output connector: Feldera output views -> DuckLake tables.

Several views of one Feldera program may belong to one *output group*, whose
rows must land in DuckLake as one atomic publication.  Feldera, however, gives
one stream per view, so the connector has to reassemble the group's per-step
result from several streams before it can publish anything.  That reassembly is
:class:`StepAssembler`: it accumulates the rows every view delivers and only
releases them at a *boundary* — a step marker ``processed_steps > 0`` — once every
view has reached that same boundary.  Because Feldera reports step numbers but
never labels rows with them, the boundary is what makes a set of rows a group
result rather than an arbitrary cut through the streams.

The publisher then applies the step with :meth:`catalog.DuckLake.publish`,
which commits the payload and the guard row in a single DuckLake transaction.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from . import codec
from .catalog import DuckLake, GuardInvariantError, PublicationResult
from .feldera import FelderaClient, FelderaError, OutputChunk
from .protocol import Batch, Decision, ProtocolError, batch_from_feldera_chunk


class PublicationRefused(ProtocolError):
    """A batch was refused by the guard (stale, wrong owner, or wrong generation)."""

    def __init__(self, decision: Decision, batch: Batch, frontier: int | None = None) -> None:
        self.decision = decision
        self.batch = batch
        self.frontier = frontier
        detail = f" (current frontier {frontier})" if frontier is not None else ""
        super().__init__(f"publication of {batch.batch_id} refused: {decision.value}{detail}")


@dataclass(frozen=True)
class OutputSpec:
    """One Feldera view and the DuckLake table its rows land in."""

    view: str
    table: str
    key_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class StepEvent:
    """One group result, assembled across all views of the group."""

    kind: str
    """``resync`` for a snapshot batch, ``step`` for a normal step."""

    base: int
    end: int
    updates: tuple[Mapping[str, Any], ...]


@dataclass
class OutputStatus:
    output_group: str
    generation: str
    epoch: int
    frontier: int
    applied: int = 0
    duplicates: int = 0
    refused: dict[str, int] = field(default_factory=dict)
    resyncs: int = 0
    events: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "output_group": self.output_group,
            "generation": self.generation,
            "epoch": self.epoch,
            "frontier": self.frontier,
            "applied": self.applied,
            "duplicates": self.duplicates,
            "refused": dict(self.refused),
            "resyncs": self.resyncs,
            "events": self.events,
        }


class StepAssembler:
    """Joins per-view output streams into per-step group results.

    Feldera's egress stream does not label rows with the step that produced
    them.  Measured against v0.374.0, the wire protocol is:

    * a chunk carrying ``processed_steps == 0`` carries *rows*, produced by the
      steps that follow;
    * a chunk carrying ``processed_steps == P > 0`` carries *no rows* and marks
      the boundary "all steps up to P are done, and every row produced by them
      has been delivered";
    * ``processed_steps`` is a pipeline-wide counter, so every view reports the
      same boundary numbers;
    * a stream started with ``send_snapshot`` first delivers the view's full
      content (``snapshot: true``, ``processed_steps: 0``) and then the boundary
      for the step the snapshot was taken at.

    A group result is therefore the set of rows delivered between two
    consecutive boundaries, published with the later boundary as the frontier.
    A batch is only released once every view has reached that boundary, which
    is what makes the publication the group's result rather than one view's.
    """

    def __init__(
        self,
        view_tables: Mapping[str, str],
        *,
        frontier: int,
        require_snapshot: bool = True,
    ) -> None:
        if not view_tables:
            raise ProtocolError("the assembler needs at least one view")
        self.view_tables = dict(view_tables)
        self.frontier = frontier
        self.require_snapshot = require_snapshot
        self._cond = threading.Condition()
        self._pending: dict[str, list[Mapping[str, Any]]] = {view: [] for view in view_tables}
        self._boundary: dict[str, int] = {view: 0 for view in view_tables}
        self._first_boundary: dict[str, int | None] = {view: None for view in view_tables}
        self._seen: set[str] = set()
        self._snapshot_seen: set[str] = set()
        self._resynced = False
        self._pending_start = frontier
        self._error: BaseException | None = None
        self._closed = False
        self.last_content_at = time.monotonic()
        self.content_events = 0

    # -- producer side -----------------------------------------------------

    def _tag(self, view: str, updates: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        table = self.view_tables[view]
        tagged: list[Mapping[str, Any]] = []
        for update in updates:
            for op_name, payload in update.items():
                tagged.append({op_name: {"table": table, "row": dict(payload)}})
        return tagged

    def feed(self, view: str, chunk: OutputChunk) -> None:
        rows = self._tag(view, chunk.updates)
        step = int(chunk.processed_steps or 0)
        with self._cond:
            self._seen.add(view)
            if chunk.is_snapshot:
                self._snapshot_seen.add(view)
            if step > 0:
                if self._first_boundary[view] is None:
                    self._first_boundary[view] = step
                self._boundary[view] = max(self._boundary[view], step)
            if rows:
                self._pending[view].extend(rows)
            self._cond.notify_all()

    def fail(self, error: BaseException) -> None:
        with self._cond:
            self._error = self._error or error
            self._cond.notify_all()

    def close_stream(self, view: str) -> None:
        """Called when a view's egress stream ends.

        An egress stream is meant to stay open.  If it ends, later steps of that
        view are unobservable, so the assembler refuses to advance rather than
        publish a group result with a view missing.
        """
        self.fail(
            FelderaError(
                f"the egress stream for view {view!r} ended at step "
                f"{self._boundary.get(view, 0)}; the group can no longer be assembled from all "
                "of its views"
            )
        )

    def stop(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    # -- consumer side -----------------------------------------------------

    @property
    def has_pending(self) -> bool:
        with self._cond:
            return any(self._pending.values())

    def next_event(self, timeout: float | None = None) -> StepEvent | None:
        """Return the next group result, or ``None`` when the timeout expires."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                if self._error is not None:
                    error = self._error
                    self._error = None
                    raise error
                event = self._try_event()
                if event is not None:
                    return event
                if self._closed:
                    return None
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                if not self._cond.wait(remaining):
                    return None

    def _all_started(self) -> bool:
        return len(self._seen) == len(self.view_tables)

    def _drain(self) -> list[Mapping[str, Any]]:
        rows: list[Mapping[str, Any]] = []
        for view in sorted(self._pending):
            rows.extend(self._pending[view])
            self._pending[view] = []
        return rows

    def _event(self, kind: str, base: int, end: int, rows: list[Mapping[str, Any]]) -> StepEvent:
        self._pending_start = end
        self.frontier = end
        if rows:
            self.content_events += 1
            self.last_content_at = time.monotonic()
        return StepEvent(kind, base, end, tuple(rows))

    def _try_event(self) -> StepEvent | None:
        if not self._all_started():
            return None
        if self.require_snapshot and len(self._snapshot_seen) < len(self.view_tables):
            return None
        firsts = [self._first_boundary[view] for view in self.view_tables]
        if any(first is None for first in firsts):
            return None

        if not self._resynced:
            end = max(firsts)  # type: ignore[arg-type]
            if self.require_snapshot:
                # The group result is the union, at a common step, of every
                # view's snapshot plus the changes delivered up to that step.
                if any(self._boundary[view] < end for view in self.view_tables):
                    return None
                self._resynced = True
                return self._event("resync", 0, end, self._drain())
            # Continuing an existing group without a snapshot: the streams must
            # resume exactly at the published frontier.  A stream that starts
            # earlier replays published steps, whose rows would be re-applied
            # (or, worse, mixed with unpublished ones); a stream that starts
            # later skips steps whose rows can no longer be recovered.  Neither
            # is safe to guess about, so both are refused.  Attaching while the
            # pipeline is paused is what makes the frontier the stream's start.
            if end < self.frontier:
                raise ProtocolError(
                    f"egress reports step {end} but the published frontier is {self.frontier}; "
                    "the stream is replaying steps that were already published, so the rows it "
                    "delivers cannot be attributed to an unpublished interval"
                )
            if end > self.frontier:
                raise ProtocolError(
                    f"egress started at step {end} but the published frontier is {self.frontier}; "
                    "the changes of the steps in between cannot be recovered without a snapshot. "
                    "Restart the connector with send_snapshot enabled."
                )
            if any(self._pending.values()):
                raise ProtocolError(
                    f"the egress stream delivered rows for step {end}, which is already published "
                    f"at frontier {self.frontier}; attach the egress streams while the pipeline is "
                    "paused so that no step can slip past the frontier"
                )
            self._resynced = True
            self._pending_start = self.frontier
            # the first boundary beyond the frontier is published like any other

        watermark = min(self._boundary.values())
        if watermark <= self._pending_start:
            # No view has moved the group past the last accounted boundary, so
            # the buffered rows belong to an interval that is still open.  They
            # must not be drained: a view that runs ahead of the group is
            # normal, and dropping its rows here would lose them silently.
            return None
        return self._event("step", self._pending_start, watermark, self._drain())


class OutputConnector:
    """Publishes the steps of an output group into DuckLake."""

    def __init__(
        self,
        lake: DuckLake,
        client: FelderaClient,
        *,
        pipeline: str,
        output_group: str,
        generation: str,
        specs: Sequence[OutputSpec],
        epoch: int | None = None,
        send_snapshot: bool = True,
        raise_on_refusal: bool = True,
    ) -> None:
        if not specs:
            raise ProtocolError("the output connector needs at least one view")
        self.lake = lake
        self.client = client
        self.pipeline = pipeline
        self.output_group = output_group
        self.generation = generation
        self.specs = tuple(specs)
        self.epoch = epoch
        self.send_snapshot = send_snapshot
        self.raise_on_refusal = raise_on_refusal
        self.tables = tuple(spec.table for spec in specs)
        unknown = [table for table in self.tables if table not in set(lake.tables)]
        if unknown:
            raise ProtocolError(
                f"output tables {unknown} are not configured on the catalog, which owns "
                f"{list(lake.tables)}"
            )
        self.assembler: StepAssembler | None = None
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.status = OutputStatus(
            output_group=output_group,
            generation=generation,
            epoch=epoch or 1,
            frontier=0,
        )

    # -- setup -------------------------------------------------------------

    def prepare(self) -> int:
        """Make the guard row exist, take ownership, and build the assembler."""
        self.lake.create_guard_table()
        try:
            guard = self.lake.read_guard(self.output_group)
        except GuardInvariantError:
            guard = self.lake.provision_guard(
                self.output_group, self.generation, epoch=self.epoch or 1
            )
        if guard.generation != self.generation:
            raise ProtocolError(
                f"output group {self.output_group!r} belongs to generation "
                f"{guard.generation!r}, not {self.generation!r}; a new pipeline generation "
                "requires an explicit reset (see reset_group) so that stale results are not "
                "mixed with the new run"
            )
        if self.epoch is None:
            self.epoch = guard.epoch
        elif self.epoch > guard.epoch:
            guard = self.take_over(self.epoch)
        elif self.epoch < guard.epoch:
            raise ProtocolError(
                f"epoch {self.epoch} is fenced: output group {self.output_group!r} is owned at "
                f"epoch {guard.epoch}"
            )
        self.status.epoch = guard.epoch
        self.status.frontier = guard.frontier
        self.assembler = StepAssembler(
            {spec.view: spec.table for spec in self.specs},
            frontier=guard.frontier,
            require_snapshot=self.send_snapshot,
        )
        return guard.frontier

    def take_over(self, epoch: int):
        guard = self.lake.take_over(self.output_group, epoch)
        self.epoch = guard.epoch
        return guard

    def reset_group(self, generation: str, *, epoch: int = 1):
        """Rebase the group on a new pipeline generation, clearing its tables."""
        guard = self.lake.reset_group(self.output_group, generation, epoch=epoch)
        self.generation = generation
        self.epoch = epoch
        return guard

    # -- streaming ---------------------------------------------------------

    def start_streams(self) -> None:
        for spec in self.specs:
            thread = threading.Thread(
                target=self._reader,
                args=(spec,),
                name=f"egress-{spec.view}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def _reader(self, spec: OutputSpec) -> None:
        assert self.assembler is not None
        try:
            for chunk in self.client.egress(
                self.pipeline,
                spec.view,
                backpressure=True,
                send_snapshot=self.send_snapshot,
            ):
                if self._stop.is_set():
                    return
                self.assembler.feed(spec.view, chunk)
        except BaseException as exc:
            self.assembler.fail(exc)
        else:
            if not self._stop.is_set():
                self.assembler.close_stream(spec.view)

    def stop(self) -> None:
        self._stop.set()
        if self.assembler is not None:
            self.assembler.stop()

    # -- publishing --------------------------------------------------------

    def _batch(self, event: StepEvent) -> Batch:
        updates = [codec.decode_update(update) for update in event.updates]
        return batch_from_feldera_chunk(
            output_group=self.output_group,
            generation=self.generation,
            epoch=self.epoch or 1,
            base=event.base,
            end=event.end,
            rows=updates,
            table_columns={table: () for table in self.tables},
            resync=event.kind == "resync",
        )

    def publish_event(self, event: StepEvent, *, max_attempts: int = 8) -> PublicationResult:
        batch = self._batch(event)
        result = self.lake.publish(batch, max_attempts=max_attempts)
        self.status.events += 1
        self.status.frontier = batch.end
        if result.decision is Decision.APPLY:
            self.status.applied += 1
            if batch.resync:
                self.status.resyncs += 1
        elif result.decision is Decision.DUPLICATE:
            self.status.duplicates += 1
        else:
            key = result.decision.value
            self.status.refused[key] = self.status.refused.get(key, 0) + 1
            if self.raise_on_refusal:
                raise PublicationRefused(result.decision, batch, result.guard.frontier)
        return result

    def run(
        self,
        *,
        stop_after_idle: float = 5.0,
        max_batches: int | None = None,
        poll_interval: float = 0.2,
        on_event=None,
    ) -> OutputStatus:
        """Stream, publish, and return once the group is caught up and idle.

        "Idle" means no group result with *rows* has been published for
        ``stop_after_idle`` seconds and no rows are waiting for their boundary.
        Boundary markers alone advance the guard's frontier but are not news, so
        they do not keep the connector alive.
        """

        def idle() -> bool:
            assert self.assembler is not None
            return (
                not self.assembler.has_pending
                and time.monotonic() - self.assembler.last_content_at >= stop_after_idle
            )

        self.prepare()
        assert self.assembler is not None
        self.start_streams()
        try:
            while True:
                event = self.assembler.next_event(timeout=poll_interval)
                if event is None:
                    if self._stop.is_set():
                        break
                    if idle():
                        break
                    continue
                result = self.publish_event(event)
                if on_event is not None:
                    on_event(event, result)
                if max_batches is not None and self.status.applied >= max_batches:
                    break
                if idle():
                    break
        finally:
            self.stop()
        return self.status
