"""Tests for the step assembler.

Feldera's egress protocol is not self-describing: rows arrive in chunks that
report ``processed_steps == 0`` and the step numbers arrive later, in chunks that
carry no rows.  These tests pin the resulting contract, because a plausible
implementation (one that treats each row-bearing chunk as belonging to the *next*
step it hears about, or that releases a batch as soon as one view reports it)
loses or duplicates rows without any error being raised.

The chunks here are built from JSON with :func:`chunk_from_json`, i.e. the same
path the live connector takes, so the tests exercise the wire shape rather than
the dataclass.
"""

from __future__ import annotations

import json

import pytest

from fd_connectors.feldera import FelderaError, chunk_from_json
from fd_connectors.output_connector import StepAssembler
from fd_connectors.protocol import ProtocolError

VIEWS = {"v_a": "t_a", "v_b": "t_b"}


def rows(seq: int, *updates: dict) -> object:
    """A row-bearing chunk: ``processed_steps`` is 0, as Feldera sends it."""
    return chunk_from_json(
        {
            "sequence_number": seq,
            "processed_steps": 0,
            "snapshot": False,
            "json_data": list(updates),
        }
    )


def boundary(seq: int, step: int) -> object:
    """A step boundary: carries a step number and no rows."""
    return chunk_from_json(
        {"sequence_number": seq, "processed_steps": step, "snapshot": False, "json_data": []}
    )


def snapshot(seq: int, *updates: dict) -> object:
    return chunk_from_json(
        {
            "sequence_number": seq,
            "processed_steps": 0,
            "snapshot": True,
            "json_data": list(updates),
        }
    )


def insert(row_id: int, **rest) -> dict:
    return {"insert": {"id": row_id, **rest}}


def delete(row_id: int, **rest) -> dict:
    return {"delete": {"id": row_id, **rest}}


def payload(event) -> set:
    return {json.dumps(update, sort_keys=True) for update in event.updates}


def one_event(assembler: StepAssembler):
    event = assembler.next_event(timeout=0.05)
    assert event is not None, "expected the assembler to release a group result"
    return event


def test_group_result_is_released_at_the_boundary_all_views_agree_on() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)

    assembler.feed("v_a", snapshot(0, insert(1)))
    assembler.feed("v_b", snapshot(0, insert(1, kind="b")))
    # Only one view has reached step 9: the group is not complete.
    assembler.feed("v_a", boundary(1, 9))
    assert assembler.next_event(timeout=0.05) is None

    assembler.feed("v_b", boundary(1, 9))
    event = one_event(assembler)
    assert (event.kind, event.base, event.end) == ("resync", 0, 9)
    assert {u["insert"]["table"] for u in event.updates} == {"t_a", "t_b"}


def test_rows_of_a_later_boundary_are_not_in_an_earlier_batch() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", snapshot(0, insert(1)))
    assembler.feed("v_b", snapshot(0))
    assembler.feed("v_a", boundary(1, 5))
    assembler.feed("v_b", boundary(1, 5))
    assert one_event(assembler).end == 5

    # A change that belongs to the next segment must wait for its boundary, and
    # must not be folded into the segment that was already published.
    assembler.feed("v_a", rows(2, insert(2)))
    assert assembler.next_event(timeout=0.05) is None

    assembler.feed("v_a", boundary(3, 7))
    assert assembler.next_event(timeout=0.05) is None  # v_b is still at 5

    assembler.feed("v_b", boundary(3, 7))
    event = one_event(assembler)
    assert (event.kind, event.base, event.end) == ("step", 5, 7)
    assert payload(event) == {
        json.dumps({"insert": {"table": "t_a", "row": {"id": 2}}}, sort_keys=True)
    }


def test_snapshot_resync_waits_for_the_slowest_view() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", snapshot(0, insert(1)))
    assembler.feed("v_a", boundary(1, 10))
    assembler.feed("v_b", snapshot(0, insert(1, kind="b")))

    # v_b's snapshot is at a later step, so releasing at 10 would publish v_a's
    # state and v_b's state at different points in time.
    assert assembler.next_event(timeout=0.05) is None


def test_resync_payload_is_the_content_at_the_resync_step() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", snapshot(0, insert(1)))
    assembler.feed("v_b", snapshot(0, insert(1, kind="b")))
    assembler.feed("v_a", boundary(1, 10))
    assembler.feed("v_b", boundary(1, 12))
    # A change delivered after the snapshot but before the resync boundary is
    # part of the state at 12 and so belongs in the payload.
    assembler.feed("v_a", rows(2, insert(2)))
    assembler.feed("v_a", boundary(3, 12))

    event = one_event(assembler)
    assert (event.kind, event.base, event.end) == ("resync", 0, 12)
    ids = {(u["insert"]["table"], u["insert"]["row"]["id"]) for u in event.updates}
    assert ids == {("t_a", 1), ("t_b", 1), ("t_a", 2)}


def test_non_snapshot_resume_continues_from_the_frontier() -> None:
    assembler = StepAssembler(VIEWS, frontier=5, require_snapshot=False)
    # Attaching while the pipeline is paused makes the first boundary the
    # frontier, with nothing delivered for it.
    assembler.feed("v_a", boundary(0, 5))
    assembler.feed("v_b", boundary(0, 5))
    assert assembler.next_event(timeout=0.05) is None

    assembler.feed("v_a", rows(1, insert(2)))
    assembler.feed("v_a", boundary(2, 9))
    assembler.feed("v_b", boundary(2, 9))
    event = one_event(assembler)
    assert (event.kind, event.base, event.end) == ("step", 5, 9)
    assert [u["insert"]["row"]["id"] for u in event.updates] == [2]


def test_non_snapshot_resume_refuses_a_stream_behind_the_frontier() -> None:
    assembler = StepAssembler(VIEWS, frontier=5, require_snapshot=False)
    assembler.feed("v_a", boundary(0, 4))
    assembler.feed("v_b", boundary(0, 4))
    with pytest.raises(ProtocolError, match="already published"):
        assembler.next_event(timeout=0.05)


def test_non_snapshot_resume_refuses_an_unattributable_row() -> None:
    # Rows delivered for a step that is already published mean the stream was
    # attached before the pipeline reached the frontier, so later rows cannot be
    # told apart from already-published ones.
    assembler = StepAssembler(VIEWS, frontier=5, require_snapshot=False)
    assembler.feed("v_a", rows(0, insert(1)))
    assembler.feed("v_b", rows(0, insert(1, kind="b")))
    assembler.feed("v_a", boundary(1, 5))
    assembler.feed("v_b", boundary(1, 5))
    with pytest.raises(ProtocolError, match="pipeline is paused"):
        assembler.next_event(timeout=0.05)


def test_ended_egress_stream_refuses_to_advance() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", snapshot(0, insert(1)))
    assembler.feed("v_b", snapshot(0, insert(1, kind="b")))
    assembler.feed("v_a", boundary(1, 3))
    assembler.feed("v_b", boundary(1, 3))
    assert one_event(assembler).end == 3

    assembler.close_stream("v_b")
    with pytest.raises(FelderaError, match="v_b"):
        assembler.next_event(timeout=0.05)


def test_empty_boundaries_do_not_count_as_content() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", snapshot(0, insert(1)))
    assembler.feed("v_b", snapshot(0))
    assembler.feed("v_a", boundary(1, 5))
    assembler.feed("v_b", boundary(1, 5))
    one_event(assembler)
    content_at = assembler.last_content_at
    content_events = assembler.content_events

    for step in (6, 7, 8):
        assembler.feed("v_a", boundary(step, step))
        assembler.feed("v_b", boundary(step, step))
        event = one_event(assembler)
        assert event.updates == ()

    assert assembler.content_events == content_events
    assert assembler.last_content_at == content_at


def test_a_group_without_snapshots_is_not_released_when_snapshots_are_required() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", rows(0, insert(1)))
    assembler.feed("v_b", rows(0, insert(1, kind="b")))
    assembler.feed("v_a", boundary(1, 4))
    assembler.feed("v_b", boundary(1, 4))
    assert assembler.next_event(timeout=0.05) is None
    assert assembler.has_pending


def test_deletes_are_tagged_for_their_view_table() -> None:
    assembler = StepAssembler(VIEWS, frontier=0, require_snapshot=True)
    assembler.feed("v_a", snapshot(0))
    assembler.feed("v_b", snapshot(0))
    assembler.feed("v_a", boundary(1, 2))
    assembler.feed("v_b", boundary(1, 2))
    one_event(assembler)

    assembler.feed("v_a", rows(2, delete(7)))
    assembler.feed("v_a", boundary(3, 4))
    assembler.feed("v_b", boundary(3, 4))
    event = one_event(assembler)
    assert event.updates == ({"delete": {"table": "t_a", "row": {"id": 7}}},)
