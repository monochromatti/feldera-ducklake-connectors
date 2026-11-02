"""Regression tests for Feldera's egress update encoding.

The shapes here are copied verbatim from a live pipeline-manager v0.374.0
egress capture (``GET /v0/pipelines/<p>/egress/<view>?format=json``):

    {"sequence_number":0,"snapshot":false,"json_data":[{"insert":{"id":9001,"name":"zzprobe","price":3.5}}]}
    {"sequence_number":1,"snapshot":false,"processed_steps":10}
    {"sequence_number":2,"snapshot":false,"json_data":[{"delete":{"id":9001,"name":"zzprobe","price":3.5}}]}
    {"sequence_number":3,"snapshot":false,"processed_steps":12}
    {"sequence_number":4,"snapshot":false}

``json_data`` is a *list* of one-key ``{op: row}`` objects.  An earlier
decoder assumed each element was the wrapper and raised ``ProtocolError`` on
every real chunk; these tests exist so that cannot come back.
"""

from __future__ import annotations

import json

import pytest

from fd_connectors.protocol import Op, ProtocolError, batch_from_feldera_chunk

# Exactly what the running pipeline sent, minus the output-table multiplexing.
INSERT_ROW = {"id": 9001, "name": "zzprobe", "price": 3.5}


def _batch(rows, table_columns):
    return batch_from_feldera_chunk(
        output_group="probe",
        generation="g1",
        epoch=1,
        base=0,
        end=10,
        rows=rows,
        table_columns=table_columns,
    )


def test_one_key_wrapper_is_unwrapped_to_the_row():
    batch = _batch([{"insert": dict(INSERT_ROW)}], {"probe": ("id", "name", "price")})

    assert [(d.table, d.op, d.row) for d in batch.deltas] == [("probe", Op.INSERT, INSERT_ROW)]


def test_the_same_row_may_be_deleted_later():
    batch = _batch([{"delete": dict(INSERT_ROW)}], {"probe": ("id", "name", "price")})

    assert [d.op for d in batch.deltas] == [Op.DELETE]


def test_a_json_array_is_unwrapped_element_by_element():
    rows = [{"insert": {"id": 1}}, {"insert": {"id": 2}}]

    batch = _batch(rows, {"probe": ("id",)})

    assert [d.row["id"] for d in batch.deltas] == [1, 2]


def test_deletes_are_ordered_before_inserts_regardless_of_arrival():
    rows = [{"insert": {"id": 2}}, {"delete": {"id": 1}}]

    batch = _batch(rows, {"probe": ("id",)})

    assert [(d.op, d.row["id"]) for d in batch.deltas] == [
        (Op.DELETE, 1),
        (Op.INSERT, 2),
    ]


def test_a_row_that_is_only_a_table_and_row_pair_is_treated_as_multiplexed():
    rows = [{"insert": {"table": "order_totals", "row": {"id": 1}}}]

    batch = _batch(rows, {"order_totals": ("id",), "order_lines": ("id",)})

    assert [(d.table, d.row) for d in batch.deltas] == [("order_totals", {"id": 1})]


def test_a_bare_row_with_several_tables_in_scope_is_refused_not_guessed():
    rows = [{"insert": {"id": 1}}]

    with pytest.raises(ProtocolError, match="several output tables are in scope"):
        _batch(rows, {"order_totals": ("id",), "order_lines": ("id",)})


def test_a_payload_that_is_not_an_object_is_refused():
    with pytest.raises(ProtocolError, match="not an object"):
        _batch([{"insert": [1, 2, 3]}], {"probe": ("id",)})


def test_the_captured_chunks_round_trip_through_json():
    for chunk in (
        {"sequence_number": 0, "snapshot": False, "json_data": [{"insert": dict(INSERT_ROW)}]},
        {"sequence_number": 1, "snapshot": False, "processed_steps": 10},
        {"sequence_number": 4, "snapshot": False},
    ):
        # The keep-alive chunk has no payload; the others must decode.
        decoded = json.loads(json.dumps(chunk))
        if "json_data" not in decoded:
            continue
        batch = _batch(decoded["json_data"], {"probe": ("id", "name", "price")})
        assert batch.deltas[0].row == INSERT_ROW
