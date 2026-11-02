"""End-to-end acceptance test against a real Feldera pipeline-manager + DuckLake.

Unlike ``tests/test_assembler.py`` (unit-level step assembly) and the DuckLake-only
SQL scenarios in ``scenarios.py``, this module exercises the two connectors
against a live Feldera instance and a real DuckLake catalog:

1. DuckLake change feed -> ``InputConnector`` -> Feldera input table;
2. Feldera output views -> ``OutputConnector`` -> guarded DuckLake publication.

The whole module skips when ``$FELDERA_URL`` (default ``http://127.0.0.1:18080``)
is not answering ``/v0/config``, so ``uv run pytest tests/`` stays green with no
stack running.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from fd_connectors.catalog import DuckLake
from fd_connectors.feldera import FelderaClient
from fd_connectors.input_connector import InputConnector, InputSpec
from fd_connectors.journal import Journal, JournalError
from fd_connectors.output_connector import (
    OutputConnector,
    OutputSpec,
    PublicationRefused,
    StepEvent,
)
from fd_connectors.protocol import Batch, Decision, Delta, Op

FELDERA_URL = os.environ.get("FELDERA_URL", "http://127.0.0.1:18080").rstrip("/")


def _feldera_is_up(url: str) -> bool:
    try:
        response = httpx.get(f"{url}/v0/config", timeout=3.0)
    except httpx.HTTPError:
        return False
    return response.status_code == 200


FELDERA_AVAILABLE = _feldera_is_up(FELDERA_URL)

pytestmark = pytest.mark.skipif(
    not FELDERA_AVAILABLE,
    reason=f"no live Feldera pipeline-manager at {FELDERA_URL}",
)

# Input rows used by every test.  The aggregate is computed from this list
# independently below; it is never derived with connector code.
SOURCE_ROWS: list[tuple[int, str, float]] = [
    (1, "a", 10.0),
    (2, "a", 5.0),
    (3, "b", 7.0),
    (4, "c", 2.5),
]

# ``orders`` is materialized so the test can read back what actually landed in
# Feldera (only materialized tables and views are queryable), rather than
# trusting the connector's own row counter.
PIPELINE_SQL = """
CREATE TABLE orders (id BIGINT, customer VARCHAR, amount DOUBLE)
  WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW order_totals AS
  SELECT customer, SUM(amount) AS total
  FROM orders
  GROUP BY customer;
"""

# DuckLake lives behind the ``dl`` catalog alias, so its ``orders`` table never
# clashes with the Feldera input table of the same name.
LOAD_ORDERS_SQL = ", ".join(
    f"({row_id}, '{customer}', {amount})" for row_id, customer, amount in SOURCE_ROWS
)


def _expected_totals() -> dict[str, float]:
    totals: dict[str, float] = {}
    for _row_id, customer, amount in SOURCE_ROWS:
        totals[customer] = totals.get(customer, 0.0) + amount
    return totals


def _make_lake(root: Path) -> DuckLake:
    """A throwaway DuckLake catalog with the source and output tables."""
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    lake = DuckLake(
        catalog=f"ducklake:{root / 'metadata.ducklake'}",
        data_path=str(data_dir),
        tables=["order_totals"],
        key_columns={"order_totals": ["customer"]},
        inlining_row_limit=10_000,
    )
    lake.execute(
        "CREATE TABLE IF NOT EXISTS dl.main.orders (id BIGINT, customer VARCHAR, amount DOUBLE)"
    )
    lake.execute("CREATE TABLE IF NOT EXISTS dl.main.order_totals (customer VARCHAR, total DOUBLE)")
    lake.create_guard_table()
    return lake


def _start_pipeline(client: FelderaClient, name: str) -> None:
    client.put_pipeline(name, PIPELINE_SQL)
    client.start_and_wait(name, timeout=240.0)


def _cleanup_pipeline(client: FelderaClient, name: str) -> None:
    """Best-effort teardown.

    A graceful stop is enterprise-only in the community pipeline-manager, but
    ``FelderaClient.delete`` force-stops on its own, so a pipeline that has
    been started is still removed rather than leaked.
    """
    with contextlib.suppress(Exception):
        client.delete(name, timeout=60.0)


def _output_rows(lake: DuckLake) -> list[tuple]:
    return lake.connection.execute(
        "SELECT customer, total FROM dl.main.order_totals ORDER BY customer"
    ).fetchall()


@dataclass
class Stack:
    lake: DuckLake
    client: FelderaClient
    pipeline: str
    generation: str
    output_group: str
    expected: dict[str, float]
    status: object
    final_batch: Batch
    guard: object
    input_state: Path


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory):
    """A fully exercised happy-path stack: source rows in, view rows out."""
    root = tmp_path_factory.mktemp("e2e-happy")
    run = uuid.uuid4().hex[:8]
    generation = f"gen-{run}"
    pipeline = f"e2e_{run}"
    output_group = "totals"

    lake = _make_lake(root)
    lake.provision_guard(output_group, generation, epoch=1)
    client = FelderaClient(FELDERA_URL)

    try:
        _start_pipeline(client, pipeline)

        lake.execute(f"INSERT INTO dl.main.orders VALUES {LOAD_ORDERS_SQL}")

        input_state = root / "input-state"
        spec = InputSpec(source_table="orders", feldera_table="orders", schema="main")
        input_connector = InputConnector(
            lake,
            client,
            pipeline=pipeline,
            generation=generation,
            specs=[spec],
            state_dir=input_state,
            require_completion_token=True,
        )
        progress = input_connector.poll_once()
        assert progress[0].rows == len(SOURCE_ROWS)

        output_connector = OutputConnector(
            lake,
            client,
            pipeline=pipeline,
            output_group=output_group,
            generation=generation,
            specs=[
                OutputSpec(
                    view="order_totals",
                    table="order_totals",
                    key_columns=("customer",),
                )
            ],
        )

        recorded: list[tuple[StepEvent, object, Batch]] = []

        def on_event(event: StepEvent, result: object) -> None:
            recorded.append((event, result, output_connector._batch(event)))

        status = output_connector.run(stop_after_idle=4.0, on_event=on_event)

        guard = lake.read_guard(output_group)
        committed = [
            batch
            for _event, result, batch in recorded
            if result.applied and batch.batch_id == guard.batch_id
        ]
        assert committed, f"no published batch matches the committed guard {guard!r}"

        yield Stack(
            lake=lake,
            client=client,
            pipeline=pipeline,
            generation=generation,
            output_group=output_group,
            expected=_expected_totals(),
            status=status,
            final_batch=committed[-1],
            guard=guard,
            input_state=input_state,
        )
    finally:
        _cleanup_pipeline(client, pipeline)
        client.close()
        lake.close()


def test_happy_path_aggregate_matches_and_frontier_advanced(stack: Stack) -> None:
    actual = {customer: total for customer, total in _output_rows(stack.lake)}
    assert actual == stack.expected
    assert actual == {"a": 15.0, "b": 7.0, "c": 2.5}

    guard = stack.lake.read_guard(stack.output_group)
    assert guard.frontier > 0
    assert guard.batch_id is not None
    assert guard.frontier == stack.guard.frontier == stack.final_batch.end
    assert stack.status.applied >= 1


def test_replaying_committed_batch_is_a_duplicate_noop(stack: Stack) -> None:
    before_rows = _output_rows(stack.lake)
    before_guard = stack.lake.read_guard(stack.output_group)

    result = stack.lake.publish(stack.final_batch)

    assert result.decision is Decision.DUPLICATE
    assert _output_rows(stack.lake) == before_rows
    assert stack.lake.read_guard(stack.output_group).to_row() == before_guard.to_row()


def test_stale_and_wrong_generation_are_refused(stack: Stack) -> None:
    guard = stack.lake.read_guard(stack.output_group)
    assert guard.frontier > 0

    stale = Batch(
        output_group=stack.output_group,
        generation=stack.generation,
        epoch=guard.epoch,
        base=guard.frontier - 1,
        end=guard.frontier + 1,
        deltas=(),
    )
    assert stack.lake.publish(stale).decision is Decision.STALE

    wrong_generation = Batch(
        output_group=stack.output_group,
        generation="some-other-generation",
        epoch=guard.epoch,
        base=guard.frontier,
        end=guard.frontier + 1,
        deltas=(),
    )
    assert stack.lake.publish(wrong_generation).decision is Decision.WRONG_GENERATION

    # The connector raises PublicationRefused instead of returning a decision.
    connector = OutputConnector(
        stack.lake,
        stack.client,
        pipeline=stack.pipeline,
        output_group=stack.output_group,
        generation="some-other-generation",
        specs=[
            OutputSpec(
                view="order_totals",
                table="order_totals",
                key_columns=("customer",),
            )
        ],
    )
    connector.epoch = guard.epoch
    with pytest.raises(PublicationRefused):
        connector.publish_event(
            StepEvent(kind="step", base=guard.frontier, end=guard.frontier + 1, updates=())
        )


@dataclass
class InputStack:
    lake: DuckLake
    client: FelderaClient
    pipeline: str
    generation: str
    spec: InputSpec
    state_dir: Path


@pytest.fixture
def input_stack(tmp_path: Path):
    run = uuid.uuid4().hex[:8]
    pipeline = f"e2e_in_{run}"
    generation = f"gen-in-{run}"

    lake = _make_lake(tmp_path)
    client = FelderaClient(FELDERA_URL)
    try:
        _start_pipeline(client, pipeline)
        lake.execute(f"INSERT INTO dl.main.orders VALUES {LOAD_ORDERS_SQL}")
        yield InputStack(
            lake=lake,
            client=client,
            pipeline=pipeline,
            generation=generation,
            spec=InputSpec(source_table="orders", feldera_table="orders", schema="main"),
            state_dir=tmp_path / "input-state",
        )
    finally:
        _cleanup_pipeline(client, pipeline)
        client.close()
        lake.close()


def _input_connector(stack: InputStack) -> InputConnector:
    return InputConnector(
        stack.lake,
        stack.client,
        pipeline=stack.pipeline,
        generation=stack.generation,
        specs=[stack.spec],
        state_dir=stack.state_dir,
        require_completion_token=True,
    )


def test_input_restart_does_not_replay_consumed_changes(input_stack: InputStack) -> None:
    first = _input_connector(input_stack)
    progress = first.poll_once()

    assert progress[0].rows == len(SOURCE_ROWS)
    assert progress[0].batches == 1
    checkpoint = first.progress["orders"].checkpoint
    assert checkpoint is not None
    assert checkpoint.source_snapshot > 0
    assert first.uncertain() == {}

    # Re-run with the same state_dir: the checkpoint must be honoured and no
    # already-consumed change may be pushed again.
    second = _input_connector(input_stack)
    restarted = second.poll_once()

    assert restarted[0].batches == 0
    assert restarted[0].rows == 0
    assert second.progress["orders"].checkpoint == checkpoint
    assert second.uncertain() == {}


def test_recover_fails_on_uncertain_batch_and_rebootstraps(tmp_path: Path) -> None:
    lake = _make_lake(tmp_path)
    client = FelderaClient(FELDERA_URL)
    try:
        state_dir = tmp_path / "uncertain-state"
        # A batch record with no matching "committed" record is uncertain: the
        # process may have died between journaling and acknowledgement.
        journal = Journal(state_dir / "orders")
        journal.append_batch(
            source_table="orders",
            source_start=1,
            source_end=2,
            deltas=[Delta(table="orders", op=Op.INSERT, row={"id": 99, "customer": "z"})],
            batch_id="gen-uncertain:1->2",
        )

        connector = InputConnector(
            lake,
            client,
            pipeline="irrelevant",
            generation="gen-uncertain",
            specs=[InputSpec(source_table="orders", feldera_table="orders")],
            state_dir=state_dir,
        )

        assert connector.uncertain() == {"orders": [1]}

        with pytest.raises(JournalError):
            connector.recover("fail")
        with pytest.raises(JournalError):
            connector.recover("not-a-policy")

        connector.recover("rebootstrap")
        assert connector.progress["orders"].checkpoint is None
        assert connector.uncertain() == {}
        archived = list((state_dir / "orders").glob("*.superseded"))
        assert archived, "rebootstrap should archive the uncertain journal"
    finally:
        client.close()
        lake.close()


def test_incremental_poll_does_not_duplicate_rows(input_stack: InputStack) -> None:
    """Regression guard for a real double-count bug.

    DuckLake's ``ducklake_table_changes`` is inclusive on *both* bounds, so a
    poll that forwards its checkpoint snapshot unchanged re-emits the changes of
    the snapshot it was taken at.  After a bootstrap those rows are already in
    Feldera, so the failure is a silent double count rather than an error: the
    assertion that matters is on the state Feldera actually holds.
    """
    connector = _input_connector(input_stack)
    connector.poll_once()

    input_stack.lake.execute(
        f"INSERT INTO {input_stack.lake.qualified('orders')} VALUES (5, 'a', 1.5)"
    )
    # ``poll_once`` returns the connector's live progress objects, and ``rows``
    # is cumulative, so the count has to be read before the second poll.
    second = connector.poll_once()

    assert second[0].batches == 2

    in_feldera = input_stack.client.adhoc_query(
        input_stack.pipeline, "SELECT id, customer, amount FROM orders ORDER BY id"
    )
    assert [(row["id"], row["customer"], row["amount"]) for row in in_feldera] == [
        *SOURCE_ROWS,
        (5, "a", 1.5),
    ]
