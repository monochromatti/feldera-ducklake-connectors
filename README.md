# feldera-ducklake-connectors

Move data between a [DuckLake](https://ducklake.select) catalog and a
[Feldera](https://feldera.com) pipeline without losing rows or writing them
twice — including when a process dies in the middle of the handoff.

```
DuckLake source table ──change feed──▶ InputConnector ──HTTP ingress──▶ Feldera
                                                                          │
DuckLake output table ◀──guarded publication── OutputConnector ◀──egress──┘
```

Both directions are ordinary Python you can run from a script, a CLI, or a
cron job. Neither needs a Feldera connector binary, an enterprise license, or a
second control plane.

## The problem this solves

A Feldera pipeline and a DuckLake catalog cannot commit together. The engine
reports a step as processed in one HTTP response; the lake commits in another;
the process can die between the two. Naively wiring the two together gives you
the classic pair of bugs:

- **Lost output** — the engine reports success, the writer crashes before
  committing, and the step is never retried.
- **Duplicated output** — the writer retries a step whose commit actually
  landed, and the rows are applied twice. With `SUM`/`COUNT` views that is
  silent corruption, not a visible error.

This repository fixes that with a **guard row**: a single protected row in the
catalog, updated in the *same* DuckLake transaction as the output it describes,
which makes a publication atomic and a retry recognisable.

## Quickstart

No Feldera instance required — this runs the protocol against real, throwaway
DuckLake catalogs in-process:

```bash
git clone https://github.com/monochromatti/feldera-ducklake-connectors
cd feldera-ducklake-connectors
devenv shell          # or: uv sync --group dev
uv run fd-connectors validate-local
```

```text
PASS  guard/apply-advances-frontier
...
18/18 scenarios passed
```

That is the check to run before and after touching the connectors. To go
end-to-end against a live stack instead:

```bash
feldera-up                                 # pipeline-manager on :18080
uv run pytest tests/test_end_to_end.py -q  # skips itself if no stack is up
```

## Use it

### DuckLake → Feldera

```python
from fd_connectors import DuckLake, FelderaClient, InputConnector, InputSpec

lake = DuckLake(catalog="ducklake:postgres:dbname=ducklake host=127.0.0.1")
client = FelderaClient("http://127.0.0.1:18080")

connector = InputConnector(
    lake,
    client,
    pipeline="orders",
    generation="gen-1",
    specs=[InputSpec(source_table="orders", feldera_table="orders")],
    state_dir="./state",          # durable journal + checkpoint
)
connector.recover("fail")         # refuse to guess about an unacknowledged batch
for progress in connector.run(interval=1.0):
    print(progress[0].rows)
```

Each batch is journaled *before* Feldera can see it, and the checkpoint advances
only *after* the engine reports the data processed. A batch that was journaled
but never acknowledged is left uncertain rather than replayed blindly.

### Feldera → DuckLake

```python
from fd_connectors import DuckLake, FelderaClient, OutputConnector, OutputSpec

lake = DuckLake(
    catalog="ducklake:postgres:dbname=ducklake host=127.0.0.1",
    tables=("order_totals",),
    key_columns={"order_totals": ("customer",)},
)
client = FelderaClient("http://127.0.0.1:18080")

connector = OutputConnector(
    lake,
    client,
    pipeline="orders",
    output_group="summary",       # views that must land together
    generation="gen-1",
    specs=[OutputSpec(view="order_totals", table="order_totals")],
)
connector.prepare()               # create/claim the guard row, build the assembler
connector.run()                   # stream, publish each completed step, return when idle
```

Retrying an already-committed step is a no-op. A step whose payload differs from
the one already committed is reported as divergence instead of quietly winning.

## How it works

For an output group — one or more views that must land together — the catalog
holds exactly one **guard row**: `(output_group, generation, epoch, frontier,
batch_id, digest)`.  The first four decide *whether* a batch may be applied;
the last two make a legitimate replay distinguishable from a republish of the
same interval with different content.
A batch carries `(generation, epoch, base, end, payload)` and is applied by a
single transaction that:

1. re-reads the guard;
2. refuses a foreign generation, a stale owner, or a `base` that does not match
   the committed `frontier`;
3. applies the payload and advances the frontier together;
4. commits.

Ownership is an `epoch` on that same row, so a takeover and every competing
publisher serialize through it. Because the payload and the guard move in one
transaction, a concurrent reader sees either both or neither.

The input side is the mirror image: a durable journal in front, an
acknowledgement-driven checkpoint behind, and an explicit `recover()` for the
window in between.

## Validated against

| Component | Version exercised |
|---|---|
| Feldera pipeline-manager | `0.374.0` (community edition, container) |
| DuckDB | `2.0.0` |
| DuckLake | official extension bundled with that DuckDB build |
| Postgres | `17` (for production-shaped catalogs; a DuckDB file works too) |

- **18 protocol scenarios** (`validate-local`) against real DuckLake catalogs,
  each checked with a second cursor so a publisher that forgets the guard update
  fails: atomic visibility, rollback, retry idempotence, fencing, competing
  publishers, keyed vs multiset replacement, and the append-only-receipt
  lost-update counterexample.
- **25 tests** (`pytest tests/`), including six end-to-end tests driving a real
  pipeline-manager and a real DuckLake catalog: bootstrap → aggregate view →
  guarded publication, duplicate/stale/foreign-generation refusal, restart
  without replay or loss, and incremental polling that leaves exactly one copy
  of every row in Feldera.

## Limitations

- **Engine-side crash recovery is out of scope.** These connectors supply the
  connector-side contract (journalled input, idempotent publication). Feldera's
  own checkpointing is enterprise-only, so a restart without it replays from the
  guard frontier — safe because publication is idempotent, but not free.
- **A DuckDB-file catalog is single-writer.** Use a PostgreSQL-backed catalog for
  cross-process access, as the tests do.
- **`send_snapshot` is assumed** unless egress streams are attached while the
  pipeline is paused. The assembler refuses a stream whose first boundary is not
  the published frontier rather than guessing which rows already landed.
- **Row codecs are checked, not proven.** `codec.py` verifies round trips for
  supported types and rejects the rest rather than encoding lossily.
- **A process killed mid-push is tested at the journal level, not by signal.**
  The recovery paths are exercised against a hand-written uncertain journal.

## Repository layout

| Path | Contents |
|---|---|
| `src/fd_connectors/protocol.py` | Pure protocol: `Batch` identity, `Delta`, `Guard`, the decision function. No I/O. |
| `src/fd_connectors/catalog.py` | DuckLake access: snapshots, change feed, guarded publication transaction. |
| `src/fd_connectors/input_connector.py` | `DuckLake → Feldera`, with replayable batches. |
| `src/fd_connectors/output_connector.py` | `Feldera → DuckLake`, with step assembly and guarded publication. |
| `src/fd_connectors/journal.py` | Durable input journal and checkpoint (fsync'd, atomic rename). |
| `src/fd_connectors/codec.py` | SQL ↔ JSON codecs with round-trip checks; rejects unsupported column types. |
| `src/fd_connectors/feldera.py` | Minimal Feldera REST client (pipelines, ingress, egress, completion tokens). |
| `src/fd_connectors/scenarios.py` | The 18 SQL scenario checks behind `validate-local`. |
| `src/fd_connectors/cli.py` | `fd-connectors` entry point. |
| `tests/` | Unit tests for step assembly and the egress wire format, plus end-to-end tests against a live stack. |
| `formal/` | The vendored Lean 4 development the protocol comes from, checksummed in `formal/MANIFEST.json`. Read `formal/REPORT.md` first. |
| `docs/design.md` | Which theorem justifies which code path, and what the model does *not* cover. |
| `AGENTS.md` | Working rules for agent-assisted changes. |

## CLI

```bash
fd-connectors validate-local [--only PREFIX] [--json]
fd-connectors guard-show --catalog 'ducklake:meta.ducklake' --output-group summary
fd-connectors input-run  --feldera-url URL --pipeline P --generation G \
    --source-table orders --feldera-table orders --state-dir ./state \
    --catalog 'ducklake:meta.ducklake'
fd-connectors output-run --feldera-url URL --pipeline P --generation G \
    --output-group summary --view order_totals:order_totals \
    --catalog 'ducklake:meta.ducklake'
```

## Development

```bash
devenv shell   # python 3.13, uv, duckdb CLI, psql, postgres, lean4, docker-client
fd-test        # unit tests
fd-scenarios   # protocol scenarios against throwaway DuckLake catalogs
feldera-up     # start the Feldera pipeline-manager container on :18080
fd-e2e         # end-to-end test against the live stack (skips if absent)
```

The e2e module **skips silently** when no pipeline-manager answers `$FELDERA_URL`,
so check that it actually ran rather than passing vacuously. Re-check the proof
model with Lean 4.19.0 via `cd formal && bash verify.sh`.

## Operating the local stack

Traps that cost real time and look like code failures:

- **A full disk makes every compile fail.** The pipeline-manager accumulates
  compiled artifacts; when the container's filesystem fills, *every* pipeline
  fails with `program failed to compile` / `RustError`, including
  `CREATE TABLE t (x INT);`. Check before debugging code:
  `docker exec feldera df -h /`, then clear the cache with
  `docker exec feldera bash -c 'rm -rf /home/ubuntu/.feldera/compiler/*'`.
  A leaked cache reached 33 GB in one session.
- **A compile failure against a trivial program is never your SQL.** Reproduce it
  with a minimal pipeline before suspecting the connectors.
- **Cleanup is best-effort.** Community edition returns `501 EnterpriseFeature`
  for a graceful stop (`FelderaClient.delete` force-stops first for this reason),
  and refuses to delete a pipeline that is not fully stopped (`400
  DeleteRestrictedToFullyStopped`). Pipelines left in a failed state may be
  undeletable; they are harmless but they accumulate.
- **Do not mix JSON body config with query parameters** on the egress endpoints,
  and note that `send_snapshot: true` requires a *materialized* output view.
- **Verifying a checkout in isolation:** the editable install is a plain `.pth`,
  so `PYTHONPATH=<tree>/src` wins — but assert `fd_connectors.__file__` points
  into that tree, or you will silently test the wrong copy.

## Provenance

The publication protocol is not invented here. It comes from a Lean 4 development
(38 checked declarations: 24 positive theorems and 14 counterexamples, no
`sorry`, `admit`, new `axiom` or `native_decide`, axiom use limited to `propext`
and `Quot.sound`) together with nine SQL witnesses, vendored unmodified in
[`formal/`](formal/) with per-file SHA-256 checksums. `formal/REPORT.md` is the
authoritative record of what was proved, under which assumptions, and what
remains unverified; [`docs/design.md`](docs/design.md) maps each theorem to the
code path that discharges it. `validate-local` is the executable descendant of
those nine SQL witnesses.

One caveat about this repository's own history: it was **reconstructed** as an
atomic, linear split of an earlier two-commit history, and some intermediate
`__init__.py` contents were derived mechanically. Commit order is a reading
order, not a chronological record — do not infer development sequence from
`git log` or `git blame`.