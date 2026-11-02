# TODO: native Feldera connector for DuckLake, in Rust

**Status:** not started. This file is the complete brief for the port.
**Audience:** an agent or engineer with no access to the conversation that produced this repo.
**Predecessor:** the Python implementation in `src/fd_connectors/` (a validated *external*
bridge) plus the proof bundle in `formal/`.

Read, in this order, before writing any Rust:

1. `formal/REPORT.md` — what was proved, under which assumptions, and what was not.
2. `docs/design.md` — theorem → code path mapping; the trust boundary.
3. `src/fd_connectors/protocol.py` — the entire protocol is the pure function `decide`.
4. `src/fd_connectors/catalog.py`, especially `publish` (`catalog.py:380`) — the atomic commit.
5. `AGENTS.md` — repo working rules; they still apply here.

---

## How to start

**Repo identity.** This repository is `monochromatti/feldera-ducklake-connectors`
(https://github.com/monochromatti/feldera-ducklake-connectors), normally checked out at
`~/Code/feldera-ducklake-connectors`. It is the **specification of record**, not the port
target. Nothing here is rewritten into Rust in place.

**Two checkouts, side by side:**

```
~/Code/feldera/                      # the Feldera workspace — the port target
~/Code/feldera-ducklake-connectors/  # this repo — the spec, read-only in practice
```

```bash
cd ~/Code && git clone https://github.com/feldera/feldera
cd feldera && git switch -c ducklake-connector
cargo build -p pipeline-manager          # baseline build BEFORE any edits
```

**Pin what you built against.** Record the upstream commit SHA and the pipeline-manager
version under §8. The wire format in `tests/test_wire_format.py` was captured against
pipeline-manager **0.374.0**; if upstream `main` has since moved the egress protocol, that
capture — not the new engine — is the contract. Resolve any divergence explicitly and in
writing rather than regenerating the capture, which would silently discard the ground
truth the encoder is built on.

**This is a multi-session project.** The closest analogue (`delta_table`) is ~900 KB of
Rust. Do **not** attempt the five stages of §3 in one sitting; you will either thrash or
fake a completion. Stop at each boundary with a written result:

1. **Session 1 — the §2 spike verdicts, then stop.** S1 is a go/no-go: if `libduckdb`
   cannot be linked into the connector, report that and build nothing.
2. **Session 2 — Stage A**, plus a no-op endpoint that compiles, so the enum variant,
   feature flag and config plumbing are proven end to end before real logic lands.
3. **Session 3+ — Stage B**, then C, D, E.

**Check your permissions before promising anything:** network access to crates.io and to
fetch the DuckLake extension binary, and `docker` for the local stack. `gh auth status`
is needed for option (B) only. If any is unavailable, say so in the session-1 result
instead of working around it silently.

**Commit conventions differ by repo.** In *this* repo: Conventional Commits, atomic,
linear (`git log` is a reading order). In the *fork*: follow upstream `CONTRIBUTING.md`.

---

## 0. BLOCKING FINDING: there is no connector plugin mechanism

**Do not start by creating a standalone connector crate. It cannot work.** Feldera does
not load connectors dynamically. Verified against `feldera/feldera` at `main`:

- The transport is a **closed enum**, `pub enum TransportConfig` at
  `crates/feldera-types/src/config.rs:3864`, with ~22 variants (`FileInput`, `KafkaInput`,
  `DeltaTableOutput`, `PostgresCdcInput`, `AdHocInput`, `ClockInput`, …). Adding a
  connector means adding an enum variant here.
- Endpoints are instantiated by **exhaustive `match` arms**, not a registry:
  `create_integrated_output_endpoint` in `crates/connectors/src/integrated.rs` matches
  `&connector_config.transport` and constructs e.g. `delta_table::DeltaTableWriter`.
- There is **no dynamic loading**: no `libloading`, no `dylib`, no `plugin` dependency in
  `crates/connectors/Cargo.toml` or the root `Cargo.toml`.
- Integrated connectors are **feature-gated**: `with-deltalake`, `with-dynamodb`,
  `with-postgres-cdc`.

**Consequence.** "A native connector" here means *a patch to the Feldera workspace*, not a
separate project. Two delivery options, identical in implementation, different in output:

- **(A) Maintained fork** (pragmatic default). Vendor/patch `feldera/feldera` on a branch
  (e.g. `ducklake-connector`), build the pipeline-manager from it, use it locally. The
  fork must be re-based on upstream periodically. This is the only option that stays
  self-contained.
- **(B) Upstream PR.** Same patch, offered to `feldera/feldera`. Expect review to push
  back hard on a `libduckdb` FFI dependency inside the engine process (see Spike S1).

Either way the code lives in `crates/connectors/src/integrated/ducklake/` plus the enum
variant. **Record which option was chosen at the top of this file when you start.**

If you pursue option (B), read `CONTRIBUTING.md` **and `AI_POLICY.md`** in the Feldera
repository before writing code. That policy file exists and was not read in preparing this
brief — do not assume it is permissive of agent-authored contributions, and do not treat
this TODO as authorization to open an upstream PR.

**Closest analogue to copy:** the Delta Lake integrated connector,
`crates/connectors/src/integrated/delta_table/` — `output.rs` 228 KB, `input.rs` 325 KB,
`test.rs` 367 KB. That is the realistic scale of this work (roughly 900 KB of Rust for
Delta Lake), and it is the structural template: an integrated connector implements both
the transport endpoint and the `Encoder` trait because its protocol and format are
tightly coupled.

---

## 1. What must NOT change

The value of the existing work is a *validated protocol*; the Rust port is a
re-implementation of its transport, not a redesign. These invariants are proved in
`formal/Connectors.lean` and exercised by 18 scenarios. Breaking any of them silently
destroys the reason this exists.

### 1.1 The atomic commit (the core contract)

`catalog.py:380` `publish` runs, in **one** DuckDB transaction:

```
BEGIN TRANSACTION
read guard row for output_group
decision = decide(guard, batch)          # pure; see 1.2
if decision is not APPLY: ROLLBACK, return
UPDATE guard SET frontier=?, batch_id=?, digest=?, updated_at=now()
  WHERE output_group=? AND generation=? AND epoch=? AND frontier=?
-- read the guard back; if it did not advance to (end, batch_id), raise:
--   "guard for ... did not advance to ...; a concurrent publisher changed it"
apply payload deltas
COMMIT
-- on write-write conflict: retry, with the SAME batch identity
```

Three properties to preserve exactly:

- **Conditional guarded update (compare-and-swap), not a lock.** The `WHERE` clause
  carries `output_group, generation, epoch, frontier`. Writes nothing if another writer won.
- **Read-back verification.** The conditional UPDATE is *not* trusted; the guard is
  re-read inside the transaction and a mismatch raises. This is what makes
  `concurrent_append_receipts_are_not_deduplication` unrepresentable.
- **Retry with identical batch identity**, up to `max_attempts = 8`. A retry after a lost
  acknowledgement must be a no-op, which is only true because identity is checked *before*
  staleness (1.2).

### 1.2 `decide`: check order is load-bearing

`protocol.decide(guard, batch)` must be ported **verbatim, as a pure function with no
I/O**, including the order of checks: **identity first, then base/frontier staleness.**
A retry carries the same identity while the guard has moved on, so a naive
"is it stale?" check first would reject a legitimate retry. `protocol.py` has no I/O for
exactly this reason — keep it that way in Rust so it stays unit-testable without a
circuit.

`payload_digest` must also sort deltas before hashing: Feldera chunk order is not
meaningful, so an unsorted digest makes replay look like a different step
(counterexample `unordered_chunk_replay_changes_step`).

### 1.3 The two defects already found — regress them

Both were real bugs found by validation. They must not reappear in Rust; each needs a
Rust test:

- **Inclusive change interval.** DuckLake `table_changes(start, end)` includes **both**
  endpoints, so the next poll must start at `start_snapshot + 1`. Using `end` as the next
  `start` re-delivers the boundary snapshot every poll. Python assertions:
  `tests/test_end_to_end.py::test_incremental_poll_does_not_duplicate_rows`, scenario
  `source/change-interval-is-not-replayed`.
- **Undeclared `pytz`.** DuckDB's `DuckLake.snapshots()` raises
  `InvalidInputException: Required module 'pytz' failed to import` when `pytz` is absent
  (`catalog.py:199-207`, reached from `check_history` at `input_connector.py:179`). A
  spike must confirm the Rust side has an equivalent dependency story for timezone
  handling.

---

## 2. Blocking spikes — do these FIRST, in order

Do not write connector code until S1 and S2 have a written verdict appended to this file.
S1 can invalidate the whole approach.

### S1 — Can DuckDB + the DuckLake extension run inside the pipeline process?

DuckLake is a **DuckDB extension**; every catalog mutation is SQL executed by DuckDB. So
the native connector needs `libduckdb` in-process (FFI) or it must re-implement the
DuckLake catalog, which would invalidate the formal model's "atomic durable target
commits" assumption and require re-verification.

Answer for the Rust side:

- Is DuckDB already used anywhere in the workspace? (Expected: no — verify, don't assume.)
- Can `libduckdb` be linked (a `duckdb` crate, or vendored C++ via CMake)? Feldera's
  connector build already pulls in CMake as a dev dependency, so toolchain-wise this is
  plausible — but confirm.
- Is the **DuckLake extension** loadable from a Rust process (autoload vs explicit
  `LOAD ducklake`), and are its extension binaries portable enough to ship?
- Does the CMake build of `libduckdb` clash with Feldera's own CMake-based build
  (`runtime_compat.cpp` in `formal/` is unrelated; ignore it).

**If S1 fails, stop and escalate** — option (B) upstream PR is very unlikely to be
accepted with a DuckDB FFI inside the engine.

### S2 — Single-writer atomicity

The protocol's commit is a DuckDB **write transaction**. DuckDB allows one writer per
database file/process. Determine:

- Must the connector hold an exclusive DuckDB connection for the life of the pipeline?
  If so, is that acceptable for the intended deployment (one pipeline per catalog)?
- Does the guard-row CAS plus read-back still hold when the writer is the engine process?
- What happens to in-flight transactions on engine shutdown/restart?

If exclusivity is unacceptable, the protocol's atomicity assumption is broken for this
deployment and that must be escalated — it is a design change, not an implementation bug.

### S3 — Map engine fault tolerance onto the protocol

The Python input connector journals to a local `state_dir` (`journal.py`, `Journal`,
`write_atomic`, `uncertain_batches` at `journal.py:201`) and recovers connector-side
(`input_connector.recover` at `input_connector.py:203`, `uncertain()` at
`input_connector.py:193`). Native should instead use the engine's own machinery. Verify:

- `feldera_adapterlib::transport`: `InputEndpoint` (`transport.rs:41`),
  `TransportInputEndpoint` (`:60`), `IntegratedInputEndpoint` (`:95`), `InputReader`
  (`:851`), `InputConsumer` (`:950`), `InputQueue` (`:370`), `Resume` (`:1225`),
  `InputQueueEntry` (`:294`), `InputReaderCommand` (`:178`).
- How `Resume` and checkpointing interact with **exactly-once** delivery to the endpoint,
  and whether the guard row survives an engine restore. The Python design deliberately
  leaves "real process kill mid-push" untested (see README *Limitations*); native should
  close that gap, not inherit it.

---

## 3. Implementation stages

Each stage must leave the workspace building and its tests green on its own — the same
rule `AGENTS.md` sets for this repo.

| # | Stage | Deliverable | Gate |
|---|-------|-------------|------|
| A | Scaffolding | `TransportConfig::DuckLakeInput` / `DuckLakeOutput` in `crates/feldera-types/src/config.rs`; `with-ducklake` feature; config structs | workspace compiles; config round-trips through JSON (see `openapi.json`) |
| B | Output connector | `crates/connectors/src/integrated/ducklake/`: implements `OutputEndpoint` + `Encoder` (mirror `IntegratedOutputEndpoint`); publishes the guard row from §1.1 | engine-free unit tests (the `dbsp_connectors` suite pattern) |
| C | Input connector | `TransportInputEndpoint` + `InputReader`/`InputConsumer`; change-interval tracking with `start_snapshot + 1`; bootstrap vs incremental | engine-free unit tests |
| D | Engine tests | `#[engine_test]` cases registered in `crates/static-connectors`, `requires(...)`/`services(...)` as needed | `cargo test -p static-connectors --test connector_suites` |
| E | Docs | connector page under `docs.feldera.com/docs/connectors/sources/` + a sink page; note the DuckLake schema contract | docs build |

Stage B before C: the output connector is where the atomic commit lives, and it is the
half that can be tested without a second system.

---

## 4. Python → Rust porting map

| Python | Lines | Rust target | What must survive |
|---|---|---|---|
| `src/fd_connectors/protocol.py` | 486 | a pure module (no I/O) | `decide` check order; `advance`; `validate`; `payload_digest` with sorted deltas |
| `src/fd_connectors/catalog.py` | 498 | DuckLake access layer (FFI) | `publish` transaction §1.1; `table_changes` +1 rule; second-cursor reads |
| `src/fd_connectors/codec.py` | 156 | `Encoder` implementation | `check_round_trip`; `reject_unsupported_column_types` |
| `src/fd_connectors/output_connector.py` | 494 | Stage B endpoint | `prepare`/frontier; step assembly; `send_snapshot` requires a **materialized** output view |
| `src/fd_connectors/input_connector.py` | 315 | Stage C endpoint | bootstrap vs incremental; `check_history`; completion-token discipline |
| `src/fd_connectors/journal.py` | 216 | engine checkpoints (S3) | atomicity of the journal write; "uncertain" must fail closed, not guess |
| `src/fd_connectors/feldera.py` | 457 | **delete** — the REST client is what native replaces | ingress/egress REST calls disappear entirely |
| `src/fd_connectors/scenarios.py` | 704 | Rust analogues of all 18 scenarios | see §5 |
| `src/fd_connectors/cli.py` | 202 | optional debug tool | — |

`tests/test_wire_format.py` (103 lines) holds a **verbatim capture of the live v0.374.0
egress wire format** — it is the ground truth for the `Encoder`. Do not re-derive it.

---

## 5. Definition of done

### How validation works for a native connector (read this first)

**The existing Python harness cannot validate the Rust connector.** `scenarios.py` drives
`src/fd_connectors/` directly, whereas a native connector is exercised through a pipeline
that declares `WITH ('connectors' = '[{"transport": {"name": "ducklake", ...}}]')`.
So "port the 18 scenarios" does **not** mean running `fd-scenarios` against Rust.

Three layers, increasing in cost:

1. **Engine-free unit tests** — `decide`, `payload_digest`, `advance`, codec round trips.
   These are pure, so test them exhaustively. Cheapest safety net; do this first.
2. **Catalog-level tests through libduckdb** — the guard-row CAS, the `start + 1` interval
   rule, resync, rollback. These are *catalog* semantics, not transport semantics, and can
   be tested without a circuit. Build the catalog exactly as `scenarios.py:80` does:
   `ATTACH 'ducklake:<tmpdir>/meta.ducklake'` (file-backed metadata; no Postgres needed).
3. **`#[engine_test]` connector suites** — end to end through a real pipeline.

The Python implementation stays a usable **oracle** for layers 1 and 2: its expectations
were validated against a live stack, so a Rust test disagreeing with a Python scenario is
a signal to investigate, never a reason to edit the Python scenario.

### The gates

- All 18 Python scenarios have Rust counterparts (`scenarios.py`), grouped as:
  `guard/{apply-advances-frontier, duplicate-retry-is-a-no-op, stale-publisher-is-refused,
  foreign-generation-is-refused, ownership-requires-explicit-takeover,
  divergent-payload-for-committed-identity-is-detected}`,
  `ducklake/{concurrent-publishers-converge-to-one-outcome,
  keyed-update-is-delete-then-insert, multiset-delete-removes-one-copy,
  payload-and-guard-are-atomic-on-payload-failure, reader-sees-payload-and-guard-together,
  resync-replaces-content-and-is-idempotent, rollback-leaves-history-replayable}`,
  `source/{change-interval-is-not-replayed, guard-update-is-conditional,
  identity-is-the-interval-not-the-digest}`,
  `protocol/decision-is-total`, `report/lost-update-counterexample`.
- Both defects in §1.3 have dedicated Rust regressions.
- `guard/divergent-payload-for-committed-identity-is-detected` and
  `ducklake/concurrent-publishers-converge-to-one-outcome` pass against **two concurrent
  engines**, not two cursors in one process. The Python version could only approximate
  this; native can do it properly.
- A demo pipeline in the fork that reads a DuckLake table into Feldera and writes a view
  back, run end to end against a real catalog.
- The Python repo is left in place as the specification of record; the Rust port does not
  delete it.

---

## 6. Environment

- This repo: `devenv shell` (python 3.13, duckdb CLI, psql, postgres, lean4, docker-client).
  `fd-scenarios` / `validate-local` remain the fastest way to check protocol intent —
  against the *Python* implementation. They are the spec's test suite, not the port's.
- The fork: `cargo build -p pipeline-manager` to build; `cargo test -p static-connectors
  --test connector_suites` for connector suites; engine-free suites run without a circuit.
- The Feldera fork: `rust-toolchain.toml` pins the toolchain. Connector-suite tests need
  **CMake**, plus **redpanda** and a **NATS server** binary (the existing suites declare
  which services they need via `services(...)` — declare only what DuckLake needs).
- Local stack for manual testing: `feldera-up` in this repo starts the pipeline-manager on
  `:18080`. **Known trap:** a full container disk makes *every* pipeline fail with
  `program failed to compile` / `RustError`, including `CREATE TABLE t (x INT);`. Check
  `docker exec feldera df -h /` and clear
  `/home/ubuntu/.feldera/compiler/*` before believing a compile failure. See README
  *Operating the local stack*.

---

## 7. Non-goals

- No changes to the proof model in `formal/` (checksummed, `AGENTS.md` forbids editing it).
  If the Rust port needs different assumptions, that is a finding to report, not a
  license to edit.
- Do not port `feldera.py`'s REST client; native removes that layer.
- Do not attempt multi-table version skew; it is unverified in the model and out of scope
  here too (see README *Limitations*).

---

## 8. Open decisions

Resolve each of these before starting the stage they gate. Defaults are given so that a
fresh agent is not blocked waiting for a human; changing a default is the human's call.

- [ ] **Delivery:** fork (A) vs upstream PR (B). §0. **Default: fork (A).**
- [ ] **Upstream SHA + pipeline-manager version** the fork is based on. **Default: upstream
      `main` at clone time — record the SHA, do not leave it implicit.**
- [ ] **S1 verdict:** is a DuckDB FFI dependency acceptable inside the engine? **Go/no-go;
      no default — a negative verdict ends the project as scoped.**
- [ ] **S2 verdict:** is single-writer DuckDB exclusivity acceptable for the target
      deployment? **Default: assume one pipeline per catalog, i.e. yes.**
- [ ] **Guard-table ownership:** does the connector provision `guard_table` itself (as the
      Python version does, via `catalog.provision_guard`), or is it a documented DuckLake-side
      requirement? **Default: the connector provisions it**, matching the validated behaviour.