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

- [x] **Delivery:** fork (A) vs upstream PR (B). §0. **Default: fork (A).** — **Resolved:
      fork (A)** (§9.1). Option (B) stays open on paper; `AI_POLICY.md` exists at the pinned
      SHA (HTTP 200) and was not read, so it is not a decision this session can make.
- [x] **Upstream SHA + pipeline-manager version** the fork is based on. **Default: upstream
      `main` at clone time — record the SHA, do not leave it implicit.** — **Resolved:
      `288cd64a192ed0da491eca97a02937e746e2d720`, 2026-10-31, workspace version `0.376.0`
      (no `v0.376.0` tag exists upstream; pin the SHA).**
- [x] **S1 verdict:** is a DuckDB FFI dependency acceptable inside the engine? **Go/no-go;
      no default — a negative verdict ends the project as scoped.** — **GO, with the stock
      `duckdb` crate: version `1.20000.1` already targets DuckDB `v2.0.0` (the second version
      component encodes it) and its default `download-lib` feature fetches the prebuilt
      library. §9.2.**
- [x] **S2 verdict:** is single-writer DuckDB exclusivity acceptable for the target
      deployment? **Default: assume one pipeline per catalog, i.e. yes.** — **YES. Two
      regimes, both compatible with the protocol; file catalogs enforce it mechanically,
      PostgreSQL catalogs enforce it at commit (§9.3).**
- [ ] **Guard-table ownership:** does the connector provision `guard_table` itself (as the
      Python version does, via `catalog.provision_guard`), or is it a documented DuckLake-side
      requirement? **Default: the connector provisions it**, matching the validated behaviour.
      **Still open.** §9.3 only established that a *publisher* can lose a concurrent write; whether
      two processes can race the initial `CREATE` of the guard row was **not** tested (the
      file-backed catalog cannot even be attached twice, so only a PostgreSQL catalog could
      exercise it). Keep the default and add a provisioning-race test in Stage B.

---

## 9. Session 1 result (S0/S1/S2/S3)

Date: 2026-11-02. **No connector code was written**, per the session rule. Everything below
is empirical: each number, checksum and error string was produced by a program in this
session, not quoted from the brief.

Harness: `~/Code/ducklake-ffi-spike` — a ~300-line Rust binary that hand-declares the
subset of the DuckDB C API it needs (`extern "C"`, `duckdb_open_ext`, `duckdb_query`, …)
and links `libduckdb` directly. It exists to answer S1/S2 without any connector scaffolding
and is throwaway; it is not the connector, and its hand-written FFI declarations exist only
because the harness started before the crate was checked — the connector should use the
`duckdb` crate instead (§9.2).

### 9.1 Environment, baseline build, S0

- **Baseline `cargo build -p pipeline-manager` is green** on macOS arm64 (artifact
  `~/Code/feldera_spike_clone/target/debug/pipeline-manager`, 162 MB) — but only with two
  workarounds, both needed in a fresh checkout:
  1. `WEBCONSOLE_BUILD_DIR=/tmp/webconsole-stub` — `crates/pipeline-manager/build.rs:49`
     otherwise runs the web-console build, which needs `bun` (absent) and fails. The env var
     is checked at :47 and prints `cargo:warning=WEBCONSOLE_BUILD_DIR is set, skipping
     web-console build`.
  2. The `rdkafka`/`openssl`/`zstd` native deps come from `nix shell nixpkgs#rdkafka
     nixpkgs#pkg-config nixpkgs#openssl nixpkgs#zstd`, not from Homebrew. There is no `bun`
     and no `pkg-config` on the default `PATH`.
  Record both; a session that skips them concludes the workspace does not build.
- **Checkout location:** the clone is at `~/Code/feldera_spike_clone`, **not** at the
  documented `~/Code/feldera` (that path does not exist on this machine). Stage A must
  create the fork at the documented path from SHA `288cd64a` and branch
  `ducklake-connector`; until then §How-to-start's paths are aspirational.
- `git describe`: `main` is 13 commits past `v0.374.0`; workspace version is `0.376.0`, for
  which **no tag exists** upstream. `target/` is 15 GB and the volume has 56 GiB free —
  tight enough that a second full build may not fit.
- Permissions (checked, per §How-to-start): `docker` **ok**, `gh auth status` **ok**
  (account `monochromatti`), network to crates.io, github.com and extensions.duckdb.org
  **ok**, `postgres` reachable. No permission gap to report.
- **S0 confirmed by grep, not assumption:** no `duckdb` string anywhere in
  `crates/*/Cargo.toml` or `crates/**/*.rs` in the workspace. DuckDB really is a new
  dependency.

### 9.2 S1 — GO, with the stock `duckdb` crate

**Correction (same session, second measurement).** An earlier version of this section
claimed the crates.io crate stops at DuckDB 1.2.0 and that the connector must hand-write its
bindings. **That was wrong**, and the error is worth recording because the version scheme is
what invites it: `1.20000.1` is not "DuckDB 1.2" — the second component encodes the DuckDB
version as an integer (`20000` → `2.0.0`), documented in
`crates/libduckdb-sys/src/build_version.rs`: `let duckdb_major = encoded / 10_000;` etc.
The `1.` prefix is a crate epoch, not DuckDB's major.

So there is no binding problem at all. Verified end to end in a throwaway crate:

```toml
[dependencies]
duckdb = "1.20000.1"     # default features: download-lib
```

```
cargo run   (22 s cold, no env vars, no CMake, no vendored dylib)
  library: v2.0.0
  library_version: v2.0.0
  LOAD ducklake: ok
  extension: ducklake 417a9117f5 true
```

- `libduckdb-sys`'s **default** feature set is `["capi-v1", "vcpkg", "pkg-config",
  "download-lib"]`, so the prebuilt library is fetched automatically, and the pin is
  explicit: `crates/libduckdb-sys/.duckdb-release` → `DUCKDB_RELEASE_VERSION=v2.0.0`,
  `DUCKDB_RELEASE_URL=https://install.duckdb.org/v2.0.0`,
  `DUCKDB_RELEASE_COMMIT=d54cbbe3df17e8e5ce68312f483b4155836112aa`. That file is the single
  source of truth for which DuckDB the crates target.
- **`duckdb-neo` `0.20000.1`** is the same project's wrapper for DuckDB's **v2 C API**
  ("experimental", `Environment`/`StorageLocation` handles instead of `duckdb_open_ext`) —
  also published, also pinned to v2.0.0. Worth evaluating for Stage A, but it is explicitly
  unstable, so the conservative choice is the `capi-v1` surface the `duckdb` crate exposes.
- **Linked-against-a-local-library mode has one rough edge:** with `DUCKDB_LIB_DIR` set by
  hand, `libduckdb-sys` emits no `LC_RPATH`, so the binary needs `DYLD_LIBRARY_PATH` (macOS)
  or `LD_LIBRARY_PATH` (Linux) at run time. The default `download-lib` path does not have
  this problem. Do not vendor the dylib by hand unless something forces it.

What survives from the original finding, because it is about the *extension*, not the
crate: DuckLake extension binaries are **per-platform and version-locked to libduckdb**. The
`LOAD` above worked by autoloading from the extension repository; a deployment that must not
reach the network still vendors one file per target and checksums it (see below). An
upstream PR (option B) would still be asked to admit a prebuilt C++ library into the engine
process — the supply-chain objection is unchanged, only its size.

Everything else about S1 is a clean yes, verified in a Rust process linked against
`libduckdb` **v2.0.0**:

- **Linking works with no CMake step at all**, which is what the crate's `download-lib`
  feature automates. DuckDB publishes prebuilt artifacts, so the feared "CMake clash with
  Feldera's build" does not arise — there is nothing to compile from C++ source:
  `duckdb-shared-libs-{linux,osx}-{amd64,arm64}.tar.gz` and
  `duckdb-static-libs-{linux,osx}-{amd64,arm64}.tar.gz` (`duckdb-static-libs-osx-arm64`
  present in the v2.0.0 release; `libduckdb-osx-arm64.zip`, the name used by older
  releases, is **404** — the shared/static-libs names replaced it).
- **The dylib is relocatable:** install name is `@rpath/libduckdb.dylib`, dependencies are
  `/usr/lib/libc++.1.dylib` and `/usr/lib/libSystem.B.dylib` only. Vendoring it and
  setting an rpath is sufficient; no absolute build-tree path leaks in.
- **The extension loads, autoloaded from the repository:**
  `LOAD ducklake` → `ducklake | true | 417a9117f5 | REPOSITORY` in `duckdb_extensions()`,
  with `extension_directory` pointed at a scratch dir so the download is explicit.
- **The extension also loads from a vendored absolute path**, with no repository access:
  `LOAD '/path/ducklake.duckdb_extension'` → `extension_version 417a9117f5`. The file is
  43,165,870 bytes, `sha256 = 78736bf48c045548444dd31ce8e5176dfbcb8df4eb58d8518ce93cb40c3a39bc`.
  Its `.info` sidecar records url
  `http://extensions.duckdb.org/v2.0.0/osx_arm64/ducklake.duckdb_extension.gz` and
  signature `782279e7bec2941a1b28276782068b4f`. **Vendoring is therefore viable, and it is
  the recommended shipping mode**: no network at pipeline start, checksum pinned in-tree.
- **The shipped binary is properly signed:** it loads with `allow_unsigned_extensions`
  left at its default `false`. The spike's `--no-unsigned` path is the proof; no
  security-weakening config is needed as long as the vendored file is the official build.
- **Extension binaries are per-platform**, so "portable enough to ship" means one vendored
  file per target (`linux_amd64`, `linux_arm64`, `osx_amd64`, `osx_arm64`,
  `windows_amd64` were all probed and all return 200 at v2.0.0). A build that ships one
  platform's file on another fails at `LOAD`.
- **Threading is manageable.** DuckDB connections are single-threaded objects, and the C
  API is not async, so the connector either keeps one connection on a dedicated thread and
  talks to it over a channel, or uses `tokio::task::spawn_blocking` per statement. Both fit
  the `OutputEndpoint`/`InputReader` shapes, whose methods are already allowed to block on
  a dedicated thread (see §9.4 — with the cancellation rule attached).

**Verdict: GO.** The engine can host `libduckdb` + DuckLake in-process, and the bindings
come from crates.io rather than from us. The cost is a vendored *extension* file per
target plus a pinned crate version — not a redesign of the protocol.

### 9.3 S2 — YES, and the two catalog backends fail differently

The question assumes one regime; there are two, and the protocol needs the difference.

**File-backed catalog (`ducklake:/path/meta.ducklake`, a DuckDB file) — hard exclusivity,
including readers.** A second *process* cannot even attach while the first holds the file,
and `(READ_ONLY)` does not help, because DuckLake opens the metadata catalog read-write
regardless:

```
IO Error: Failed to attach DuckLake MetaData "__ducklake_metadata_dl" at path + "/tmp/s2/meta.ducklake"
Could not set lock on file "/private/tmp/s2/meta.ducklake": Conflicting lock is held in
/Users/monochromatti/Code/ducklake-ffi-spike/target/debug/spike (PID 36005) by user monochromatti.
See also https://duckdb.org/docs/current/connect/concurrency
```

This is good news for the protocol: "one pipeline per catalog" is not a policy the
operator must respect, it is mechanically enforced by the storage. **Crash behaviour:**
`SIGKILL` mid-transaction (row inserted, `COMMIT` never reached) leaves the catalog
consistent — the row is absent, no snapshot was appended, and the OS releases the lock, so
the next process attaches normally. Uncommitted staging is invisible, as `stage_is_invisible`
requires.

**PostgreSQL catalog** (`ducklake:postgres:dbname=ducklake host=127.0.0.1 port=55432
user=postgres password=…`, the realistic production shape) — **no file lock; several
processes attach and write concurrently.** Verified with two independent OS processes
against the local `ducklake-pg` container:

- Both processes run `UPDATE … WHERE output_group='a' AND frontier=0` inside `BEGIN`, both
  see **1 affected row** (each read its own snapshot where the guard was still `0`), both
  call `COMMIT`. The loser is rejected *at commit*:
  ```
  TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.
  Transaction conflict - attempting to insert into table with index "2" - but another transaction has deleted inlined data from it
  ```
  Final state: `a | 2` — one winner, no lost update, no duplicate. The two-connection case
  in a *single* process is identical with `index "1"`.
- **The CAS in the `WHERE` clause alone is therefore not the safety net**; it cannot even
  see the other writer's uncommitted row, which is why both writers report 1 affected row.
  The net is (a) the catalog's commit-time conflict, which must be caught and retried with
  the **same** batch identity, and (b) the read-back inside the transaction.
- **Read-back works and is load-bearing.** A stale publisher on a *fresh* snapshot (no
  concurrency) gets `UPDATE … 0` rows, its `COMMIT` succeeds as a no-op, and the guard is
  unchanged; the in-transaction re-read sees the other publisher's frontier (`5`), which is
  the mismatch `publish` raises on. Both mechanisms are needed; neither is redundant.

**Consequences the port must honour (recorded so they are not rediscovered as bugs):**
1. The connector holds one connection (or one connection pool pinned to one thread) for the
   pipeline's lifetime. File-catalog deployments cannot have a second writer, and that is
   acceptable — it is what the storage does anyway.
2. Retry on commit-time conflict, up to 8 attempts, with identical
   `(generation, epoch, base, end, digest)`. "UPDATE affected 1 row" is **not** success.
3. Re-read the guard in the same transaction before `COMMIT`; on mismatch, roll back and
   treat as "someone else committed".
4. In-flight transactions die with the process and are rolled back by the catalog; the
   protocol does not need a recovery path for them — the identity check covers the retry.

**Verdict: YES**, unchanged default (one pipeline per catalog), with the added finding
that concurrent-writer correctness rests on commit-time conflicts plus read-back, not on
the conditional `WHERE`.

### 9.4 S3 — viable, but four engine facts change the connector design

Verified against `288cd64a`. The engine has the machinery, and using it removes
`journal.py` entirely — but the output side is *not* a place where returning an error is
enough.

**Input replay (good news).** The engine persists the seek position in each checkpoint and
journals every step since, with the replay payload and a checksum
(`crates/connectors/src/controller/journal.rs:11-45`, `InputLog { data: RmpValue, metadata:
JsonValue, checksums: InputChecksums }` at `:64-78`). A connector opts in with
`InputEndpoint::fault_tolerance() -> Option<FtModel>`, gets the position back through
`TransportInputEndpoint::open(..., resume_info)`, then receives one
`InputReaderCommand::Replay { metadata, data }` per journaled step and calls
`InputConsumer::replayed(amt, hash)` and
`extended(amt, Some(Resume::Replay { seek, replay, hash }), watermarks)` **for every step**
(`crates/adapterlib/src/transport.rs:1219-1225`). Checksum mismatch fails the circuit. So
the Rust port's "journal" is the engine's, and `Resume::seek()` (a `JsonValue`) is the only
connector state that survives — a DuckLake position expressible as JSON is exactly that.

**Four caveats:**
1. **Fault tolerance is enterprise-gated.** `crates/connectors/src/server.rs:945-947`
   refuses to start with `ControllerError::EnterpriseFeature("fault tolerance")` when
   `feldera-enterprise` is off and the config enables FT; `can_checkpoint()`
   (`crates/connectors/src/controller.rs:9708-9730`) additionally requires configured
   `storage` and every input endpoint reporting FT. An OSS build of the fork gets
   at-least-once, and the connector must not assume journaling exists.
2. **There is no connector-internal checkpoint hook and no "checkpoint committed"
   callback.** The only signal is `InputReader::queue(checkpoint_requested: bool)`, which
   means *this step is a barrier*, not *a checkpoint landed*. Anything not derivable from
   the persisted `seek: JsonValue` is lost on restore — a reason to keep the reader's
   position a pure function of catalog state.
3. **Output errors do not stop the pipeline, so the endpoint must hold.**
   `push_batch_to_encoder` (`crates/connectors/src/controller.rs:9123-9136`) and
   `OutputProbe` (`:10528-10587`) route failures to
   `output_transport_error`/`encode_error`, which only bump stats and call the error
   callback; `is_fatal_controller_error` (`crates/connectors/src/server.rs:1452-1460`)
   matches only `DbspError`/`DbspPanic`/`ControllerPanic`, and progress accounting advances
   regardless (`controller.rs:9286-9292` drives `steps_processed` from the circuit). **A
   failed publish is therefore counted as a processed step** — precisely the loss the
   protocol exists to prevent. The `OutputEndpoint` trait has no `flush`/`close`, and
   `batch_end` is the durability point that no shutdown path calls. The fix is the Kafka FT
   sink's pattern: report fatal, **hold the output thread until shutdown**, and stay
   cancellation-responsive via `ControllerInner::shutdown_token` +
   `CancellationToken::run_until_cancelled` — the trait's own doc says an unbounded wait
   hangs teardown (`crates/adapterlib/src/transport.rs:1440-1457`).
4. **Where the batch identity comes from, and the one config that breaks it.** There is no
   `EncoderMetadata` and no `Encoder::encode_txn` in this workspace (grep: no hits). The
   `Encoder` trait is two methods (`crates/adapterlib/src/format.rs:716-729`: `consumer()`,
   `encode(batch)`), and identity arrives through `OutputConsumer::batch_start(step, kind)`
   (`format.rs:737`), whose first argument is the **DBSP transaction id**, documented at
   `crates/connectors/src/controller.rs:6849-6852` as "reproduced deterministically across
   replay, unlike the physical step". With the default config that is exactly one batch per
   transaction, empty steps skipped (`controller.rs:9255-9263`) — the shape the protocol
   needs. With `enable_output_buffer: true`
   (`crates/feldera-types/src/config.rs:3787`, default `false`) batches are regrouped by
   size/time and `batch_start` receives only `output_buffer.buffered_transaction`,
   "Transaction of the last update in the buffer" (`controller.rs:7084-7086`, `:7153`;
   flush at `:9198-9215`). One publication would then span several transactions while
   claiming the last one's id, silently violating the identity rule `publish_idempotent`
   relies on. **The connector must reject `enable_output_buffer: true` in its config
   validation rather than tolerate it.**

**Verdict: usable.** Stage C is simpler than the Python version (no local journal). Stage B
carries a hard requirement the brief did not state: on any publish failure the endpoint
must block until cancelled rather than return, or the pipeline will report progress it did
not make.

### 9.5 Catalog behaviour found while spiking (supersedes assumptions in §1)

- **`UPDATE … RETURNING` is rejected on DuckLake tables:**
  `Binder Error: RETURNING clause not yet supported for updates of a DuckLake table`.
  `publish` must `UPDATE` then `SELECT` in the same transaction — which is what
  `catalog.py:380` already does, and what `formal/REPORT.md` records. Any Rust attempt to
  use `RETURNING` fails at the binder, not at runtime.
- **`ducklake_table_changes` is inclusive on both bounds, confirmed directly:**
  `ducklake_table_changes('dl','main','t',3,4)` and `(…,4,4)` return the **identical** row
  set, so using `end` as the next `start` re-delivers the boundary snapshot. This is §1.3's
  defect #1, reproduced in the Rust harness; the `start + 1` rule is not optional.
- **`ducklake_table_changes` has no `table_name` column** (`Binder Error: Referenced column
  "table_name" not found`); its columns are `snapshot_id, rowid, change_type, <table
  columns>`. Change rows are identified by `(snapshot_id, rowid)`.
- **`ducklake_snapshots('dl')` columns** are `snapshot_id, snapshot_time, schema_version,
  changes MAP(VARCHAR, VARCHAR[]), author, commit_message, commit_extra_info`.
  `ducklake_current_snapshot('dl')` returns a single column named **`id`**, not
  `snapshot_id`.
- **PostgreSQL attach syntax** (no file path):
  `ATTACH 'ducklake:postgres:dbname=ducklake host=127.0.0.1 port=55432 user=postgres
  password=…' AS dl`. The local container `ducklake-pg` (`postgres:16-alpine`, port 55432,
  password `duck`, database `ducklake`) serves this and is what the concurrency spike used.
- **The timezone hazard in §1.3 (defect #2, `pytz`) does not exist in this shape:** it is a
  *Python* dependency of `DuckLake.snapshots()`, reached through `catalog.py`'s JSON
  handling. Going through the C API reads `TIMESTAMP WITH TIME ZONE` as text, so the Rust
  side has no `pytz` equivalent to declare — but it inherits the formatting rule, and the
  `snapshot_time` string must not be parsed to decide protocol state (only the integer
  snapshot id is protocol state).

### 9.6 Wire-format divergence check (§How-to-start)

`v0.374.0 → 288cd64a` is 13 commits / 38 changed files, and only two of them touch
connector-adjacent code: `crates/connectors/src/integrated/delta_table/input.rs` and
`crates/feldera-types/src/config/ignored.rs`. Nothing under `crates/adapterlib/src/`
changed — `transport.rs`, `format.rs` (`Encoder`, `OutputConsumer`) and the controller's
egress path are identical. **The captured v0.374.0 egress format remains the contract; do
not regenerate `tests/test_wire_format.py`.**

### 9.7 What this changes in the rest of this brief

Recorded rather than silently editing the stages:

- **§3 Stage A** must pin the crate (`duckdb = "1.20000.1"`, `download-lib` by default)
  rather than invent bindings, and decide whether the extension is autoloaded or vendored
  (§9.2).
- **§3 Stage B** gains the "hold until cancelled on failure" requirement from §9.4.3, the
  retry/read-back rules from §9.3 as explicit tests, and a config check that refuses
  `enable_output_buffer: true` (§9.4.4).
- **§3 Stage C** loses the local journal: replay is `Resume::Replay` against the engine's
  journal, and the reader's position must be a pure function of catalog state.
- **§5 validation** can use the file-backed catalog for the two-publisher scenarios only in
  *one* process; across processes, only the PostgreSQL catalog exercises them (file
  catalogs refuse the second attach). Both belong in the suite: the file catalog proves the
  "one pipeline per catalog" regime, the PostgreSQL catalog proves the CAS.
- **§2's S2 question "must the connector hold an exclusive connection?"** is answered
  differently per backend — keep it that way in the docs rather than promising one answer.

### 9.8 Session 2 entry conditions

1. `git clone` (or copy) upstream to `~/Code/feldera`, `git switch -c ducklake-connector` at
   `288cd64a`, and rebuild the baseline there with the two workarounds from §9.1 so the
   first commit starts from a known-green tree.
2. Stage A only: `TransportConfig::DuckLakeInput`/`DuckLakeOutput`, `with-ducklake` feature
   (mirroring `with-deltalake` in `crates/connectors/Cargo.toml:21-28`), config structs, and
   a no-op endpoint that compiles — no catalog access yet.
3. Re-read §9.3 and §9.4 before writing Stage B: they carry requirements that are cheap to
   satisfy at design time and expensive to retrofit.
## 10. Session 2 result (Stage A)

Stage A is in the `feldera/` checkout, not here: `845e79d` on branch `ducklake-connector`
(parent `288cd64`, the pinned baseline of §9.1). The skeleton compiles and is registered; no
catalog is opened and every data path returns `unimplemented`, so nothing can silently lose
output.

**What exists now.** `crates/feldera-types/src/transport/ducklake.rs` carries the config
surface: `DuckLakeCatalogConfig` (metadata location vs. `jdbc:` Postgres, data path),
`DuckLakeWriterConfig` (output group, guard location, time travel bounds),
`DuckLakeReaderConfig` (table, schema, catalog, row limit). Registered in
`TransportConfig` (`crates/feldera-types/src/transport.rs:206-209`) — `#[serde(tag = "transport")]`
like the community connectors, not `#[serde(flatten)]` as in the enterprise ones — classified in
`crates/connectors/src/transport.rs:653-662` as an integrated connector, and added to the
`/pipeline` OpenAPI schema list in `crates/pipeline-manager/src/api/main.rs` (new transports do
not appear in `openapi.json` unless named there explicitly, then regenerated with
`pipeline-manager --dump-openapi`).

`crates/connectors/src/integrated/ducklake.rs` carries the programs: `DuckLakeWriter`
implementing `OutputConsumer` + `Encoder` (the delta-shaped pair — `batch_start`/`encode` see the
same step) and `OutputEndpoint::is_fault_tolerant() -> false`, and `DuckLakeInputEndpoint`
implementing `InputEndpoint` + `IntegratedInputEndpoint` with `fault_tolerance() =
AtLeastOnce` (§9.6: the change-feed range is closed, so the reader can promise at-least-once but
not exactly-once). The module docstring explains the layout choice and states what Stage B/C owe,
rather than describing the stubs.

**Gate.** `cargo test -p feldera-types --lib` 264 passed. `cargo check -p dbsp_connectors -p
rest-api -p pipeline-manager --all-targets` clean. `cargo test -p dbsp_connectors` (default
features: Kafka, Delta, Iceberg, Avro, Pub/Sub) 738 passed, 53 failed, 3 ignored — and every
failure is a test that needs a service this machine does not run: 21 NATS tests (want a local
`nats-server`), `integrated::postgres::output::tests::{test_postgres_bad_field_name,
test_postgres_field_name, test_postgres_key_field_name, test_postgres_table_name,
parallel::batch_start_gives_up_while_the_pipeline_is_stopping,
parallel::test_progress_counter_batch_start_resets}`, `integrated::postgres::test::cdc_tests::
test_cdc_runtime_shutdown_closes_hosted_connections` (want Postgres on `:5432`), and
`integrated::dynamodb::test::dynamodb_progress_counter_batch_start_resets` (wants DynamoDB
local). Re-running with those five suites skipped gives 422 passed / 29 failed — the same
service-dependent set. None of them exercise this stage's code, so "no new failures" rests on
the mechanism (one added `match` arm plus a stub module) rather than on a differential run
against `288cd64`; that baseline costs ~30 minutes and was not spent.

**No `with-ducklake` feature yet.** Nothing in the skeleton has a dependency to gate — the
switch would gate only the presence of a stub — so it arrives with the FFI in Stage B. The
commit body records that choice.

**Build recipe (macOS arm64, no docker, no `bun`).** Recorded here rather than in a commit body
because it wastes an afternoon to rediscover. The workspace enables `rdkafka`'s
`dynamic-linking`, so librdkafka has to be discoverable by `pkg-config` — and `rdkafka.pc` lives
in rdkafka's **`dev`** output, not the default one. Pointing at the default output fails with
`librdkafka 2.12.1 cannot be found on the system: Aborted because RDKAFKA_NO_PKG_CONFIG is set`
(or plain `Dynamic linking failed. Exiting.`):

```bash
RDKAFKA_DEV=$(nix build --no-link --print-out-paths nixpkgs#rdkafka.dev)
nix shell nixpkgs#rdkafka nixpkgs#pkg-config nixpkgs#openssl nixpkgs#zstd --command bash -c "
  export PKG_CONFIG_PATH=$RDKAFKA_DEV/lib/pkgconfig:\$PKG_CONFIG_PATH
  export WEBCONSOLE_BUILD_DIR=/tmp/webconsole-stub   # build.rs:49 needs this or `bun`
  ~/.cargo/bin/cargo test -p dbsp_connectors"
```

`cargo` must be the rustup proxy (`rust-toolchain.toml` pins channel `1.99.0`); a `nix shell`
cargo is `1.96.1` and fails on toolchain mismatch. `cargo fmt` reflows some pre-existing lines
in the two files it touches; accepted rather than bypassed with `#[rustfmt::skip]`. The
`key_schema` field is kept although Stage A does not read it, because the constructor's
arguments are the framework's and dropping the field means rewriting the constructor in Stage B.

**Open objectives:** Stage B (writer: guard row, one-transaction apply, hold-until-cancelled,
read-back verification) — done, see §11; Stage C (reader) not started (m00001 §3).

## 11. Session 3 result (Stage B — the writer)

Committed in `~/Code/feldera` as `a2d2081 feat: publish DuckLake output with a guard row per
step` (parent `845e79d`), branch `ducklake-connector`, pushed to the **fork**
`monochromatti/feldera` — `origin` is `feldera/feldera`, which refuses the push (`Permission to
feldera/feldera.git denied to monochromatti`, HTTP 403). The fork remote is named `fork`.

### 11.1 What is there

| Module | Contents |
|---|---|
| `crates/connectors/src/integrated/ducklake/protocol.rs` | batch identity, `Guard`, total `decide`, `advance`, `acquire_ownership`, `validate`, digest. Pure: no I/O, no connections |
| `crates/connectors/src/integrated/ducklake/catalog.rs` | attach, guard row, `publish` (one transaction), `take_over`, identifier/literal quoting |
| `crates/connectors/src/integrated/ducklake/ffi.rs` | DuckDB C API subset via `dlopen`; `Value`/`Rows` adapters |
| `crates/connectors/src/integrated/ducklake/writer.rs` | `OutputEndpoint` + `Encoder` + `OutputConsumer` on one type |

19 unit tests in the module (`cargo test -p dbsp_connectors --lib integrated::ducklake`).

### 11.2 The claims, and how they are checked

- **One transaction.** `Catalog::publish` does `BEGIN` → read guard → `decide` → conditional
  `UPDATE ... WHERE output_group = ? AND generation = ? AND epoch = ? AND frontier = ?` → payload
  → re-read the guard → `COMMIT`. A publisher that lost the race updates zero rows and the
  re-read raises instead of double-applying. A conflict (`conflict`, `transactioncontext`,
  `failed to commit`, `optimistic`, `write-write`) is retried with the *same* batch identity.
- **Replay is a duplicate.** `frontier == end` ⇒ `Duplicate`, and — new relative to
  `protocol.py` — a committed digest that differs from the replayed payload is an **error**, not
  a duplicate, so a divergence stops the writer instead of being papered over.
- **Overtaken is stale.** `frontier > end` ⇒ `Stale`; a `base` that is not the frontier ⇒
  `Stale`. Because a DBSP transaction id is monotone across replay (unlike the Python bridge's
  restarting step counter) `base` degrades from identity input to a consistency assertion.
- **Real catalog, real commit.** `a_published_step_and_a_replayed_step_leave_one_copy_of_its_rows`
  attaches a DuckLake metadata file through the same `Catalog::open` a deployment uses, publishes
  a step, replays it (duplicate, `count(*)` stays `1`), then offers an overtaken step (stale,
  rows unchanged). It skips unless `DUCKLAKE_DUCKDB_LIB` and `DUCKLAKE_EXTENSION_DIR` are set,
  since the connector does not build DuckDB in.

### 11.3 Decisions that constrain Stage C

- **No `with-ducklake` cargo feature.** Nothing is a build-time dependency: `libduckdb` is
  `dlopen`ed from `$DUCKLAKE_DUCKDB_LIB` (or the loader's search path) and the extension is a
  vendored signed binary pointed at by `extension_directory`. The only new dependency is `hex`
  (workspace pin, for digest rendering), added next to the existing `sha2`. Stage A's note said
  the feature would "arrive with the FFI"; it turned out there is nothing for it to gate, and a
  knob that gates only a stub's presence is worse than no knob.
- **Rows are read from the batch reader, not from a byte buffer.** `Encoder::encode` walks
  `batch.cursor(RecordFormat::Json(JsonFlavor::Default))` and turns `serialize_key` into a
  `serde_json::Value` per row, expanding weight into repeated deltas (cap `MAX_MULTIPLICITY =
  1024`, above which it errors rather than materialising a huge multiplicity). The identity
  arrives separately at `OutputConsumer::batch_start(transaction, kind)` — verified in
  `crates/connectors/src/controller.rs:9123-9136`, which calls `consumer().batch_start(...)`,
  `encoder.encode(batch)`, `consumer().batch_end()`.
- **A failed publish holds the output thread.** It reports through
  `OutputEndpointCallbacks::error(fatal, ..)` and then sleeps in 200 ms steps until the shutdown
  token is cancelled. Returning an error instead would let the engine count the step as
  processed while its rows are not in the catalog. Holding is bounded: the output thread is only
  joined *after* the token is cancelled.
- **`is_fault_tolerant()` now returns `true`**, which is the guard row's whole point: a replayed
  step is a no-op.

### 11.4 Build/test recipe correction (supersedes §9.1/§10)

`nix shell nixpkgs#rdkafka ...` alone is not enough on this machine: `rdkafka-sys` wants
`rdkafka >= 2.12.1` and the nixpkgs `rdkafka` output that lands on `PATH` is `2.12.0`. The dev
output in the store is `2.13.0-dev`, which satisfies it, and `pkg-config` must be on `PATH`:

```bash
export PKG_CONFIG_PATH=/nix/store/dva2i5412aw8g9c3w1fvw6h5rnnf0mdf-rdkafka-2.13.0-dev/lib/pkgconfig:/nix/store/1r8aqzrrpghyxwb7wx3b0hmm0d7aq364-openssl-3.6.5-dev/lib/pkgconfig:/nix/store/0a483npd9jfmlfbxd9m67vf7zqyk0jmc-zstd-1.5.7-dev/lib/pkgconfig
export WEBCONSOLE_BUILD_DIR=/tmp/webconsole-stub
nix shell nixpkgs#pkg-config -c ~/.cargo/bin/cargo test -p dbsp_connectors --lib integrated::ducklake
```

`cargo check -p dbsp_connectors --no-default-features` needs no kafka at all and is the fast
loop; its one remaining warning (`MemoryUseReporter::current` unused) is pre-existing and caused
by the disabled features, not by this work.

**Open objectives:** Stage C (reader: snapshot bootstrap, `ducklake_table_changes` polling,
`start_snapshot + 1`), not started (m00001 §3).

## 12. The Nix test environment (added after Stage B, unblocks Stage C)

Stage C's reader needs a real DuckLake and **not** a real Feldera: its obligations are
attach → `ducklake_current_snapshot` → `ducklake_table_changes` over `(applied, current]` →
rows into an `InputConsumer`, and all of that can be driven in-process with a fake consumer.
What was missing was a hermetic DuckDB 2.0.0 and the matching extension. That is now pinned.

### 12.1 What is pinned, and where

`nix/ducklake-2.0.nix` exposes three derivations, wired into `devenv.nix`:

| Derivation | Source | Why not nixpkgs |
|---|---|---|
| `libduckdb` (2.0.0) | `duckdb-shared-libs-<platform>.tar.gz` from the `v2.0.0` GitHub release | nixpkgs is on **1.5.2**; an extension loads only into the build it was compiled against, so the version is ABI, not preference |
| `cli` (2.0.0) | `duckdb-cli-<platform>.tar.gz`, same release | a 1.5.x CLI cannot open a 2.0 catalog file at all |
| `extension` | `extensions.duckdb.org/v2.0.0/<platform>/ducklake.duckdb_extension.gz` | needs egress at `INSTALL` time; version-specific anyway |

Asset names differ between the two archives — `osx-arm64` / `linux-amd64` in the release,
`osx_arm64` / `linux_amd64` in the extension repo — and are spelled out per platform so a
typo fails at evaluation rather than at the first `LOAD`. Hashes recorded: `aarch64-darwin`
and `x86_64-linux`; any other platform `throw`s with the `nix store prefetch-file` command
that records one, rather than guessing.

The shell exports what the Rust tests look for:

```bash
DUCKLAKE_DUCKDB_LIB   # .../lib/libduckdb.dylib|so   what `ffi.rs` dlopens
DUCKLAKE_EXTENSION_DIR  # .../v2.0.0/<platform>/     what `extension_directory` points at
```

and nothing else: there is deliberately no `fd-rust-integration` script. The tests live beside the
Rust code in the Feldera tree, and this shell cannot run them anyway — its `ld` is nixpkgs'
`cctools` `ld64`, which refuses to link the workspace's test binary (`ld: b(l) ARM64 branch out
of range (178028860 max is +/-128MB)` from a `ctor` object). Overriding the linker
(`CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=/usr/bin/clang`) is not enough, because Apple's
`clang` still resolves `ld` from `PATH`, which is the nix one. Run them in a plain shell per
§12.3 instead.

### 12.2 Facts verified in the nix shell (2026-06, DuckDB 2.0.0 "Cyanoptera" `d54cbbe3df`)

- **`dlopen` of the store copy works** — `ctypes.CDLL($DUCKLAKE_DUCKDB_LIB)` reports
  `v2.0.0`, so no codesign/xattr handling is needed on darwin and `ffi.rs`'s loader is
  correct as written.
- **`LOAD` reads the extension straight out of the read-only store directory.** Only
  `INSTALL` writes `.info` files; a test therefore needs no writable copy of the extension,
  only a writable place for the *catalog* and its data.
- **Failure mode when the window's end does not exist:** `ducklake_table_changes` with an
  `end_snapshot` beyond the catalog's current snapshot is an error, not a clamp —
  `Invalid Input Error: No snapshot found at version 100`. The reader must read
  `ducklake_current_snapshot('dl')` (single column `id`, `UINT64`) and clamp to it.
- **Inclusivity re-confirmed on a fresh catalog** (create=1, insert=2, update=3, delete=4):
  `ducklake_table_changes('dl','main','t',2,4)` returns snapshot 2's rows, and `(…,4,4)`
  returns snapshot 4's — so the `start + 1` rule is exactly right and `(s, s)` is one
  snapshot, not zero.
- **An `UPDATE` yields two rows with the same `rowid`**, one `update_preimage` (old values)
  and one `update_postimage` (new values); a `DELETE` carries the *old* row. The reader must
  turn preimage into a retraction and postimage into an insert, and must not assume one row
  per changed row.
- **`ATTACH … (READ_ONLY)` works for reads and refuses writes**
  (`Cannot execute statement of type "INSERT" on database "dl" which is attached in
  read-only mode!`). So a read-only reader is expressible — but §9.3 still applies: with a
  *file* catalog a second process cannot attach at all, so concurrency needs PostgreSQL.
- **`extension_directory` is deprecated in 2.0** (`The 'extension_directory' setting is
  deprecated and will be removed in a future release`) but still honoured. Its replacement is
  `extension_directories`, a *list*; see the note in §17.3. The connector's own config field
  keeps its name — it is the connector's vocabulary, not DuckDB's.

**Open objectives:** Stage C (reader: snapshot bootstrap, `ducklake_table_changes` polling
over `(applied, current]`, `InputConsumer` output, the new-vs-initially-present identity
mechanism from §11.3), not started (m00001 §3).

### 12.3 The writer is live-tested (added after this section was first written)

`crates/connectors/src/integrated/ducklake/tests_live.rs` in the Feldera tree runs the real
thing — real `libduckdb`, real signed extension, real catalog file — and both of its tests pass
under the nix shell (0.59s):

- `publishing_the_same_batch_twice_applies_it_once` — first publish `Apply`, republication
  `Duplicate`, frontier `1`, `count(*) = 2`.
- `the_frontier_survives_reopening_the_catalog` — after reattaching, the frontier is still `1`
  and a replay applies nothing.

They skip when `DUCKLAKE_DUCKDB_LIB`/`DUCKLAKE_EXTENSION_DIR` are unset, so a plain
`cargo test` stays green. Two corrections to what was assumed earlier in this section:

- **There is no `with-ducklake` cargo feature**, and there should not be one. The module is
  compiled unconditionally: it adds no dependency (`libc`'s `dlopen`/`dlsym` is already there)
  and the DuckDB library is loaded at run time, not linked. The Stage A commit message said the
  feature "arrives with the FFI"; that was wrong and is corrected here.
- **The build needs `PKG_CONFIG_PATH`/`LIBRARY_PATH`**, not just the two `DUCKLAKE_*`
  variables. The working invocation on darwin/arm64:

  ```bash
  nix shell nixpkgs#rdkafka nixpkgs#pkg-config nixpkgs#openssl.dev nixpkgs#zstd -c bash -c '
    export PKG_CONFIG_PATH=$(for d in $(echo "$PATH" | tr : "\n"); do p=$(dirname "$d")/lib/pkgconfig; [ -d "$p" ] && echo "$p"; done | tr "\n" ":")
    export LIBRARY_PATH=$(for d in $(echo "$PATH" | tr : "\n"); do p=$(dirname "$d")/lib; [ -d "$p" ] && echo "$p"; done | tr "\n" ":")
    cargo test -p dbsp_connectors --lib tests_live -- --test-threads=1
  '
  ```

  The `$PATH`-derivation is what makes it work without hard-coding store paths, and it also
  works as a one-liner in a plain shell if the four packages are already in `PATH`.

  `--no-default-features` is not an escape hatch from the `librdkafka` dependency: upstream's
  `server.rs` tests do not compile without the default features (unresolved imports of
  `csv_messages`, `pause_input_endpoint`, `start_input_endpoint`).

## 13. Stage C result (the reader)

The reader is in the Feldera tree as `crates/connectors/src/integrated/ducklake/reader.rs`,
behind the `IntegratedInputEndpoint` impl in `ducklake.rs`; `catalog.rs` gained the shared
`attach_statement(catalog, read_only)`, so a reader attaches `READ_ONLY` and cannot write the
guard row even by accident.

It is a cursor over committed snapshots: one transactional **bootstrap** read of the table,
then `ducklake_table_changes(alias, schema, table, applied + 1, current)` for every newly
committed snapshot, pushing each row with the polarity its `change_type` implies
(`insert`/`update_postimage` +1, `delete`/`update_preimage` −1). An unknown `change_type` is
an error, not a skipped row.

### 13.1 Two corrections to §12

- **`ducklake_current_snapshot(catalog)` is the reader's function, not
  `ducklake_last_committed_snapshot(catalog)`.** The latter returns `NULL` in a session that has
  not itself committed anything, which is exactly the reader's situation. `current_snapshot`
  returns the last committed snapshot in a fresh session, inside a read-only transaction, and
  inside a transaction that has uncommitted writes (it keeps reporting 3 while 4 is pending).
  Measured against a real 2.0.0 catalog.
- **Reading the snapshot number and the rows must be one transaction.** Taking the number
  first and the rows second can double count a concurrent commit; the other order can miss one.
  This is why every read attempt runs inside `BEGIN … COMMIT` on one connection.

### 13.2 What the change feed actually returns (measured)

```
snapshot_id | rowid | change_type      | id | name
2           | 0     | insert           | 1  | a
3           | 0     | update_postimage | 1  | a2
3           | 2     | insert           | 3  | c
3           | 0     | update_preimage  | 1  | a
3           | 1     | delete           | 2  | b
```

- the window is **inclusive at both ends**: `(3, 3)` is snapshot 3 alone, and reading `(2, 3)`
  returns snapshot 2's inserts as well — hence the lower bound `applied + 1`;
- the table's own columns are interleaved with `snapshot_id`/`rowid`/`change_type`, so the
  reader projects `to_json({'col': "col", …})` from the relation rather than `to_json(*)`;
- an `end` beyond the last snapshot is an error (`No snapshot found at version 10`), so the
  reader only asks when `current > applied`.

### 13.3 Evidence

`cargo test -p dbsp_connectors --lib ducklake`: **23 passed**, including four live tests against
a real `libduckdb` 2.0.0 + signed extension (`tests_live.rs`): the two writer tests from §12.3,
plus `bootstrap_reads_the_table_once_and_records_its_snapshot` and
`a_new_snapshot_is_replayed_with_its_change_types` (0.79s with the environment set; without
`DUCKLAKE_DUCKDB_LIB`/`DUCKLAKE_EXTENSION_DIR` they skip, so check they ran, not just passed).

### 13.4 Not done

- **No pipeline has run against it.** The four live tests drive `advance()` and the writer
  directly, not a controller, so the `InputReader` command protocol (`Extend`/`Pause`/`Queue`/
  `Replay`/`Disconnect`), the checkpoint metadata round trip and `AtLeastOnce` recovery are
  written to the documented contract but not yet observed. That needs the Feldera stack
  (`feldera-up`) and is the next step.
- `send_snapshot` in the reader config is declared but unused: the bootstrap always sends the
  snapshot (deduplicating an `insert_delete` *update* stream cannot start without it).
- A PostgreSQL catalog lets a reader and a writer run concurrently; only the file-catalog
  single-process behaviour is covered by tests.
- No end-to-end run of reader + writer in one pipeline (a pipeline that both reads and writes
  one catalog is the interesting shape, and it is untested).

**Open objectives:** the pipeline-level verification in §13.4 (reader command protocol,
checkpoint resume, `feldera-up` end-to-end), not started (m00001 §3, m01088).

---

## 14. Stage D result (engine tests)

Date: 2026-11-05. Commits: feldera `73c833d` (compile the connectors into the suite),
`275bb5a` (register the module), `a23d732` (the suite and the fixes it found).
**Both cases pass**: `2 passed; 0 failed … finished in 5.49s`, alongside 24 library tests.

### 14.1 How the suite is run (this is not obvious, and a session will otherwise report "no tests")

`#[engine_test]` does **not** generate a `#[test]`. It submits an entry to
`inventory`, and only `static-connectors::test::StaticTestEngine` collects them. So:

- `cargo test -p dbsp_connectors …` compiles and runs the library tests but **silently runs
  zero engine tests**. That is the trap: no error, no skip line, just nothing.
- The suite is run as `cargo test -p static-connectors --test connector_suites -- ducklake`.
  `crates/static-connectors/tests/connector_suites.rs` is one line:
  `dbsp_connectors::test::run_suites(static_connectors::test::StaticTestEngine)`.
- `dbsp_connectors` must build with `--features controller-tests`… it has no such feature;
  the harness lives behind `cfg(test)`, so the runnable command above is the only one that
  matters.

Working invocation (macOS, nix shell, from `~/Code/feldera`):

```bash
nix shell nixpkgs#rdkafka nixpkgs#pkg-config nixpkgs#openssl nixpkgs#zstd --command bash -c '
  export PKG_CONFIG_PATH=$(for d in $(echo "$PATH" | tr : "\n"); do p=$(dirname "$d")/lib/pkgconfig; [ -d "$p" ] && echo "$p"; done | tr "\n" ":")
  export LIBRARY_PATH=$(for d in $(echo "$PATH" | tr : "\n"); do p=$(dirname "$d")/lib; [ -d "$p" ] && echo "$p"; done | tr "\n" ":")
  export DUCKLAKE_DUCKDB_LIB=/nix/store/f24jslbwxkwp4klh5p75h0pk5bnjhfkb-libduckdb-2.0.0/lib/libduckdb.dylib
  export DUCKLAKE_EXTENSION_DIR=/nix/store/ik5fal2kmpkr85m395f3hmq91kk3yly6-ducklake-extension-2.0.0
  ~/.cargo/bin/cargo test -p static-connectors --test connector_suites -- ducklake'
```

Two build facts cost time this session:

- **Disk.** A full-feature debug build with default debug info kept **60 GB** in
  `target/debug/deps` and filled the disk; the linker then failed with
  `No space left on device` and no amount of `rm` in `/tmp` helped. `CARGO_INCREMENTAL=0
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0` is the right default on this machine
  unless a backtrace is wanted.
- **Native deps.** Without the nix shell, `rdkafka-sys` fails in its build script; every
  build and test command needs it, including `cargo build --tests`.

### 14.2 What the suite asserts, and what it deliberately does not

- `ducklake_output_publishes_the_step_it_committed`: identity circuit, DuckLake sink, two
  rows; after `stop()` the catalog is reattached and both rows must be present.
- `ducklake_input_bootstraps_and_then_follows`: DuckLake source → JSON file; the bootstrap
  snapshot must arrive, then a commit made *while the pipeline runs* must arrive as changes.
- **The step number is not asserted.** My first version asserted `frontier == 1` and failed:
  the step the rows land in depends on how many steps the controller took first. The suite
  asserts `frontier >= 1` ("a step was recorded") and leaves exact bookkeeping to the
  protocol tests, which control the step. Weakening this was not a loss: the guard-row
  semantics are covered where they can be controlled.

### 14.3 Three reader bugs the suite found (none were visible to the loop test)

1. **No step was ever requested.** The reader wrote rows into its stream and waited, but
   nothing tells the controller that the endpoint has news; the `clock` connector calls
   `consumer.request_step()` on every tick that produces something. Without it the rows sat
   buffered and the pipeline never stepped.
2. **The poll was gated on `Extend`.** The controller sends `Extend` only when it already
   expects data, so after the first step the gate closed and the connector never polled
   again — the pipeline hung on `Pause`-equivalent behaviour that nothing would undo.
   `Pause` is now honoured where it matters: nothing is handed over except in answer to a
   `Queue`.
3. **`Queue` was answered only when the buffer was non-empty**, which deadlocks a step on a
   quiet catalog. It is now always answered (flush, then `extended`), even with 0 records.

The loop test could not have caught (2) or (3): it re-sends `Queue` until rows arrive, which
a real controller never does. That is the argument for this stage existing at all.

### 14.4 A wiring fact worth keeping

An **integrated output endpoint never receives `connect`**: the controller creates it with
`create_integrated_output_endpoint` and immediately takes `into_encoder`, so the object that
runs the steps has had no connection attempt. Setup therefore happens on first use
(`ensure_connected` in `writer.rs`); `connect` calls the same idempotent function for any
caller that does call it.

### 14.5 Not done

- **PostgreSQL-backed catalog under two engines.** Both cases use a file-backed catalog
  (single-process). §9.3 measured that a PostgreSQL catalog is the only way to run a reader
  and a writer concurrently, and that has not been exercised natively.
- **One pipeline with both connectors** (read catalog A, write catalog B). Each is tested
  against the engine separately.
- **The 18 protocol scenarios have no 1:1 Rust counterparts.** The library tests cover the
  same behaviours (24 tests) but are not named after `scenarios.py`, and the two that need
  *two concurrent engines* (`guard/divergent-payload-for-committed-identity-is-detected`,
  `ducklake/concurrent-publishers-converge-to-one-outcome`) are still approximated by two
  cursors in one process.
- **Stage E (docs)** — the Feldera docs pages and the schema contract — not started.

**Open objectives:** PostgreSQL-catalog two-engine test, the reader+writer pipeline, the
18-scenario parity audit, and Stage E docs (m00001 §3, m01222).

---

## 15. The two-engine PostgreSQL test

Date: 2026-11-05. feldera `7d53c7e`, connectors (this commit).
**`3 passed; 0 failed … finished in 6.65s`** — the two file-backed cases plus the new one.

### 15.1 Two facts a session needs before writing this test

1. **DuckLake does not create the PostgreSQL database it stores metadata in.**
   `ATTACH 'ducklake:postgres:dbname=dl_probe host=127.0.0.1 port=55432 user=postgres password=x'`
   fails with
   `IO Error: … Unable to connect to Postgres …: database "dl_probe" does not exist`.
   The test therefore creates it through a plain `postgres` connection
   (`CREATE DATABASE {name} TEMPLATE template0`), the way a deployment would, using the
   harness's `Postgres` service (`POSTGRES_URL`, default
   `postgres://postgres:password@localhost:5432`).
2. **`postgres_scanner` must be vendored beside `ducklake`.** Attaching a PostgreSQL
   catalog makes DuckDB autoload it; if it is not in `extension_directory`, DuckDB
   *installs* it there, and on Nix that path is the read-only store:
   `Cannot open file "…/postgres_scanner.duckdb_extension.tmp-<uuid>.duckdb_extension": Permission denied`.
   `nix/ducklake-2.0.nix` now fetches both extensions (hashes recorded for
   aarch64-darwin and x86_64-linux), so `DUCKLAKE_EXTENSION_DIR` contains both and no
   install is attempted. This also corrected the claim in `devenv.nix` that "no
   writable copy is needed" — true only for extensions already present.

### 15.2 What the case asserts

`two_engines_publishing_the_same_step_converge_on_one_outcome`:

- Two controllers, each with its own file input, both publishing **the same two rows as
  step 1** to one PostgreSQL-backed catalog. Each engine has its own DuckDB instance,
  its own transaction and its own guard-row attempt — the configuration §9.3 could only
  approximate with two cursors in one process.
- The step numbers collide deliberately. The protocol makes the second writer see
  `frontier == batch.end` with an identical digest and return `Duplicate`
  (`protocol.rs`, `decide`), rather than applying the step twice.
- **The assertion goes through a reader**, not through row counting: a third pipeline
  reads the same catalog and must produce exactly two rows with weight `+1`. That also
  covers what §13.4 listed as untested — a reader and writers sharing a catalog that is
  not file-backed.

### 15.3 What the case does not do

- It publishes *identical* content, so the divergence branch (`decide` bailing when the
  digest differs for a committed identity) is not exercised here; it stays covered by the
  protocol unit test, which can control the digest directly.
- The two engines are two controllers in one test process, not two OS processes. Each has
  its own DuckDB instance and connections, which is what made the file catalog unusable
  for concurrency in the first place, but a two-process run through
  `pipeline-manager` remains untested.

## 16. Stage E result (docs)

Two pages, in the shape the other integrated connectors use
(`docs.feldera.com/docs/connectors/{sinks,sources}/ducklake.md`), plus the sidebar entries.
Feldera commit `fe160cb`.

What they pin down, chosen because none of it is guessable from the code: the guard row and its
columns; the schema contract (names match, DuckDB casts the types, deletes are key matches or
one-row bag deletions); the `epoch`/`generation` takeover rule; and that the **file catalog is
single-process while the PostgreSQL one is not**, which is what decides whether two pipelines
may share a catalog.

### 16.1 Three claims the docs forced me to correct

Writing the pages down is what caught these; each was wrong in a way that only matters when
someone acts on it.

1. **"The connector adds itself as an exactly-once sink"** — false. No integrated output
   connector declares a fault-tolerance model; that is the *pipeline's* setting. The writer is
   safe *under* exactly-once because of the guard row, which is a different claim.
2. **"`TRUNCATE`/`MERGE`/`REPLACE` arrive as row-level changes"** — unverified. The connector
   does only ever emit row deltas, which is the true and sufficient statement; what a
   particular statement of the *catalog's* turns into is the catalog's business.
3. **"`enable_output_buffer` must be disabled"** — it was not enforced anywhere. See §16.2.

### 16.2 A real gap found while writing the docs

The writer never received `enable_output_buffer`, so a buffered pipeline would have handed it a
batch spanning several steps and it would have recorded one step number for rows belonging to
several — precisely the confusion the guard row exists to prevent. It is now refused at
construction (`ControllerError::NotSupported`), passed through from `integrated.rs`, and the
docs say "rejected" rather than "don't". Feldera commit `736d397`.

That same commit relaxes the two-engine test's PostgreSQL bootstrap to a skip: it connected
with `expect()`, so a machine without a server failed the suite for an environment reason.
Re-run both ways to confirm — `3 passed` with the server absent (skip announced on stderr) and
`3 passed` with it up.

### 16.3 Not verified

- **The docs site was not built.** `docs.feldera.com` needs `yarn install` (network) to run
  `docusaurus build`; there is no `node_modules` in this checkout. Markdown structure and
  anchors were checked by hand against a sibling page (`sinks/delta.md`), and the two pages
  only use constructs that page already uses.
- Nothing checks that the documented defaults equal the code's. They were compared by hand
  against `crates/feldera-types/src/transport/ducklake.rs` (`schema` = `main`,
  `guard_table` = `connector_guard`, `alias` = `dl`, `poll_interval` = `1s`,
  `send_snapshot` = `true`); the OpenAPI round-trip test covers the shapes, not the prose.

## 17. Verification gaps closed

Three gaps were left open at the end of §16. All three are closed, by tests rather than by
prose. The tests live in `crates/connectors/src/integrated/ducklake/{protocol,catalog,tests_live,test}.rs`.

### 17.1 The 18 scenarios, mapped

`src/fd_connectors/scenarios.py` names 18 protocol requirements. Auditing the Rust suite
against them by name found six with no counterpart at the level the scenario tests, and one
whole requirement (`protocol/decision-is-total`) that no Rust test covered at all. All are now
mapped; the ones added in this pass are marked **new**.

| `scenarios.py` | Rust |
|---|---|
| `guard/apply-advances-frontier` | `protocol::a_batch_on_top_of_the_frontier_applies`, `protocol::advancing_records_the_identity_and_the_digest`, `tests_live::a_published_step_and_a_replayed_step_leave_one_copy_of_its_rows` |
| `guard/duplicate-retry-is-a-no-op` | `protocol::replaying_the_committed_batch_is_a_duplicate_not_a_second_apply`, `tests_live::publishing_the_same_batch_twice_applies_it_once` |
| `guard/stale-publisher-is-refused` | `protocol::an_overtaken_interval_is_stale_even_with_an_older_base`, `protocol::a_batch_whose_base_is_not_the_frontier_is_stale` |
| `guard/foreign-generation-is-refused` | `protocol::another_generation_is_refused_outright` |
| `guard/ownership-requires-explicit-takeover` | `protocol::taking_over_requires_a_strictly_greater_epoch`, `protocol::a_fenced_writer_is_refused_even_with_a_higher_epoch` |
| `guard/divergent-payload-for-committed-identity-is-detected` | `protocol::the_same_identity_with_different_content_stops_the_writer`, **new** `tests_live::a_divergent_payload_for_a_committed_identity_is_refused` |
| `ducklake/payload-and-guard-are-atomic-on-payload-failure` | **new** `tests_live::a_payload_failure_leaves_the_guard_and_the_history_where_they_were` |
| `ducklake/reader-sees-payload-and-guard-together` | same test — see the deviation below |
| `ducklake/rollback-leaves-history-replayable` | same test: the frontier **and** the committed snapshot are unchanged |
| `ducklake/resync-replaces-content-and-is-idempotent` | **new** `tests_live::a_resync_replaces_the_content_and_replays_as_a_duplicate` |
| `ducklake/keyed-update-is-delete-then-insert` | **new** `tests_live::a_keyed_update_leaves_the_post_image` |
| `ducklake/multiset-delete-removes-one-copy` | **new** `tests_live::a_multiset_delete_removes_one_copy` |
| `ducklake/concurrent-publishers-converge-to-one-outcome` | `test::two_engines_publishing_the_same_step_converge_on_one_outcome` |
| `source/guard-update-is-conditional` | the conditional `UPDATE` in `Catalog::update_guard`, plus the two-engine case |
| `source/change-interval-is-not-replayed` | `tests_live::a_new_snapshot_is_replayed_with_its_change_types`, the loop test |
| `source/identity-is-the-interval-not-the-digest` | `tests_live::the_loop_bootstraps_once_and_replays_from_the_checkpoint_it_reported` |
| `protocol/decision-is-total` | **new** `protocol::the_decision_is_total` — a sweep over 5 × 4 × 5 × 2 × 3 guard/batch combinations |
| `report/lost-update-counterexample` | `test::two_engines_publishing_the_same_step_converge_on_one_outcome` |

**One deliberate deviation.** `ducklake/reader-sees-payload-and-guard-together` proves
isolation with two cursors on one connection: a reader inside an open transaction sees neither
the payload nor the guard, then sees both after `COMMIT`. The Rust connector cannot be tested
that way — its reader and writer are separate DuckDB instances, and a file catalog admits one
attachment — so the requirement is tested where it can actually fail here: a publication
advances `ducklake_current_snapshot()` by exactly one, so the payload and the guard row are in
one snapshot and no reader can observe one without the other. What the Python scenario proves
about DuckDB's isolation is DuckDB's to guarantee; what the Rust test proves about the
connector is the connector's.

### 17.2 One pipeline, both connectors

`test::ducklake_to_ducklake_copies_a_table_into_another` runs a DuckLake source and a
DuckLake sink in one pipeline and asserts the sink table holds what the source table held.
Two catalogs, not one: a **file**-backed catalog admits one attachment, so a reader and a
writer of the same file catalog fence each other off. The same-catalog case is the PostgreSQL
two-engine case above.

### 17.3 A commit from another process

`test::ducklake_input_follows_a_commit_from_another_process` runs the `duckdb` CLI as a
genuine second process and expects the pipeline to follow its commit. This is the deployment
shape every other case fakes, and it is only reachable with the CLI: the reader re-attaches on
every poll precisely because an attachment it opened earlier never sees another process's
commit (`reader.rs`, `refresh`).

Two environment facts found while writing it, both worth knowing outside the tests:

- **The `duckdb` CLI must match the vendored extension's version.** `PATH` here offers
  `1.5.2` — newer than `1.5` and older than `2.0` — and loading a `2.0.0` extension into it
  fails in a way that reads like a connector bug. The case checks `--version` for `v2.0.0` and
  skips otherwise (`DUCKLAKE_DUCKDB_CLI`).
- **`extension_directory` must be writable only if DuckDB has to install something.** A
  PostgreSQL catalog makes DuckLake load `postgres_scanner`, and DuckDB `INSTALL`s a missing
  extension into `extension_directory`. A read-only vendored directory that lacks it (a Nix
  store path) fails with `Cannot open file "…/postgres_scanner.duckdb_extension.tmp-…":
  Permission denied`. **Corrected:** once `404b738` vendored `postgres_scanner` beside
  `ducklake`, the read-only store path works as is — verified by `nix build` of the derivation
  (`/nix/store/70w7q91…-ducklake-extension-2.0.0`, containing both) and a `ducklake:postgres:`
  `ATTACH` through the CLI, then by running the two PostgreSQL engine cases against that path
  with no copy. So the rule is "it must *contain* every extension the catalog needs, or be
  writable so DuckDB can install the missing one", which is what the docs now say, and the test
  helper uses the vendored directory as is whenever it holds `postgres_scanner`.
- **`extension_directory` is deprecated in DuckDB 2.0.0** (warned on first use:
  `The 'extension_directory' setting is deprecated and will be removed in a future release`).
  Its replacement is `extension_directories`, which is a **list** — `SET
  extension_directories = ['/vendored/ext']` — and setting it *replaces* the default
  `~/.duckdb/extensions` search path rather than adding to it. Measured: the singular key is no
  longer listed in `duckdb_settings()` at all, `duckdb_set_config(config,
  "extension_directories", "/some/dir")` **fails** (a bare path cannot cast to `VARCHAR[]`)
  while `duckdb_open_ext` still succeeds — a silently dropped setting — and with the list form
  a missing extension is `INSTALL`ed into its first entry, so the "must contain every extension
  or be writable" rule of §17.3 is unchanged. The connector therefore sets the list form and
  falls back to the deprecated key only if it is refused (no version sniffing), which keeps the
  offline guarantee on any 2.x.
- **A DuckLake attachment only sees the snapshot it attached with.** Measured with two
  attachments in one process: after `B` inserts rows into a table `A` created, `A` still reads
  0, and a table that `B` creates never appears in `A` at all, while a *fresh* attachment sees
  everything. The change-feed functions do see later commits (the reader's "another process
  commits" case passes on a long-lived attachment), so this is specific to reading table
  contents. Consequence for the engine tests: a poll for published output must **re-attach**
  each iteration instead of reading through one attachment — polling a long-lived one waited
  the full 60 s and looked exactly like a connector that never publishes. Consequence for the
  writer: none, because `update_guard` re-reads the guard row *inside* the transaction and
  requires it to equal the batch's step, so a stale read can only cause a bail-out and a
  retry, never a duplicate commit.

### 17.4 A conflicting commit

Everything above runs against a **file**-backed catalog, and a file catalog serialises writers:
the second attachment waits instead of conflicting. So the retry path — re-read the guard inside
the transaction, find the step uncommitted, re-apply the same batch identity — had never run
against a real conflict. Two cases now produce one, on a PostgreSQL catalog (a fresh database
per case, created by the same `crate::test::postgres_catalog` helper the engine suite uses):

- `tests_live::two_groups_commit_concurrently_into_one_catalog` — two threads, two output
groups, one catalog, six steps each, rendezvousing before **every** `publish` so that each step's
conflict is between two simultaneous commits. Asserts each group applied its six steps exactly
once, sees only its own `3 × 6` rows, and its own guard at step 6; a third attachment then reads
group A's guard. Measured retries per run: `A 3, B 3 of 6` (`A 4, B 2 of 6` on an earlier run).
- `tests_live::a_resync_clears_only_its_own_group` — group B resyncs while group A's rows are in
the same catalog; A keeps its two rows and its guard.

The retry count is **printed, not asserted**: how many of the attempts DuckLake resolves as
conflicts is its business, and a `>= 1` assertion would be a flaky test, not a stronger one. The
assertions are on the outcome, which must be identical whether or not a conflict happened.

A note on how they are written: the first version was a separate `tests_pg.rs` with its own
`DUCKLAKE_PG_URL` plumbing and a hand-created database. That duplicated the harness the engine
tests already had, and its per-run data path fought DuckLake's data-path check (a catalog records
one data path; a mismatched `ATTACH` is refused). The cases live in `tests_live.rs` now, on a
fresh database per case, and the only new export is `postgres_catalog`'s visibility.

### 17.5 State

`cargo test -p dbsp_connectors --lib ducklake` → 35 passed (30 before this pass); with
PostgreSQL 17 up on `127.0.0.1:5432` all 35 run and 33 would pass without it, the two above
printing why they skipped.
`cargo test -p static-connectors --test connector_suites -- ducklake` → 5 passed, with
PostgreSQL 17 running on `127.0.0.1:5432` and `DUCKLAKE_DUCKDB_CLI` pointing at DuckDB 2.0.0.
Without PostgreSQL two of the five skip themselves and say so; without the CLI a third does.

One build note for the `--lib` target on darwin/arm64: the test binary is close to the 128 MB
`ARM64` branch range and the debug profile fails to link (`ld: b(l) ARM64 branch out of range`,
from `ctor`'s priority-constructor). `CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_TEST_OPT_LEVEL=1`
links it; `--test connector_suites` is unaffected.

**Open objectives:** none from `TODO.md` §3. What remains unverified is stated where it lives:
the docs site was never built (§16), and no test drives `pipeline-manager` over HTTP — the
cross-process case above is the closest thing, and it is a better probe of the connector
because it does not also test the REST API.

## 18. Parity review: Rust writer vs `protocol.py`

A read-only review compared the Rust writer against the Python bridge the protocol was defined
by, and found four places where they disagree in the direction that loses rows. All four are
fixed and pushed to `fork/ducklake-connector` as `d006e71`.

| Divergence | Python | Rust (before) | Consequence |
|---|---|---|---|
| A resync's scope | deletes **every configured table** — sound there, because a group is assembled and committed as a whole, so its configured set *is* the batch's set | deleted **every configured table** too, but one endpoint commits alone, so its configured set is wider than what it writes | endpoint B's resync wiped the rows endpoint A had just published, with both guards advancing past them |
| A failed `encode` on a snapshot | raises; nothing is written | left `pending.resync` set with no rows, so `batch_end` published an empty resync | the table was emptied and the guard advanced over the hole |
| A keyed delete | `= ?` (same bug) | `= ?`, no row limit | a NULL key matched nothing while the insert added a copy ⇒ unbounded growth on every republish; and a single delete removed *all* rows with that key |
| `records_written` | tracks frontier and applied rows separately | stored the **frontier** in the record counter | the endpoint's record metric reported a transaction number |

Both implementations share the keyed-delete defect, so it is not a porting error — it is a
protocol error that the reference has too. The Rust side now uses `IS NOT DISTINCT FROM ?`
limited to one row, matching what both sides already did for non-keyed tables.

Three further findings are engine-side or inherent, and are recorded rather than fixed (also in
the commissioning doc's "Known limits"):

- **An `encode` error still leaves the step uncounted only if the connector holds.** The
  controller treats an encoder error as non-fatal and calls `batch_end` regardless, so a
  connector that merely returns loses the step. Holding is the only place it can be stopped.
- **The engine never delivers an empty snapshot** (`controller.rs` skips `data: None`), so a
  restart against a non-empty table with an empty view keeps stale rows forever.
- **`is_fault_tolerant()`'s premise is unverified.** It is currently inert (no call site outside
  the connectors), and its at-least-once reasoning only holds for replays at or above the
  frontier.

### 18.1 The interop claim, narrowed

`protocol.rs` claimed the vocabulary is shared "so that a catalog written by one is read by the
other", and that the frontier is a DBSP transaction id stable across restarts. The second half is
an assumption about which counter the engine passes to an output endpoint, and the first depends
on it: if they differ, the guard classifies every step of an inherited catalog as stale and holds.
`76eb97d` states this as an open question and keeps the identity check, which is what still
recognises a replay if the counters do not line up.
