# Feldera–DuckLake connector: first Lean verification round

Date: 31 October 2026

## Verdict

There is a viable protocol using existing DuckDB/DuckLake transactions. This round
establishes conditional connector safety in an abstract Lean model and provides
SQL evidence for the required publication mechanism. It does **not** establish
that all Feldera guarantees hold for a finished connector or arbitrary deployment.
There is no finished connector in this bundle.

The strongest result is `end_to_end_stream_operator_correct`: if the source
supplies exact signed deltas, the engine implements its specified stream
operator, and publication follows the modeled atomic protocol, every reachable
published state equals the specified result at its recorded frontier. The
operator can depend on input history; it need not be a memoryless SQL query.

All 38 theorem declarations check with Lean 4.19.0: 24 positive results and 14
counterexample results. There are no `sorry`, `admit`, new `axiom` declarations,
or `native_decide` proofs. The complete axiom audit uses only Lean's standard
`propext` and `Quot.sound`; some theorems use no axioms. These proofs establish
properties of their definitions and hypotheses, not of unmodeled software.

Nine SQL scenarios passed with DuckDB 2.0.0 and official DuckLake extension build
`417a9117f5`. They use multiple connections in one DuckDB instance with a local
DuckDB metadata catalog. PostgreSQL catalog behavior and independent service
processes have not been exercised.

## What Feldera actually promises

Feldera's [correctness article](https://www.feldera.com/blog/correctness-at-feldera)
separates machine-checked DBSP theory from implementation testing. Its consistency
claim concerns results for the same processed prefix of inputs. The existence of
a mathematical DBSP proof does not verify the Rust engine, SQL compiler,
connectors, or an external storage implementation.

The [original formalization](https://github.com/tchajed/database-stream-processing-theory/tree/fb6ff4416108710448ccd87473c5513c1a1179ea)
uses Lean 3.51.1 and a pinned mathlib revision. It includes differentiation,
integration, incrementalization, Z-sets and relational operators. This bundle is
an independent Lean 4 protocol model. It does not import, port, or recheck that
development. The engine correctness premise remains explicit.

The reviewed [connector interface](https://github.com/feldera/feldera/blob/c7918355c6a8a2eea4062e92d8cc0ea63a2a752f/crates/adapterlib/src/transport.rs)
adds operational obligations:

- Exactly-once-capable inputs provide replay information for every engine step,
  including a replay hash; a seekable snapshot boundary alone is insufficient.
- Fault-tolerant outputs suppress already-published batch identities and keep a
  batch invisible until its completion boundary.
- Connector waits must respond to shutdown rather than hang teardown.

[Checkpoint and fault-tolerance documentation](https://docs.feldera.com/pipelines/fault-tolerance/)
places these recovery facilities in Feldera Enterprise. A durable external
cursor does not restore engine state. The recovery lemma here requires a
checkpoint containing matching state and position plus an exact logical journal.
It does not prove that a deployed engine writes, restores or retains them.

Preserving a transaction across several source tables, or publishing several
output views atomically, is a useful **additional** contract. The core DBSP input
prefix guarantee alone does not promise either external transaction boundary.

## Checked properties and remaining obligations

| Property | Lean result | Link to actual implementation still required |
|---|---|---|
| Backfill followed by exact changes reconstructs snapshots | `bootstrap_then_follow`, `source_prefix_exact` | Fixed bootstrap boundary; complete logical feed; correct inclusive bounds |
| Source correctness composes with a specified engine | `source_engine_composition`, `source_operator_composition` | Engine/SQL correctness; any nondeterminism captured in replay |
| Supported row codecs must preserve identity | `supported_codec_is_injective` | Real SQL/Arrow round trips and a supported-type policy |
| Checkpoint plus exact journal restores input state | `coherent_checkpoint_and_exact_journal_recover` | Durable journal before consumption, coherent checkpoint, replay hooks |
| Publication preserves the target result | `publish_preserves_correctness` | Atomic SQL transaction and faithful output serialization |
| Retries and lost acknowledgement do not duplicate output | `publish_idempotent`, `retry_after_lost_ack_is_idempotent` | Stable generation/batch/base identity and correct commit-outcome handling |
| Stale owner and wrong-generation writes do nothing | `stale_epoch_is_noop`, `takeover_fences_previous_owner`, `wrong_generation_is_noop` | Guard write conflict and ownership acquisition protocol |
| Uncommitted staging is invisible | `stage_is_invisible` | All outputs and guard in one transaction; readers use one snapshot |
| Safety survives arbitrary modeled action traces | `all_traces_preserve_safety` | SQL/connector traces refine the modeled transitions |
| Published result matches the specified stream operator | `end_to_end_stream_operator_correct` | All preceding premises; no outside writers to connector-owned state |
| Eventual progress and bounded shutdown | Not established | Availability/fairness assumptions, cancellation and timeout implementation |
| Every Feldera type, query and lifecycle configuration | Not established | Broader semantics and lifecycle refinement, including query changes |

`Bag Row` is `Row -> Int`. The pointwise algebra applies in particular to finite
Z-relations, but finite-support representations are not implemented. The sink
model covers signed multiplicities abstractly; the SQL witness implements only
keyed replacements. A generic duplicate-preserving materialized-table writer
has not been verified or implemented.

## Publication with existing SQL

Provision exactly one protected guard row per output group. It holds an
immutable pipeline generation, an ownership epoch, and the last committed
logical boundary. All publishers follow the protocol and update this same row.
No outside process may insert/delete guard rows or mutate connector-owned output.

For a batch carrying `(generation, epoch, base, end, payload)`:

1. Start a DuckLake transaction; read and validate the singleton guard.
2. Reject a mismatching generation or owner epoch. Treat already-committed end
   boundaries as duplicates. Require `base` to match the committed frontier.
3. Update the guard row, and apply the entire output payload, in that transaction.
4. Commit, then acknowledge success. An unknown outcome is resolved by retrying
   with the same immutable identity against the durable guard.

The payload must equal the output difference between `base` and `end`. Lean
proves correctness **given** that premise; the receiver does not recompute the
query to check it. Batches may span nonconsecutive transaction IDs and empty
steps if the covered interval and payload are correct.

The guard is also the serialization point for ownership acquisition. A takeover
updates its epoch in a transaction. Old-snapshot transactions that mutate the
same guard must conflict; old owners starting later see the new epoch and fail.
The model represents the resulting committed order, not SQL transaction internals.

[DuckLake conflict rules](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution)
describe conflicting deletes/updates. Tests observed the required conflict for
two publishers updating the same guard, and after a committed epoch change.
This is evidence for the assumed serialization, not a formal proof of those rules.

A primary-key constraint on a data table is unnecessary for this protected
singleton protocol. Its initial singleton invariant must be established during
administrative provisioning and preserved by every actor. Unrestricted competing
clients or concurrent automatic provisioning invalidate the argument.

All guard and target writes must be in the **same DuckLake catalog transaction**.
An independently committed PostgreSQL ledger would reopen the crash window.
Readers comparing the guard and several output tables must use a shared read
snapshot; separate autocommit reads can straddle a publication.

DuckLake `UPDATE ... RETURNING` was rejected by the tested build. The executable
witness uses an ordinary conditional update followed by a read in the same
transaction. The successful protocol does not require `RETURNING`.

## Input replay with existing interfaces

Use a fixed source snapshot for backfill. Follow later snapshots with correct
inclusive bounds, translating insert/postimage to +1 and delete/preimage to -1.
[The documented feed](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
changes its projected schema with the end bound and depends on retained history.
Use a fixed validated schema initially and fail on incompatible evolution or
missing history.

For exact replay, record the exact logical batch payload, metadata and boundaries
through Feldera's `Resume::Replay` journal support before allowing the relevant
step to count as recoverable. Re-reading a snapshot or sorting arbitrary physical
file chunks is not by itself an exact replay implementation. A replay hash
detects many mistakes but is not an injective encoding or a substitute for data.

Journaling already-read batches reduces dependence on old DuckLake files during
recovery. It cannot reconstruct an unconsumed change that the source has already
expired. Retention remains an operational contract, with missing history causing
a clear failure rather than silently advancing the input position.

## Feldera integration details that matter

The reviewed [controller](https://github.com/feldera/feldera/blob/c7918355c6a8a2eea4062e92d8cc0ea63a2a752f/crates/connectors/src/controller.rs#L9258)
skips empty output batches and passes transaction identities to output endpoints.
Time-based buffering can merge several batches. The connector must preserve the
covered interval and stable identity through replay. Begin with deterministic
transaction-aligned publication rather than arbitrary timer-driven regrouping.

Returning an output error is not sufficient to establish stop-and-recover
semantics: the controller's output probe records some failures and continues
completion accounting. The existing [fault-tolerant Kafka sink](https://github.com/feldera/feldera/blob/c7918355c6a8a2eea4062e92d8cc0ea63a2a752f/crates/connectors/src/transport/kafka/ft/output.rs#L467)
contains a relevant pattern: report a fatal failure and hold the output thread
until shutdown, while remaining cancellation-responsive. Encoding failures must
also prevent any partially encoded batch from being published.

For the stronger multi-table source-snapshot contract, current
[Feldera transaction behavior](https://docs.feldera.com/pipelines/transactions/)
requires special care: failure/removal of a connector can commit what it already
supplied, and transactions cannot roll back. A coordinator and failure gate must
prevent an incomplete source snapshot from being published. This is an additional
integration obligation, not a disproof of DBSP's weaker processed-prefix claim.

## Counterexamples checked in Lean

The 14 witnesses cover data-before-receipt duplication, receipt-before-data loss,
concurrent append-only receipts, deleting all duplicates, repeating the inclusive
bootstrap boundary, mixed table versions and their query effect, irreversible
history expiration, unordered replay, narrowing collisions, finite hash
collisions, mismatched checkpoint position/state, and partial multi-output
publication. These are counterexamples to naive designs, not new product bug
reports. One deliberately duplicates the same torn-state witness under the
separate output-publication interpretation.

The expired-history result is an impossibility witness: two histories have the
same retained endpoint values and different intermediate values, so no function
of just the endpoints can recover both correctly.

## What the SQL run actually exercised

`sql-results.json` records nine scenarios: five source-change windows; replay
after inline flushing and file rewriting; staged invisibility and rollback;
lost-acknowledgement retry; shared-guard writer conflict; ownership fencing;
gap/generation rejection; nonconsecutive boundaries; and the append-only-receipt
counterexample. The chosen historic raw feed windows also remained identical
after maintenance; this is not a universal feed-stability proof.

Limitations: no real Feldera pipeline, Rust/Arrow connector, PostgreSQL catalog,
network partitions, process death, power loss, schema-change matrix or exhaustive
concurrency exploration. An explicit rollback and ignored acknowledgement are
controlled SQL simulations, not crash-durability experiments. Storage and SQL
implementation correctness remain trusted.

## Recommended next work

Proceed with native Feldera adapters and the separate DuckLake access service.
Use this model as their executable specification. No new DuckDB/DuckLake primitive
has been shown necessary for the tested publication protocol; my earlier claim
that an idempotency extension might be required was stronger than the evidence.

The next gate is a Rust/service implementation whose recorded traces refine this
model: exact input journaling, interval-aware atomic guarded publication, codec
round trips, explicit fail/hold behavior, and protected ownership. Then run the
same witnesses against PostgreSQL-backed catalogs and real killed/restarted
Feldera and service processes. A separate decision is required for any guarantee
of atomicity across several source tables or output views.

## Trust boundary and reproducibility

Lean checks the abstract proofs. Trusted premises include a complete source
change feed, faithful codecs and identities, Feldera's engine/recovery behavior,
atomic durable target commits, shared-guard conflict semantics, namespace
protection, and retained unread source history. Liveness needs additional
availability assumptions. This is not source-to-machine-code verification.

`verify.sh` rechecks the Lean files with a normal Lean 4.19.0 installation. The
original DBSP Lean 3 project is not a dependency. The SQL script requires
`duckdb==2.0.0`; installation of the extension may retrieve a newer build in the
future, so compare the recorded extension version and checksum with `MANIFEST.json`.

The run used the unmodified official Lean release binary. This workspace does
not expose the executable path under `/proc`, so a small runtime compatibility
shim supplied that path. Its source is included as `runtime_compat.cpp`; it
does not change Lean's kernel, tactics, standard library or proof terms. A normal
host does not need the shim. Large tool binaries and SQL test databases are not
included in the bundle.
