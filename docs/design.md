# Design: from the verified model to this code

This repository is the executable counterpart of the Lean development vendored in
[`../formal/`](../formal/). That bundle is the authoritative record of *why* the
protocol looks the way it does; this document is the bridge from its theorems to
the code paths that discharge them.

Read in this order:

1. [`formal/REPORT.md`](../formal/REPORT.md) — verdict, assumptions, obligations,
   trust boundary. The sections *Verdict*, *Checked properties and remaining
   obligations* and *Trust boundary and reproducibility* are the important ones.
2. This file — the theorem ↔ code mapping.
3. `src/fd_connectors/protocol.py` — the model's decisions as executable Python.

## The model in one paragraph

`Bag Row` is `Row → Int`, a signed-multiset relation, so inserts and deletes are
the same algebra and a `delete` of a row that is absent is representable. A
`Batch` is the publisher's *claim*: "when the frontier is `base`, the group's new
state is the old state plus these deltas, and the frontier becomes `end`". A
`Guard` is the one protected row per output group holding generation, epoch and
frontier. `Runtime` models staging, committing and crashing, and `Transition` /
`Trace` quantify over arbitrary interleavings of those actions.

`end_to_end_stream_operator_correct` is the strongest single result: given exact
signed deltas from the source, an engine implementing its specified stream
operator, and publication through the modeled atomic protocol, every reachable
published state equals the specified result at its recorded frontier. The
operator may depend on input history; it need not be a memoryless query.

Everything in Lean is a statement about **definitions and hypotheses**, not about
unmodeled software. Each row below is therefore an obligation that this code has
to meet, not a property it inherits for free.

## Verified properties and where they are discharged

| Property | Lean theorem | Discharged here by |
|---|---|---|
| Backfill followed by exact changes reconstructs snapshots | `bootstrap_then_follow`, `source_prefix_exact` | `InputConnector.bootstrap` + `catalog.DuckLake.table_changes`, which advances the change interval by one because `ducklake_table_changes` is inclusive on both bounds. Scenario `source/change-interval-is-not-replayed`; test `test_incremental_poll_does_not_duplicate_rows` |
| Source correctness composes with a specified engine | `source_engine_composition`, `source_operator_composition` | Exercised end to end rather than proved: `tests/test_end_to_end.py::test_happy_path_aggregate_matches_and_frontier_advanced` reads the aggregate back from Feldera and the frontier back from the catalog |
| Supported row codecs preserve identity | `supported_codec_is_injective` | `codec.to_json` / `codec.from_json` refuse anything without an exact JSON form (`CodecError`), and `reject_unsupported_column_types` fails a spec up front. Test `test_the_captured_chunks_round_trip_through_json` |
| Checkpoint plus exact journal restores input state | `coherent_checkpoint_and_exact_journal_recover` | `journal.Journal` writes before the engine can see a batch (`write_atomic`, fsync, atomic rename); the checkpoint advances only on acknowledgement. Tests `test_input_restart_does_not_replay_consumed_changes`, `test_recover_fails_on_uncertain_batch_and_rebootstraps` |
| Publication preserves the target result | `publish_preserves_correctness` | `catalog.DuckLake.publish` applies payload and guard in one transaction. Scenario `guard/apply-advances-frontier` |
| Retries and lost acknowledgement do not duplicate output | `publish_idempotent`, `retry_after_lost_ack_is_idempotent` | `protocol.decide` tests batch identity **before** `base`, so a retry is recognised as already-committed rather than as stale. Scenario `guard/duplicate-retry-is-a-no-op`; test `test_replaying_committed_batch_is_a_duplicate_noop` |
| Stale owner and wrong-generation writes do nothing | `stale_epoch_is_noop`, `takeover_fences_previous_owner`, `wrong_generation_is_noop` | Conditional guard update; `epoch` must strictly increase and only `acquire_ownership` changes it. Scenarios `guard/stale-publisher-is-refused`, `guard/foreign-generation-is-refused`, `guard/ownership-requires-explicit-takeover`; test `test_stale_and_wrong_generation_are_refused` |
| Uncommitted staging is invisible | `stage_is_invisible` | One transaction per publication, and every scenario reads committed state through a **second cursor** rather than through the writing connection. Scenarios `ducklake/reader-sees-payload-and-guard-together`, `ducklake/payload-and-guard-are-atomic-on-payload-failure` |
| Publication is idempotent under resync | `publish_idempotent` | `prepare()`/resync replaces content deterministically. Scenario `ducklake/resync-replaces-content-and-is-idempotent` |
| Safety survives arbitrary modeled traces | `all_traces_preserve_safety`, `transition_preserves_safety` | Not directly discharged. The model quantifies over interleavings; the code only has the scenarios and e2e tests below, which are samples, not a quantification |
| Published result matches the specified stream operator | `end_to_end_stream_operator_correct` | `tests/test_end_to_end.py` as a whole |
| Eventual progress and bounded shutdown | **not established** | `OutputConnector.run(stop_after_idle=..., max_batches=...)` is an implementation choice. Liveness needs fairness and availability assumptions the model does not make |
| Every Feldera type, query and lifecycle configuration | **not established** | Bounded by `reject_unsupported_column_types` and the Limitations section of the README |

## Counterexamples and the guard that avoids each

These are counterexamples to **naive designs**, not reports of product bugs.

| Lean witness | The naive design it kills | Guard in this repo |
|---|---|---|
| `non_atomic_data_then_marker_duplicates` | Release rows as soon as a row-bearing chunk arrives | `StepAssembler` holds rows until a boundary (`processed_steps > 0`) that every view of the group agrees on. Test `test_group_result_is_released_at_the_boundary_all_views_agree_on` |
| `non_atomic_marker_then_data_loses` | Treat a boundary marker as completing the step | Same hold-and-release; a group without snapshots is not released when snapshots are required (`test_a_group_without_snapshots_is_not_released_when_snapshots_are_required`) |
| `concurrent_append_receipts_are_not_deduplication` | Append-only receipts as a dedup mechanism | The guard is a single **mutable** row, so two concurrent publishers conflict instead of both succeeding. Scenario `report/lost-update-counterexample`, `ducklake/concurrent-publishers-converge-to-one-outcome` |
| `delete_all_matching_rows_breaks_bags` | `DELETE ... WHERE key = ?` for a keyed update | Deltas carry signed multiplicity; a keyed update is a delete of one copy then an insert. Scenarios `ducklake/multiset-delete-removes-one-copy`, `ducklake/keyed-update-is-delete-then-insert`; test `test_the_same_row_may_be_deleted_later` |
| `duplicate_inclusive_bootstrap_boundary` | Treat the change feed's bounds as half-open | `catalog.DuckLake.table_changes` advances the start by one. Scenario `source/change-interval-is-not-replayed` |
| `mixed_table_versions_are_not_a_snapshot`, `mixed_table_versions_change_query` | Read several tables at whatever version happens to be current | Reads go through an explicit snapshot version (`catalog.DuckLake.read_snapshot`, `current_snapshot`) rather than `latest`. **Obligation:** version skew across several tables in one group is not separately tested |
| `expired_history_is_information_loss` | Recover a gap from the retained endpoints alone | `catalog.HistoryExpiredError` surfaces the impossibility instead of inventing an answer |
| `no_universal_recovery_from_endpoints` | Infer the input state from the last observed boundary | `InputConnector.uncertain()` plus `recover(policy="fail")`; the batch is left uncertain and a human decides. Test `test_recover_fails_on_uncertain_batch_and_rebootstraps` |
| `unordered_chunk_replay_changes_step` | Treat chunk arrival order as meaningful | `protocol.payload_digest` sorts deltas before hashing, so reordering is not a payload change. Test `test_deletes_are_ordered_before_inserts_regardless_of_arrival` |
| `narrow_encoding_collides` | Encode into a narrower type and hope | `codec.to_json` refuses rather than narrowing |
| `finite_hash_equality_is_not_payload_equality` | Use the payload digest as the batch identity | Identity is `(generation, interval)`; the digest is only used to detect divergence. Scenarios `source/identity-is-the-interval-not-the-digest`, `guard/divergent-payload-for-committed-identity-is-detected` |
| `offset_without_matching_state_loses_input` | Resume from an offset without the state that offset refers to | Journal and checkpoint are written together and reconciled on `recover()` |
| `output_only_failure_is_not_source_snapshot_atomicity` | Assume an output failure implies the source did not advance | Publication and the input checkpoint are independent; the input checkpoint follows the engine's acknowledgement only |

## Parity appendix: the same protocol in two languages

There are two implementations of this protocol — `src/fd_connectors/` (Python, the reference)
and `crates/connectors/src/integrated/ducklake/` in the Feldera fork (Rust). They were written
against the same model but not against each other, so the risk is not either one being wrong; it
is the two of them disagreeing in a place where only one has a test.

This table maps each requirement to the test that pins it **on each side**. A `—` is not a gap in
behaviour, it is a gap in *evidence*; the note says which.

| Requirement | Python scenario | Rust test |
|---|---|---|
| The decision is total over every guard/batch pairing | `protocol/decision-is-total` | `protocol::the_decision_is_total` (unit) |
| A batch on the frontier applies and advances it | `guard/apply-advances-frontier` | `protocol::a_batch_on_top_of_the_frontier_applies`, `protocol::advancing_records_the_identity_and_the_digest`, `tests_live::the_frontier_survives_reopening_the_catalog` |
| Replaying a committed batch is a no-op | `guard/duplicate-retry-is-a-no-op` | `protocol::replaying_the_committed_batch_is_a_duplicate_not_a_second_apply`, `tests_live::publishing_the_same_batch_twice_applies_it_once` |
| A publisher behind the frontier is stale | `guard/stale-publisher-is-refused` | `protocol::a_batch_whose_base_is_not_the_frontier_is_stale`, `protocol::an_overtaken_interval_is_stale_even_with_an_older_base` |
| A foreign generation is refused | `guard/foreign-generation-is-refused` | `protocol::another_generation_is_refused_outright`, `protocol::a_fenced_writer_is_refused_even_with_a_higher_epoch` |
| Ownership moves only by a strictly greater epoch | `guard/ownership-requires-explicit-takeover` | `protocol::taking_over_requires_a_strictly_greater_epoch` |
| Same identity with different content stops the writer | `guard/divergent-payload-for-committed-identity-is-detected` | `protocol::the_same_identity_with_different_content_stops_the_writer`, `tests_live::a_divergent_payload_for_a_committed_identity_is_refused` |
| The digest ignores delta order | `test_the_captured_chunks_round_trip_through_json` (via `payload_digest`) | `protocol::the_digest_ignores_delta_order` |
| Payload and guard commit atomically, or not at all | `ducklake/payload-and-guard-are-atomic-on-payload-failure` | `tests_live::a_payload_failure_leaves_the_guard_and_the_history_where_they_were` |
| A reader never sees the payload without the guard | `ducklake/reader-sees-payload-and-guard-together` | — the Rust writer commits in one transaction, so there is no second cursor to check from; the property follows from the single statement pair in `writer.rs::publish` |
| A rolled-back publication leaves history replayable | `ducklake/rollback-leaves-history-replayable` | — no counterpart: the Rust side has no journal and no replay buffer to leave behind |
| A resync replaces content and replays as a duplicate | `ducklake/resync-replaces-content-and-is-idempotent` | `tests_live::a_resync_replaces_the_content_and_replays_as_a_duplicate` |
| A resync clears only its own group | `guard/resync-clears-only-the-tables-it-repopulates` | `tests_live::a_resync_clears_only_its_own_group` |
| A resync behind the frontier moves the frontier back | — Python applies a resync from an unrelated frontier; that is the `Apply` branch, not a separate rule | `protocol::a_resync_behind_the_frontier_moves_the_frontier_back`, `protocol::a_resync_applies_from_an_unrelated_frontier` |
| A NULL key part is matched, not skipped | `guard/keyed-replacement-matches-a-null-key` | `tests_live::a_null_key_part_is_matched_not_skipped` |
| A keyed update leaves the post-image | `ducklake/keyed-update-is-delete-then-insert` | `tests_live::a_keyed_update_leaves_the_post_image` |
| A bag delete removes one copy, not all | `ducklake/multiset-delete-removes-one-copy` | `tests_live::a_multiset_delete_removes_one_copy` |
| Concurrent publishers converge on one outcome | `ducklake/concurrent-publishers-converge-to-one-outcome` | `two_engines_publishing_the_same_step_converge_on_one_outcome`, `tests_live::two_groups_commit_concurrently_into_one_catalog` |
| The guard write is conditional | `source/guard-update-is-conditional` | — white-box only on the Python side: the scenario calls `_update_guard` with a stale guard. `Catalog::update_guard` is private, and the closest black-box witness is `two_writers_attach_one_fresh_catalog_at_once` |
| A duplicated guard row is still one guard | `guard/a-duplicated-guard-row-is-still-one-guard` | `tests_live::two_writers_attach_one_fresh_catalog_at_once` |
| An update that matched no row is refused | `guard/an-update-that-matched-nothing-is-refused` | — same visibility limit; the nonce check is in `Catalog::update_guard` and has no white-box test (TODO §20) |
| The change interval is not replayed | `source/change-interval-is-not-replayed` | `tests_live::bootstrap_reads_the_table_once_and_records_its_snapshot`, `tests_live::a_new_snapshot_is_replayed_with_its_change_types` |
| A catalog that goes backwards is refused | `source/catalog-rewind-is-refused` | `tests_live::a_catalog_that_went_backwards_is_refused` |
| Identity is the interval, not the digest | `source/identity-is-the-interval-not-the-digest` | `protocol::advancing_records_the_identity_and_the_digest` |
| Append-only receipts do not deduplicate | `report/lost-update-counterexample` | `two_engines_publishing_the_same_step_converge_on_one_outcome` |
| A checkpoint that does not parse is refused | `journal.Checkpoint.from_json` raises `KeyError`; no scenario pins it | `tests_live::a_checkpoint_that_does_not_parse_is_refused` |
| The output frontier survives a restart | `tests/test_end_to_end.py::test_input_restart_does_not_replay_consumed_changes` is the *input* side | `test::ducklake_output_continues_from_its_frontier_after_a_restart` — the *output* side has no Python counterpart, because there is no engine behind the Python output connector to restart |
| One implementation's catalog is readable by the other | — no engine on the Python side | `test::ducklake_to_ducklake_copies_a_table_into_another` |

### Deliberate divergences

- **The journal.** Python keeps one (`journal.py`) because it must name the batch it is about to
  hand to the engine *before* the engine acknowledges it; the Rust reader takes its state from the
  engine's checkpoint and needs no second copy. Neither is a port of the other.
- **Who drives DuckDB.** Python opens the catalog itself (a file or a PostgreSQL database) and
  installs the DuckLake extension in-process; Rust runs inside the engine's DuckDB session and
  loads the extension from a configured directory
  (`tests_live::the_extension_is_loaded_from_the_configured_directory`).
- **Error surfacing.** Python raises typed exceptions (`ProtocolError`, `GuardInvariantError`,
  `HistoryExpiredError`); Rust bails with context strings. The *messages* are therefore not
  shared: a monitoring rule keyed on text works on one side only, and the tests assert the
  exception type on one side and the message on the other.
- **Shape of the entry point.** Python's `publish(batch)` is called directly by the scenario
  harness; Rust's hangs off `batch_start`/`batch_end`. This is why the Python suite can be
  white-box about `_update_guard` and the Rust one cannot.

### What this appendix does not claim

It is a mapping of intent, not an automated equivalence check: no test asserts that the two
implementations produce the same guard row for the same input. Where both sides have a test, they
assert the same property in different words, and the words are what a reader has to compare.

## Trust boundary

The theorems hold under these premises, all of which come from the world, not
from this repository (see `formal/REPORT.md` §*Trust boundary*):

- the source change feed is complete for the interval asked of it, and unread
  history is still retained;
- codecs and batch identities are faithful;
- Feldera implements its specified engine and recovery behaviour;
- target commits are atomic and durable, and the guard row has the shared-conflict
  semantics relied on (`UPDATE ... WHERE` on a single row);
- nothing outside the connector writes to connector-owned state.

Liveness is *not* covered: progress needs availability and fairness assumptions
that the model deliberately does not make. This is not source-to-machine-code
verification.

## Re-checking the model

`formal/` is vendored byte-for-byte; `formal/MANIFEST.json` records a SHA-256 for
every file, so drift is detectable:

```bash
cd formal && python3 - <<'PY'
import hashlib, json, pathlib
for name, want in json.load(open("MANIFEST.json"))["file_sha256"].items():
    got = hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest()
    print(("ok  " if got == want else "DIFF"), name)
PY
```

Re-check the proofs with Lean 4.19.0 (`devenv shell` provides `pkgs.lean4`; the
bundle pins the toolchain in `formal/lean-toolchain`):

```bash
cd formal && bash verify.sh
```

Expected: 38 declarations check (24 positive, 14 counterexamples), no `sorry`,
`admit`, new `axiom` or `native_decide`, and an axiom audit using only `propext`
and `Quot.sound`.

The bundle's `formal/sql_checks.py` is the **first round**: nine SQL scenarios
against DuckDB 2.0.0 and DuckLake build `417a9117f5`, recorded in
`formal/sql-results.json`. `src/fd_connectors/scenarios.py` is the evolved,
executable descendant of those checks — 23 scenarios, run by `fd-connectors
validate-local`, and the thing CI gates on. When the two disagree, the code and
the scenarios are what ship; the model is what justifies them, and changing the
protocol means changing the model too.