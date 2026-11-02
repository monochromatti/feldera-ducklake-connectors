import Std
import Lean.Elab.Tactic.Omega

/-!
An independent Lean 4 connector protocol model, not a verification of Feldera,
DuckDB, DuckLake, their SQL parsers, storage, FFI, or a deployed connector.

Rows carry mathematical integer multiplicities. Finite support is unnecessary
for these pointwise theorems, so they also apply to finite Z-relations.
`target` denotes the correct engine output for each logical batch boundary.
Payload correctness is an explicit hypothesis, not something the sink checks.
Atomic `publish` models a transaction that mutates a pre-provisioned singleton
guard and the output in one DuckLake catalog. Its SQL refinement is NOT proved.
-/

namespace DuckLakeProof

abbrev Bag (Row : Type) := Row → Int

def add {Row : Type} (x y : Bag Row) : Bag Row := fun r => x r + y r
def delta {Row : Type} (x y : Bag Row) : Bag Row := fun r => y r - x r

theorem apply_delta {Row : Type} (x y : Bag Row) : add x (delta x y) = y := by
  funext r
  simp only [add, delta]
  omega

def integrateFrom {Row : Type} (initial : Bag Row) (changes : Nat → Bag Row) :
    Nat → Bag Row
  | 0 => initial
  | n + 1 => add (integrateFrom initial changes n) (changes n)

def SourceExact {Row : Type} (snapshots changes : Nat → Bag Row) : Prop :=
  ∀ n, changes n = delta (snapshots n) (snapshots (n + 1))

theorem source_prefix_exact {Row : Type} (snapshots changes : Nat → Bag Row)
    (h : SourceExact snapshots changes) (n : Nat) :
    integrateFrom (snapshots 0) changes n = snapshots n := by
  induction n with
  | zero => rfl
  | succ n ih =>
    simp only [integrateFrom, ih, h n]
    exact apply_delta _ _

theorem bootstrap_then_follow {Row : Type} (snapshots : Nat → Bag Row)
    (boundary count : Nat) :
    integrateFrom (snapshots boundary)
      (fun n => delta (snapshots (boundary + n))
        (snapshots (boundary + (n + 1)))) count = snapshots (boundary + count) := by
  induction count with
  | zero => simp only [integrateFrom, Nat.add_zero]
  | succ n ih =>
    simp only [integrateFrom, ih]
    exact apply_delta _ _

/-- The engine contract is passed explicitly; this file does not import/reprove
the original Lean 3 DBSP development or verify Feldera's implementation. -/
theorem source_engine_composition {In Out : Type}
    (snapshots changes : Nat → Bag In) (query : Bag In → Bag Out)
    (engine : Nat → Bag Out) (hsource : SourceExact snapshots changes)
    (hengine : ∀ n, engine n = query (integrateFrom (snapshots 0) changes n))
    (n : Nat) : engine n = query (snapshots n) := by
  rw [hengine n, source_prefix_exact snapshots changes hsource n]

/-- Stream-operator version: permits history-sensitive queries. No memoryless
relational-query restriction is imposed on `operator`. Engine correctness is
still a hypothesis; nondeterministic inputs must be captured for exact replay. -/
theorem source_operator_composition {In Out : Type}
    (snapshots changes : Nat → Bag In)
    (operator : (Nat → Bag In) → (Nat → Bag Out)) (engine : Nat → Bag Out)
    (hsource : SourceExact snapshots changes)
    (hengine : engine = operator (integrateFrom (snapshots 0) changes)) :
    engine = operator snapshots := by
  have hs : integrateFrom (snapshots 0) changes = snapshots := by
    funext n
    exact source_prefix_exact snapshots changes hsource n
  rw [hengine, hs]

theorem supported_codec_is_injective {Row Wire : Type}
    (encode : Row → Wire) (decode : Wire → Option Row) (supported : Row → Prop)
    (roundtrip : ∀ r, supported r → decode (encode r) = some r)
    (a b : Row) (ha : supported a) (hb : supported b)
    (heq : encode a = encode b) : a = b := by
  have hd := congrArg decode heq
  rw [roundtrip a ha, roundtrip b hb] at hd
  exact Option.some.inj hd

structure Checkpoint (Row : Type) where
  position : Nat
  input : Bag Row

/-- A checkpoint must contain matching engine input state and source position.
The journal here denotes the exact logical batch payloads, not file offsets or
hashes. Actual byte serialization and durability are refinement obligations. -/
theorem coherent_checkpoint_and_exact_journal_recover {Row : Type}
    (snapshots journal : Nat → Bag Row) (checkpoint : Checkpoint Row)
    (hcheckpoint : checkpoint.input = snapshots checkpoint.position)
    (hjournal : SourceExact snapshots journal) (count : Nat) :
    integrateFrom checkpoint.input (fun n => journal (checkpoint.position + n)) count =
      snapshots (checkpoint.position + count) := by
  have hj : (fun n => journal (checkpoint.position + n)) =
      (fun n => delta (snapshots (checkpoint.position + n))
        (snapshots (checkpoint.position + (n + 1)))) := by
    funext n
    rw [hjournal (checkpoint.position + n)]
    congr 2
  rw [hj, hcheckpoint]
  exact bootstrap_then_follow snapshots checkpoint.position count

structure Durable (Row : Type) where
  generation : Nat
  epoch : Nat
  frontier : Nat
  rows : Bag Row

structure Batch (Row : Type) where
  generation : Nat
  epoch : Nat
  base : Nat
  seq : Nat
  payload : Bag Row

def Eligible {Row : Type} (d : Durable Row) (b : Batch Row) : Prop :=
  b.generation = d.generation ∧ b.epoch = d.epoch ∧
    b.base = d.frontier ∧ d.frontier < b.seq

instance {Row : Type} (d : Durable Row) (b : Batch Row) : Decidable (Eligible d b) :=
  inferInstanceAs (Decidable (_ ∧ _ ∧ _ ∧ _))

/-- Duplicate, stale, foreign-generation and out-of-order requests leave the
durable state unchanged. Implementations must distinguish retry/error outcomes. -/
def publish {Row : Type} (d : Durable Row) (b : Batch Row) : Durable Row :=
  if Eligible d b then
    { d with frontier := b.seq, rows := add d.rows b.payload }
  else d

def Safe {Row : Type} (target : Nat → Bag Row) (d : Durable Row) : Prop :=
  d.rows = target d.frontier

def ValidPayload {Row : Type} (target : Nat → Bag Row) (b : Batch Row) : Prop :=
  b.payload = delta (target b.base) (target b.seq)

theorem publish_preserves_correctness {Row : Type} (target : Nat → Bag Row)
    (d : Durable Row) (b : Batch Row) (hsafe : Safe target d)
    (hpayload : ValidPayload target b) : Safe target (publish d b) := by
  by_cases h : Eligible d b
  · have hprev : b.base = d.frontier := h.2.2.1
    simp only [publish, if_pos h, Safe]
    change add d.rows b.payload = target b.seq
    rw [hpayload, hprev, hsafe]
    exact apply_delta _ _
  · simpa only [publish, if_neg h] using hsafe

theorem duplicate_is_noop {Row : Type} (d : Durable Row) (b : Batch Row)
    (h : b.seq ≤ d.frontier) : publish d b = d := by
  have hn : ¬ Eligible d b := by intro he; have := he.2.2.2; omega
  simp only [publish, if_neg hn]

theorem wrong_generation_is_noop {Row : Type} (d : Durable Row) (b : Batch Row)
    (h : b.generation ≠ d.generation) : publish d b = d := by
  have hn : ¬ Eligible d b := fun he => h he.1
  simp only [publish, if_neg hn]

theorem stale_epoch_is_noop {Row : Type} (d : Durable Row) (b : Batch Row)
    (h : b.epoch < d.epoch) : publish d b = d := by
  have hn : ¬ Eligible d b := by intro he; have := he.2.1; omega
  simp only [publish, if_neg hn]

theorem base_mismatch_is_noop {Row : Type} (d : Durable Row) (b : Batch Row)
    (h : b.base ≠ d.frontier) : publish d b = d := by
  have hn : ¬ Eligible d b := fun he => h he.2.2.1
  simp only [publish, if_neg hn]

theorem eligible_batch_advances {Row : Type} (d : Durable Row) (b : Batch Row)
    (h : Eligible d b) : (publish d b).frontier = b.seq := by
  simp only [publish, if_pos h]

theorem frontier_monotone {Row : Type} (d : Durable Row) (b : Batch Row) :
    d.frontier ≤ (publish d b).frontier := by
  by_cases h : Eligible d b
  · rw [eligible_batch_advances d b h]; have := h.2.2.2; omega
  · simp only [publish, if_neg h]; omega

theorem publish_idempotent {Row : Type} (d : Durable Row) (b : Batch Row) :
    publish (publish d b) b = publish d b := by
  by_cases h : Eligible d b
  · apply duplicate_is_noop
    rw [eligible_batch_advances d b h]
    exact Nat.le_refl _
  · simp only [publish, if_neg h]

def takeover {Row : Type} (d : Durable Row) : Durable Row :=
  { d with epoch := d.epoch + 1 }

theorem takeover_preserves_correctness {Row : Type} (target : Nat → Bag Row)
    (d : Durable Row) (h : Safe target d) : Safe target (takeover d) := h

theorem takeover_fences_previous_owner {Row : Type} (d : Durable Row) (b : Batch Row)
    (h : b.epoch = d.epoch) : publish (takeover d) b = takeover d := by
  apply stale_epoch_is_noop
  simp only [takeover]
  omega

structure Runtime (Row : Type) where
  durable : Durable Row
  pending : Option (Batch Row)

def stage {Row : Type} (s : Runtime Row) (b : Batch Row) : Runtime Row :=
  { s with pending := some b }

def crash {Row : Type} (s : Runtime Row) : Runtime Row :=
  { s with pending := none }

/-- Pending request remains until acknowledgement: repeated commit represents
retry after a lost acknowledgement. -/
def commit {Row : Type} (s : Runtime Row) : Runtime Row :=
  match s.pending with
  | none => s
  | some b => { s with durable := publish s.durable b }

def StateSafe {Row : Type} (target : Nat → Bag Row) (s : Runtime Row) : Prop :=
  Safe target s.durable ∧ ∀ b, s.pending = some b → ValidPayload target b

theorem stage_is_invisible {Row : Type} (s : Runtime Row) (b : Batch Row) :
    (stage s b).durable = s.durable := rfl

theorem crash_keeps_committed_state {Row : Type} (s : Runtime Row) :
    (crash s).durable = s.durable := rfl

theorem retry_after_lost_ack_is_idempotent {Row : Type} (s : Runtime Row) :
    commit (commit s) = commit s := by
  cases hp : s.pending with
  | none => simp [commit, hp]
  | some b => simp [commit, hp, publish_idempotent]

inductive Transition {Row : Type} (target : Nat → Bag Row) :
    Runtime Row → Runtime Row → Prop
  | stage (s : Runtime Row) (b : Batch Row) (h : ValidPayload target b) :
      Transition target s (DuckLakeProof.stage s b)
  | commit (s : Runtime Row) : Transition target s (DuckLakeProof.commit s)
  | crash (s : Runtime Row) : Transition target s (DuckLakeProof.crash s)
  | acknowledge (s : Runtime Row) : Transition target s (DuckLakeProof.crash s)
  | fence (s : Runtime Row) :
      Transition target s { s with durable := takeover s.durable }

theorem transition_preserves_safety {Row : Type} (target : Nat → Bag Row)
    (s t : Runtime Row) (hstep : Transition target s t) (hs : StateSafe target s) :
    StateSafe target t := by
  cases hstep with
  | stage b hv =>
    constructor
    · exact hs.1
    · intro b' heq
      have he : b = b' := Option.some.inj heq
      simpa only [← he] using hv
  | commit =>
    cases hp : s.pending with
    | none => simpa only [commit, hp] using hs
    | some b =>
      constructor
      · simpa only [commit, hp] using
          publish_preserves_correctness target s.durable b hs.1 (hs.2 b hp)
      · simpa only [commit, hp] using hs.2
  | crash => exact ⟨hs.1, by intro b heq; cases heq⟩
  | acknowledge => exact ⟨hs.1, by intro b heq; cases heq⟩
  | fence => exact ⟨hs.1, hs.2⟩

inductive Trace {Row : Type} (target : Nat → Bag Row) :
    Runtime Row → Runtime Row → Prop
  | refl (s : Runtime Row) : Trace target s s
  | next {s t u : Runtime Row} (earlier : Trace target s t)
      (step : Transition target t u) : Trace target s u

/-- Arbitrarily many staging, publication, lost acknowledgements, crashes and
ownership changes preserve the visible output invariant under explicit premises. -/
theorem all_traces_preserve_safety {Row : Type} (target : Nat → Bag Row)
    (s t : Runtime Row) (htrace : Trace target s t) (hs : StateSafe target s) :
    StateSafe target t := by
  induction htrace with
  | refl => exact hs
  | next earlier step ih => exact transition_preserves_safety target _ _ step ih

theorem end_to_end_snapshot_correct {In Out : Type}
    (snapshots changes : Nat → Bag In) (query : Bag In → Bag Out)
    (engine : Nat → Bag Out) (hsource : SourceExact snapshots changes)
    (hengine : ∀ n, engine n = query (integrateFrom (snapshots 0) changes n))
    (s t : Runtime Out) (hs : StateSafe engine s) (htrace : Trace engine s t) :
    t.durable.rows = query (snapshots t.durable.frontier) := by
  have hv := (all_traces_preserve_safety engine s t htrace hs).1
  exact hv.trans (source_engine_composition snapshots changes query engine hsource
    hengine t.durable.frontier)

theorem end_to_end_stream_operator_correct {In Out : Type}
    (snapshots changes : Nat → Bag In)
    (operator : (Nat → Bag In) → (Nat → Bag Out)) (engine : Nat → Bag Out)
    (hsource : SourceExact snapshots changes)
    (hengine : engine = operator (integrateFrom (snapshots 0) changes))
    (s t : Runtime Out) (hs : StateSafe engine s) (htrace : Trace engine s t) :
    t.durable.rows = operator snapshots t.durable.frontier := by
  have hv := (all_traces_preserve_safety engine s t htrace hs).1
  rw [source_operator_composition snapshots changes operator engine hsource hengine] at hv
  exact hv

/-! Counterexamples are kernel-checked concrete witnesses. They describe naive
protocols, not claims that a particular released product executes those protocols. -/
namespace Counterexamples

theorem non_atomic_data_then_marker_duplicates : (0 : Int) + 1 + 1 ≠ 0 + 1 := by decide

theorem non_atomic_marker_then_data_loses : (0 : Int) ≠ 0 + 1 := by decide

/-- Two transactions both observe absent receipts and append disjoint new rows;
ordinary snapshot isolation need not conflict. A common write guard is required. -/
theorem concurrent_append_receipts_are_not_deduplication :
    ((0 : Int) + 1 + 1, true) ≠ (1, true) := by decide

theorem delete_all_matching_rows_breaks_bags : (0 : Int) ≠ 2 - 1 := by decide

theorem duplicate_inclusive_bootstrap_boundary : (1 : Int) + 1 ≠ 1 := by decide

/-- Two tables change from (0,0) to (1,1) atomically. Mixing their snapshots can
produce (1,0), which is no committed source snapshot. -/
theorem mixed_table_versions_are_not_a_snapshot :
    ((1 : Int), (0 : Int)) ≠ (0, 0) ∧ ((1 : Int), (0 : Int)) ≠ (1, 1) := by decide

/-- A join-like query exposes the torn update even though each table reader
individually returns a real version. -/
theorem mixed_table_versions_change_query :
    ((1 : Int) - 0) ≠ 0 - 0 ∧ ((1 : Int) - 0) ≠ 1 - 1 := by decide

def historyA (n : Nat) : Int := if n = 1 then 1 else 0
def historyB (n : Nat) : Int := if n = 1 then 2 else 0

theorem expired_history_is_information_loss :
    historyA 0 = historyB 0 ∧ historyA 2 = historyB 2 ∧ historyA 1 ≠ historyB 1 := by
  decide

/-- No reconstruction from retained endpoint values alone can recover both
missing intermediate snapshots. Journaling or retained history is necessary. -/
theorem no_universal_recovery_from_endpoints :
    ¬ ∃ recover : Int → Int → Int,
      recover (historyA 0) (historyA 2) = historyA 1 ∧
      recover (historyB 0) (historyB 2) = historyB 1 := by
  intro ⟨recover, ha, hb⟩
  change recover 0 0 = 1 at ha
  change recover 0 0 = 2 at hb
  omega

theorem unordered_chunk_replay_changes_step :
    ([1, 2] : List Int) ≠ [2, 1] := by decide

theorem narrow_encoding_collides :
    (0 : Nat) % (2 ^ 64) = (2 ^ 64) % (2 ^ 64) ∧ (0 : Nat) ≠ 2 ^ 64 := by decide

theorem finite_hash_equality_is_not_payload_equality :
    (0 : Nat) % 256 = 256 % 256 ∧ (0 : Nat) ≠ 256 := by decide

theorem offset_without_matching_state_loses_input :
    (0 : Int) + (2 - 1) ≠ 2 := by decide

theorem output_only_failure_is_not_source_snapshot_atomicity :
    ((1 : Int), (0 : Int)) ≠ (0, 0) ∧ ((1 : Int), (0 : Int)) ≠ (1, 1) := by decide

end Counterexamples

#print axioms source_prefix_exact
#print axioms publish_preserves_correctness
#print axioms publish_idempotent
#print axioms all_traces_preserve_safety
#print axioms end_to_end_snapshot_correct
#print axioms end_to_end_stream_operator_correct
#print axioms supported_codec_is_injective
#print axioms coherent_checkpoint_and_exact_journal_recover
#print axioms Counterexamples.no_universal_recovery_from_endpoints

end DuckLakeProof
