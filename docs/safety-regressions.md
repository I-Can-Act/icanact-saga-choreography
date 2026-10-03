# Production safety remediation: coverage ledger

Audit baseline: `583cd830fda8b4518a7aa7a6a6d7e8f654079a1f`.
Plan: [Production Saga Safety](plans/2026-10-02-1558-fix-production-saga-safety-plan.md).
The original thirteen QA probes asserted defects. They remain preserved in the separate
QA worktree and are **not** green acceptance tests for this branch. The tests below
assert safe behavior instead.

## All thirteen observations mapped to safety assertions

1. `qa_confirms_failed_wal_writes_still_execute_and_publish_success`:
   `tests/qa_remediation_helpers.rs::failed_intent_write_runs_no_effect_and_publishes_no_success`
   runs sync and async paths; `tests/qa_remediation_workflows.rs::failed_intent_write_prevents_workflow_effect_and_quarantines_visibly`
   covers workflow ingress.
2. `qa_confirms_failed_completion_append_loses_compensation_data_but_reports_success`:
   helpers `failed_result_write_publishes_no_success_and_keeps_compensation_evidence` and
   workflows `failed_result_write_cannot_publish_workflow_success`. Typed
   `ParticipantReconciliationEvidence` keeps compensation bytes without exposing them
   in public reasons; neither path publishes completion.
3. `qa_confirms_quarantine_prunes_evidence_and_restart_allows_same_effect_again`:
   helpers `quarantine_retains_journal_and_dedupe_and_blocks_restart_reexecution`;
   `tests/durability_lmdb_integration.rs::workflow_reopen::quarantined_run_keeps_evidence_and_blocks_reuse_after_lmdb_reopen`.
4. `qa_confirms_completed_saga_replay_duplicates_effect_after_participant_restart`:
   helpers `terminal_run_replay_after_restart_does_not_repeat_effect` and
   `failed_run_fence_survives_cache_eviction`; workflows
   `completed_run_does_not_repeat_effect_after_restart_and_later_run_is_admitted`.
5. `qa_confirms_lmdb_restart_reexecutes_completed_and_quarantined_sagas`:
   the two reopened-LMDB terminal/quarantine tests in `tests/durability_lmdb_integration.rs`.
6. `qa_confirms_retained_resolver_history_does_not_prevent_lmdb_participant_replay_effect`:
   `workflow_reopen::resolver_and_participant_lmdb_reopen_fence_replay_before_fanout`
   reopens both real LMDB stores, tests bus admission before fanout and independently
   exercises participant workflow ingress. Actual actor backpressure is covered separately.
7. `qa_confirms_lmdb_resolver_cannot_decode_its_own_persisted_rows`:
   `tests/qa_remediation_resolver_journal.rs::varied_payload_lengths_survive_live_read_and_reopen`;
   corruption stays an error. Participant varied-length and malformed-row tests cover
   the other aligned archive boundary.
8. `qa_confirms_overall_and_stalled_timeouts_skip_known_and_pending_compensation`:
   `tests/qa_remediation_resolver.rs::forward_overall_and_stalled_timeouts_compensate_or_quarantine`.
   Forward expiry with effects requests undo; unresolved rollback expiry quarantines.
9. `qa_confirms_recovered_timeout_escapes_before_binding_and_recovery_activation`:
   `tests/qa_remediation_bus.rs::recovered_deadline_is_held_until_activation_then_delivered_once`
   plus `ingress_driven_recovery_output_is_held_until_activation_too` and
   `new_start_is_not_admitted_before_recovery_activation`.
10. `qa_confirms_late_allof_sibling_effect_is_not_added_to_pending_rollback`:
    resolver `late_allof_effect_is_undone_before_failure_is_terminal`; integration
    `completion_after_accepted_effect_was_undone_escalates_terminal_failure_to_quarantine`
    also closes the late-after-terminal boundary.
11. `qa_confirms_anyof_sibling_completion_succeeds_during_pending_rollback`:
    resolver `anyof_success_cannot_complete_while_rollback_is_pending` and
    `non_compensating_failure_during_rollback_does_not_bypass_obligations`.
12. `qa_confirms_compensation_list_order_is_not_enforced_by_participants`:
    resolver `undo_is_singleton_reverse_order_and_ignores_duplicates` and the accepted-undo
    successor barrier; helpers' legacy-head tests run both sync and async; workflow
    `a_non_owned_undo_request_does_not_consume_the_owned_successor_dedupe_key` covers legacy
    non-head rejection and a later owned singleton. Actor e2e tests exercise integration.
13. `qa_confirms_partial_delivery_emits_terminal_failure_without_compensation`:
    bus `actor_forward_failure_after_effect_quarantines_instead_of_failing` and
    `partially_delivered_compensation_retains_reconciliation_evidence` use a real saturated
    capacity-one actor channel. The bus unit
    `saga_started_with_live_delivery_shortfall_quarantines_possible_effects` proves that
    partial start fanout can already have reached an effect owner.

## Conditional/P2 and integration boundaries

- Strict run dedupe distinguishes `Duplicate` from storage failure; helper and workflow
  tests require visible quarantine, not suppressed transition validation.
- Sync, async and workflow declared-effect tests verify dispatch, fail-closed defaults and
  failed dispatch. Compensation data is durable before dispatch. Application receipts
  and stable external keys remain necessary across crash windows.
- `tests/qa_remediation_state.rs` pins previous archive tags and appended tags 12–14,
  terminal/stale replay, later valid reuse, active ownership and ambiguous legacy history.
- `tests/qa_remediation_integration.rs` covers quarantine dominance, late-after-undo
  effects, definitive final undo failure, active durable bus ownership and admission
  precedence over stale contract diagnostics.
- Resolver cache bounding and safe journal maintenance have focused tests. Compaction
  preserves **all** quarantine/unresolved detail and permanent terminal fences; small-map
  exhaustion and unsupported maintenance fail visibly.
- The parent reproduced and repaired the bus/resolver lifecycle ownership cycle:
  `dropping_the_last_public_bus_releases_resolver_journal_ownership` proves shutdown
  releases journal ownership so stores can genuinely close/reopen.
- Extra parent RED/GREEN tests reject changed-trace forward reexecution, persist local
  workflow quarantine without self-delivery, preserve a new accepted owner on stale
  timeouts and fence an active successor when older-run uncertainty arrives. Stale
  generic execution/undo recovers as quarantine with its original identity; unidentified
  stale history blocks recovery instead of fabricating an ordinary terminal result.
- Existing tests that expected automatic terminal pruning or ordinary failure after
  unsuccessful undo now assert retained evidence/quarantine. Recovery fixtures share a
  single run identity and assert singleton, dependency-ordered compensation.

## Independent review follow-up coverage

The first independent review of `ece1cb4` returned **BLOCK** (two P1, nine P2).
The following assertions address its concrete findings; a passing suite is not a review
verdict. The same reviewer is re-challenged only after parent verification/commit.

- **P1-1, unresolved forward effects:** `tests/qa_review_resolver.rs` exercises long-running
  and accepted work, known undo plus unknown obligations, and late effects after ordinary
  failure/full or compacted history. `tests/qa_review_state.rs` fences unsafe failure and
  pins confirmed outcome tag 15. Helper/workflow follow-up suites require visible
  quarantine, strict proof before success and retained result-write/late-callback evidence.
- **P1-2, healthy restart:** `tests/qa_review_durability.rs` covers ancient idle admission,
  healthy accepted deadlines, immediate open-intent/unconfirmed-result quarantine, and
  original-context confirmed replay. `tests/durability_lmdb_integration.rs` mirrors real
  close/reopen, hydration and cold undo.
- **P2-1/2, cold undo and AnyOf:** `tests/qa_review_helpers.rs` and
  `tests/qa_review_durability.rs` verify retained compensation, acknowledgement-only undo
  replay, and already-confirmed dependency firing without effects or false quarantine.
  Receipted declared dispatch is already confirmed too, not a reason to re-dispatch.
- **P2-3/4, identity and admission races:** `tests/qa_review_bus.rs` covers concurrent
  starts, strict append failure, one history read, reentrant fanout, refused-owner waiter
  protection, full type/ID/start binding, successor outcome isolation, one-time outcome
  consumption and quarantine dominance. Ephemeral resolvers serialize live admission too.
- **P2-5/6/7, low-level and callback APIs:** `tests/qa_review_durability.rs` verifies durable
  acceptance/resolution fences, failed-write evidence, recorded panic identity, explicit
  unidentified-history errors and duplicate/stale callback suppression. Late cold results
  remain context-bearing evidence and cannot become a successor's generic result.
- **P2-8, evicted terminal ingress:** `tests/qa_review_bus.rs` retains new effect uncertainty
  and fences a successor while ignoring known completion replay. Resolver cache loss never
  authorizes an older run to mutate fresh resolution state.
- **P2-9, compatibility:** [migration.md](migration.md) describes enum/tag, hook, recovery,
  accepted-API and publication prerequisites. There is no invented administrative repair API.

Parent integration reproduced further bus and durability edge failures before fixing them;
logs include `review-bus-parent-red.log`, `review-durability-parent-red.log`,
`review-shared-state-red.log` and `review-foreign-owner-parent-red.log`. Historical worker
and iteration logs are not substituted for final SHA-bound verification.

## Re-challenge of `0f2a242`: remediation assertions

The same reviewer's first re-challenge returned **BLOCK** (P1-A/P1-B, P2-a..g,
and AllOf restart liveness). The following assertions address that report; they are
not a replacement for the subsequent independent verdict or production validation.

- **P1-A:** `tests/qa_rechallenge_bus.rs` and `bus::admission_tests` distinguish successful
  AnyOf/Quorum/trailing siblings from new uncertainty after failed resolution. Real LMDB
  compaction/reopen preserves success and known failed replay without erasing quarantine.
- **P1-B:** `tests/qa_rechallenge_state.rs` closes definitive accepted failure, preserving
  actual results, prior compensating failure and already-requested undo bytes. The
  durability suite includes real-resolver rejection **with** potential compensation
  bytes and a real accepted watchdog; `resolver::tests` covers a definitively rejected
  sibling without saga-failure authority. The step disposition precedes terminal output.
- **P2-a:** both helper modes and managed workflow ingress verify `StepStarted` is on a
  real bus after durable intent and before slow business work. Failed publication prevents
  execution; wrappers preserve hooks without duplicate start publication.
- **P2-b:** startup resends only a proven `CompensationCompleted`, without a fresh request
  or physical undo. Unknown/terminally resolved history is not acknowledged; real LMDB
  close/reopen mirrors the lost-acknowledgement path.
- **P2-c:** a compatibility waiter registered after admission binds only to the unique
  active full run; ambiguous multi-type ownership and stale history remain unbound.
- **P2-d:** admission tests cover ID **and run** limits, active fingerprint overflow,
  oversized unresolved restore, protected current-ID eviction, failure-on-lookup, and
  durable reload with one global history read at attach. Journal tests verify bounded
  per-ID reads, schema-1 transactional backfill and row/index consistency on map exhaustion.
- **P2-e:** plain and accepted completion use one full tag-15 proof append. Declared
  dispatch keeps its pre-handoff raw result and post-handoff proof. Faults preserve typed
  bytes and never infer proof from an old raw row; cold undo works from proof-only history.
- **P2-f/g:** [operations.md](operations.md) and [migration.md](migration.md) document
  irreversible effects, explicit safe/no-undo timeout contracts, baseline draining,
  schema/API changes and application-owned reconciliation; no repair API is invented.
- **AllOf:** both helpers and workflows journal relevant tag-16 input before dedupe/seen
  updates and hydrate exact type/ID/start observations. Restart between branches, real
  LMDB reopen, other-run input and append/read failures are covered.

Parent inspection found and reproduced remaining integration gaps before fixing them.
Behavioral RED evidence includes `review3-cache-parent-red.log` (three failures),
`review3-accepted-resolver-parent-red2.log`, `review3-accepted-watchdog-parent-red.log`,
`review3-accepted-authority-parent-red.log` and `review3-dependency-order-parent-red.log`.
The earlier accepted-resolver compile error is **not** behavioral RED. Changed old row and
output-sequence assertions retain the same compensation/quarantine requirements while
checking the new single-proof path and per-step timeout disposition. The lifecycle test
still asserts immediate disappearance of public ownership and bounded release of the
journal: pinned pooled shutdown from a scheduler callback does not promise an immediate
join. Native callback-drop coverage is characterization, not behavioral RED.

## Re-challenge of `d35b58e`: compaction, rollback and release assertions

The re-challenge of `d35b58e` returned **BLOCK** (P1-C and P2-1..3; no P0).
Report delivery and the five returned review4 writers were not safety acceptance.
The shared rejection foundation is `ccd4f83`; these integrated assertions were subsequently
reviewed at `8ffa003` as **OK with notes** (no P0/P1, six P2 notes). That verdict did not
complete the all-findings plan; the remaining notes are mapped below.

- **P1-C, closed reconstruction:** `tests/qa_review4_bus.rs` uses real LMDB failed
  parallel rollback, compact/close/reopen and a second restart. Activation invents no
  terminal/undo output or waiter result, exact known fingerprints remain replay-safe,
  and a permitted successor still runs. Parent tests add failure compacted **before** a
  later quarantine, plus a WAL quarantine crash boundary fencing an already-admitted
  active successor. Actual quarantine rows propagate fences without re-deriving success
  or rollback from gapped detail; admission still reads full retained history.
- **P2-1, rollback rejection:** `resolver::tests` removes only an unrequested rejected
  potential step, preserving a real sibling's undo and already-requested ownership.
  `tests/qa_review4_state.rs` derives default-false exact-run rejection, preserves request
  bytes/true-failure obligations, and rejects real result, undo, quarantine and ambiguous
  evidence. `tests/qa_review4_participants.rs` covers sync/async generic and workflow
  no-effect completion with strict proof before ack and no business undo, write-error
  quarantine/hooks, cold resend, startup request/proof recovery, and negative evidence.
  Parent guards cover a stale accepted cache and conflicting startup histories. Physical
  undo that is actually proved complete remains a valid acknowledgement; absence is not.
- **P2-2, pressure:** `bus::admission_tests` reclaims reloadable failed fingerprints,
  protects the current ID's complete history and recovers temporary durable pressure
  only after real room. Parent direct merge tests preserve old/new phases, quarantine
  and prejournaled flags with one bounded lookup/no global scan. Ephemeral and
  unrepresented/unpersisted overflow remain conservative. A parent storage-fault test
  proves published quarantine cannot downgrade after storage returns; a known quarantine
  fence is persisted before later retained evidence.
- **P2-3, release and limits:** `SagaBusReleaseWaiter` owns coordination only. Tests
  cover live-owner timeout, multiple clones/contracts, native callback drop and real
  LMDB reopen after completion. A parent blocked subscriber-destructor regression proves
  true is delayed until bus-owned resources finish destruction. Documentation describes
  off-actor use, application-owned references/work and irreducible capacity/retention costs.

Genuine parent assertion REDs are `review4-bus-parent-red.log` (false replayed success
and premature release) and `review4-bus-storage-parent-red.log` (quarantine downgrade).
Worker state/resolver/no-effect first-ack and write-error/startup failures were behavioral
RED. The worker duplicate-ack expectation, its wrong-trace bus baseline attempt, new API
release tests, passing parent guards and compiler-only fixture corrections are **not**
acceptance RED. Corrected duplicate tests require no repeated work; cold resend uses a
fresh ingress dedupe and durable proof. Failed logs/reports remain preserved; only fresh
SHA-bound parent final gates establish implementation verification, not release readiness.

## Re-challenge of `8ffa003`: six P2 notes

The same reviewer's **OK with notes** was a local recommendation, not all-findings or
production acceptance. Two isolated native writers supplied disjoint helper and coupled
bus/resolver handoffs. The parent preserved their actual diffs, native receipts and logs,
then integrated and strengthened them; the next independent verdict remains separate.

- **N1, non-resolver quarantine waiters:** `tests/qa_review5_bus.rs` requires owned-run
  participant and delivery-shortfall quarantines to resolve exact-run and correctly bound
  legacy waiters, leaving foreign/other-run waiters untouched. Parent
  `parent_review5_recovery_quarantine_resolves_held_waiters_at_activation_only` proves
  ingestion before activation holds replies, then resolves them without republishing the
  input or duplicating its durable row. Ordinary reply authority is unchanged.
- **N2, managed cache/journal lag:** `tests/qa_review5_helpers.rs` starts real managed
  sync/async accepted work, persists definitive rejection, then faults its cleanup read
  while retaining the matching run marker/cache. Owned undo performs zero business work,
  writes strict proof before ack and supports duplicate/cold resend. Proof-write failure
  emits and retains exact-run quarantine. Parent
  `managed_owned_request_post_append_read_error_never_uses_stale_cache` faults the strict
  read **after owned-request persistence**, not admission, and requires quarantine/hook,
  retained ownership and no undo/ack. True failure still owns real undo.
- **N3, old uncertainty versus successor:** the parked-append barrier tests serialized
  admission versus quarantine retention; the successor is refused or visibly/durably
  quarantined. Both restart journal orders are tested, with a quiet second restart.
  Parent LMDB compact/close/reopen tests additionally fence an ordinarily completed
  successor whose resolver state/detail is gone, including a persisted-old-quarantine/
  missing-propagation crash boundary with **no new ingress**. Propagation uses actual
  quarantine rows and admitted newer-run markers, never gapped business detail; startup
  output remains held until activation and ordinary closed history invents no success.
- **N4, discarded cross-run outputs:**
  `bus::admission_tests::capacity_failure_of_unknown_newer_run_publishes_and_persists_the_active_run_quarantine`
  bounds the waiter assertion and verifies the original active owner is notified, visibly
  quarantined, persisted and fenced against reuse despite newer-run capacity failure.
- **N5, storage recovery without more evidence:** pending resident exact-run fences retry
  on the active watchdog, independently of deadline expiry. The memory and real LMDB
  close/reopen tests require durable quarantine after recovery without another event.
  `bus::admission_tests::pending_fence_retry_is_activation_gated_bounded_and_stops_after_durability`
  checks no pre-activation retry, one failure/no spin, eight-per-tick progress, deduplicated
  bounded tracking and cessation after successful persistence.
- **N6, redundant durable fences:** live, pending-then-recovered and validated restored
  quarantine tests retain late evidence in order without a synthetic fence per event/tick.
  Restarts do not add duplicate successor output/rows. Actual input evidence is still
  retained; this is not destructive compaction or a safe-clear API.

Genuine parent assertion REDs are `review5-helpers-parent-post-request-red.log`
(physical undo despite unreadable post-request evidence),
`review5-bus-parent-activation-successor-red.log` (lost activation reply and closed
successor fencing), and `review5-bus-parent-closed-successor-recovery-red.log`
(restored crash-boundary propagation). Worker RED logs establish the other defects;
passing guards/characterization and compiler/style diagnostics are not RED. The originally
hung bus regression was terminated only after process identity verification; forced
termination is not RED. Its subsequent bounded assertion failure is preserved separately,
along with native transcript/events and the disclosed overwritten attempt log.

Pending retry does not certify a crash-safe fence before its append succeeds, nor flush
on release. Unrepresented/sticky uncertainty remains conservative and may not fit retry
tracking. Application-held evidence, audited reconciliation, external idempotency and
irreducible retention/capacity costs remain explicit operational limits. Fresh SHA-bound
parent gates and the same retained reviewer's committed-tree recommendation—not writer
self-checks or this ledger—determine local remediation acceptance.

## Evidence and limits

Behavior-bearing writers supplied pre-change RED/characterization logs. Parent inspection
added separate RED logs for compaction quarantine retention, partial starts, late terminal
boundaries, typed result evidence, storage-error visibility, active bus ownership,
ingress-driven activation, workflow legacy ordering and lifecycle release.

The parent runs authoritative default/all-feature debug, all-feature release, both Clippy
configurations with `-D warnings`, formatting, rustdoc with `-D warnings` and diff hygiene.
Local artifacts/logs are under `/tmp/saga-remediation-20261002/`; the parent checkpoint
binds their results to the integrated commit. Exactly one fresh serial native independent
reviewer owns the review; subsequent challenges resume that same reviewer against verified
commits, not worker assertions. No push, PR, deployment or
main-branch merge is part of this task. The plan's unavailable document-review mechanism
did not run and is not represented as an independent code review.

Breaking changes and the release prerequisite (no version bump here) are in
[migration.md](migration.md). Review follow-ups are not claimed fixed by documentation;
this ledger is not completed verification, review or production certification.

See [operations.md](operations.md) for startup, capacity, archives, destructive admin
primitives and external idempotency. This change does not establish Linux/Windows runtime,
production load, real external-service or current dependency-advisory certification;
README examples are not rustdoc-tested.
