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

## Evidence and limits

Behavior-bearing writers supplied pre-change RED/characterization logs. Parent inspection
added separate RED logs for compaction quarantine retention, partial starts, late terminal
boundaries, typed result evidence, storage-error visibility, active bus ownership,
ingress-driven activation, workflow legacy ordering and lifecycle release.

The parent runs authoritative default/all-feature debug, all-feature release, both Clippy
configurations with `-D warnings`, formatting, rustdoc with `-D warnings` and diff hygiene.
Local artifacts/logs are under `/tmp/saga-remediation-20261002/`; the parent checkpoint
binds their results to the integrated commit. One fresh serial native independent reviewer
is gated on that verified commit, not on worker assertions. No push, PR, deployment or
main-branch merge is part of this task. The plan's unavailable document-review mechanism
did not run and is not represented as an independent code review.

See [operations.md](operations.md) for startup, capacity, archives, destructive admin
primitives and external idempotency. This change does not establish Linux/Windows runtime,
production load, real external-service or current dependency-advisory certification;
README examples are not rustdoc-tested.
