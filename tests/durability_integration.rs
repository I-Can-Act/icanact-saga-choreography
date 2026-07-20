use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use icanact_saga_choreography::durability::{
    ActiveSagaExecution, ActiveSagaExecutionPhase, DEFAULT_RECOVERY_SAGA_TYPE,
    HasActiveSagaExecution, PANIC_QUARANTINE_PUBLISH_KEY, RecoveryDecision, RecoveryPolicy,
    apply_sync_participant_saga_ingress, apply_sync_participant_saga_ingress_with_hooks,
    classify_recovery, collect_startup_recovery_events,
    collect_startup_recovery_events_for_saga_type, default_runtime_dir, is_panic_quarantine_reason,
    is_valid_emitted_transition, open_saga_lmdb_actor, panic_message_from_payload,
    panic_quarantine_reason, panic_quarantine_reason_from_entries,
    publish_active_saga_panic_quarantine, run_participant_phase_with_panic_quarantine,
};
#[cfg(feature = "lmdb")]
use icanact_saga_choreography::{
    AcceptedStepCompletion, AcceptedStepPolicy, AcceptedStepTimeoutOutcome, accept_workflow_step,
    complete_accepted_workflow_step,
};
use icanact_saga_choreography::{
    CompensationError, DependencySpec, HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal,
    JournalEntry, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    SagaChoreographyBus, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant,
    SagaParticipantState, SagaParticipantSupport, SagaStateEntry, SagaStateExt, StepError,
    StepExecutionId, StepOutput,
};

const ORDER_LIFECYCLE: &str = "order_lifecycle";
const TEST_STEP: &str = "test_step";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecuteMode {
    Normal,
    DropStateBeforeReturn,
}

struct TestParticipant {
    step_name: &'static str,
    mode: ExecuteMode,
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    active_saga_execution: Option<ActiveSagaExecution>,
}

impl TestParticipant {
    fn new(step_name: &'static str, mode: ExecuteMode) -> Self {
        Self {
            step_name,
            mode,
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            active_saga_execution: None,
        }
    }
}

impl HasSagaParticipantSupport for TestParticipant {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

#[cfg(feature = "lmdb")]
struct LmdbAcceptedStepActor {
    saga: SagaParticipantSupport<
        icanact_saga_choreography::durability::lmdb::LmdbJournal,
        icanact_saga_choreography::durability::lmdb::LmdbDedupe,
    >,
}

#[cfg(feature = "lmdb")]
impl HasSagaParticipantSupport for LmdbAcceptedStepActor {
    type Journal = icanact_saga_choreography::durability::lmdb::LmdbJournal;
    type Dedupe = icanact_saga_choreography::durability::lmdb::LmdbDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl HasActiveSagaExecution for TestParticipant {
    fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
        &mut self.active_saga_execution
    }
}

impl SagaParticipant for TestParticipant {
    type Error = String;

    fn step_name(&self) -> &str {
        self.step_name
    }

    fn saga_types(&self) -> &[&'static str] {
        &[ORDER_LIFECYCLE, "mature_pool_refresh"]
    }

    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    fn execute_step(
        &mut self,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        if self.mode == ExecuteMode::DropStateBeforeReturn {
            self.saga.saga_states.clear();
        }

        Ok(StepOutput::Completed {
            output: b"ok".to_vec(),
            compensation_data: vec![],
        })
    }

    fn compensate_step(
        &mut self,
        _context: &SagaContext,
        _compensation_data: &[u8],
    ) -> Result<icanact_saga_choreography::CompensationOutput, CompensationError> {
        Ok(icanact_saga_choreography::CompensationOutput::Completed)
    }
}

fn context(saga_id: u64, saga_type: &'static str, step_name: &'static str) -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: saga_type.into(),
        step_name: step_name.into(),
        correlation_id: saga_id,
        causation_id: saga_id,
        trace_id: saga_id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

#[cfg(feature = "lmdb")]
#[test]
fn lmdb_open_recovers_accepted_step_metadata_for_restart_completion() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let ctx = context(90, ORDER_LIFECYCLE, TEST_STEP);
    let execution_id = StepExecutionId::new("external-90");
    let policy = AcceptedStepPolicy {
        idle_timeout: std::time::Duration::from_millis(100),
        hard_timeout: std::time::Duration::from_millis(250),
        timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        },
    };

    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_type(
            temp.path(),
            TEST_STEP,
            ORDER_LIFECYCLE,
        )
        .expect("support should open before restart");
    let mut actor = LmdbAcceptedStepActor { saga: support };
    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy,
    )
    .expect("step should be accepted before restart");
    drop(actor);

    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_type(
            temp.path(),
            TEST_STEP,
            ORDER_LIFECYCLE,
        )
        .expect("support should reopen after restart");
    let mut reopened = LmdbAcceptedStepActor { saga: support };
    let completed = complete_accepted_workflow_step(
        &mut reopened,
        ctx.saga_id,
        execution_id,
        AcceptedStepCompletion {
            completed_at_millis: 1_700_000_000_900,
            output: b"completed-after-lmdb-restart".to_vec(),
            compensation_data: Vec::new(),
        },
    )
    .expect("recovered accepted step should complete after lmdb restart");

    assert!(matches!(
        completed,
        SagaChoreographyEvent::StepCompleted {
            ref context,
            ref output,
            ..
        } if context.saga_id == ctx.saga_id
            && context.event_timestamp_millis == 1_700_000_000_900
            && output == b"completed-after-lmdb-restart"
    ));
}

#[cfg(feature = "lmdb")]
#[test]
fn lmdb_open_recovers_accepted_steps_for_each_declared_saga_type() {
    const OPEN_POSITION: &str = "open_position";
    const CLOSE_POSITION: &str = "close_position";

    let temp = tempfile::tempdir().expect("tempdir should open");
    let open_context = context(190, OPEN_POSITION, TEST_STEP);
    let close_context = context(191, CLOSE_POSITION, TEST_STEP);
    let policy = AcceptedStepPolicy {
        idle_timeout: std::time::Duration::from_secs(60),
        hard_timeout: std::time::Duration::from_secs(60),
        timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: true,
        },
    };
    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_types(
            temp.path(),
            TEST_STEP,
            &[OPEN_POSITION, CLOSE_POSITION],
        )
        .expect("multi-workflow support should open before restart");
    let mut actor = LmdbAcceptedStepActor { saga: support };
    accept_workflow_step(
        &mut actor,
        open_context.clone(),
        "order-manager".into(),
        StepExecutionId::new("open-190"),
        policy.clone(),
    )
    .expect("open workflow step should be accepted");
    accept_workflow_step(
        &mut actor,
        close_context.clone(),
        "order-manager".into(),
        StepExecutionId::new("close-191"),
        policy,
    )
    .expect("close workflow step should be accepted");
    drop(actor);

    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_types(
            temp.path(),
            TEST_STEP,
            &[OPEN_POSITION, CLOSE_POSITION],
        )
        .expect("multi-workflow support should reopen after restart");
    assert!(
        support
            .accepted_workflow_steps
            .contains_key(&open_context.saga_id)
    );
    assert!(
        support
            .accepted_workflow_steps
            .contains_key(&close_context.saga_id)
    );
}

#[cfg(feature = "lmdb")]
#[test]
fn lmdb_open_does_not_rehydrate_expired_accepted_step() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let ctx = context(91, ORDER_LIFECYCLE, TEST_STEP);
    let execution_id = StepExecutionId::new("external-91");
    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_type(
            temp.path(),
            TEST_STEP,
            ORDER_LIFECYCLE,
        )
        .expect("support should open before restart");
    support
        .journal
        .append(
            ctx.saga_id,
            ParticipantEvent::AcceptedStepRecorded {
                context: ctx.clone(),
                participant_id: "order-manager".into(),
                execution_id: execution_id.clone(),
                saga_input: Vec::new(),
                compensation_data: Vec::new(),
                idle_timeout_millis: 1,
                hard_timeout_millis: 1,
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
                accepted_at_millis: 1,
                deadline_at_millis: 1,
                hard_deadline_at_millis: 1,
            },
        )
        .expect("accepted metadata append should succeed");
    drop(support);

    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_type(
            temp.path(),
            TEST_STEP,
            ORDER_LIFECYCLE,
        )
        .expect("support should reopen after restart");
    assert_eq!(support.accepted_workflow_step_count(), 0);
    assert!(
        support.startup_recovery_events.iter().any(|event| matches!(
            event,
            SagaChoreographyEvent::StepFailed { context, .. } if context.saga_id == ctx.saga_id
        )),
        "expired accepted step should recover as timeout event"
    );
    let mut reopened = LmdbAcceptedStepActor { saga: support };
    let completion = AcceptedStepCompletion {
        completed_at_millis: SagaContext::now_millis(),
        output: Vec::new(),
        compensation_data: Vec::new(),
    };
    assert!(matches!(
        complete_accepted_workflow_step(&mut reopened, ctx.saga_id, execution_id, completion),
        Err(icanact_saga_choreography::AcceptedStepError::NotFound { .. })
    ));
}

#[cfg(feature = "lmdb")]
#[test]
fn lmdb_open_rehydrates_expired_compensable_step_with_forward_tombstone() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let ctx = context(92, ORDER_LIFECYCLE, TEST_STEP);
    let execution_id = StepExecutionId::new("external-92");
    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_type(
            temp.path(),
            TEST_STEP,
            ORDER_LIFECYCLE,
        )
        .expect("support should open before restart");
    support
        .journal
        .append(
            ctx.saga_id,
            ParticipantEvent::AcceptedStepRecorded {
                context: ctx.clone(),
                participant_id: "order-manager".into(),
                execution_id: execution_id.clone(),
                saga_input: b"create-order-input".to_vec(),
                compensation_data: b"cancel-order-92".to_vec(),
                idle_timeout_millis: 1,
                hard_timeout_millis: 1,
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
                accepted_at_millis: 1,
                deadline_at_millis: 1,
                hard_deadline_at_millis: 1,
            },
        )
        .expect("accepted metadata append should succeed");
    drop(support);

    let support =
        icanact_saga_choreography::durability::lmdb::open_lmdb_participant_support_for_saga_type(
            temp.path(),
            TEST_STEP,
            ORDER_LIFECYCLE,
        )
        .expect("support should reopen after restart");
    assert_eq!(support.accepted_workflow_step_count(), 1);
    assert_eq!(support.resolved_workflow_step_count(), 1);
    assert_eq!(
        support
            .accepted_workflow_steps
            .get(&ctx.saga_id)
            .expect("expired compensable step must recover")
            .compensation_data,
        b"cancel-order-92"
    );
    assert!(support.startup_recovery_events.iter().any(|event| matches!(
        event,
        SagaChoreographyEvent::StepFailed {
            context,
            requires_compensation: true,
            ..
        } if context.saga_id == ctx.saga_id
    )));

    let mut reopened = LmdbAcceptedStepActor { saga: support };
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut reopened,
            ctx.saga_id,
            execution_id,
            AcceptedStepCompletion {
                completed_at_millis: SagaContext::now_millis(),
                output: b"stale-success".to_vec(),
                compensation_data: b"cancel-order-92".to_vec(),
            },
        ),
        Err(icanact_saga_choreography::AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn ingress_applies_side_effects_and_publishes_valid_emitted_events() {
    let mut participant = TestParticipant::new(TEST_STEP, ExecuteMode::Normal);

    let bus = SagaChoreographyBus::new();
    static DELIVERED_STEP_COMPLETED: AtomicUsize = AtomicUsize::new(0);
    DELIVERED_STEP_COMPLETED.store(0, Ordering::Relaxed);
    let _sub = bus.subscribe_saga_type_fn(ORDER_LIFECYCLE, move |event| {
        if matches!(event, SagaChoreographyEvent::StepCompleted { .. }) {
            DELIVERED_STEP_COMPLETED.fetch_add(1, Ordering::Relaxed);
        }
        true
    });
    participant.saga.attach_bus(bus);

    let mut terminal_side_effect_calls = 0usize;
    let mut invalid_transition_calls = 0usize;
    let mut emitted_transition_calls = 0usize;

    let start = SagaChoreographyEvent::SagaStarted {
        context: context(10, ORDER_LIFECYCLE, "start"),
        payload: b"payload".to_vec(),
    };

    apply_sync_participant_saga_ingress_with_hooks(
        &mut participant,
        start,
        |_actor, _incoming| {
            terminal_side_effect_calls += 1;
        },
        |_invalid| {
            invalid_transition_calls += 1;
        },
        |_actor, _emitted| {
            emitted_transition_calls += 1;
        },
    );

    assert_eq!(terminal_side_effect_calls, 1);
    assert_eq!(invalid_transition_calls, 0);
    assert_eq!(emitted_transition_calls, 2);
    assert_eq!(DELIVERED_STEP_COMPLETED.load(Ordering::Relaxed), 1);
    assert!(matches!(
        participant.saga_states_ref().get(&SagaId::new(10)),
        Some(SagaStateEntry::Completed(_))
    ));
}

#[test]
fn ingress_suppresses_invalid_emitted_transition_when_state_is_missing() {
    let mut participant = TestParticipant::new(TEST_STEP, ExecuteMode::DropStateBeforeReturn);

    let bus = SagaChoreographyBus::new();
    static DELIVERED_STEP_COMPLETED: AtomicUsize = AtomicUsize::new(0);
    DELIVERED_STEP_COMPLETED.store(0, Ordering::Relaxed);
    let _sub = bus.subscribe_saga_type_fn(ORDER_LIFECYCLE, move |event| {
        if matches!(event, SagaChoreographyEvent::StepCompleted { .. }) {
            DELIVERED_STEP_COMPLETED.fetch_add(1, Ordering::Relaxed);
        }
        true
    });
    participant.saga.attach_bus(bus);

    let mut invalid_transition_calls = 0usize;
    let mut emitted_transition_calls = 0usize;

    let start = SagaChoreographyEvent::SagaStarted {
        context: context(11, ORDER_LIFECYCLE, "start"),
        payload: b"payload".to_vec(),
    };

    apply_sync_participant_saga_ingress_with_hooks(
        &mut participant,
        start,
        |_actor, _incoming| {},
        |_invalid| {
            invalid_transition_calls += 1;
        },
        |_actor, _emitted| {
            emitted_transition_calls += 1;
        },
    );

    assert_eq!(invalid_transition_calls, 1);
    assert_eq!(emitted_transition_calls, 1);
    assert_eq!(DELIVERED_STEP_COMPLETED.load(Ordering::Relaxed), 0);
    assert!(
        participant
            .saga_states_ref()
            .get(&SagaId::new(11))
            .is_none()
    );
}

#[test]
fn panic_quarantine_records_journal_marks_dedupe_and_publishes() {
    let mut participant = TestParticipant::new(TEST_STEP, ExecuteMode::Normal);

    let bus = SagaChoreographyBus::new();
    static DELIVERED_QUARANTINE: AtomicUsize = AtomicUsize::new(0);
    DELIVERED_QUARANTINE.store(0, Ordering::Relaxed);
    let _sub = bus.subscribe_saga_type_fn(ORDER_LIFECYCLE, move |event| {
        if matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }) {
            DELIVERED_QUARANTINE.fetch_add(1, Ordering::Relaxed);
        }
        true
    });
    participant.saga.attach_bus(bus);

    let saga_context = context(12, ORDER_LIFECYCLE, TEST_STEP);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_participant_phase_with_panic_quarantine(
            &mut participant,
            &saga_context,
            ActiveSagaExecutionPhase::StepExecution,
            |_actor| -> () {
                panic!("panic-from-test");
            },
        )
    }));

    assert!(
        result.is_err(),
        "panic should be rethrown after quarantine publish"
    );
    assert!(participant.active_saga_execution.is_none());
    assert_eq!(DELIVERED_QUARANTINE.load(Ordering::Relaxed), 1);

    let entries = participant
        .saga
        .journal
        .read(saga_context.saga_id)
        .expect("journal read should succeed");
    let panic_reason = panic_quarantine_reason_from_entries(&entries)
        .expect("panic quarantine reason should be recorded");
    assert!(is_panic_quarantine_reason(panic_reason.as_ref()));

    assert!(
        participant
            .saga
            .dedupe
            .contains(saga_context.saga_id, PANIC_QUARANTINE_PUBLISH_KEY,)
            .expect("dedupe contains should succeed")
    );
}

#[test]
fn recovery_collection_replays_panic_quarantine_once_and_classifies_states() {
    let journal = InMemoryJournal::new();
    let dedupe = InMemoryDedupe::new();

    let saga_id = SagaId::new(13);
    let reason = panic_quarantine_reason(ActiveSagaExecutionPhase::CompensationExecution, "boom");
    journal
        .append(
            saga_id,
            ParticipantEvent::Quarantined {
                reason,
                quarantined_at_millis: SagaContext::now_millis(),
            },
        )
        .expect("append should succeed");

    let first = collect_startup_recovery_events_for_saga_type(
        &journal,
        &dedupe,
        "risk_gate",
        "mature_pool_refresh",
    )
    .expect("startup recovery should collect");
    assert_eq!(first.len(), 1);
    assert!(matches!(
        &first[0],
        SagaChoreographyEvent::SagaQuarantined { context, .. }
            if context.saga_type.as_ref() == "mature_pool_refresh"
    ));

    let second = collect_startup_recovery_events_for_saga_type(
        &journal,
        &dedupe,
        "risk_gate",
        "mature_pool_refresh",
    )
    .expect("startup recovery should collect");
    assert!(
        second.is_empty(),
        "dedupe should prevent duplicate replay events"
    );

    let stale_entries = vec![JournalEntry {
        sequence: 1,
        recorded_at_millis: 100,
        event: ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 100,
        },
    }];
    assert_eq!(
        classify_recovery(
            &stale_entries,
            10_000,
            RecoveryPolicy {
                stale_after_ms: 500
            }
        ),
        RecoveryDecision::QuarantineStale
    );

    let accepted_entries = vec![JournalEntry {
        sequence: 2,
        recorded_at_millis: 100,
        event: ParticipantEvent::AcceptedStepRecorded {
            context: context(14, ORDER_LIFECYCLE, TEST_STEP),
            participant_id: TEST_STEP.into(),
            execution_id: StepExecutionId::new("external-14"),
            saga_input: Vec::new(),
            compensation_data: Vec::new(),
            idle_timeout_millis: 1_000,
            hard_timeout_millis: 20_000,
            timeout_outcome: icanact_saga_choreography::AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
            accepted_at_millis: 100,
            deadline_at_millis: 1_100,
            hard_deadline_at_millis: 20_100,
        },
    }];
    assert_eq!(
        classify_recovery(
            &accepted_entries,
            10_000,
            RecoveryPolicy {
                stale_after_ms: 500
            }
        ),
        RecoveryDecision::Continue
    );

    let expired_journal = InMemoryJournal::new();
    let expired_dedupe = InMemoryDedupe::new();
    expired_journal
        .append(
            SagaId::new(15),
            ParticipantEvent::AcceptedStepRecorded {
                context: context(15, ORDER_LIFECYCLE, TEST_STEP),
                participant_id: TEST_STEP.into(),
                execution_id: StepExecutionId::new("external-15"),
                saga_input: Vec::new(),
                compensation_data: Vec::new(),
                idle_timeout_millis: 1,
                hard_timeout_millis: 1,
                timeout_outcome: icanact_saga_choreography::AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
                accepted_at_millis: 1,
                deadline_at_millis: 1,
                hard_deadline_at_millis: 1,
            },
        )
        .expect("append should succeed");
    let expired = collect_startup_recovery_events_for_saga_type(
        &expired_journal,
        &expired_dedupe,
        TEST_STEP,
        ORDER_LIFECYCLE,
    )
    .expect("startup recovery should collect expired accepted step");
    assert!(matches!(
        expired.as_slice(),
        [SagaChoreographyEvent::StepFailed {
            error,
            requires_compensation: true,
            ..
        }] if error.contains("accepted step hard timeout after restart")
    ));

    let terminal_entries = vec![JournalEntry {
        sequence: 2,
        recorded_at_millis: 100,
        event: ParticipantEvent::CompensationCompleted {
            completed_at_millis: 100,
        },
    }];
    assert_eq!(
        classify_recovery(
            &terminal_entries,
            10_000,
            RecoveryPolicy {
                stale_after_ms: 500
            }
        ),
        RecoveryDecision::TerminalNoAction
    );

    let replay_entries = vec![JournalEntry {
        sequence: 3,
        recorded_at_millis: 100,
        event: ParticipantEvent::Quarantined {
            reason: panic_quarantine_reason(ActiveSagaExecutionPhase::StepExecution, "panic"),
            quarantined_at_millis: 100,
        },
    }];
    assert_eq!(
        classify_recovery(
            &replay_entries,
            10_000,
            RecoveryPolicy {
                stale_after_ms: 500
            }
        ),
        RecoveryDecision::ReplayPanicQuarantine
    );
}

struct MockLmdbBackedActor {
    actor_id: &'static str,
    base: PathBuf,
}

fn base_state(
    saga_id: SagaId,
    saga_type: &str,
    step_name: &str,
) -> SagaParticipantState<icanact_saga_choreography::Idle> {
    SagaParticipantState::new(
        saga_id,
        saga_type.into(),
        step_name.into(),
        saga_id.get(),
        saga_id.get(),
        [0; 32],
        SagaContext::now_millis(),
    )
}

fn completed_entry(saga_id: SagaId, saga_type: &str, step_name: &str) -> SagaStateEntry {
    let now = SagaContext::now_millis();
    SagaStateEntry::Completed(
        base_state(saga_id, saga_type, step_name)
            .trigger("test", now)
            .start_execution(now)
            .complete(vec![], vec![], now),
    )
}

fn compensated_entry(saga_id: SagaId, saga_type: &str, step_name: &str) -> SagaStateEntry {
    let now = SagaContext::now_millis();
    SagaStateEntry::Compensated(
        base_state(saga_id, saga_type, step_name)
            .trigger("test", now)
            .start_execution(now)
            .complete(vec![], vec![], now)
            .start_compensation(now)
            .complete_compensation(now),
    )
}

fn quarantined_entry(saga_id: SagaId, saga_type: &str, step_name: &str) -> SagaStateEntry {
    let now = SagaContext::now_millis();
    SagaStateEntry::Quarantined(
        base_state(saga_id, saga_type, step_name)
            .trigger("test", now)
            .start_execution(now)
            .complete(vec![], vec![], now)
            .start_compensation(now)
            .quarantine("panic".into(), now),
    )
}

fn failed_entry(saga_id: SagaId, saga_type: &str, step_name: &str) -> SagaStateEntry {
    let now = SagaContext::now_millis();
    SagaStateEntry::Failed(
        base_state(saga_id, saga_type, step_name)
            .trigger("test", now)
            .start_execution(now)
            .fail("boom".into(), true, now),
    )
}

impl icanact_saga_choreography::durability::SagaLmdbBackedActor for MockLmdbBackedActor {
    fn open_with_saga_lmdb_base(
        actor_id: &'static str,
        saga_lmdb_base: &Path,
    ) -> Result<Self, String> {
        Ok(Self {
            actor_id,
            base: saga_lmdb_base.to_path_buf(),
        })
    }
}

#[test]
fn default_runtime_dir_and_open_actor_helpers_work() {
    let runtime_dir = default_runtime_dir("THIS_ENV_SHOULD_NOT_EXIST_123", "runtime-data");
    assert!(
        runtime_dir.to_string_lossy().contains("target/test-tmp")
            || runtime_dir.as_path() == Path::new("runtime-data")
    );

    let opened = open_saga_lmdb_actor::<MockLmdbBackedActor>("mock-id", Path::new("/tmp/saga"))
        .expect("actor open helper should delegate to actor impl");
    assert_eq!(opened.actor_id, "mock-id");
    assert_eq!(opened.base, PathBuf::from("/tmp/saga"));
}

#[derive(Debug)]
struct FailingLmdbBackedActor;

impl icanact_saga_choreography::durability::SagaLmdbBackedActor for FailingLmdbBackedActor {
    fn open_with_saga_lmdb_base(
        _actor_id: &'static str,
        _saga_lmdb_base: &Path,
    ) -> Result<Self, String> {
        Err("forced-open-error".to_string())
    }
}

#[derive(Clone)]
struct StaticJournal {
    rows: Vec<(SagaId, Vec<JournalEntry>)>,
}

impl StaticJournal {
    fn new(rows: Vec<(SagaId, Vec<JournalEntry>)>) -> Self {
        Self { rows }
    }
}

impl ParticipantJournal for StaticJournal {
    fn append(
        &self,
        _saga_id: SagaId,
        _event: ParticipantEvent,
    ) -> Result<u64, icanact_saga_choreography::JournalError> {
        Err(icanact_saga_choreography::JournalError::Storage(
            "append not supported in StaticJournal".into(),
        ))
    }

    fn read(
        &self,
        saga_id: SagaId,
    ) -> Result<Vec<JournalEntry>, icanact_saga_choreography::JournalError> {
        Ok(self
            .rows
            .iter()
            .find(|(id, _)| *id == saga_id)
            .map(|(_, entries)| entries.clone())
            .unwrap_or_default())
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, icanact_saga_choreography::JournalError> {
        Ok(self.rows.iter().map(|(id, _)| *id).collect())
    }

    fn prune(&self, _saga_id: SagaId) -> Result<(), icanact_saga_choreography::JournalError> {
        Ok(())
    }
}

#[test]
fn helper_and_wrapper_apis_cover_default_branches() {
    let static_payload: &'static str = "boom-static";
    assert_eq!(
        panic_message_from_payload(&static_payload as &(dyn std::any::Any + Send)),
        "boom-static".into()
    );

    let owned_payload = String::from("boom-owned");
    assert_eq!(
        panic_message_from_payload(&owned_payload as &(dyn std::any::Any + Send)),
        "boom-owned".into()
    );

    let numeric_payload: u64 = 99;
    assert_eq!(
        panic_message_from_payload(&numeric_payload as &(dyn std::any::Any + Send)),
        "panic (non-string payload)".into()
    );

    assert!(panic_quarantine_reason_from_entries(&[]).is_none());

    let non_quarantine_entries = vec![JournalEntry {
        sequence: 1,
        recorded_at_millis: 1,
        event: ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 1,
        },
    }];
    assert!(panic_quarantine_reason_from_entries(&non_quarantine_entries).is_none());

    let non_panic_quarantine_entries = vec![JournalEntry {
        sequence: 2,
        recorded_at_millis: 2,
        event: ParticipantEvent::Quarantined {
            reason: "manual_quarantine".into(),
            quarantined_at_millis: 2,
        },
    }];
    assert!(panic_quarantine_reason_from_entries(&non_panic_quarantine_entries).is_none());

    assert_eq!(
        classify_recovery(
            &[],
            10_000,
            RecoveryPolicy {
                stale_after_ms: 500
            }
        ),
        RecoveryDecision::TerminalNoAction
    );

    let recent_entries = vec![JournalEntry {
        sequence: 3,
        recorded_at_millis: 9_900,
        event: ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 9_900,
        },
    }];
    assert_eq!(
        classify_recovery(
            &recent_entries,
            10_000,
            RecoveryPolicy {
                stale_after_ms: 500
            }
        ),
        RecoveryDecision::Continue
    );

    let mut participant = TestParticipant::new(TEST_STEP, ExecuteMode::Normal);
    apply_sync_participant_saga_ingress(
        &mut participant,
        SagaChoreographyEvent::SagaStarted {
            context: context(77, ORDER_LIFECYCLE, "start"),
            payload: b"payload".to_vec(),
        },
        |_actor, _incoming| {},
        |_invalid| {},
    );
    assert!(matches!(
        participant.saga_states_ref().get(&SagaId::new(77)),
        Some(SagaStateEntry::Completed(_))
    ));

    let normal_context = context(78, ORDER_LIFECYCLE, TEST_STEP);
    let returned_value = run_participant_phase_with_panic_quarantine(
        &mut participant,
        &normal_context,
        ActiveSagaExecutionPhase::CompensationExecution,
        |_actor| -> u64 { 42 },
    );
    assert_eq!(returned_value, 42);
    assert!(participant.active_saga_execution.is_none());

    let saga_context = context(78, ORDER_LIFECYCLE, TEST_STEP);
    let completed = completed_entry(SagaId::new(78), ORDER_LIFECYCLE, TEST_STEP);
    assert!(is_valid_emitted_transition(
        Some(&completed),
        &SagaChoreographyEvent::StepCompleted {
            context: saga_context.clone(),
            output: vec![],
            saga_input: vec![],
            compensation_available: false,
        }
    ));
    assert!(!is_valid_emitted_transition(
        None,
        &SagaChoreographyEvent::StepCompleted {
            context: saga_context.clone(),
            output: vec![],
            saga_input: vec![],
            compensation_available: false,
        }
    ));
    let failed = failed_entry(SagaId::new(81), ORDER_LIFECYCLE, TEST_STEP);
    assert!(is_valid_emitted_transition(
        Some(&failed),
        &SagaChoreographyEvent::StepFailed {
            context: saga_context.clone(),
            participant_id: "pid".into(),
            error_code: None,
            error: "err".into(),
            requires_compensation: true,
        }
    ));
    let compensated = compensated_entry(SagaId::new(79), ORDER_LIFECYCLE, TEST_STEP);
    assert!(is_valid_emitted_transition(
        Some(&compensated),
        &SagaChoreographyEvent::CompensationCompleted {
            context: saga_context.clone(),
        }
    ));
    let quarantined = quarantined_entry(SagaId::new(80), ORDER_LIFECYCLE, TEST_STEP);
    assert!(is_valid_emitted_transition(
        Some(&quarantined),
        &SagaChoreographyEvent::CompensationFailed {
            context: saga_context.clone(),
            participant_id: "pid".into(),
            error: "err".into(),
            is_ambiguous: false,
        }
    ));
    assert!(is_valid_emitted_transition(
        Some(&quarantined),
        &SagaChoreographyEvent::SagaQuarantined {
            context: saga_context.clone(),
            reason: "panic".into(),
            step: "step".into(),
            participant_id: "pid".into(),
        }
    ));
    assert!(is_valid_emitted_transition(
        None,
        &SagaChoreographyEvent::StepStarted {
            context: saga_context,
        }
    ));
}

#[test]
fn startup_recovery_collectors_cover_default_and_stale_paths() {
    let stale_saga = SagaId::new(88);
    let stale_journal = StaticJournal::new(vec![(
        stale_saga,
        vec![JournalEntry {
            sequence: 1,
            recorded_at_millis: 0,
            event: ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 0,
            },
        }],
    )]);
    let stale_events = collect_startup_recovery_events_for_saga_type(
        &stale_journal,
        &InMemoryDedupe::new(),
        "risk_gate",
        "mature_pool_refresh",
    )
    .expect("startup recovery should collect");
    assert_eq!(stale_events.len(), 1);
    assert!(matches!(
        &stale_events[0],
        SagaChoreographyEvent::SagaFailed { context, reason, .. }
            if context.saga_type.as_ref() == "mature_pool_refresh"
                && context.saga_id == stale_saga
                && reason.as_ref().contains("startup recovery quarantined stale saga")
    ));

    let default_saga = SagaId::new(89);
    let default_journal = InMemoryJournal::new();
    default_journal
        .append(
            default_saga,
            ParticipantEvent::Quarantined {
                reason: panic_quarantine_reason(ActiveSagaExecutionPhase::StepExecution, "boom"),
                quarantined_at_millis: SagaContext::now_millis(),
            },
        )
        .expect("append should succeed");
    let default_events =
        collect_startup_recovery_events(&default_journal, &InMemoryDedupe::new(), "risk_gate")
            .expect("startup recovery should collect");
    assert_eq!(default_events.len(), 1);
    assert!(matches!(
        &default_events[0],
        SagaChoreographyEvent::SagaQuarantined { context, .. }
            if context.saga_type.as_ref() == DEFAULT_RECOVERY_SAGA_TYPE
                && context.saga_id == default_saga
    ));

    let mixed_saga_empty = SagaId::new(90);
    let mixed_saga_continue = SagaId::new(91);
    let mixed_saga_terminal = SagaId::new(92);
    let mixed_journal = StaticJournal::new(vec![
        (mixed_saga_empty, vec![]),
        (
            mixed_saga_continue,
            vec![JournalEntry {
                sequence: 1,
                recorded_at_millis: SagaContext::now_millis(),
                event: ParticipantEvent::StepExecutionStarted {
                    attempt: 1,
                    started_at_millis: SagaContext::now_millis(),
                },
            }],
        ),
        (
            mixed_saga_terminal,
            vec![JournalEntry {
                sequence: 1,
                recorded_at_millis: SagaContext::now_millis(),
                event: ParticipantEvent::CompensationCompleted {
                    completed_at_millis: SagaContext::now_millis(),
                },
            }],
        ),
    ]);
    let mixed_events = collect_startup_recovery_events_for_saga_type(
        &mixed_journal,
        &InMemoryDedupe::new(),
        "risk_gate",
        "mature_pool_refresh",
    )
    .expect("startup recovery should collect");
    assert!(
        mixed_events.is_empty(),
        "empty, continue and terminal recovery states should not emit recovery events"
    );
}

#[test]
fn helper_propagates_open_errors_and_honors_env_runtime_dir() {
    let err = open_saga_lmdb_actor::<FailingLmdbBackedActor>("mock-id", Path::new("/tmp/saga"))
        .expect_err("open helper should propagate actor open errors");
    assert_eq!(err, "forced-open-error".to_string());

    let mut support_without_bus =
        SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new());
    let no_bus_context = context(93, ORDER_LIFECYCLE, TEST_STEP);
    publish_active_saga_panic_quarantine(
        &mut support_without_bus,
        &no_bus_context,
        ActiveSagaExecutionPhase::StepExecution,
        &"boom-without-bus",
        TEST_STEP,
        TEST_STEP.into(),
    );
    assert!(
        !support_without_bus
            .dedupe
            .contains(no_bus_context.saga_id, PANIC_QUARANTINE_PUBLISH_KEY)
            .expect("dedupe contains should succeed"),
        "without bus delivery we should not mark panic quarantine dedupe key"
    );
}
