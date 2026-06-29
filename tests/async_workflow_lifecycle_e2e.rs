#![cfg(feature = "test-harness")]

use std::sync::Arc;
use std::time::Duration;

use icanact_saga_choreography::{
    accept_workflow_step, complete_accepted_workflow_step, fail_accepted_workflow_step,
    poll_accepted_workflow_step_timeouts, record_accepted_workflow_step_progress,
    recover_accepted_workflow_steps_for_saga_type, AcceptedStepCompletion, AcceptedStepError,
    AcceptedStepFailure, AcceptedStepPolicy, AcceptedStepTimeoutOutcome, FailureAuthority,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, ParticipantEvent,
    ParticipantJournal, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport,
    SagaStateExt, SagaTerminalOutcome, SagaTestWorld, StepExecutionId, SuccessCriteria,
    TerminalPolicy, TerminalResolver,
};

struct HarnessActor {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
}

impl Default for HarnessActor {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        }
    }
}

impl HasSagaParticipantSupport for HarnessActor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

struct RestartedHarnessActor {
    saga: SagaParticipantSupport<Arc<InMemoryJournal>, InMemoryDedupe>,
}

impl RestartedHarnessActor {
    fn new(journal: Arc<InMemoryJournal>) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal, InMemoryDedupe::new()),
        }
    }
}

impl HasSagaParticipantSupport for RestartedHarnessActor {
    type Journal = Arc<InMemoryJournal>;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

fn context(step_name: &str, saga_id: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: "order_lifecycle".into(),
        step_name: step_name.into(),
        correlation_id: saga_id,
        causation_id: saga_id,
        trace_id: saga_id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: 1_700_000_000_000,
        event_timestamp_millis: 1_700_000_000_000,
    }
}

fn policy(timeout_outcome: AcceptedStepTimeoutOutcome) -> AcceptedStepPolicy {
    AcceptedStepPolicy {
        idle_timeout: Duration::from_millis(100),
        hard_timeout: Duration::from_millis(250),
        timeout_outcome,
    }
}

fn completion(
    completed_at_millis: u64,
    output: impl Into<Vec<u8>>,
    saga_input: impl Into<Vec<u8>>,
    compensation_data: impl Into<Vec<u8>>,
) -> AcceptedStepCompletion {
    AcceptedStepCompletion {
        completed_at_millis,
        output: output.into(),
        saga_input: saga_input.into(),
        compensation_data: compensation_data.into(),
    }
}

fn failure(
    failed_at_millis: u64,
    reason: impl Into<Box<str>>,
    requires_compensation: bool,
) -> AcceptedStepFailure {
    AcceptedStepFailure {
        failed_at_millis,
        reason: reason.into(),
        requires_compensation,
    }
}

fn terminal_policy() -> TerminalPolicy {
    let mut required = std::collections::HashSet::new();
    required.insert("create_order".into());
    TerminalPolicy::new(
        "order_lifecycle".into(),
        "order_lifecycle/default".into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AllOf(required),
        Duration::from_secs(30),
        Duration::from_secs(10),
        &[],
    )
}

#[test]
fn accepted_step_does_not_complete_saga_until_late_completion() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 1);
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-1"),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");

    assert!(matches!(
        accepted,
        SagaChoreographyEvent::StepAccepted {
            ref context,
            ref execution_id,
            ..
        } if context.step_name.as_ref() == "create_order"
            && execution_id.as_ref() == "effect-1"
    ));
    assert!(
        resolver.ingest(&accepted).is_empty(),
        "accepted step is progress, not completion"
    );
}

#[test]
fn accepted_step_uses_actor_clock_for_context_and_deadlines() {
    let mut actor = HarnessActor::default();
    let mut ctx = context("create_order", 20);
    ctx.event_timestamp_millis = 1;

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-20"),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");

    let SagaChoreographyEvent::StepAccepted {
        context,
        deadline_at_millis,
        hard_deadline_at_millis,
        ..
    } = accepted
    else {
        panic!("expected step accepted event");
    };

    assert!(
        context.event_timestamp_millis > ctx.event_timestamp_millis,
        "acceptance timestamp must come from participant local time"
    );
    assert_eq!(
        deadline_at_millis,
        context.event_timestamp_millis + Duration::from_millis(100).as_millis() as u64
    );
    assert_eq!(
        hard_deadline_at_millis,
        context.event_timestamp_millis + Duration::from_millis(250).as_millis() as u64
    );
}

#[test]
fn saga_run_tracking_reset_allows_same_saga_id_to_accept_again() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 22);
    let execution_id = StepExecutionId::new("effect-22");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("first run should accept step");

    let failed = fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        failure(1_700_000_000_175, "late participant failure", false),
    )
    .expect("first run should resolve step");
    assert!(matches!(failed, SagaChoreographyEvent::StepFailed { .. }));

    actor.clear_in_memory_saga_run_tracking(ctx.saga_id);

    let accepted_again = accept_workflow_step(
        &mut actor,
        ctx,
        "order-manager".into(),
        execution_id,
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    );
    assert!(
        matches!(
            accepted_again,
            Ok(SagaChoreographyEvent::StepAccepted { .. })
        ),
        "reset/prune must clear accepted and resolved step maps for saga id reuse"
    );
}

#[test]
fn accepted_step_can_complete_after_participant_restart() {
    let journal = Arc::new(InMemoryJournal::new());
    let mut actor = RestartedHarnessActor::new(journal.clone());
    let ctx = context("create_order", 23);
    let execution_id = StepExecutionId::new("effect-23");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted before restart");

    let mut reopened = RestartedHarnessActor::new(journal);
    recover_accepted_workflow_steps_for_saga_type(
        reopened.saga_support_mut(),
        "create_order",
        "order_lifecycle",
    )
    .expect("accepted step metadata should recover");

    let completed = complete_accepted_workflow_step(
        &mut reopened,
        ctx.saga_id,
        execution_id,
        completion(
            1_700_000_000_230,
            b"created-after-restart",
            b"input",
            Vec::new(),
        ),
    )
    .expect("recovered accepted step should complete after restart");
    assert!(matches!(
        completed,
        SagaChoreographyEvent::StepCompleted {
            ref context,
            ref output,
            ..
        } if context.saga_id == ctx.saga_id
            && context.event_timestamp_millis == 1_700_000_000_230
            && output == b"created-after-restart"
    ));
}

#[test]
fn accepted_step_late_completion_completes_saga_once() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 2);
    let execution_id = StepExecutionId::new("effect-2");
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    assert!(resolver.ingest(&accepted).is_empty());

    let completed = complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        completion(1_700_000_000_050, b"created", b"input", Vec::new()),
    )
    .expect("late completion should resolve accepted step");
    let terminal = resolver.ingest(&completed);

    assert!(matches!(
        completed,
        SagaChoreographyEvent::StepCompleted { .. }
    ));
    assert!(matches!(
        terminal.as_slice(),
        [SagaChoreographyEvent::SagaCompleted { .. }]
    ));
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_060, b"created-again", Vec::new(), Vec::new()),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn accepted_step_completion_records_actual_completion_time() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 21);
    let execution_id = StepExecutionId::new("effect-21");
    let completed_at_millis = 1_700_000_000_175;

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");

    let completed = complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        completion(completed_at_millis, b"created", b"input", Vec::new()),
    )
    .expect("late completion should resolve accepted step");
    assert!(matches!(
    completed,
    SagaChoreographyEvent::StepCompleted { ref context, .. }
    if context.event_timestamp_millis == completed_at_millis
    ));

    let entries = actor
        .saga
        .journal
        .read(ctx.saga_id)
        .expect("journal read should succeed");
    assert!(entries.iter().any(|entry| {
        matches!(
            &entry.event,
            ParticipantEvent::StepExecutionCompleted {
                completed_at_millis: observed,
                ..
            } if *observed == completed_at_millis
        )
    }));
}

#[test]
fn prune_clears_accepted_and_resolved_workflow_step_state() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 23);
    let execution_id = StepExecutionId::new("effect-23");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        completion(1_700_000_000_050, b"created", b"input", Vec::new()),
    )
    .expect("accepted step should complete");

    actor.prune_saga(ctx.saga_id);

    accept_workflow_step(
        &mut actor,
        ctx,
        "order-manager".into(),
        execution_id,
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("pruned saga id should be reusable with the same execution id");
}

#[test]
fn duplicate_accept_for_same_saga_is_rejected_without_losing_original_step() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 22);
    let original = StepExecutionId::new("effect-22-a");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        original.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("first execution should be accepted");
    assert!(matches!(
        accept_workflow_step(
            &mut actor,
            ctx.clone(),
            "order-manager".into(),
            StepExecutionId::new("effect-22-b"),
            policy(AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            }),
        ),
        Err(AcceptedStepError::AlreadyAccepted { .. })
    ));

    fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        original,
        failure(1_700_000_000_060, "original execution failed later", false),
    )
    .expect("original execution should remain pending after duplicate rejection");
}

#[test]
fn accepted_step_late_failure_fails_saga_with_authority() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 3);
    let execution_id = StepExecutionId::new("effect-3");
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    assert!(resolver.ingest(&accepted).is_empty());

    let failed = fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        failure(1_700_000_000_070, "transport dispatch failed", false),
    )
    .expect("late failure should resolve accepted step");
    let terminal = resolver.ingest(&failed);

    assert!(matches!(
            failed,
            SagaChoreographyEvent::StepFailed {
                ref participant_id,
                ref error,
                ..
    } if participant_id.as_ref() == "order-manager"
    && error.contains("transport dispatch failed")
    ));
    assert!(matches!(
    failed,
    SagaChoreographyEvent::StepFailed { ref context, .. }
    if context.event_timestamp_millis == 1_700_000_000_070
    ));
    assert!(matches!(
    terminal.as_slice(),
    [SagaChoreographyEvent::SagaFailed { failure: Some(details), .. }]
                if details.participant_id.as_ref() == "order-manager"
        ));
}

#[test]
fn accepted_step_timeout_can_quarantine_ambiguous_saga() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 4);
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-4"),
        policy(AcceptedStepTimeoutOutcome::QuarantineSaga),
    )
    .expect("step should be accepted");
    let SagaChoreographyEvent::StepAccepted {
        deadline_at_millis, ..
    } = &accepted
    else {
        panic!("expected accepted step");
    };
    assert!(resolver.ingest(&accepted).is_empty());

    let timed_out_at_millis = deadline_at_millis + 1;
    let timed_out = poll_accepted_workflow_step_timeouts(&mut actor, timed_out_at_millis);

    assert!(matches!(
                timed_out.as_slice(),
        [SagaChoreographyEvent::SagaQuarantined {
        context,
        reason,
        participant_id,
    ..
    }] if context.event_timestamp_millis == timed_out_at_millis
    && reason.contains("accepted step idle timeout")
    && participant_id.as_ref() == "order-manager"
    ));
}

#[test]
fn accepted_step_progress_extends_idle_deadline_but_not_hard_deadline() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 5);
    let execution_id = StepExecutionId::new("effect-5");
    let mut resolver = TerminalResolver::new(TerminalPolicy::new(
        "order_lifecycle".into(),
        "order_lifecycle/stalled-test".into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AllOf(["create_order".into()].into()),
        Duration::from_secs(30),
        Duration::from_millis(100),
        &[],
    ));

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    let SagaChoreographyEvent::StepAccepted {
        context,
        hard_deadline_at_millis,
        ..
    } = &accepted
    else {
        panic!("expected accepted step");
    };
    let accepted_at_millis = context.event_timestamp_millis;
    assert!(resolver.ingest(&accepted).is_empty());

    let progress = record_accepted_workflow_step_progress(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        accepted_at_millis + 90,
    )
    .expect("progress should extend idle deadline");
    assert!(matches!(
        progress,
        SagaChoreographyEvent::StepAccepted {
            ref context,
            ref execution_id,
            deadline_at_millis,
            ..
        } if context.event_timestamp_millis == accepted_at_millis + 90
            && execution_id.as_ref() == "effect-5"
            && deadline_at_millis > accepted_at_millis + 180
    ));
    assert!(resolver.ingest(&progress).is_empty());
    assert!(
        poll_accepted_workflow_step_timeouts(&mut actor, accepted_at_millis + 180).is_empty(),
        "idle timeout should be refreshed by progress"
    );

    let hard_timed_out_at_millis = hard_deadline_at_millis + 1;
    let hard_timed_out = poll_accepted_workflow_step_timeouts(&mut actor, hard_timed_out_at_millis);
    assert!(matches!(
    hard_timed_out.as_slice(),
    [SagaChoreographyEvent::StepFailed { context, error, .. }]
    if context.event_timestamp_millis == hard_timed_out_at_millis
    && error.contains("accepted step hard timeout")
    ));
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_260, b"too-late", Vec::new(), Vec::new()),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn saga_test_world_waits_for_and_resolves_accepted_steps() {
    let world = SagaTestWorld::new();
    let _resolver = world
        .attach_terminal_resolver(terminal_policy(), "testkit-terminal")
        .expect("terminal resolver should attach");
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 6);
    let execution_id = StepExecutionId::new("effect-6");

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    world.publish(accepted);

    let observed =
        world.wait_for_step_accepted(ctx.saga_id, "create_order", Duration::from_secs(1));
    assert!(matches!(
        observed,
        SagaChoreographyEvent::StepAccepted {
            ref execution_id,
            ..
        } if execution_id.as_ref() == "effect-6"
    ));

    world
        .complete_accepted_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_050, b"created", b"input", Vec::new()),
        )
        .expect("testkit helper should publish late completion");

    assert!(matches!(
        world.wait_for_terminal(ctx.saga_id, Duration::from_secs(1)),
        SagaTerminalOutcome::Completed { .. }
    ));
}
