//! R01 / ADR-0002 (T01D): the workflow adapter never runs an effect without a durable intent and
//! never acknowledges a result whose journal commit failed. Every failure returns a typed
//! outcome, and emits a failure/quarantine event where that is safe (owner decisions Q5/Q6).
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use icanact_saga_choreography::durability::{
    accept_workflow_step, apply_sync_workflow_participant_saga_ingress_with_hooks,
    complete_accepted_workflow_step,
};
use icanact_saga_choreography::{
    AcceptedStepCompletion, AcceptedStepPolicy, AcceptedStepTimeoutOutcome, CommitStage,
    CompensationError, CompensationOutput, HasSagaParticipantSupport, HasSagaWorkflowParticipants,
    InMemoryDedupe, InMemoryJournal, IngressOutcome, IngressReport, ParticipantJournal, PeerId,
    ReconciliationCause, RunIncarnation, RunKey, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipantSupport, SagaStateEntry, SagaWorkflowParticipant, StepError, StepExecutionId,
    StepOutput,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

const SAGA: &str = "r01_saga";
const STEP: &str = "reserve";

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    executed: usize,
    step_fails: bool,
}

struct Step;

impl SagaWorkflowParticipant<Actor> for Step {
    fn step_name(&self) -> &'static str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.executed += 1;
        if actor.step_fails {
            return Err(StepError::RequireCompensation {
                reason: "business failure".into(),
            });
        }
        Ok(StepOutput::Completed {
            output: vec![1],
            compensation_data: vec![9],
        })
    }
    fn compensate_step(
        &self,
        _: &mut Actor,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        Ok(CompensationOutput::Completed)
    }
}

static STEP_IMPL: Step = Step;
static STEPS: [&dyn SagaWorkflowParticipant<Actor>; 1] = [&STEP_IMPL];

impl HasSagaWorkflowParticipants for Actor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &STEPS
    }
}

impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.saga
    }
}

fn actor() -> (Actor, Journal, Dedupe) {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let actor = Actor {
        saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
        executed: 0,
        step_fails: false,
    };
    (actor, journal, dedupe)
}

/// A run that started just now, so replay-horizon admission accepts it.
fn started_at() -> u64 {
    static NOW: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *NOW.get_or_init(SagaContext::now_millis)
}

fn ctx() -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(7),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 7,
        causation_id: 7,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at(),
        event_timestamp_millis: started_at(),
    }
}

fn run() -> RunKey {
    ctx().run_key()
}

fn feed(actor: &mut Actor) -> (IngressReport, Vec<SagaChoreographyEvent>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let valid = Arc::clone(&seen);
    let invalid = Arc::clone(&seen);
    let report = apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        SagaChoreographyEvent::SagaStarted {
            context: ctx(),
            payload: vec![7],
        },
        |_, _| {},
        move |e| invalid.lock().expect("lock").push(e.clone()),
        move |_, e| valid.lock().expect("lock").push(e.clone()),
    );
    let events = seen.lock().expect("lock").clone();
    (report, events)
}

fn has_step_completed(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
}

fn quarantine_reason(events: &[SagaChoreographyEvent]) -> Option<String> {
    events.iter().find_map(|e| match e {
        SagaChoreographyEvent::SagaQuarantined { reason, .. } => Some(reason.to_string()),
        _ => None,
    })
}

#[test]
fn intent_commit_failure_skips_the_effect_and_emits_step_failed() {
    let (mut actor, journal, _dedupe) = actor();
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionStarted"),
    );
    let (report, events) = feed(&mut actor);

    assert_eq!(
        actor.executed, 0,
        "callback must not run without a durable intent"
    );
    match report.outcome {
        IngressOutcome::Failed(failure) => {
            assert_eq!(failure.stage, CommitStage::Intent);
            assert_eq!(failure.run, run());
        }
        other => panic!("expected Failed(Intent), got {other:?}"),
    }
    assert!(!has_step_completed(&events));
    let failed = events.iter().find_map(|e| match e {
        SagaChoreographyEvent::StepFailed {
            error_code,
            requires_compensation,
            ..
        } => Some((error_code.clone(), *requires_compensation)),
        _ => None,
    });
    assert_eq!(
        failed,
        Some((Some("intent_commit_failed".into()), true)),
        "events: {events:?}"
    );
    assert!(matches!(
        actor.saga.saga_states.get(&run()),
        Some(SagaStateEntry::Failed(_))
    ));
}

#[test]
fn result_commit_failure_after_effect_quarantines_instead_of_completing() {
    let (mut actor, journal, _dedupe) = actor();
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionCompleted"),
    );
    let (report, events) = feed(&mut actor);

    assert_eq!(actor.executed, 1);
    match report.outcome {
        IngressOutcome::ReconciliationNeeded(needed) => {
            assert!(matches!(
                needed.cause,
                ReconciliationCause::ResultCommitFailed(_)
            ));
            assert_eq!(needed.run, run());
            assert_eq!(needed.compensation_data, vec![9]);
        }
        other => panic!("expected ReconciliationNeeded, got {other:?}"),
    }
    assert!(!has_step_completed(&events), "events: {events:?}");
    let reason = quarantine_reason(&events).expect("SagaQuarantined emitted");
    assert!(reason.contains("result_commit_failed"), "{reason}");
    assert!(matches!(
        actor.saga.saga_states.get(&run()),
        Some(SagaStateEntry::Quarantined(_))
    ));
}

#[test]
fn failed_result_commit_failure_quarantines_instead_of_step_failed() {
    let (mut actor, journal, _dedupe) = actor();
    actor.step_fails = true;
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionFailed"),
    );
    let (report, events) = feed(&mut actor);

    assert_eq!(actor.executed, 1);
    assert!(
        matches!(
            report.outcome,
            IngressOutcome::ReconciliationNeeded(ref n)
                if matches!(n.cause, ReconciliationCause::ResultCommitFailed(_))
        ),
        "got {:?}",
        report.outcome
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepFailed { .. })),
        "events: {events:?}"
    );
    assert!(quarantine_reason(&events).is_some(), "events: {events:?}");
    assert!(matches!(
        actor.saga.saga_states.get(&run()),
        Some(SagaStateEntry::Quarantined(_))
    ));
}

#[test]
fn dedupe_error_quarantines_without_running_the_effect() {
    let (mut actor, _journal, dedupe) = actor();
    dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
    let (report, events) = feed(&mut actor);

    assert_eq!(actor.executed, 0);
    match report.outcome {
        IngressOutcome::Failed(failure) => assert_eq!(failure.stage, CommitStage::Dedupe),
        other => panic!("expected Failed(Dedupe), got {other:?}"),
    }
    let reason = quarantine_reason(&events).expect("SagaQuarantined emitted");
    assert!(reason.contains("dedupe_check_failed"), "{reason}");
    assert!(matches!(
        actor.saga.saga_states.get(&run()),
        Some(SagaStateEntry::Quarantined(_))
    ));
}

#[test]
fn successful_step_is_applied_and_its_result_is_in_the_journal_outbox() {
    let (mut actor, journal, _dedupe) = actor();
    let (report, events) = feed(&mut actor);

    assert!(
        matches!(report.outcome, IngressOutcome::Applied),
        "{:?}",
        report.outcome
    );
    assert!(report.publish_failures.is_empty());
    assert!(has_step_completed(&events));
    let pending = journal
        .outbox_for_replay(RunIncarnation::new(0))
        .expect("outbox read");
    assert!(
        pending
            .iter()
            .any(|r| matches!(r.event, SagaChoreographyEvent::StepCompleted { .. })),
        "StepCompleted must be journaled as an outbox obligation: {pending:?}"
    );
}

#[test]
fn accepted_step_completion_failure_keeps_the_step_accepted_and_success_journals_outbox() {
    let (mut actor, journal, _dedupe) = actor();
    let policy = AcceptedStepPolicy {
        idle_timeout: Duration::from_secs(5),
        hard_timeout: Duration::from_secs(10),
        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
    };
    let execution_id = StepExecutionId::new("exec-1");
    accept_workflow_step(
        &mut actor,
        ctx(),
        STEP.into(),
        execution_id.clone(),
        policy,
        vec![7],
        vec![9],
    )
    .expect("accepted");
    let completion = || AcceptedStepCompletion {
        completed_at_millis: started_at() + 5,
        output: vec![1],
        compensation_data: vec![9],
    };

    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionCompleted"),
    );
    complete_accepted_workflow_step(
        &mut actor,
        SagaId::new(7),
        execution_id.clone(),
        completion(),
    )
    .expect_err("journal failure must surface");
    assert!(actor.saga.accepted_workflow_steps.contains_key(&run()));

    complete_accepted_workflow_step(&mut actor, SagaId::new(7), execution_id, completion())
        .expect("retry commits");
    let pending = journal
        .outbox_for_replay(RunIncarnation::new(0))
        .expect("outbox read");
    assert!(
        pending
            .iter()
            .any(|r| matches!(r.event, SagaChoreographyEvent::StepCompleted { .. })),
        "{pending:?}"
    );
}
