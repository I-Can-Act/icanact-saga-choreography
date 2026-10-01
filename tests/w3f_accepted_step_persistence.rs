//! W3 review MEDIUM 6 + pre-existing: the ordinary-path accepted-step persistence failure is a
//! typed `ReconciliationNeeded` (matching the workflow adapter), and the accepted-step metadata
//! row is run-scoped so `finalize_run` removes it (a finalized run cannot be resurrected).
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::time::Duration;

use icanact_saga_choreography::durability::collect_startup_recovery_events_for_saga_type;
use icanact_saga_choreography::{
    AcceptedStepPolicy, AcceptedStepTimeoutOutcome, CompensationError, CompensationOutput,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, IngressOutcome, ParticipantJournal,
    PeerId, ReconciliationCause, RunIncarnation, RunKey, RunTerminalOutcome, RunTombstone,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport, StepError,
    StepExecutionId, StepOutput, handle_saga_event_with_emit,
};
use support::{FaultJournal, FaultTrigger, JournalOp};

const STEP: &str = "reserve";
const TYPE: &str = "order";

type Journal = FaultJournal<InMemoryJournal>;

struct Actor {
    saga: SagaParticipantSupport<Journal, InMemoryDedupe>,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, InMemoryDedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, InMemoryDedupe> {
        &mut self.saga
    }
}

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("exec-1"),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(5),
                hard_timeout: Duration::from_secs(10),
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            },
            compensation_data: vec![9],
        })
    }
    fn compensate_step(
        &mut self,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        Ok(CompensationOutput::Completed)
    }
}

fn actor() -> (Actor, Journal) {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let actor = Actor {
        saga: SagaParticipantSupport::new(journal.clone(), InMemoryDedupe::new()),
    };
    (actor, journal)
}

fn ctx() -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(3),
        saga_type: TYPE.into(),
        step_name: STEP.into(),
        correlation_id: 3,
        causation_id: 3,
        trace_id: 3,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

fn started(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: context.clone(),
        payload: vec![7],
    }
}

#[test]
fn accepted_step_persistence_failure_needs_reconciliation() {
    let (mut actor, journal) = actor();
    journal.fail_always(
        JournalOp::Append,
        FaultTrigger::EventKind("AcceptedStepRecorded"),
    );
    let context = ctx();
    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(&mut actor, started(&context), |e| emitted.push(e));
    assert!(
        matches!(
            &outcome,
            IngressOutcome::ReconciliationNeeded(r)
                if r.run == context.run_key()
                    && matches!(r.cause, ReconciliationCause::ResultCommitFailed(_))
        ),
        "accepted-step persistence failure must need reconciliation, got {outcome:?}"
    );
    assert!(
        emitted.iter().all(|e| !matches!(
            e,
            SagaChoreographyEvent::StepAccepted { .. }
                | SagaChoreographyEvent::StepCompleted { .. }
        )),
        "no acknowledgement may be emitted: {emitted:?}"
    );
}

#[test]
fn finalize_run_removes_accepted_step_rows_so_restart_cannot_resurrect_it() {
    let (mut actor, journal) = actor();
    let context = ctx();
    let run: RunKey = context.run_key();
    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(&mut actor, started(&context), |e| emitted.push(e));
    assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepAccepted { .. })),
        "step must be accepted: {emitted:?}"
    );
    assert!(
        !journal.read_run(&run).expect("read_run").is_empty(),
        "accepted-step row must be in the run's own partition"
    );

    let tombstone = RunTombstone::new(run.clone(), RunTerminalOutcome::Completed, 1);
    journal
        .finalize_run(&tombstone, RunIncarnation::new(0))
        .expect("finalize");

    assert!(
        journal.read(run.saga_id()).expect("read").is_empty(),
        "no legacy-partition accepted-step row may survive finalize_run"
    );
    let events =
        collect_startup_recovery_events_for_saga_type(&journal, &InMemoryDedupe::new(), STEP, TYPE)
            .expect("recovery collection");
    assert!(
        events.is_empty(),
        "a finalized run must not recover anything: {events:?}"
    );
}

#[test]
fn poll_outcome_with_errors_is_not_empty() {
    use icanact_saga_choreography::durability::poll_accepted_workflow_step_timeouts;
    let (mut actor, journal) = actor();
    let context = ctx();
    let mut emitted = Vec::new();
    handle_saga_event_with_emit(&mut actor, started(&context), |e| emitted.push(e));
    journal.fail_always(JournalOp::Append, FaultTrigger::EventKind("Quarantined"));
    let polled = poll_accepted_workflow_step_timeouts(&mut actor, u64::MAX);
    assert_eq!(
        polled.errors.len(),
        1,
        "resolution failure reported: {polled:?}"
    );
    assert!(polled.has_no_events());
    assert!(!polled.is_clean());
    assert!(
        !polled.is_empty(),
        "a poll that failed is not empty: {polled:?}"
    );
}
