//! R19 / ADR-0002 (T19D): a failed heartbeat commit leaves the deadline untouched and reports the
//! error; a failed timeout-resolution commit is reported by the poll and the step stays pending.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::time::Duration;

use icanact_saga_choreography::durability::{
    accept_workflow_step, poll_accepted_workflow_step_timeouts,
    record_accepted_workflow_step_progress,
};
use icanact_saga_choreography::{
    AcceptedStepError, AcceptedStepPolicy, AcceptedStepTimeoutOutcome, HasSagaParticipantSupport,
    InMemoryDedupe, InMemoryJournal, PeerId, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipantSupport, StepExecutionId,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
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

fn ctx() -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(19),
        saga_type: "r19_saga".into(),
        step_name: "reserve".into(),
        correlation_id: 19,
        causation_id: 19,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

#[test]
fn heartbeat_commit_failure_preserves_deadline_and_poll_reports_storage_error() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let mut actor = Actor {
        saga: SagaParticipantSupport::new(journal.clone(), dedupe),
    };
    let exec = StepExecutionId::new("r19-exec");
    let accepted = accept_workflow_step(
        &mut actor,
        ctx(),
        "p".into(),
        exec.clone(),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_millis(100),
            hard_timeout: Duration::from_secs(60),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
        },
        vec![1],
        vec![],
    )
    .expect("accept");
    let SagaChoreographyEvent::StepAccepted {
        deadline_at_millis, ..
    } = accepted
    else {
        panic!("expected StepAccepted");
    };
    let saga_id = SagaId::new(19);

    // Heartbeat whose metadata commit fails: caller gets the error, deadline must not move.
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("AcceptedStepRecorded"),
    );
    let heartbeat = record_accepted_workflow_step_progress(
        &mut actor,
        saga_id,
        exec.clone(),
        deadline_at_millis - 50,
    );
    assert!(
        matches!(heartbeat, Err(AcceptedStepError::Durability { .. })),
        "heartbeat must surface the storage error, got {heartbeat:?}"
    );

    // Had the deadline been extended to (deadline - 50) + 100, this poll would see nothing expired.
    // The timeout resolution commit also fails: the error is reported and the step stays pending.
    journal.fail_always(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionFailed"),
    );
    let polled = poll_accepted_workflow_step_timeouts(&mut actor, deadline_at_millis + 20);
    assert!(
        polled.events.is_empty(),
        "no uncommitted outcome: {polled:?}"
    );
    assert_eq!(
        polled.errors.len(),
        1,
        "poll must report the storage error: {polled:?}"
    );
    assert!(matches!(
        polled.errors[0],
        AcceptedStepError::Durability { .. }
    ));
    assert_eq!(
        actor.saga.accepted_workflow_step_count(),
        1,
        "step retained"
    );

    // Storage recovers: the next poll retries and commits the timeout.
    journal.disarm();
    let retried = poll_accepted_workflow_step_timeouts(&mut actor, deadline_at_millis + 30);
    assert!(retried.errors.is_empty());
    assert!(matches!(
        retried.events.as_slice(),
        [SagaChoreographyEvent::StepFailed { .. }]
    ));
}
