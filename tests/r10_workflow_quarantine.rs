//! R10 / ADR-0002 (T10D): a dedupe-store failure in the workflow adapter is never a false duplicate;
//! the participant-originated quarantine is reported, not filtered by the emitted-transition validator.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::sync::{Arc, Mutex};

use icanact_saga_choreography::durability::apply_sync_workflow_participant_saga_ingress_with_hooks;
use icanact_saga_choreography::{
    CommitStage, CompensationError, CompensationOutput, HasSagaParticipantSupport,
    HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal, IngressOutcome, PeerId,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport, SagaWorkflowParticipant,
    StepError, StepOutput,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger};

const SAGA: &str = "r10_saga";
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

#[test]
fn workflow_dedupe_failure_quarantine_is_not_filtered() {
    let (mut actor, _journal, dedupe) = actor();
    dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let invalid = Arc::clone(&seen);
    let valid = Arc::clone(&seen);
    let report = apply_sync_workflow_participant_saga_ingress_with_hooks(
        &mut actor,
        SagaChoreographyEvent::SagaStarted {
            context: ctx(),
            payload: vec![7],
        },
        |_, _| {},
        move |e| invalid.lock().expect("lock").push(("invalid", e.clone())),
        move |_, e| valid.lock().expect("lock").push(("valid", e.clone())),
    );
    let seen = seen.lock().expect("lock").clone();

    assert_eq!(actor.executed, 0, "no effect on dedupe failure");
    match report.outcome {
        IngressOutcome::Failed(f) => assert_eq!(f.stage, CommitStage::Dedupe),
        other => panic!("expected Failed{{Dedupe}}, got {other:?}"),
    }
    assert!(
        seen.iter().any(|(kind, e)| *kind == "valid"
            && matches!(e, SagaChoreographyEvent::SagaQuarantined { reason, .. }
                if reason.contains("dedupe_check_failed"))),
        "quarantine must be accepted as valid, got {seen:?}"
    );
    assert!(
        !seen.iter().any(|(kind, _)| *kind == "invalid"),
        "quarantine must not be reported as an invalid transition: {seen:?}"
    );
    assert!(
        report.publish_failures.is_empty(),
        "no bus, so no publish failures"
    );
}
