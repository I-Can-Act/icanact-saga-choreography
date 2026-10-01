//! R21 / ADR-0002 §2.2: the workflow adapter never reports `CompletedWithEffect` as plain
//! completion; it fails closed like the generic engines.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::sync::{Arc, Mutex};

use icanact_saga_choreography::durability::apply_sync_workflow_participant_saga_ingress_with_hooks;
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, HasSagaWorkflowParticipants,
    InMemoryDedupe, InMemoryJournal, IngressOutcome, IngressReport, ParticipantJournal, PeerId,
    ReconciliationCause, RunKey, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipantSupport, SagaStateEntry, SagaStateExt, SagaWorkflowParticipant, StepError,
    StepOutput,
};
use support::{FaultDedupe, FaultJournal};

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
        Ok(StepOutput::CompletedWithEffect {
            output: vec![1],
            compensation_data: vec![9],
            effect: "notify-warehouse".into(),
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

#[test]
fn workflow_completed_with_effect_is_quarantined_not_completed() {
    let (mut actor, journal, _) = actor();
    let (report, events) = feed(&mut actor);
    assert_eq!(actor.executed, 1);
    match &report.outcome {
        IngressOutcome::ReconciliationNeeded(r) => {
            assert_eq!(r.run, run());
            assert_eq!(r.compensation_data, vec![9], "undo evidence kept");
            assert!(
                matches!(&r.cause, ReconciliationCause::UnsupportedEffect { effect } if &**effect == "notify-warehouse"),
                "{:?}",
                r.cause
            );
        }
        other => panic!("expected ReconciliationNeeded, got {other:?}"),
    }
    assert!(
        events.iter().any(
            |e| matches!(e, SagaChoreographyEvent::SagaQuarantined { reason, .. }
            if reason.starts_with("reconciliation_needed: unsupported_effect"))
        ),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. })),
        "no misleading completion: {events:?}"
    );
    assert!(matches!(
        actor.saga_states_ref().get(&run()),
        Some(SagaStateEntry::Quarantined(_))
    ));
    let rows = journal.read_run(&run()).expect("read");
    assert!(
        rows.iter().any(|r| matches!(&r.event,
            icanact_saga_choreography::ParticipantEvent::StepExecutionCompleted { compensation_data, .. }
                if compensation_data == &[9])),
        "undo evidence must be durable"
    );
}
