//! R10 / ADR-0002: a dedupe-failure quarantine never destroys the run's prior state or evidence.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use icanact_saga_choreography::durability::{
    accept_workflow_step, apply_sync_workflow_participant_saga_ingress_with_hooks,
};
use icanact_saga_choreography::{
    AcceptedStepPolicy, AcceptedStepTimeoutOutcome, CommitStage, CompensationError,
    CompensationOutput, HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryDedupe,
    InMemoryJournal, IngressOutcome, IngressReport, ParticipantJournal, PeerId, RunKey,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport, SagaStateEntry,
    SagaStateExt, SagaWorkflowParticipant, StepError, StepExecutionId, StepOutput,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger};

const SAGA: &str = "r10_keep_saga";
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
    feed_event(
        actor,
        SagaChoreographyEvent::SagaStarted {
            context: ctx(),
            payload: vec![7],
        },
    )
}

/// An upstream event of the same run: admitted as the current run, so it reaches the dedupe check.
fn upstream_completed() -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx().next_step("upstream".into()),
        output: vec![],
        saga_input: vec![],
        compensation_available: false,
    }
}

fn feed_event(
    actor: &mut Actor,
    event: SagaChoreographyEvent,
) -> (IngressReport, Vec<SagaChoreographyEvent>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let valid = Arc::clone(&seen);
    let invalid = Arc::clone(&seen);
    let report = apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_, _| {},
        move |e| invalid.lock().expect("lock").push(e.clone()),
        move |_, e| valid.lock().expect("lock").push(e.clone()),
    );
    let events = seen.lock().expect("lock").clone();
    (report, events)
}

fn assert_dedupe_failed(report: IngressReport, events: &[SagaChoreographyEvent]) {
    match report.outcome {
        IngressOutcome::Failed(f) => assert_eq!(f.stage, CommitStage::Dedupe),
        other => panic!("expected Failed{{Dedupe}}, got {other:?}"),
    }
    assert!(
        events.iter().any(
            |e| matches!(e, SagaChoreographyEvent::SagaQuarantined { reason, .. }
            if reason.contains("dedupe_check_failed"))
        ),
        "{events:?}"
    );
}

#[test]
fn dedupe_failure_on_completed_run_keeps_compensation_data() {
    let (mut actor, journal, dedupe) = actor();
    let (report, _) = feed(&mut actor);
    assert!(matches!(report.outcome, IngressOutcome::Applied));
    assert!(matches!(
        actor.saga_states_ref().get(&run()),
        Some(SagaStateEntry::Completed(_))
    ));

    dedupe.fail_check_and_mark(FaultTrigger::NthCall(dedupe.mark_calls() + 1));
    let (report, events) = feed_event(&mut actor, upstream_completed());
    assert_dedupe_failed(report, &events);

    assert_eq!(actor.executed, 1, "no second effect");
    match actor.saga_states_ref().get(&run()) {
        Some(SagaStateEntry::Completed(s)) => {
            assert_eq!(s.state.compensation_data, vec![9], "undo data retained");
        }
        other => panic!(
            "completed state must not be clobbered, got {:?}",
            other.map(|_| "other")
        ),
    }
    let rows = journal.read_run(&run()).expect("read");
    assert!(
        rows.iter().any(|r| matches!(
            r.event.transition(),
            icanact_saga_choreography::ParticipantEvent::StepExecutionCompleted { compensation_data, .. }
                if compensation_data == &[9]
        )),
        "completion row must stay in the journal: {rows:?}"
    );
}

#[test]
fn dedupe_failure_on_executing_run_is_quarantined() {
    let (mut actor, _journal, dedupe) = actor();
    let policy = AcceptedStepPolicy {
        idle_timeout: Duration::from_secs(5),
        hard_timeout: Duration::from_secs(10),
        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
    };
    accept_workflow_step(
        &mut actor,
        ctx(),
        STEP.into(),
        StepExecutionId::new("exec-1"),
        policy,
        vec![7],
        vec![9],
    )
    .expect("accepted");
    assert!(matches!(
        actor.saga_states_ref().get(&run()),
        Some(SagaStateEntry::Executing(_))
    ));

    dedupe.fail_check_and_mark(FaultTrigger::NthCall(dedupe.mark_calls() + 1));
    let (report, events) = feed_event(&mut actor, upstream_completed());
    assert_dedupe_failed(report, &events);

    match actor.saga_states_ref().get(&run()) {
        Some(SagaStateEntry::Quarantined(s)) => {
            assert_eq!(&*s.step_name, STEP);
        }
        other => panic!("expected quarantined, got {:?}", other.map(|_| "other")),
    }
}

mod generic {
    use super::*;
    use icanact_saga_choreography::{SagaParticipant, handle_saga_event_with_emit};

    type GSupport = SagaParticipantSupport<InMemoryJournal, Dedupe>;

    struct GActor {
        saga: GSupport,
    }

    impl HasSagaParticipantSupport for GActor {
        type Journal = InMemoryJournal;
        type Dedupe = Dedupe;
        fn saga_support(&self) -> &GSupport {
            &self.saga
        }
        fn saga_support_mut(&mut self) -> &mut GSupport {
            &mut self.saga
        }
    }

    impl SagaParticipant for GActor {
        type Error = String;
        fn step_name(&self) -> &str {
            STEP
        }
        fn saga_types(&self) -> &[&'static str] {
            &[SAGA]
        }
        fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
            Ok(StepOutput::Completed {
                output: vec![1],
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

    #[test]
    fn generic_dedupe_failure_on_completed_run_keeps_compensation_data() {
        let dedupe = FaultDedupe::new(InMemoryDedupe::new());
        let mut actor = GActor {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), dedupe.clone()),
        };
        let started = SagaChoreographyEvent::SagaStarted {
            context: ctx(),
            payload: vec![7],
        };
        let outcome = handle_saga_event_with_emit(&mut actor, started, |_| {});
        assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");

        dedupe.fail_check_and_mark(FaultTrigger::NthCall(dedupe.mark_calls() + 1));
        let mut emitted = Vec::new();
        let outcome =
            handle_saga_event_with_emit(&mut actor, upstream_completed(), |e| emitted.push(e));
        assert!(
            matches!(&outcome, IngressOutcome::Failed(f) if f.stage == CommitStage::Dedupe),
            "{outcome:?}"
        );
        assert!(emitted.iter().any(
            |e| matches!(e, SagaChoreographyEvent::SagaQuarantined { reason, .. }
                if reason.contains("dedupe_check_failed"))
        ));
        match actor.saga_states_ref().get(&run()) {
            Some(SagaStateEntry::Completed(s)) => {
                assert_eq!(s.state.compensation_data, vec![9]);
            }
            other => panic!("completed state clobbered: {:?}", other.map(|_| "other")),
        }
    }
}
