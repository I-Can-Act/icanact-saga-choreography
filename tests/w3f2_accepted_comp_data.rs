//! W3 review2 P2-4: an accepted step whose metadata could not be persisted reports its
//! compensation data in `ReconciliationNeeded` (and keeps it on the in-memory quarantine).
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::time::Duration;

use icanact_saga_choreography::durability::apply_sync_workflow_participant_saga_ingress_with_hooks;
use icanact_saga_choreography::{
    AcceptedStepPolicy, AcceptedStepTimeoutOutcome, CompensationError, CompensationOutput,
    HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal,
    IngressOutcome, PeerId, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant,
    SagaParticipantSupport, SagaStateEntry, SagaStateExt, SagaWorkflowParticipant, StepError,
    StepExecutionId, StepOutput, handle_saga_event_with_emit,
};
use support::{FaultJournal, FaultTrigger, JournalOp};

const SAGA: &str = "w3f2_comp";
const STEP: &str = "reserve";

type Journal = FaultJournal<InMemoryJournal>;
type Support = SagaParticipantSupport<Journal, InMemoryDedupe>;

fn accepted() -> StepOutput {
    StepOutput::Accepted {
        execution_id: StepExecutionId::new("exec-1"),
        policy: AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(5),
            hard_timeout: Duration::from_secs(10),
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
        },
        compensation_data: vec![9, 8],
    }
}

fn ctx() -> SagaContext {
    static NOW: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let now = *NOW.get_or_init(SagaContext::now_millis);
    SagaContext {
        saga_id: SagaId::new(31),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 31,
        causation_id: 31,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

fn started() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx(),
        payload: vec![7],
    }
}

fn assert_carries(outcome: &IngressOutcome) {
    match outcome {
        IngressOutcome::ReconciliationNeeded(r) => assert_eq!(
            r.compensation_data,
            vec![9, 8],
            "the undo data of the accepted step must travel with the outcome"
        ),
        other => panic!("expected ReconciliationNeeded, got {other:?}"),
    }
}

fn assert_memory_keeps(actor: &impl SagaStateExt) {
    match actor.saga_states_ref().get(&ctx().run_key()) {
        Some(SagaStateEntry::Quarantined(s)) => {
            assert_eq!(s.state.compensation_data.as_deref(), Some(&[9u8, 8][..]));
        }
        other => panic!("expected quarantined, got {:?}", other.map(|_| "other")),
    }
}

struct Generic {
    saga: Support,
}

impl HasSagaParticipantSupport for Generic {
    type Journal = Journal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl SagaParticipant for Generic {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        Ok(accepted())
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
fn generic_accepted_step_reconciliation_carries_compensation_data() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    journal.fail_always(
        JournalOp::Append,
        FaultTrigger::EventKind("AcceptedStepRecorded"),
    );
    let mut actor = Generic {
        saga: SagaParticipantSupport::new(journal, InMemoryDedupe::new()),
    };
    let outcome = handle_saga_event_with_emit(&mut actor, started(), |_| {});
    assert_carries(&outcome);
    assert_memory_keeps(&actor);
}

struct Workflow {
    saga: Support,
}

impl HasSagaParticipantSupport for Workflow {
    type Journal = Journal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

struct Step;

impl SagaWorkflowParticipant<Workflow> for Step {
    fn step_name(&self) -> &'static str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn execute_step(
        &self,
        _: &mut Workflow,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(accepted())
    }
    fn compensate_step(
        &self,
        _: &mut Workflow,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        Ok(CompensationOutput::Completed)
    }
}

static STEP_IMPL: Step = Step;
static STEPS: [&dyn SagaWorkflowParticipant<Workflow>; 1] = [&STEP_IMPL];

impl HasSagaWorkflowParticipants for Workflow {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &STEPS
    }
}

#[test]
fn workflow_accepted_step_reconciliation_carries_compensation_data() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    journal.fail_always(
        JournalOp::Append,
        FaultTrigger::EventKind("AcceptedStepRecorded"),
    );
    let mut actor = Workflow {
        saga: SagaParticipantSupport::new(journal, InMemoryDedupe::new()),
    };
    let report = apply_sync_workflow_participant_saga_ingress_with_hooks(
        &mut actor,
        started(),
        |_, _| {},
        |_| {},
        |_, _| {},
    );
    assert_carries(&report.outcome);
    assert_memory_keeps(&actor);
}
