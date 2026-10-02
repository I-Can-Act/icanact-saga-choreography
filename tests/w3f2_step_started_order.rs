//! W3 review2 P2-3: `StepStarted` is published to the bus before the business effect runs on the
//! sync, async and workflow adapters, so the resolver can see a step in flight. All other emitted
//! events stay buffered until the handler returns.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::sync::{Arc, Mutex};

use icanact_saga_choreography::durability::{
    apply_async_participant_saga_ingress_with_hooks,
    apply_sync_participant_saga_ingress_with_hooks,
    apply_sync_workflow_participant_saga_ingress_with_hooks,
};
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, HasSagaParticipantSupport,
    HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal, PeerId, SagaBoxFuture,
    SagaChoreographyBus, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant,
    SagaParticipantSupport, SagaWorkflowParticipant, StepError, StepOutput,
};

const SAGA: &str = "w3f2_order";
const STEP: &str = "reserve";

type Support = SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>;
type Log = Arc<Mutex<Vec<&'static str>>>;

fn ctx() -> SagaContext {
    static NOW: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let now = *NOW.get_or_init(SagaContext::now_millis);
    SagaContext {
        saga_id: SagaId::new(51),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 51,
        causation_id: 51,
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
        payload: vec![1],
    }
}

fn probe(log: &Log) -> (Support, SagaChoreographyBus) {
    let bus = SagaChoreographyBus::new();
    let seen = Arc::clone(log);
    // Keep the subscription alive for the whole test by leaking nothing: the returned bus owns it.
    let sub = bus.subscribe_saga_type_fn(SAGA, move |event| {
        match event {
            SagaChoreographyEvent::StepStarted { .. } => seen.lock().expect("lock").push("started"),
            SagaChoreographyEvent::StepCompleted { .. } => {
                seen.lock().expect("lock").push("completed")
            }
            _ => {}
        }
        true
    });
    std::mem::forget(sub);
    let mut support = SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new());
    support.attach_bus(bus.clone());
    (support, bus)
}

fn assert_order(log: &Log) {
    assert_eq!(
        *log.lock().expect("lock"),
        vec!["started", "effect", "completed"],
        "StepStarted must reach subscribers before the effect; the result only after"
    );
}

struct Sync {
    saga: Support,
    log: Log,
}

impl HasSagaParticipantSupport for Sync {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl SagaParticipant for Sync {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.log.lock().expect("lock").push("effect");
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
fn sync_adapter_publishes_step_started_before_effect() {
    let log = Log::default();
    let (saga, _bus) = probe(&log);
    let mut actor = Sync {
        saga,
        log: Arc::clone(&log),
    };
    let report = apply_sync_participant_saga_ingress_with_hooks(
        &mut actor,
        started(),
        |_, _| {},
        |_| {},
        |_, _| {},
    );
    assert!(report.publish_failures.is_empty(), "{report:?}");
    assert_order(&log);
}

struct Async {
    saga: Support,
    log: Log,
}

impl HasSagaParticipantSupport for Async {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl AsyncSagaParticipant for Async {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn execute_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        self.log.lock().expect("lock").push("effect");
        Box::pin(async {
            Ok(StepOutput::Completed {
                output: vec![1],
                compensation_data: vec![9],
            })
        })
    }
    fn compensate_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        Box::pin(async { Ok(CompensationOutput::Completed) })
    }
}

#[tokio::test]
async fn async_adapter_publishes_step_started_before_effect() {
    let log = Log::default();
    let (saga, _bus) = probe(&log);
    let mut actor = Async {
        saga,
        log: Arc::clone(&log),
    };
    let report = apply_async_participant_saga_ingress_with_hooks(
        &mut actor,
        started(),
        |_, _| {},
        |_| {},
        |_, _| {},
    )
    .await;
    assert!(report.publish_failures.is_empty(), "{report:?}");
    assert_order(&log);
}

struct Workflow {
    saga: Support,
    log: Log,
}

impl HasSagaParticipantSupport for Workflow {
    type Journal = InMemoryJournal;
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
        actor: &mut Workflow,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.log.lock().expect("lock").push("effect");
        Ok(StepOutput::Completed {
            output: vec![1],
            compensation_data: vec![9],
        })
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
fn workflow_adapter_publishes_step_started_before_effect() {
    let log = Log::default();
    let (saga, _bus) = probe(&log);
    let mut actor = Workflow {
        saga,
        log: Arc::clone(&log),
    };
    let report = apply_sync_workflow_participant_saga_ingress_with_hooks(
        &mut actor,
        started(),
        |_, _| {},
        |_| {},
        |_, _| {},
    );
    assert!(report.publish_failures.is_empty(), "{report:?}");
    assert_order(&log);
}
