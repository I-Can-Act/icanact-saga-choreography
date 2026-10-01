//! R10 / ADR-0002 (T10P): a dedupe-store error is never a duplicate; the run is quarantined.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

// Fixture re-exports are shared by every test crate; this one uses a subset.
#[allow(unused_imports)]
mod support;

use icanact_saga_choreography::{
    AsyncSagaParticipant, CommitStage, CompensationError, CompensationOutput,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, IngressOutcome, PeerId,
    ReplayHorizon, RunKey, SagaBoxFuture, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt, StepError, StepOutput,
    handle_async_saga_event_with_emit, handle_saga_event_with_emit,
};
use support::{EffectLedger, FaultDedupe, FaultTrigger};

const STEP: &str = "reserve";
const TYPE: &str = "order";

type Journal = InMemoryJournal;
type Dedupe = FaultDedupe<InMemoryDedupe>;
type Support = SagaParticipantSupport<Journal, Dedupe>;

#[derive(Clone, Copy, Debug)]
enum Engine {
    Sync,
    Async,
}

struct Actor {
    saga: Support,
    ledger: EffectLedger,
    engine: Engine,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = FaultDedupe<InMemoryDedupe>;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

fn ok() -> StepOutput {
    StepOutput::Completed {
        output: vec![1],
        compensation_data: vec![9],
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
    fn execute_step(&mut self, c: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.ledger.forward(&c.run_key().to_string());
        Ok(ok())
    }
    fn compensate_step(
        &mut self,
        c: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        let _ = self.ledger.undo(&c.run_key().to_string());
        Ok(CompensationOutput::Completed)
    }
}

impl AsyncSagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn execute_step<'a>(
        &'a mut self,
        c: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        self.ledger.forward(&c.run_key().to_string());
        Box::pin(async { Ok(ok()) })
    }
    fn compensate_step<'a>(
        &'a mut self,
        c: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        let _ = self.ledger.undo(&c.run_key().to_string());
        Box::pin(async { Ok(CompensationOutput::Completed) })
    }
}

impl Actor {
    fn new(engine: Engine) -> (Self, Dedupe, EffectLedger) {
        let dedupe = FaultDedupe::new(InMemoryDedupe::new());
        let ledger = EffectLedger::new();
        let actor = Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), dedupe.clone())
                .with_replay_horizon(
                    ReplayHorizon::new(std::time::Duration::from_secs(100 * 365 * 24 * 3600))
                        .expect("horizon above the floor"),
                ),
            ledger: ledger.clone(),
            engine,
        };
        (actor, dedupe, ledger)
    }

    fn feed(
        &mut self,
        event: SagaChoreographyEvent,
    ) -> (IngressOutcome, Vec<SagaChoreographyEvent>) {
        let mut emitted = Vec::new();
        let outcome = match self.engine {
            Engine::Sync => handle_saga_event_with_emit(self, event, |e| emitted.push(e)),
            Engine::Async => tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime")
                .block_on(handle_async_saga_event_with_emit(self, event, |e| {
                    emitted.push(e)
                })),
        };
        (outcome, emitted)
    }
}

fn ctx() -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(1),
        saga_type: TYPE.into(),
        step_name: STEP.into(),
        correlation_id: 1,
        causation_id: 1,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: 1_000,
        event_timestamp_millis: 1_000,
    }
}

fn started() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx(),
        payload: vec![7],
    }
}

fn run() -> RunKey {
    ctx().run_key()
}

fn kind(entry: Option<&SagaStateEntry>) -> &'static str {
    match entry {
        Some(SagaStateEntry::Completed(_)) => "completed",
        Some(SagaStateEntry::Failed(_)) => "failed",
        Some(SagaStateEntry::Quarantined(_)) => "quarantined",
        Some(SagaStateEntry::Compensating(_)) => "compensating",
        Some(SagaStateEntry::Executing(_)) => "executing",
        Some(_) => "other",
        None => "none",
    }
}

fn check(engine: Engine) {
    let (mut actor, dedupe, ledger) = Actor::new(engine);
    dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
    let (outcome, emitted) = actor.feed(started());
    assert_eq!(
        ledger.forward_count(&run().to_string()),
        0,
        "{engine:?}: no effect"
    );
    assert!(
        matches!(&outcome, IngressOutcome::Failed(f) if f.stage == CommitStage::Dedupe && f.run == run()),
        "{engine:?}: dedupe error must be Failed{{Dedupe}}, got {outcome:?}"
    );
    assert!(
        emitted.iter().any(
            |e| matches!(e, SagaChoreographyEvent::SagaQuarantined { reason, .. }
            if reason.starts_with("reconciliation_needed: dedupe_check_failed"))
        ),
        "{engine:?}: {emitted:?}"
    );
    assert_eq!(
        kind(actor.saga_states_ref().get(&run())),
        "quarantined",
        "{engine:?}"
    );
}

#[test]
fn dedupe_failure_is_not_duplicate_sync_async() {
    check(Engine::Sync);
    check(Engine::Async);
}
