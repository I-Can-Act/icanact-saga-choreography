//! R01 / ADR-0002 (T01P): a failed durable append on the ordinary step path never lets the
//! callback run unrecorded and never acknowledges an effect it could not record.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

// Fixture re-exports are shared by every test crate; this one uses a subset.
#[allow(unused_imports)]
mod support;

use icanact_saga_choreography::{
    AsyncSagaParticipant, CommitStage, CompensationError, CompensationOutput,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, IngressOutcome, PeerId,
    ReconciliationCause, ReplayHorizon, RunKey, SagaBoxFuture, SagaChoreographyEvent, SagaContext,
    SagaId, SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt, StepError,
    StepOutput, compensation_requested, handle_async_saga_event_with_emit,
    handle_saga_event_with_emit,
};
use support::{EffectLedger, FaultJournal, FaultTrigger, JournalOp};

const STEP: &str = "reserve";
const TYPE: &str = "order";

type Journal = FaultJournal<InMemoryJournal>;
type Support = SagaParticipantSupport<Journal, InMemoryDedupe>;

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
    type Dedupe = InMemoryDedupe;
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
    fn new(engine: Engine) -> (Self, Journal, EffectLedger) {
        let journal = FaultJournal::new(InMemoryJournal::new());
        let ledger = EffectLedger::new();
        let actor = Self {
            saga: SagaParticipantSupport::new(journal.clone(), InMemoryDedupe::new())
                .with_replay_horizon(
                    ReplayHorizon::new(std::time::Duration::from_secs(100 * 365 * 24 * 3600))
                        .expect("horizon above the floor"),
                ),
            ledger: ledger.clone(),
            engine,
        };
        (actor, journal, ledger)
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

fn fail_on(journal: &Journal, kind: &'static str) {
    journal.fail_once(JournalOp::Append, FaultTrigger::EventKind(kind));
}

fn has(events: &[SagaChoreographyEvent], name: &str) -> bool {
    events.iter().any(|e| e.event_type() == name)
}

fn intent_append_failure(engine: Engine) {
    let (mut actor, journal, ledger) = Actor::new(engine);
    fail_on(&journal, "StepExecutionStarted");
    let (outcome, emitted) = actor.feed(started());
    let key = run().to_string();
    assert_eq!(
        ledger.forward_count(&key),
        0,
        "{engine:?}: callback must not run"
    );
    assert!(
        matches!(&outcome, IngressOutcome::Failed(f) if f.stage == CommitStage::Intent && f.run == run()),
        "{engine:?}: {outcome:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepFailed {
            requires_compensation: true,
            error_code: Some(code),
            ..
        } if code.as_ref() == "intent_commit_failed")),
        "{engine:?}: {emitted:?}"
    );
    assert!(!has(&emitted, "step_completed"), "{engine:?}");
    assert_eq!(
        kind(actor.saga_states_ref().get(&run())),
        "failed",
        "{engine:?}"
    );
    let (again, _) = actor.feed(started());
    assert!(
        matches!(again, IngressOutcome::Duplicate),
        "{engine:?}: {again:?}"
    );
    assert_eq!(
        ledger.forward_count(&key),
        0,
        "{engine:?}: redelivery must not run"
    );
}

fn result_append_failure(engine: Engine) {
    let (mut actor, journal, ledger) = Actor::new(engine);
    fail_on(&journal, "StepExecutionCompleted");
    let (outcome, emitted) = actor.feed(started());
    assert_eq!(ledger.forward_count(&run().to_string()), 1);
    assert!(
        matches!(&outcome, IngressOutcome::ReconciliationNeeded(r)
            if matches!(r.cause, ReconciliationCause::ResultCommitFailed(_))
            && r.compensation_data == vec![9] && r.run == run()),
        "{engine:?}: {outcome:?}"
    );
    assert!(!has(&emitted, "step_completed"), "{engine:?}: {emitted:?}");
    assert!(has(&emitted, "saga_quarantined"), "{engine:?}: {emitted:?}");
    assert_eq!(
        kind(actor.saga_states_ref().get(&run())),
        "quarantined",
        "{engine:?}"
    );
}

fn completed_actor(engine: Engine) -> (Actor, Journal, EffectLedger) {
    let (mut actor, journal, ledger) = Actor::new(engine);
    let (outcome, _) = actor.feed(started());
    assert!(matches!(outcome, IngressOutcome::Applied));
    (actor, journal, ledger)
}

fn comp_request() -> SagaChoreographyEvent {
    compensation_requested(ctx(), "downstream", "rollback", vec![STEP.to_string()])
}

fn compensation_start_failure(engine: Engine) {
    let (mut actor, journal, ledger) = completed_actor(engine);
    fail_on(&journal, "CompensationStarted");
    let (outcome, emitted) = actor.feed(comp_request());
    assert_eq!(
        ledger.undo_count(&run().to_string()),
        0,
        "{engine:?}: undo must not run"
    );
    assert!(
        matches!(&outcome, IngressOutcome::Failed(f) if f.stage == CommitStage::CompensationStart),
        "{engine:?}: {outcome:?}"
    );
    assert!(
        has(&emitted, "compensation_failed_retryable"),
        "{engine:?}: {emitted:?}"
    );
    assert_eq!(
        kind(actor.saga_states_ref().get(&run())),
        "completed",
        "{engine:?}"
    );
}

fn compensation_result_failure(engine: Engine) {
    let (mut actor, journal, ledger) = completed_actor(engine);
    fail_on(&journal, "CompensationCompleted");
    let (outcome, emitted) = actor.feed(comp_request());
    assert_eq!(ledger.undo_count(&run().to_string()), 1);
    assert!(
        matches!(&outcome, IngressOutcome::ReconciliationNeeded(r)
            if matches!(r.cause, ReconciliationCause::CompensationResultCommitFailed(_))),
        "{engine:?}: {outcome:?}"
    );
    assert!(
        !has(&emitted, "compensation_completed"),
        "{engine:?}: {emitted:?}"
    );
    assert!(has(&emitted, "saga_quarantined"), "{engine:?}: {emitted:?}");
    assert_eq!(
        kind(actor.saga_states_ref().get(&run())),
        "quarantined",
        "{engine:?}"
    );
}

#[test]
fn ordinary_step_append_failure_never_executes_or_acknowledges() {
    for engine in [Engine::Sync, Engine::Async] {
        intent_append_failure(engine);
        result_append_failure(engine);
        compensation_start_failure(engine);
        compensation_result_failure(engine);
    }
}
