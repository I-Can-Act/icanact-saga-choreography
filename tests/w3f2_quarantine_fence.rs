//! W3 review2 P2-2: a `Quarantined` entry in memory fences its run (Q6) independently of the
//! bounded terminal latch.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use icanact_saga_choreography::{
    CompensationError, CompensationOutput, DependencySpec, HasSagaParticipantSupport,
    InMemoryDedupe, InMemoryJournal, IngressOutcome, IngressRejection, PeerId, RunIncarnation,
    RunKey, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport,
    SagaStateEntry, SagaStateExt, StepError, StepOutput, handle_saga_event_with_emit,
};
use support::{FaultDedupe, FaultTrigger};

const SAGA: &str = "w3f2_fence";
const STEP: &str = "reserve";

type Support = SagaParticipantSupport<InMemoryJournal, FaultDedupe<InMemoryDedupe>>;

struct Actor {
    saga: Support,
    executed: usize,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = InMemoryJournal;
    type Dedupe = FaultDedupe<InMemoryDedupe>;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::After("upstream")
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.executed += 1;
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

fn ctx() -> SagaContext {
    static NOW: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let now = *NOW.get_or_init(SagaContext::now_millis);
    SagaContext {
        saga_id: SagaId::new(21),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 21,
        causation_id: 21,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

fn upstream_completed() -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx().next_step("upstream".into()),
        output: vec![],
        saga_input: vec![],
        compensation_available: false,
    }
}

#[test]
fn quarantined_run_stays_fenced_after_terminal_latch_eviction() {
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let mut actor = Actor {
        saga: SagaParticipantSupport::new(InMemoryJournal::new(), dedupe.clone()),
        executed: 0,
    };
    let run = ctx().run_key();

    dedupe.fail_check_and_mark(FaultTrigger::NthCall(dedupe.mark_calls() + 1));
    let outcome = handle_saga_event_with_emit(&mut actor, upstream_completed(), |_| {});
    assert!(matches!(outcome, IngressOutcome::Failed(_)), "{outcome:?}");
    assert!(matches!(
        actor.saga_states_ref().get(&run),
        Some(SagaStateEntry::Quarantined(_))
    ));
    assert!(actor.is_terminal_saga_latched(&run));

    // Exceed the bounded latch so the quarantined run's latch is evicted.
    for i in 0..5000u64 {
        let other = RunKey::new(SAGA, SagaId::new(10_000 + i), RunIncarnation::new(0));
        actor.latch_terminal_saga(&other);
    }
    assert!(
        !actor.is_terminal_saga_latched(&run),
        "latch must be evicted"
    );

    let outcome = handle_saga_event_with_emit(&mut actor, upstream_completed(), |_| {});
    assert_eq!(
        actor.executed, 0,
        "a quarantined run runs no business effect"
    );
    assert!(
        matches!(
            outcome,
            IngressOutcome::Rejected(IngressRejection::TerminalRun)
        ),
        "{outcome:?}"
    );
}
