//! W3 review2 P2-7: a panic quarantine goes through the shared quarantine path: run-scoped
//! journal row, in-memory `Quarantined`, run un-admitted and latched terminal.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use icanact_saga_choreography::durability::{
    ActiveSagaExecution, ActiveSagaExecutionPhase, HasActiveSagaExecution,
    is_panic_quarantine_reason, run_participant_phase_with_panic_quarantine,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, InMemoryDedupe,
    InMemoryJournal, ParticipantEvent, ParticipantJournal, PeerId, SagaChoreographyEvent,
    SagaContext, SagaId, SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt,
    StepError, StepOutput, handle_saga_event_with_emit,
};

const SAGA: &str = "w3f2_panic";
const STEP: &str = "reserve";

type Support = SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>;

struct Actor {
    saga: Support,
    active: Option<ActiveSagaExecution>,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl HasActiveSagaExecution for Actor {
    fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
        &mut self.active
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
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        panic!("boom-in-business-code");
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
        saga_id: SagaId::new(41),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 41,
        causation_id: 41,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

#[test]
fn panic_quarantine_uses_shared_path_run_scoped_and_latched() {
    let mut actor = Actor {
        saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        active: None,
    };
    let context = ctx();
    let run = context.run_key();

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_participant_phase_with_panic_quarantine(
            &mut actor,
            &context,
            ActiveSagaExecutionPhase::StepExecution,
            |a| {
                handle_saga_event_with_emit(
                    a,
                    SagaChoreographyEvent::SagaStarted {
                        context: ctx(),
                        payload: vec![1],
                    },
                    |_| {},
                )
            },
        )
    }));
    assert!(caught.is_err(), "the panic is rethrown");

    assert!(
        matches!(
            actor.saga_states_ref().get(&run),
            Some(SagaStateEntry::Quarantined(_))
        ),
        "panic must quarantine the run in memory"
    );
    assert!(
        !actor.saga_support().admitted_runs.contains(&run),
        "a quarantined run is no longer admitted"
    );
    assert!(actor.is_terminal_saga_latched(&run));
    let journal = &actor.saga_support().journal;
    let rows = journal.read_run(&run).expect("read_run");
    assert!(
        rows.iter().any(|r| matches!(
            r.event.transition(),
            ParticipantEvent::Quarantined { reason, .. } if is_panic_quarantine_reason(reason)
        )),
        "panic quarantine row must be in the run partition: {rows:?}"
    );
    // `read` is the union of the legacy partition and every run: nothing may exist outside the run.
    assert_eq!(
        journal.read(run.saga_id()).expect("read").len(),
        rows.len(),
        "no legacy-partition row"
    );
}
