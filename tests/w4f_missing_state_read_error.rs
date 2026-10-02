//! W4-review R4: a compensation request with no completed state stays fail-closed (quarantine).
//! A journal read error while checking whether the step was already settled is logged at `error!`
//! with the RunKey and treated as "not settled" — never swallowed.
#![cfg(feature = "test-harness")]

#[allow(unused_imports)]
mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};

use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, InMemoryDedupe,
    InMemoryJournal, IngressOutcome, PeerId, ReconciliationCause, SagaChoreographyEvent,
    SagaContext, SagaId, SagaParticipant, SagaParticipantSupport, StepError, StepOutput,
    compensation_requested, handle_saga_event_with_emit,
};
use support::{FaultJournal, FaultTrigger, JournalOp};

const STEP: &str = "reserve";
const TYPE: &str = "w4f_r4";

type Journal = FaultJournal<InMemoryJournal>;
type Support = SagaParticipantSupport<Journal, InMemoryDedupe>;

struct Actor {
    saga: Support,
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

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
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

#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn journal_read_error_in_missing_state_guard_is_logged_and_fails_closed() {
    let now = SagaContext::now_millis();
    let context = SagaContext {
        saga_id: SagaId::new(41),
        saga_type: TYPE.into(),
        step_name: STEP.into(),
        correlation_id: 41,
        causation_id: 41,
        trace_id: 41,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    };
    let run = context.run_key();
    let request = || {
        compensation_requested(
            context.clone(),
            "downstream",
            "boom",
            vec![STEP.to_string()],
        )
    };
    let new_actor = |journal: &Journal| Actor {
        saga: SagaParticipantSupport::new(journal.clone(), InMemoryDedupe::new()),
    };
    // Dry run: the settled-check is the last journal read of a request that finds no state.
    let dry = FaultJournal::new(InMemoryJournal::new());
    let _ = handle_saga_event_with_emit(&mut new_actor(&dry), request(), |_| {});
    let last_read = dry.call_count(JournalOp::Read);
    assert!(last_read > 0);

    let journal = FaultJournal::new(InMemoryJournal::new());
    let mut actor = new_actor(&journal);
    journal.fail_once(JournalOp::Read, FaultTrigger::NthCall(last_read));

    let logs = LogBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer({
            let logs = logs.clone();
            move || logs.clone()
        })
        .with_ansi(false)
        .finish();
    let mut emitted = Vec::new();
    let outcome = tracing::subscriber::with_default(subscriber, || {
        handle_saga_event_with_emit(&mut actor, request(), |e| emitted.push(e))
    });

    assert!(
        matches!(
            &outcome,
            IngressOutcome::ReconciliationNeeded(r)
                if r.run == run && matches!(r.cause, ReconciliationCause::MissingCompletedState)
        ),
        "a read error must fail closed: {outcome:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{emitted:?}"
    );
    let text = String::from_utf8(logs.0.lock().unwrap_or_else(|e| e.into_inner()).clone())
        .expect("utf8 logs");
    assert!(
        text.contains("ERROR")
            && text.contains("saga_compensation_settled_check_read_failed")
            && text.contains(&run.to_string()),
        "journal read error must be logged at error with the RunKey: {text}"
    );
}
