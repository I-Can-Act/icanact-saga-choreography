//! R02 / ADR-0002: a completed step's undo ownership survives a restart that happens before any
//! compensation request is recorded; a required compensation request with no completed state is
//! never a silent `Applied`.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

// Fixture re-exports are shared by every test crate; this one uses a subset.
#[allow(unused_imports)]
mod support;

use icanact_saga_choreography::compensation_requested;
use icanact_saga_choreography::durability::lmdb::{
    LmdbDedupe, LmdbJournal, open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, PeerId,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport,
    SagaStateEntry, SagaStateExt, StepError, StepOutput, handle_saga_event_with_emit,
};
use support::EffectLedger;

const STEP: &str = "reserve";
const TYPE: &str = "r02_order";

type Support = SagaParticipantSupport<LmdbJournal, LmdbDedupe>;

struct Actor {
    saga: Support,
    ledger: EffectLedger,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;
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
    fn execute_step(&mut self, c: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.ledger.forward(&c.run_key().to_string());
        Ok(StepOutput::Completed {
            output: vec![1],
            compensation_data: vec![9],
        })
    }
    fn compensate_step(
        &mut self,
        c: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        assert_eq!(data, [9], "undo bytes must come from the durable journal");
        let _ = self.ledger.undo(&c.run_key().to_string());
        Ok(CompensationOutput::Completed)
    }
}

fn open(base: &std::path::Path, ledger: &EffectLedger) -> Actor {
    Actor {
        saga: open_lmdb_participant_support_for_saga_type(base, STEP, TYPE).expect("open"),
        ledger: ledger.clone(),
    }
}

fn ctx(id: u64) -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: TYPE.into(),
        step_name: STEP.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

fn feed(
    actor: &mut Actor,
    event: SagaChoreographyEvent,
) -> (
    icanact_saga_choreography::IngressOutcome,
    Vec<SagaChoreographyEvent>,
) {
    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(actor, event, |e| emitted.push(e));
    (outcome, emitted)
}

fn request(context: &SagaContext) -> SagaChoreographyEvent {
    compensation_requested(
        context.clone(),
        "downstream",
        "boom",
        vec![STEP.to_string()],
    )
}

#[test]
fn completed_effect_survives_restart_before_compensation_request() {
    let temp = tempfile::tempdir().expect("tempdir");
    let ledger = EffectLedger::new();
    let context = ctx(1);
    let run = context.run_key();
    {
        let mut actor = open(temp.path(), &ledger);
        let (_, emitted) = feed(
            &mut actor,
            SagaChoreographyEvent::SagaStarted {
                context: context.clone(),
                payload: vec![7],
            },
        );
        assert!(
            emitted
                .iter()
                .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. })),
            "{emitted:?}"
        );
        assert_eq!(ledger.forward_count(&run.to_string()), 1);
    }

    // Restart before any CompensationRequestRecorded exists.
    let mut actor = open(temp.path(), &ledger);
    assert!(
        matches!(
            actor.saga_states_ref().get(&run),
            Some(SagaStateEntry::Completed(_))
        ),
        "ordinary Completed state (with undo data) must be rehydrated on open"
    );
    let (_, emitted) = feed(&mut actor, request(&context));
    assert_eq!(
        ledger.undo_count(&run.to_string()),
        1,
        "exactly one undo after restart: {emitted:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "acknowledged rollback expected: {emitted:?}"
    );
}

// A required compensation request with no Completed state quarantines the run and returns
// `ReconciliationNeeded { MissingCompletedState }` (never a silent `Applied`).
#[test]
fn compensation_request_without_completed_state_is_never_silent() {
    use icanact_saga_choreography::{IngressOutcome, ReconciliationCause};
    let temp = tempfile::tempdir().expect("tempdir");
    let ledger = EffectLedger::new();
    let context = ctx(2);
    let run = context.run_key();
    let mut actor = open(temp.path(), &ledger);
    // This participant was never started for the run: it holds no Completed state.
    let (outcome, emitted) = feed(&mut actor, request(&context));
    assert!(
        matches!(
            &outcome,
            IngressOutcome::ReconciliationNeeded(r)
                if r.run == run && matches!(r.cause, ReconciliationCause::MissingCompletedState)
        ),
        "{outcome:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{emitted:?}"
    );
    assert_eq!(ledger.undo_count(&run.to_string()), 0);
}

#[test]
fn rerequest_after_settled_compensation_and_restart_is_not_a_reconciliation_case() {
    use icanact_saga_choreography::IngressOutcome;
    let temp = tempfile::tempdir().expect("tempdir");
    let ledger = EffectLedger::new();
    let context = ctx(3);
    let run = context.run_key();
    {
        let mut actor = open(temp.path(), &ledger);
        feed(
            &mut actor,
            SagaChoreographyEvent::SagaStarted {
                context: context.clone(),
                payload: vec![7],
            },
        );
        let (_, emitted) = feed(&mut actor, request(&context));
        assert!(
            emitted
                .iter()
                .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
            "{emitted:?}"
        );
    }
    let mut actor = open(temp.path(), &ledger);
    let (outcome, emitted) = feed(&mut actor, request(&context));
    // The dedupe mark answers `Duplicate`; either way it is never a reconciliation case.
    assert!(
        matches!(outcome, IngressOutcome::Applied | IngressOutcome::Duplicate),
        "{outcome:?}"
    );
    assert!(
        !emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{emitted:?}"
    );
    assert_eq!(ledger.undo_count(&run.to_string()), 1);
}
