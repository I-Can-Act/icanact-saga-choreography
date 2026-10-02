//! R09 / ADR-0003: committed results and recovery emissions survive a crash at every publish
//! cut point: obligations embedded in rows are replayed at startup, and panic-recovery output is
//! re-derived until it is actually sent (no pre-marked replay key).
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use icanact_saga_choreography::durability::lmdb::{
    LmdbDedupe, LmdbJournal, open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::durability::{
    ActiveSagaExecution, ActiveSagaExecutionPhase, HasActiveSagaExecution,
    run_participant_phase_with_panic_quarantine,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, IngressOutcome,
    ParticipantEvent, ParticipantJournal, PeerId, RunIncarnation, RunKey, SagaChoreographyBus,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport, StepError,
    StepOutput, handle_saga_event_with_emit,
};
use support::{FaultJournal, FaultTrigger, JournalOp};

/// The run whose step panics.
const PANIC_ID: u64 = 200;

type Support<J> = SagaParticipantSupport<J, LmdbDedupe>;

struct Actor<J: ParticipantJournal> {
    saga: Support<J>,
    active: Option<ActiveSagaExecution>,
}

impl<J: ParticipantJournal> HasSagaParticipantSupport for Actor<J> {
    type Journal = J;
    type Dedupe = LmdbDedupe;
    fn saga_support(&self) -> &Support<J> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support<J> {
        &mut self.saga
    }
}

impl<J: ParticipantJournal> HasActiveSagaExecution for Actor<J> {
    fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
        &mut self.active
    }
}

impl<J: ParticipantJournal> SagaParticipant for Actor<J> {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA]
    }
    fn execute_step(&mut self, c: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        assert_ne!(c.saga_id.get(), PANIC_ID, "boom-in-business-code");
        Ok(StepOutput::Completed {
            output: b"ok".to_vec(),
            compensation_data: vec![],
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

fn open_actor<J: ParticipantJournal>(journal: J, base: &std::path::Path) -> Actor<J> {
    Actor {
        saga: SagaParticipantSupport::new(
            journal,
            LmdbDedupe::open(&base.join("dedupe")).expect("dedupe"),
        ),
        active: None,
    }
}

fn started(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: context.clone(),
        payload: vec![1],
    }
}

fn has_step_completed(events: &[SagaChoreographyEvent], run: &RunKey) -> bool {
    events.iter().any(|e| {
        matches!(e, SagaChoreographyEvent::StepCompleted { context: c, .. } if run.is_run_of(c))
    })
}

const SAGA: &str = "r09_outbox";
const STEP: &str = "reserve";

fn ctx(id: u64, started: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started,
        event_timestamp_millis: started,
    }
}

fn step_completed(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: context.clone(),
        output: b"ok".to_vec(),
        saga_input: vec![],
        compensation_available: false,
    }
}

fn reopen(base: &std::path::Path) -> Vec<SagaChoreographyEvent> {
    let mut support =
        open_lmdb_participant_support_for_saga_type(base, STEP, SAGA).expect("reopen must succeed");
    support.take_startup_recovery_events()
}

/// Real pipeline: the participant commits its result (with the embedded `StepCompleted`
/// obligation) and the process dies at each publish cut. The next open hands the obligation back.
#[test]
fn committed_result_survives_real_crash_at_each_publish_cut() {
    let now = SagaContext::now_millis();
    for (n, cut) in ["BeforeSend", "AfterSend"].into_iter().enumerate() {
        let temp = tempfile::tempdir().expect("tempdir");
        let context = ctx(100 + n as u64, now);
        let run = context.run_key();
        {
            let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal");
            let mut actor = open_actor(journal, temp.path());
            let mut emitted = Vec::new();
            let outcome =
                handle_saga_event_with_emit(&mut actor, started(&context), |e| emitted.push(e));
            assert!(
                matches!(outcome, IngressOutcome::Applied),
                "{cut}: {outcome:?}"
            );
            assert!(has_step_completed(&emitted, &run), "{cut}: {emitted:?}");
            if cut == "AfterSend" {
                // The send happened (a subscriber received it) and the process still died
                // before anything acknowledged it: the obligation must stay replayable.
                let bus = SagaChoreographyBus::new();
                let received = Arc::new(AtomicUsize::new(0));
                let counter = Arc::clone(&received);
                let _sub = bus.subscribe_saga_type_fn(SAGA, move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    true
                });
                for event in emitted {
                    bus.publish_strict(event).expect("send");
                }
                assert!(received.load(Ordering::SeqCst) >= 1);
            }
            // BeforeSend: `emitted` is dropped unsent.
        }
        let events = reopen(temp.path());
        assert!(
            has_step_completed(&events, &run),
            "{cut}: committed StepCompleted obligation lost across reopen: {events:?}"
        );
    }
}

/// Cut `BeforeResult`: the result commit itself fails (injected). No obligation was committed and
/// none is invented on reopen; the run is not completed.
#[test]
fn failed_result_commit_leaves_no_phantom_obligation() {
    let now = SagaContext::now_millis();
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(150, now);
    let run = context.run_key();
    {
        let journal =
            FaultJournal::new(LmdbJournal::open(&temp.path().join("journal")).expect("journal"));
        journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("StepExecutionCompleted"),
        );
        let mut actor = open_actor(journal, temp.path());
        let mut emitted = Vec::new();
        let outcome =
            handle_saga_event_with_emit(&mut actor, started(&context), |e| emitted.push(e));
        assert!(
            !matches!(outcome, IngressOutcome::Applied),
            "a failed result commit must not be acknowledged: {outcome:?}"
        );
        assert!(!has_step_completed(&emitted, &run), "{emitted:?}");
    }
    let events = reopen(temp.path());
    assert!(
        !has_step_completed(&events, &run),
        "an uncommitted result must not be replayed: {events:?}"
    );
}

/// Panic publish path: the panic quarantine is published through the attached bus, and because no
/// pre-mark or post-publish mark exists, every reopen re-derives it until the run is finalized.
#[test]
fn panic_quarantine_publish_path_leaves_the_emission_re_derivable() {
    let now = SagaContext::now_millis();
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(200, now);
    let run = context.run_key();
    let received = Arc::new(AtomicUsize::new(0));
    {
        let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal");
        let mut actor = open_actor(journal, temp.path());
        let bus = SagaChoreographyBus::new();
        let counter = Arc::clone(&received);
        let _sub = bus.subscribe_saga_type_fn(SAGA, move |event| {
            if matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            true
        });
        actor.saga.attach_bus(bus);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_participant_phase_with_panic_quarantine(
                &mut actor,
                &context,
                ActiveSagaExecutionPhase::StepExecution,
                |a| handle_saga_event_with_emit(a, started(&context), |_| {}),
            )
        }));
        assert!(caught.is_err(), "the panic is rethrown");
    }
    assert_eq!(
        received.load(Ordering::SeqCst),
        1,
        "the panic path must publish SagaQuarantined through the bus"
    );
    for attempt in 0..2 {
        let events = reopen(temp.path());
        assert!(
            events.iter().any(|e| matches!(
                e,
                SagaChoreographyEvent::SagaQuarantined { context: c, .. } if run.is_run_of(c)
            )),
            "open #{attempt}: SagaQuarantined must be re-derived (published or not)"
        );
    }
}

#[test]
fn obligations_beyond_the_replay_horizon_are_retained_not_replayed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let old = ctx(300, 1);
    let old_run = RunKey::new(SAGA, SagaId::new(300), RunIncarnation::new(1));
    {
        let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal");
        journal
            .commit_with_outbox(
                &old_run,
                ParticipantEvent::StepExecutionCompleted {
                    output: vec![],
                    compensation_data: vec![],
                    completed_at_millis: 1,
                },
                vec![step_completed(&old)],
            )
            .expect("commit old");
    }
    let events = reopen(temp.path());
    assert!(
        !events.iter().any(|e| matches!(
            e,
            SagaChoreographyEvent::StepCompleted { context: c, .. } if old_run.is_run_of(c)
        )),
        "obligations older than the replay cutoff must not be replayed"
    );
}
