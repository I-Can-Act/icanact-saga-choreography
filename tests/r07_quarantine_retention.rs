//! T07D (R07, ADR-0001 §2.5, owner decisions Q6): quarantined runs are never finalized or
//! forgotten; a failed dedupe prune after finalize is a typed `Finalize` failure.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use icanact_saga_choreography::{CommitStage, ParticipantJournal};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, DependencySpec, HasSagaParticipantSupport,
    InMemoryDedupe, InMemoryJournal, IngressOutcome, IngressRejection, PeerId, RunIncarnation,
    RunKey, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport,
    SagaStateExt, StepError, StepOutput, handle_saga_event_with_emit,
};
use support::{FaultDedupe, FaultTrigger};

const SAGA: &str = "r07_retention";
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

fn terminal(kind: &str) -> SagaChoreographyEvent {
    let context = ctx().next_step("saga".into());
    match kind {
        "completed" => SagaChoreographyEvent::SagaCompleted { context },
        _ => SagaChoreographyEvent::SagaFailed {
            context,
            reason: "loop-back".into(),
            failure: None,
        },
    }
}

fn actor() -> (Actor, FaultDedupe<InMemoryDedupe>) {
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let actor = Actor {
        saga: SagaParticipantSupport::new(InMemoryJournal::new(), dedupe.clone()),
        executed: 0,
    };
    (actor, dedupe)
}

#[test]
fn quarantine_preserves_evidence_and_partial_gc_is_recoverable() {
    let (mut actor, dedupe) = actor();
    let run = ctx().run_key();

    // Quarantine the run (dedupe failure on the first mark) and capture the emitted event.
    dedupe.fail_check_and_mark(FaultTrigger::NthCall(dedupe.mark_calls() + 1));
    let mut emitted = Vec::new();
    let outcome =
        handle_saga_event_with_emit(&mut actor, upstream_completed(), |e| emitted.push(e));
    assert!(matches!(outcome, IngressOutcome::Failed(_)), "{outcome:?}");
    dedupe.disarm();
    let rows_before = actor.saga_journal().read_run(&run).expect("read").len();
    assert!(rows_before > 0, "quarantine row is evidence");

    // Echo of the run's own SagaQuarantined, then a terminal loop-back.
    for e in emitted
        .into_iter()
        .filter(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. }))
    {
        let _ = handle_saga_event_with_emit(&mut actor, e, |_| {});
    }
    let _ = handle_saga_event_with_emit(&mut actor, terminal("failed"), |_| {});

    assert_eq!(
        actor.saga_journal().read_run(&run).expect("read").len(),
        rows_before,
        "quarantined run's journal rows must survive echo + terminal loop-back"
    );
    assert!(
        actor
            .saga_journal()
            .run_tombstones(SAGA, run.saga_id())
            .expect("tombstones")
            .is_empty(),
        "quarantined runs are never tombstoned"
    );

    // Even after the bounded terminal latch evicts the run, admission rejects it.
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

#[test]
fn failed_dedupe_prune_after_finalize_is_a_typed_finalize_failure() {
    let (mut actor, dedupe) = actor();
    let run = ctx().run_key();
    let outcome = handle_saga_event_with_emit(&mut actor, upstream_completed(), |_| {});
    assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");

    dedupe.fail_prune(FaultTrigger::NthCall(dedupe.prune_calls() + 1));
    let outcome = handle_saga_event_with_emit(&mut actor, terminal("completed"), |_| {});
    assert!(
        matches!(&outcome, IngressOutcome::Failed(f) if f.stage == CommitStage::Finalize),
        "{outcome:?}"
    );
    // The tombstone was written, so leftover marks are harmless and the run stays terminal.
    assert_eq!(
        actor
            .saga_journal()
            .run_tombstones(SAGA, run.saga_id())
            .expect("tombstones")
            .len(),
        1
    );
    assert!(
        actor
            .saga_journal()
            .read_run(&run)
            .expect("read")
            .is_empty()
    );
}

mod restart {
    use super::*;
    use icanact_saga_choreography::ParticipantEvent;
    use icanact_saga_choreography::SagaStateEntry;
    use icanact_saga_choreography::durability::lmdb::{
        LmdbDedupe, LmdbJournal, open_lmdb_participant_support_for_saga_type,
    };

    struct LmdbActor {
        saga: SagaParticipantSupport<LmdbJournal, LmdbDedupe>,
        executed: usize,
    }

    impl HasSagaParticipantSupport for LmdbActor {
        type Journal = LmdbJournal;
        type Dedupe = LmdbDedupe;
        fn saga_support(&self) -> &SagaParticipantSupport<LmdbJournal, LmdbDedupe> {
            &self.saga
        }
        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<LmdbJournal, LmdbDedupe> {
            &mut self.saga
        }
    }

    impl SagaParticipant for LmdbActor {
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

    fn open(base: &std::path::Path) -> LmdbActor {
        LmdbActor {
            saga: open_lmdb_participant_support_for_saga_type(base, STEP, SAGA).expect("open"),
            executed: 0,
        }
    }

    #[test]
    fn quarantined_run_stays_fenced_after_restart() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx().run_key();
        {
            let mut actor = open(temp.path());
            let outcome = handle_saga_event_with_emit(&mut actor, upstream_completed(), |_| {});
            assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");
            actor
                .record_event_run_strict(
                    &run,
                    ParticipantEvent::Quarantined {
                        reason: "evidence".into(),
                        quarantined_at_millis: 5,
                    },
                )
                .expect("quarantine row");
        }

        let mut actor = open(temp.path());
        assert!(
            matches!(
                actor.saga_states_ref().get(&run),
                Some(SagaStateEntry::Quarantined(_))
            ),
            "the journal's last Quarantined row must rehydrate a Quarantined entry"
        );
        let outcome = handle_saga_event_with_emit(&mut actor, upstream_completed(), |_| {});
        assert_eq!(
            actor.executed, 0,
            "no business effect for a quarantined run"
        );
        assert!(
            matches!(
                outcome,
                IngressOutcome::Rejected(IngressRejection::TerminalRun)
            ),
            "{outcome:?}"
        );
        assert!(
            !actor
                .saga_journal()
                .read_run(&run)
                .expect("read")
                .is_empty(),
            "quarantine evidence is retained"
        );
    }
}
