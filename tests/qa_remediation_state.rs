//! U1 shared participant protocol: durable run admission, terminal retention,
//! run-scoped dedupe, archive compatibility and effect-dispatch defaults.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::future::Future;
use std::task::{Context, Poll, Waker};

use icanact_saga_choreography::SagaBoxFuture;
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, EffectDispatchError,
    EffectDispatchRequest, HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal,
    ParticipantAdmission, ParticipantEvent, ParticipantJournal, ParticipantTerminalKind, PeerId,
    RunDedupe, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport, SagaStateExt,
    SagaWorkflowParticipant, StepError, StepOutput,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
}

impl Actor {
    fn new(journal: &Journal, dedupe: &Dedupe) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
        }
    }
}

impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.saga
    }
}

fn stores() -> (Journal, Dedupe) {
    (
        FaultJournal::new(InMemoryJournal::new()),
        FaultDedupe::new(InMemoryDedupe::new()),
    )
}

fn ctx(saga_type: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(9),
        saga_type: saga_type.into(),
        step_name: "step".into(),
        correlation_id: 9,
        causation_id: 9,
        trace_id: 9,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

#[test]
fn terminal_run_stays_fenced_after_reconstruction_and_cache_eviction() {
    let (journal, dedupe) = stores();
    let run = ctx("order", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    assert!(
        actor
            .admit_participant_event_strict(&run)
            .unwrap()
            .is_admitted()
    );
    actor
        .retain_terminal_saga_strict(&run, ParticipantTerminalKind::Completed, "done")
        .unwrap();

    // Simulated restart: fresh in-memory caches over the same durable stores.
    let restarted = Actor::new(&journal, &dedupe);
    assert_eq!(
        restarted.admit_participant_event_strict(&run).unwrap(),
        ParticipantAdmission::TerminalReplay {
            outcome: ParticipantTerminalKind::Completed
        }
    );

    // Bounded in-memory latch evicted: durable authority still rejects.
    actor.terminal_sagas().clear();
    actor.terminal_saga_order().clear();
    actor.saga_support_mut().saga_run_started_at.clear();
    assert!(matches!(
        actor.admit_participant_event_strict(&run).unwrap(),
        ParticipantAdmission::TerminalReplay { .. }
    ));

    // Evidence is retained, not pruned.
    let entries = journal.read(run.saga_id).unwrap();
    assert!(entries.iter().any(|e| matches!(
        e.event,
        ParticipantEvent::ParticipantTerminalRecorded {
            saga_started_at_millis: 100,
            ..
        }
    )));
}

#[test]
fn later_ordinary_run_is_admitted_and_older_events_become_stale() {
    let (journal, dedupe) = stores();
    let old = ctx("order", 100);
    let new = ctx("order", 200);
    let mut actor = Actor::new(&journal, &dedupe);
    assert!(
        actor
            .admit_participant_event_strict(&old)
            .unwrap()
            .is_admitted()
    );
    actor
        .retain_terminal_saga_strict(&old, ParticipantTerminalKind::Failed, "failed")
        .unwrap();

    let restarted = Actor::new(&journal, &dedupe);
    assert!(
        restarted
            .admit_participant_event_strict(&new)
            .unwrap()
            .is_admitted()
    );
    // Old run replay: still terminal (identity-exact), and a never-terminal stale run is stale.
    assert!(matches!(
        restarted.admit_participant_event_strict(&old).unwrap(),
        ParticipantAdmission::TerminalReplay { .. }
    ));
    assert_eq!(
        restarted
            .admit_participant_event_strict(&ctx("order", 150))
            .unwrap(),
        ParticipantAdmission::StaleRun {
            latest_started_at_millis: 200
        }
    );
    // The new run is not poisoned by the old tombstone.
    assert!(
        restarted
            .admit_participant_event_strict(&new)
            .unwrap()
            .is_admitted()
    );
}

#[test]
fn quarantined_run_blocks_reuse_and_retains_all_evidence() {
    let (journal, dedupe) = stores();
    let run = ctx("order", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    actor.admit_participant_event_strict(&run).unwrap();
    assert_eq!(
        actor.check_run_dedupe_strict(&run, "step_started").unwrap(),
        RunDedupe::First
    );
    actor
        .retain_terminal_saga_strict(&run, ParticipantTerminalKind::Quarantined, "ambiguous")
        .unwrap();

    let restarted = Actor::new(&journal, &dedupe);
    let admission = restarted
        .admit_participant_event_strict(&ctx("order", 200))
        .unwrap();
    assert_eq!(
        admission,
        ParticipantAdmission::QuarantinedReuse {
            quarantined_started_at_millis: 100,
            reason: "ambiguous".into()
        }
    );
    // Same run is a terminal replay of the quarantine, never admitted.
    assert!(matches!(
        restarted.admit_participant_event_strict(&run).unwrap(),
        ParticipantAdmission::TerminalReplay {
            outcome: ParticipantTerminalKind::Quarantined
        }
    ));
    assert_eq!(journal.call_count(JournalOp::Prune), 0);
    assert_eq!(dedupe.prune_calls(), 0);
    assert_eq!(
        restarted
            .check_run_dedupe_strict(&run, "step_started")
            .unwrap(),
        RunDedupe::Duplicate,
        "dedupe evidence survives quarantine"
    );
}

#[test]
fn a_new_run_cannot_replace_an_unresolved_run() {
    let (journal, dedupe) = stores();
    let actor = Actor::new(&journal, &dedupe);
    let active = ctx("order", 100);
    assert!(
        actor
            .admit_participant_event_strict(&active)
            .unwrap()
            .is_admitted()
    );
    assert!(
        !actor
            .admit_participant_event_strict(&ctx("order", 200))
            .unwrap()
            .is_admitted()
    );
    assert!(
        actor
            .admit_participant_event_strict(&active)
            .unwrap()
            .is_admitted()
    );
    assert_eq!(journal.read(active.saga_id).unwrap().len(), 1);
}

#[test]
fn run_histories_stay_separate_by_saga_type_and_start() {
    let (journal, dedupe) = stores();
    let mut actor = Actor::new(&journal, &dedupe);
    let a = ctx("alpha", 100);
    let b = ctx("beta", 100);
    actor.admit_participant_event_strict(&a).unwrap();
    actor
        .retain_terminal_saga_strict(&a, ParticipantTerminalKind::Completed, "done")
        .unwrap();
    // Same id and start time, different saga type: a distinct run identity.
    assert!(
        actor
            .admit_participant_event_strict(&b)
            .unwrap()
            .is_admitted()
    );
}

#[test]
fn admission_and_terminal_retention_fail_closed_on_journal_errors() {
    let (journal, dedupe) = stores();
    let run = ctx("order", 100);
    let mut actor = Actor::new(&journal, &dedupe);

    journal.fail_once(JournalOp::Read, FaultTrigger::NthCall(1));
    assert!(actor.admit_participant_event_strict(&run).is_err());

    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantRunRecorded"),
    );
    assert!(actor.admit_participant_event_strict(&run).is_err());
    assert!(
        journal.read(run.saga_id).unwrap().is_empty(),
        "failed run write leaves no record"
    );

    assert!(
        actor
            .admit_participant_event_strict(&run)
            .unwrap()
            .is_admitted()
    );
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantTerminalRecorded"),
    );
    assert!(
        actor
            .retain_terminal_saga_strict(&run, ParticipantTerminalKind::Completed, "done")
            .is_err()
    );
    assert!(
        !actor.is_terminal_saga_latched(run.saga_id),
        "no in-memory terminal latch without a durable tombstone"
    );
    assert!(
        actor
            .admit_participant_event_strict(&run)
            .unwrap()
            .is_admitted()
    );
}

#[test]
fn run_dedupe_separates_duplicates_runs_and_errors() {
    let (journal, dedupe) = stores();
    let actor = Actor::new(&journal, &dedupe);
    let r1 = ctx("order", 100);
    let r2 = ctx("order", 200);
    assert_eq!(
        actor.check_run_dedupe_strict(&r1, "k").unwrap(),
        RunDedupe::First
    );
    assert_eq!(
        actor.check_run_dedupe_strict(&r1, "k").unwrap(),
        RunDedupe::Duplicate
    );
    assert_eq!(
        actor.check_run_dedupe_strict(&r2, "k").unwrap(),
        RunDedupe::First
    );

    dedupe.fail_check_and_mark(FaultTrigger::NthCall(4));
    assert!(actor.check_run_dedupe_strict(&r1, "other").is_err());
}

#[test]
fn legacy_archives_remain_readable_and_variant_layout_is_unchanged() {
    use icanact_saga_choreography::JournalEntry;
    let legacy = [
        ParticipantEvent::SagaRegistered {
            saga_type: "s".into(),
            step_name: "t".into(),
            registered_at_millis: 1,
        },
        ParticipantEvent::Quarantined {
            reason: "r".into(),
            quarantined_at_millis: 2,
        },
    ];
    for event in legacy {
        let entry = JournalEntry {
            sequence: 1,
            recorded_at_millis: 1,
            event,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&entry).unwrap();
        let back = rkyv::from_bytes::<JournalEntry, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(format!("{back:?}"), format!("{entry:?}"));
    }
    // Existing variants keep their archived discriminants; new ones are appended.
    let tag = |event: ParticipantEvent| {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).unwrap();
        let size = size_of::<<ParticipantEvent as rkyv::Archive>::Archived>();
        bytes[bytes.len() - size]
    };
    assert_eq!(
        tag(ParticipantEvent::SagaRegistered {
            saga_type: "s".into(),
            step_name: "t".into(),
            registered_at_millis: 1
        }),
        0
    );
    assert_eq!(
        tag(ParticipantEvent::Quarantined {
            reason: "r".into(),
            quarantined_at_millis: 2
        }),
        9
    );
    assert_eq!(
        tag(ParticipantEvent::ParticipantRunRecorded {
            saga_type: "s".into(),
            saga_started_at_millis: 1,
            recorded_at_millis: 1
        }),
        12
    );
    assert_eq!(
        tag(ParticipantEvent::ParticipantTerminalRecorded {
            saga_type: "s".into(),
            saga_started_at_millis: 1,
            outcome: ParticipantTerminalKind::Failed,
            reason: "x".into(),
            recorded_at_millis: 1
        }),
        13
    );
    assert_eq!(
        tag(ParticipantEvent::ParticipantReconciliationEvidence {
            context: ctx("s", 1),
            output: vec![1],
            compensation_data: vec![2],
            reason: "result write failed".into(),
            recorded_at_millis: 1,
        }),
        14
    );
}

#[test]
fn legacy_execution_history_without_identity_requires_reconciliation() {
    let (journal, dedupe) = stores();
    let run = ctx("order", 100);
    journal
        .append(
            run.saga_id,
            ParticipantEvent::StepTriggered {
                triggering_event: "saga_started".into(),
                triggered_at_millis: 1,
            },
        )
        .unwrap();
    let actor = Actor::new(&journal, &dedupe);
    assert!(
        !actor
            .admit_participant_event_strict(&run)
            .unwrap()
            .is_admitted(),
        "unidentified legacy execution evidence must not permit another effect"
    );
    assert_eq!(journal.read(run.saga_id).unwrap().len(), 1);
}

#[test]
fn registration_only_legacy_history_can_admit_a_first_run() {
    let (journal, dedupe) = stores();
    let run = ctx("order", 100);
    journal
        .append(
            run.saga_id,
            ParticipantEvent::SagaRegistered {
                saga_type: "order".into(),
                step_name: "step".into(),
                registered_at_millis: 1,
            },
        )
        .unwrap();
    let actor = Actor::new(&journal, &dedupe);
    assert!(
        actor
            .admit_participant_event_strict(&run)
            .unwrap()
            .is_admitted()
    );
}

// === effect dispatch defaults fail closed ===

struct Sync;
impl SagaParticipant for Sync {
    type Error = ();
    fn step_name(&self) -> &str {
        "s"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["order"]
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        unreachable!()
    }
    fn compensate_step(
        &mut self,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        unreachable!()
    }
}

struct Async;
impl AsyncSagaParticipant for Async {
    type Error = ();
    fn step_name(&self) -> &str {
        "s"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["order"]
    }
    fn execute_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        unreachable!()
    }
    fn compensate_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        unreachable!()
    }
}

struct Wf;
impl SagaWorkflowParticipant<()> for Wf {
    fn step_name(&self) -> &'static str {
        "s"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["order"]
    }
    fn execute_step(&self, _: &mut (), _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        unreachable!()
    }
    fn compensate_step(
        &self,
        _: &mut (),
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        unreachable!()
    }
}

#[test]
fn effect_dispatch_defaults_fail_closed_for_sync_async_and_workflow() {
    let context = ctx("order", 1);
    let request = EffectDispatchRequest {
        context: &context,
        effect: "notify",
        output: b"o",
        compensation_data: b"c",
    };
    let expected = Err(EffectDispatchError::Unsupported {
        effect: "notify".into(),
    });
    assert_eq!(Sync.dispatch_effect(&request), expected);
    assert_eq!(Wf.dispatch_effect(&mut (), &request), expected);
    let mut participant = Async;
    let mut fut = std::pin::pin!(participant.dispatch_effect(&request));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(result) = fut.as_mut().poll(&mut cx) else {
        panic!("default async dispatch should be immediately ready");
    };
    assert_eq!(result, expected);
}

#[test]
fn terminal_tombstone_is_terminal_for_startup_recovery() {
    use icanact_saga_choreography::{RecoveryDecision, RecoveryPolicy, classify_recovery};
    let (journal, dedupe) = stores();
    let run = ctx("order", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    actor.admit_participant_event_strict(&run).unwrap();
    actor
        .retain_terminal_saga_strict(&run, ParticipantTerminalKind::Completed, "done")
        .unwrap();
    let entries = journal.read(run.saga_id).unwrap();
    let decision = classify_recovery(&entries, u64::MAX / 2, RecoveryPolicy { stale_after_ms: 1 });
    assert!(matches!(decision, RecoveryDecision::TerminalNoAction));
}
