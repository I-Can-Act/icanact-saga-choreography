//! Re-challenge shared evidence contract: authoritative accepted failure versus
//! actual effects, and the durable dependency-observation protocol.
#[allow(unused_imports)]
mod support;

use icanact_saga_choreography::*;
use support::{FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;

struct Actor(SagaParticipantSupport<Journal, InMemoryDedupe>);
impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, InMemoryDedupe> {
        &self.0
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, InMemoryDedupe> {
        &mut self.0
    }
}
fn actor() -> (Actor, Journal) {
    let journal = FaultJournal::new(InMemoryJournal::new());
    (
        Actor(SagaParticipantSupport::new(
            journal.clone(),
            InMemoryDedupe::new(),
        )),
        journal,
    )
}
fn ctx(start: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(1601),
        saga_type: "rechallenge-state".into(),
        step_name: "remote".into(),
        correlation_id: 1,
        causation_id: 1,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: start,
        event_timestamp_millis: start,
    }
}
fn append(actor: &Actor, c: &SagaContext, event: ParticipantEvent) {
    actor.record_event_strict(c.saga_id, event).unwrap();
}
fn accept(actor: &Actor, c: &SagaContext, compensation_data: Vec<u8>) {
    actor.admit_participant_event_strict(c).unwrap();
    append(
        actor,
        c,
        ParticipantEvent::AcceptedStepRecorded {
            context: c.clone(),
            participant_id: "remote".into(),
            execution_id: StepExecutionId::new("exec-1"),
            idle_timeout_millis: 1_000,
            hard_timeout_millis: 5_000,
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
            saga_input: vec![1],
            compensation_data,
            accepted_at_millis: 101,
            deadline_at_millis: 1_101,
            hard_deadline_at_millis: 5_101,
        },
    );
}
fn fail(actor: &Actor, c: &SagaContext, requires_compensation: bool) {
    append(
        actor,
        c,
        ParticipantEvent::StepExecutionFailed {
            error: "remote rejected".into(),
            requires_compensation,
            failed_at_millis: 110,
        },
    );
}
fn result(actor: &Actor, c: &SagaContext) {
    append(
        actor,
        c,
        ParticipantEvent::StepExecutionCompleted {
            output: vec![7],
            compensation_data: vec![42],
            completed_at_millis: 120,
        },
    );
}
fn settle(actor: &mut Actor, c: &SagaContext) -> ParticipantTerminalKind {
    actor
        .retain_terminal_saga_strict(c, ParticipantTerminalKind::Failed, "SagaFailed")
        .unwrap();
    actor.terminal_run_outcome_strict(c).unwrap().unwrap()
}

#[test]
fn authoritative_definite_rejection_closes_accepted_work_with_or_without_potential_undo_bytes() {
    for bytes in [vec![], vec![42]] {
        let (mut a, _) = actor();
        let c = ctx(100);
        accept(&a, &c, bytes.clone());
        fail(&a, &c, false);
        let e = a.participant_run_evidence_strict(&c).unwrap();
        assert!(!e.accepted_forward_pending, "bytes {bytes:?}");
        assert!(!e.failure_requires_quarantine(), "bytes {bytes:?}");
        assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
    }
}

#[test]
fn unresolved_accepted_work_is_not_closed_by_an_unrelated_saga_failure() {
    for bytes in [vec![], vec![42]] {
        let (mut a, _) = actor();
        let c = ctx(100);
        accept(&a, &c, bytes);
        assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
    }
}

#[test]
fn compensation_requiring_accepted_failure_retains_the_obligation_until_undo() {
    let (mut a, _) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![42]);
    fail(&a, &c, true);
    assert!(
        a.participant_run_evidence_strict(&c)
            .unwrap()
            .failure_requires_quarantine()
    );
    append(
        &a,
        &c,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: 130,
        },
    );
    append(
        &a,
        &c,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: 140,
        },
    );
    let e = a.participant_run_evidence_strict(&c).unwrap();
    assert!(!e.failure_requires_quarantine());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
}

#[test]
fn real_result_or_failed_undo_survives_a_later_definite_failure_marker() {
    // result first, then the failure marker
    let (mut a, _) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![42]);
    result(&a, &c);
    fail(&a, &c, false);
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
    // failed compensation after definite failure
    let (mut a, _) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![42]);
    fail(&a, &c, false);
    append(
        &a,
        &c,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: 130,
        },
    );
    append(
        &a,
        &c,
        ParticipantEvent::CompensationFailed {
            error: "undo refused".into(),
            is_ambiguous: true,
            failed_at_millis: 131,
        },
    );
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
}

#[test]
fn late_result_after_definite_failure_escalates_with_original_run_evidence() {
    let (mut a, _) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![]);
    fail(&a, &c, false);
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
    result(&a, &c);
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
    let e = a.participant_run_evidence_strict(&c).unwrap();
    assert_eq!(e.output, vec![7]);
    assert_eq!(e.compensation_data, vec![42]);
}

#[test]
fn definite_failure_classification_is_run_scoped_and_survives_cold_reconstruction() {
    let (a, journal) = actor();
    let old = ctx(100);
    accept(&a, &old, vec![42]);
    fail(&a, &old, false);
    let restarted = Actor(SagaParticipantSupport::new(journal, InMemoryDedupe::new()));
    assert!(
        !restarted
            .participant_run_evidence_strict(&old)
            .unwrap()
            .failure_requires_quarantine()
    );
    // the failure marker belongs to the old run only
    let mut newer = ctx(200);
    assert!(
        !restarted
            .participant_run_evidence_strict(&newer)
            .unwrap()
            .failure_requires_quarantine()
    );
    newer.saga_type = "other".into();
    assert!(
        !restarted
            .participant_run_evidence_strict(&newer)
            .unwrap()
            .failure_requires_quarantine()
    );
}

#[test]
fn storage_faults_around_authoritative_failure_are_visible_and_never_close_work() {
    let (mut a, journal) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![42]);
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionFailed"),
    );
    assert!(
        a.record_event_strict(
            c.saga_id,
            ParticipantEvent::StepExecutionFailed {
                error: "x".into(),
                requires_compensation: false,
                failed_at_millis: 110,
            },
        )
        .is_err()
    );
    // failed append left the obligation open
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
    // read error while classifying is an error, not "safe"
    let (a, journal) = actor();
    accept(&a, &c, vec![]);
    fail(&a, &c, false);
    journal.fail_once(
        JournalOp::Read,
        FaultTrigger::NthCall(journal.call_count(JournalOp::Read) + 1),
    );
    assert!(a.participant_run_evidence_strict(&c).is_err());
}

#[test]
fn a_definite_failure_cannot_forget_an_already_requested_undo() {
    let (mut a, _) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![42]);
    append(
        &a,
        &c,
        ParticipantEvent::CompensationRequestRecorded {
            context: c.clone(),
            failed_step: "other".into(),
            reason: "rollback".into(),
            failure: SagaFailureDetails {
                step_name: "other".into(),
                participant_id: "other".into(),
                error_code: None,
                error_message: "rollback".into(),
                at_millis: 109,
            },
            steps_to_compensate: vec!["remote".into()],
            requested_at_millis: 109,
        },
    );
    fail(&a, &c, false);
    let e = a.participant_run_evidence_strict(&c).unwrap();
    assert_eq!(
        e.compensation_data,
        vec![42],
        "a recorded undo request still owns its bytes"
    );
    assert!(
        e.failure_requires_quarantine(),
        "a requested undo needs an authoritative acknowledgement"
    );
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
}

#[test]
fn a_later_failure_marker_cannot_downgrade_a_compensating_accepted_failure() {
    let (mut a, _) = actor();
    let c = ctx(100);
    accept(&a, &c, vec![42]);
    fail(&a, &c, true);
    fail(&a, &c, false);
    let e = a.participant_run_evidence_strict(&c).unwrap();
    assert_eq!(
        e.compensation_data,
        vec![42],
        "earlier owned compensation cannot be forgotten"
    );
    assert!(e.failure_requires_quarantine());
    append(
        &a,
        &c,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: 140,
        },
    );
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
}

fn tag_of(event: &ParticipantEvent) -> u8 {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(event).unwrap();
    let size = size_of::<<ParticipantEvent as rkyv::Archive>::Archived>();
    bytes[bytes.len() - size]
}

#[test]
fn existing_archive_tags_zero_to_fifteen_keep_their_layout() {
    let c = ctx(100);
    let outcome = ParticipantForwardOutcome {
        context: c.clone(),
        output: vec![],
        saga_input: vec![],
        compensation_data: vec![],
        effect: None,
        receipt: None,
        recorded_at_millis: 1,
    };
    let events = [
        ParticipantEvent::SagaRegistered {
            saga_type: "s".into(),
            step_name: "t".into(),
            registered_at_millis: 1,
        },
        ParticipantEvent::StepTriggered {
            triggering_event: "e".into(),
            triggered_at_millis: 1,
        },
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 1,
        },
        ParticipantEvent::StepExecutionCompleted {
            output: vec![],
            compensation_data: vec![],
            completed_at_millis: 1,
        },
        ParticipantEvent::StepExecutionFailed {
            error: "e".into(),
            requires_compensation: false,
            failed_at_millis: 1,
        },
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: 1,
        },
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: 1,
        },
        ParticipantEvent::CompensationFailed {
            error: "e".into(),
            is_ambiguous: false,
            failed_at_millis: 1,
        },
    ];
    for (tag, event) in events.iter().enumerate() {
        assert_eq!(tag_of(event) as usize, tag);
    }
    assert_eq!(
        tag_of(&ParticipantEvent::Quarantined {
            reason: "r".into(),
            quarantined_at_millis: 1
        }),
        9
    );
    assert_eq!(
        tag_of(&ParticipantEvent::ParticipantRunRecorded {
            saga_type: "s".into(),
            saga_started_at_millis: 1,
            recorded_at_millis: 1
        }),
        12
    );
    assert_eq!(
        tag_of(&ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome }),
        15
    );
}

fn dep(start: u64, step: &str) -> SagaContext {
    let mut c = ctx(start);
    c.step_name = step.into();
    c
}

#[test]
fn dependency_completion_appends_archive_tag_16_and_roundtrips() {
    let event = ParticipantEvent::ParticipantDependencyCompletedRecorded {
        context: dep(100, "reserve"),
        recorded_at_millis: 77,
    };
    assert_eq!(tag_of(&event), 16);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).unwrap();
    match rkyv::from_bytes::<ParticipantEvent, rkyv::rancor::Error>(&bytes).unwrap() {
        ParticipantEvent::ParticipantDependencyCompletedRecorded {
            context,
            recorded_at_millis,
        } => {
            assert_eq!(&*context.step_name, "reserve");
            assert_eq!(context.saga_started_at_millis, 100);
            assert_eq!(recorded_at_millis, 77);
        }
        _ => unreachable!(),
    }
}

#[test]
fn dependency_completions_are_exact_run_scoped_and_survive_cold_reconstruction() {
    let (a, journal) = actor();
    let c = dep(100, "reserve");
    a.record_dependency_completion_strict(&c).unwrap();
    a.record_dependency_completion_strict(&c).unwrap(); // duplicate delivery
    a.record_dependency_completion_strict(&dep(100, "pay"))
        .unwrap();
    a.record_dependency_completion_strict(&dep(200, "ship"))
        .unwrap(); // newer run
    let mut other_type = dep(100, "foreign");
    other_type.saga_type = "other".into();
    a.record_dependency_completion_strict(&other_type).unwrap();
    let cold = Actor(SagaParticipantSupport::new(journal, InMemoryDedupe::new()));
    let got = cold.completed_dependency_steps_strict(&c).unwrap();
    assert_eq!(got.len(), 2);
    assert!(got.contains("reserve") && got.contains("pay"));
    let got = cold
        .completed_dependency_steps_strict(&dep(200, "x"))
        .unwrap();
    assert_eq!(got.len(), 1);
    assert!(got.contains("ship"));
    assert!(
        cold.completed_dependency_steps_strict(&dep(300, "x"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn dependency_only_history_is_idle_not_execution_uncertainty() {
    let (mut a, _) = actor();
    let c = dep(100, "reserve");
    a.record_dependency_completion_strict(&c).unwrap();
    // dependency receipts before admission are not legacy execution history
    assert!(a.admit_participant_event_strict(&c).unwrap().is_admitted());
    a.record_dependency_completion_strict(&dep(100, "pay"))
        .unwrap();
    assert!(!a.forward_execution_recorded_strict(&c).unwrap());
    let e = a.participant_run_evidence_strict(&c).unwrap();
    assert!(!e.failure_requires_quarantine());
    assert!(!e.forward_intent_open && !e.accepted_forward_pending && !e.forward_result_recorded);
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
}

#[test]
fn dependency_receipts_do_not_hide_or_create_effect_evidence() {
    let (mut a, _) = actor();
    let c = ctx(100);
    a.admit_participant_event_strict(&c).unwrap();
    a.record_dependency_completion_strict(&dep(100, "reserve"))
        .unwrap();
    append(
        &a,
        &c,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 101,
        },
    );
    a.record_dependency_completion_strict(&dep(100, "pay"))
        .unwrap();
    assert!(a.forward_execution_recorded_strict(&c).unwrap());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
}

#[test]
fn dependency_storage_errors_are_visible() {
    let (a, journal) = actor();
    let c = dep(100, "reserve");
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantDependencyCompletedRecorded"),
    );
    assert!(a.record_dependency_completion_strict(&c).is_err());
    assert!(a.completed_dependency_steps_strict(&c).unwrap().is_empty());
    a.record_dependency_completion_strict(&c).unwrap();
    journal.fail_once(
        JournalOp::Read,
        FaultTrigger::NthCall(journal.call_count(JournalOp::Read) + 1),
    );
    assert!(a.completed_dependency_steps_strict(&c).is_err());
}
