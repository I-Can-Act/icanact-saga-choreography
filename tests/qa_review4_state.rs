//! Review4 state foundation: compensation-requiring forward failures keep their
//! obligation without pending accepted work, and the exact-run derived
//! `forward_definitively_rejected` marker.
use icanact_saga_choreography::*;

type Journal = InMemoryJournal;

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
fn actor() -> Actor {
    Actor(SagaParticipantSupport::new(
        InMemoryJournal::new(),
        InMemoryDedupe::new(),
    ))
}
fn ctx_of(id: u64, ty: &str, start: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: ty.into(),
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
fn ctx(start: u64) -> SagaContext {
    ctx_of(1701, "review4-state", start)
}
fn append(a: &Actor, c: &SagaContext, event: ParticipantEvent) {
    a.record_event_strict(c.saga_id, event).unwrap();
}
fn accept(a: &Actor, c: &SagaContext, compensation_data: Vec<u8>) {
    a.admit_participant_event_strict(c).unwrap();
    append(
        a,
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
fn started(a: &Actor, c: &SagaContext) {
    append(
        a,
        c,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 105,
        },
    );
}
fn fail(a: &Actor, c: &SagaContext, requires_compensation: bool) {
    append(
        a,
        c,
        ParticipantEvent::StepExecutionFailed {
            error: "remote rejected".into(),
            requires_compensation,
            failed_at_millis: 110,
        },
    );
}
fn result(a: &Actor, c: &SagaContext) {
    append(
        a,
        c,
        ParticipantEvent::StepExecutionCompleted {
            output: vec![7],
            compensation_data: vec![42],
            completed_at_millis: 120,
        },
    );
}
fn request(a: &Actor, c: &SagaContext) {
    append(
        a,
        c,
        ParticipantEvent::CompensationRequestRecorded {
            context: c.clone(),
            failed_step: "other".into(),
            reason: "rollback".into(),
            failure: SagaFailureDetails {
                step_name: "other".into(),
                participant_id: "other".into(),
                error_code: None,
                error_message: "boom".into(),
                at_millis: 108,
            },
            steps_to_compensate: vec!["remote".into()],
            requested_at_millis: 109,
        },
    );
}
fn undo_started(a: &Actor, c: &SagaContext) {
    append(
        a,
        c,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: 130,
        },
    );
}
fn undo_completed(a: &Actor, c: &SagaContext) {
    append(
        a,
        c,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: 140,
        },
    );
}
fn undo_failed(a: &Actor, c: &SagaContext) {
    append(
        a,
        c,
        ParticipantEvent::CompensationFailed {
            error: "undo refused".into(),
            is_ambiguous: false,
            failed_at_millis: 131,
        },
    );
}
fn settle(a: &mut Actor, c: &SagaContext) -> ParticipantTerminalKind {
    a.retain_terminal_saga_strict(c, ParticipantTerminalKind::Failed, "SagaFailed")
        .unwrap();
    a.terminal_run_outcome_strict(c).unwrap().unwrap()
}
fn ev(a: &Actor, c: &SagaContext) -> ParticipantRunEvidence {
    a.participant_run_evidence_strict(c).unwrap()
}

// ---- Behavioral RED on existing behavior: true failure without pending accepted work.

#[test]
fn generic_forward_failure_requiring_compensation_retains_obligation_without_accepted_work() {
    let (mut a, c) = (actor(), ctx(100));
    started(&a, &c);
    fail(&a, &c, true);
    assert!(ev(&a, &c).failure_requires_quarantine());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
}

#[test]
fn true_failure_after_prior_rejection_or_repeated_true_failure_keeps_obligation() {
    let (mut a, c) = (actor(), ctx(100));
    accept(&a, &c, vec![]);
    fail(&a, &c, false);
    fail(&a, &c, true);
    assert!(ev(&a, &c).failure_requires_quarantine());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);

    let (mut a, c) = (actor(), ctx(100));
    accept(&a, &c, vec![42]);
    fail(&a, &c, true);
    fail(&a, &c, true);
    assert!(ev(&a, &c).failure_requires_quarantine());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Quarantined);
}

#[test]
fn true_failure_obligation_is_closed_only_by_completed_undo() {
    let (mut a, c) = (actor(), ctx(100));
    started(&a, &c);
    fail(&a, &c, true);
    undo_started(&a, &c);
    undo_completed(&a, &c);
    assert!(!ev(&a, &c).failure_requires_quarantine());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
}

// ---- New marker (characterization of the new API; not a RED).

#[test]
fn marker_is_false_without_authoritative_rejection_evidence() {
    let c = ctx(100);
    let a = actor();
    assert!(!ev(&a, &c).forward_definitively_rejected, "empty");

    // dependency-only / idle history
    let a = actor();
    a.record_dependency_completion_strict(&c).unwrap();
    assert!(!ev(&a, &c).forward_definitively_rejected, "dependency only");

    // accepted, no failure
    let a = actor();
    accept(&a, &c, vec![]);
    assert!(!ev(&a, &c).forward_definitively_rejected, "accepted only");

    // failure marker with no accepted metadata
    let a = actor();
    fail(&a, &c, false);
    assert!(
        !ev(&a, &c).forward_definitively_rejected,
        "missing accepted"
    );

    // SagaFailed terminal record alone
    let mut a = actor();
    settle(&mut a, &c);
    assert!(!ev(&a, &c).forward_definitively_rejected, "terminal only");

    // owned request alone
    let a = actor();
    request(&a, &c);
    assert!(!ev(&a, &c).forward_definitively_rejected, "request only");
}

#[test]
fn marker_is_true_for_authoritative_rejection_and_request_alone_does_not_erase_it() {
    let c = ctx(100);
    for bytes in [vec![], vec![42]] {
        let a = actor();
        accept(&a, &c, bytes.clone());
        fail(&a, &c, false);
        assert!(ev(&a, &c).forward_definitively_rejected, "{bytes:?}");
    }
    // request before and after the rejection (owned request alone)
    let a = actor();
    request(&a, &c);
    accept(&a, &c, vec![42]);
    started(&a, &c);
    fail(&a, &c, false);
    request(&a, &c);
    let e = ev(&a, &c);
    assert!(e.forward_definitively_rejected);
    assert!(
        e.undo_required && e.failure_requires_quarantine(),
        "obligation stays"
    );
}

#[test]
fn marker_is_cleared_by_later_real_evidence() {
    let c = ctx(100);
    let rejected = || {
        let a = actor();
        accept(&a, &c, vec![42]);
        fail(&a, &c, false);
        assert!(ev(&a, &c).forward_definitively_rejected);
        a
    };
    let a = rejected();
    result(&a, &c);
    assert!(!ev(&a, &c).forward_definitively_rejected, "result");
    let a = rejected();
    append(
        &a,
        &c,
        ParticipantEvent::ParticipantReconciliationEvidence {
            context: c.clone(),
            output: vec![1],
            compensation_data: vec![2],
            reason: "late".into(),
            recorded_at_millis: 150,
        },
    );
    assert!(!ev(&a, &c).forward_definitively_rejected, "reconciliation");
    let a = rejected();
    append(
        &a,
        &c,
        ParticipantEvent::Quarantined {
            reason: "q".into(),
            quarantined_at_millis: 160,
        },
    );
    assert!(!ev(&a, &c).forward_definitively_rejected, "quarantine");
    let a = rejected();
    undo_started(&a, &c);
    undo_failed(&a, &c);
    assert!(!ev(&a, &c).forward_definitively_rejected, "undo failure");
    let a = rejected();
    started(&a, &c);
    assert!(!ev(&a, &c).forward_definitively_rejected, "new intent");
    let a = rejected();
    accept(&a, &c, vec![]);
    assert!(!ev(&a, &c).forward_definitively_rejected, "new acceptance");
    let a = rejected();
    fail(&a, &c, true);
    assert!(!ev(&a, &c).forward_definitively_rejected, "true failure");
    assert!(ev(&a, &c).failure_requires_quarantine());
}

#[test]
fn a_later_false_failure_never_erases_prior_real_evidence() {
    let c = ctx(100);
    // actual result
    let a = actor();
    accept(&a, &c, vec![42]);
    result(&a, &c);
    fail(&a, &c, false);
    assert!(!ev(&a, &c).forward_definitively_rejected);
    // earlier compensating failure, before physical undo
    let a = actor();
    accept(&a, &c, vec![42]);
    fail(&a, &c, true);
    accept(&a, &c, vec![42]);
    fail(&a, &c, false);
    assert!(!ev(&a, &c).forward_definitively_rejected);
    assert!(ev(&a, &c).failure_requires_quarantine());
    // open/failed undo
    let a = actor();
    accept(&a, &c, vec![42]);
    undo_started(&a, &c);
    undo_failed(&a, &c);
    accept(&a, &c, vec![42]);
    fail(&a, &c, false);
    assert!(!ev(&a, &c).forward_definitively_rejected);
    assert!(ev(&a, &c).failure_requires_quarantine());
}

#[test]
fn marker_is_scoped_to_the_exact_run() {
    let old = ctx(100);
    let new = ctx(200);
    let other_type = ctx_of(1701, "other-type", 100);
    let a = actor();
    accept(&a, &old, vec![]);
    fail(&a, &old, false);
    assert!(ev(&a, &old).forward_definitively_rejected);
    assert!(!ev(&a, &new).forward_definitively_rejected, "start differs");
    assert!(
        !ev(&a, &other_type).forward_definitively_rejected,
        "type differs"
    );
}

// Parent characterization of the consumer's exact owned-request sequence.
#[test]
fn requested_then_rejected_retains_potential_bytes_until_strict_undo_completion() {
    let (mut a, c) = (actor(), ctx(100));
    accept(&a, &c, vec![42]);
    started(&a, &c);
    request(&a, &c);
    fail(&a, &c, false);
    let e = ev(&a, &c);
    assert!(e.forward_definitively_rejected);
    assert!(e.undo_required && !e.undo_completed && e.failure_requires_quarantine());
    assert_eq!(e.compensation_data, vec![42]);

    // A recorded no-op intent is not an acknowledgement; failed completion
    // persistence must leave the request conservative rather than grant work.
    undo_started(&a, &c);
    let e = ev(&a, &c);
    assert!(!e.forward_definitively_rejected);
    assert!(e.undo_intent_open && e.failure_requires_quarantine());
    undo_completed(&a, &c);
    assert!(!ev(&a, &c).failure_requires_quarantine());
    assert_eq!(settle(&mut a, &c), ParticipantTerminalKind::Failed);
}

#[test]
fn a_durable_quarantine_tombstone_invalidates_the_no_effect_marker() {
    let (mut a, c) = (actor(), ctx(100));
    accept(&a, &c, vec![42]);
    fail(&a, &c, false);
    assert!(ev(&a, &c).forward_definitively_rejected);
    a.retain_terminal_saga_strict(&c, ParticipantTerminalKind::Quarantined, "late uncertainty")
        .unwrap();
    let e = ev(&a, &c);
    assert!(!e.forward_definitively_rejected);
    assert!(e.quarantined && e.failure_requires_quarantine());
}

#[test]
fn confirmed_late_effect_invalidates_rejection_without_losing_result_bytes() {
    let (a, c) = (actor(), ctx(100));
    accept(&a, &c, vec![1]);
    fail(&a, &c, false);
    request(&a, &c);
    let outcome = ParticipantForwardOutcome {
        context: c.clone(),
        output: vec![9],
        saga_input: vec![8],
        compensation_data: vec![42],
        effect: Some("durable-handoff".into()),
        receipt: Some("receipt".into()),
        recorded_at_millis: 120,
    };
    a.record_forward_outcome_strict(&outcome).unwrap();
    fail(&a, &c, false);
    let e = ev(&a, &c);
    assert!(!e.forward_definitively_rejected);
    assert!(e.forward_result_recorded && e.forward_outcome.is_some());
    assert_eq!(e.output, vec![9]);
    assert_eq!(e.compensation_data, vec![42]);
    assert!(e.failure_requires_quarantine());
}
