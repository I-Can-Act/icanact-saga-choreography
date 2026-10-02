//! Parent-owned shared evidence contract for the independent review fixes.
use icanact_saga_choreography::*;
#[allow(unused_imports)]
mod support;
use support::{FaultJournal, FaultTrigger, JournalOp};

struct Actor(SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>);
impl HasSagaParticipantSupport for Actor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.0
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.0
    }
}
fn actor() -> Actor {
    Actor(SagaParticipantSupport::new(
        InMemoryJournal::new(),
        InMemoryDedupe::new(),
    ))
}
fn ctx(start: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(805),
        saga_type: "review-state".into(),
        step_name: "charge".into(),
        correlation_id: 805,
        causation_id: 805,
        trace_id: 805,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: start,
        event_timestamp_millis: start,
    }
}
fn append(actor: &Actor, context: &SagaContext, event: ParticipantEvent) {
    actor.record_event_strict(context.saga_id, event).unwrap();
}
fn started(actor: &Actor, context: &SagaContext) {
    actor.admit_participant_event_strict(context).unwrap();
    append(
        actor,
        context,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 101,
        },
    );
}
fn result(actor: &Actor, context: &SagaContext) {
    append(
        actor,
        context,
        ParticipantEvent::StepExecutionCompleted {
            output: vec![7],
            compensation_data: vec![42],
            completed_at_millis: 102,
        },
    );
}

#[test]
fn ordinary_failure_cannot_resolve_an_open_forward_intent() {
    let mut actor = actor();
    let c = ctx(100);
    started(&actor, &c);
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "timeout")
        .unwrap();
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}
#[test]
fn ordinary_failure_cannot_resolve_an_unreversed_compensable_result() {
    let mut actor = actor();
    let c = ctx(100);
    started(&actor, &c);
    result(&actor, &c);
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "timeout")
        .unwrap();
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}
#[test]
fn resolved_undo_and_idle_run_can_retain_ordinary_failure() {
    for undo in [true, false] {
        let mut actor = actor();
        let c = ctx(100);
        actor.admit_participant_event_strict(&c).unwrap();
        if undo {
            started(&actor, &c);
            result(&actor, &c);
            append(
                &actor,
                &c,
                ParticipantEvent::CompensationStarted {
                    attempt: 1,
                    started_at_millis: 103,
                },
            );
            append(
                &actor,
                &c,
                ParticipantEvent::CompensationCompleted {
                    completed_at_millis: 104,
                },
            );
        }
        actor
            .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "resolved")
            .unwrap();
        assert_eq!(
            actor.terminal_run_outcome_strict(&c).unwrap(),
            Some(ParticipantTerminalKind::Failed)
        );
    }
}
#[test]
fn earlier_kept_effect_does_not_poison_a_later_idle_run() {
    let mut actor = actor();
    let old = ctx(100);
    started(&actor, &old);
    result(&actor, &old);
    actor
        .retain_terminal_saga_strict(
            &old,
            ParticipantTerminalKind::Completed,
            "successful business effect kept",
        )
        .unwrap();
    let new = ctx(200);
    actor.admit_participant_event_strict(&new).unwrap();
    actor
        .retain_terminal_saga_strict(&new, ParticipantTerminalKind::Failed, "no work began")
        .unwrap();
    assert_eq!(
        actor.terminal_run_outcome_strict(&new).unwrap(),
        Some(ParticipantTerminalKind::Failed)
    );
}
#[test]
fn late_result_can_escalate_a_previously_ordinary_failure() {
    let mut actor = actor();
    let c = ctx(100);
    started(&actor, &c);
    append(
        &actor,
        &c,
        ParticipantEvent::StepExecutionFailed {
            error: "no effect known".into(),
            requires_compensation: false,
            failed_at_millis: 102,
        },
    );
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "failed")
        .unwrap();
    result(&actor, &c);
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "late materialisation")
        .unwrap();
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}

fn proof(context: &SagaContext, compensable: bool) -> ParticipantForwardOutcome {
    let mut context = context.clone();
    context.trace_id = 917;
    ParticipantForwardOutcome {
        context,
        output: vec![7],
        saga_input: vec![8, 9],
        compensation_data: if compensable { vec![42] } else { vec![] },
        effect: None,
        receipt: None,
        recorded_at_millis: 103,
    }
}
#[test]
fn confirmed_pure_result_is_distinct_from_an_unconfirmed_result() {
    let actor = actor();
    let c = ctx(100);
    started(&actor, &c);
    append(
        &actor,
        &c,
        ParticipantEvent::StepExecutionCompleted {
            output: vec![7],
            compensation_data: vec![],
            completed_at_millis: 102,
        },
    );
    let unconfirmed = actor.participant_run_evidence_strict(&c).unwrap();
    assert!(unconfirmed.forward_result_recorded);
    assert!(unconfirmed.forward_outcome.is_none());
    assert!(
        unconfirmed.failure_requires_quarantine(),
        "result alone cannot prove a declared effect dispatch succeeded"
    );
    let p = proof(&c, false);
    actor.record_forward_outcome_strict(&p).unwrap();
    let confirmed = actor.participant_run_evidence_strict(&c).unwrap();
    assert!(!confirmed.forward_intent_open);
    assert!(
        !confirmed.failure_requires_quarantine(),
        "confirmed non-effect AnyOf sibling is not uncertain work"
    );
    let event = confirmed.forward_outcome.unwrap().completion_event();
    match event {
        SagaChoreographyEvent::StepCompleted {
            context,
            output,
            saga_input,
            compensation_available,
        } => {
            assert_eq!(
                context.trace_id, 917,
                "recovery must reuse the original publication context"
            );
            assert_eq!(output, vec![7]);
            assert_eq!(saga_input, vec![8, 9]);
            assert!(!compensation_available);
        }
        _ => unreachable!(),
    }
}
#[test]
fn explicit_effect_even_without_compensation_bytes_is_not_resolved_by_failure() {
    let mut actor = actor();
    let c = ctx(100);
    started(&actor, &c);
    let mut p = proof(&c, false);
    p.effect = Some("charge".into());
    p.receipt = Some("ledger-917".into());
    actor.record_forward_outcome_strict(&p).unwrap();
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "sibling failed")
        .unwrap();
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}
#[test]
fn confirmed_evidence_is_run_scoped_and_survives_cold_reconstruction() {
    let mut actor = actor();
    let old = ctx(100);
    started(&actor, &old);
    result(&actor, &old);
    actor
        .record_forward_outcome_strict(&proof(&old, true))
        .unwrap();
    actor
        .retain_terminal_saga_strict(&old, ParticipantTerminalKind::Completed, "done")
        .unwrap();
    let mut new = ctx(200);
    actor.admit_participant_event_strict(&new).unwrap();
    let restarted = Actor(SagaParticipantSupport::new(actor.0.journal, actor.0.dedupe));
    let recovered = restarted.participant_run_evidence_strict(&old).unwrap();
    assert_eq!(recovered.compensation_data, vec![42]);
    assert_eq!(recovered.forward_outcome.unwrap().saga_input, vec![8, 9]);
    assert!(
        restarted
            .participant_run_evidence_strict(&new)
            .unwrap()
            .forward_outcome
            .is_none()
    );
    new.saga_type = "other-type".into();
    assert!(
        restarted
            .participant_run_evidence_strict(&new)
            .unwrap()
            .forward_outcome
            .is_none()
    );
}
#[test]
fn forward_proof_appends_archive_tag_15_and_roundtrips_all_fields() {
    let mut p = proof(&ctx(100), true);
    p.effect = Some("charge".into());
    p.receipt = Some("ledger-917".into());
    let event = ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome: p };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).unwrap();
    let size = size_of::<<ParticipantEvent as rkyv::Archive>::Archived>();
    assert_eq!(bytes[bytes.len() - size], 15);
    let back = rkyv::from_bytes::<ParticipantEvent, rkyv::rancor::Error>(&bytes).unwrap();
    match back {
        ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
            assert_eq!(outcome.context.trace_id, 917);
            assert_eq!(outcome.output, vec![7]);
            assert_eq!(outcome.saga_input, vec![8, 9]);
            assert_eq!(outcome.compensation_data, vec![42]);
            assert_eq!(outcome.effect.as_deref(), Some("charge"));
            assert_eq!(outcome.receipt.as_deref(), Some("ledger-917"));
            assert_eq!(outcome.recorded_at_millis, 103);
        }
        _ => unreachable!(),
    }
}
struct FaultActor(SagaParticipantSupport<FaultJournal<InMemoryJournal>, InMemoryDedupe>);
impl HasSagaParticipantSupport for FaultActor {
    type Journal = FaultJournal<InMemoryJournal>;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.0
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.0
    }
}
#[test]
fn proof_write_and_evidence_read_fail_closed() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let actor = FaultActor(SagaParticipantSupport::new(
        journal.clone(),
        InMemoryDedupe::new(),
    ));
    let c = ctx(100);
    actor.admit_participant_event_strict(&c).unwrap();
    actor
        .record_event_strict(
            c.saga_id,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 101,
            },
        )
        .unwrap();
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
    );
    assert!(
        actor
            .record_forward_outcome_strict(&proof(&c, true))
            .is_err()
    );
    assert!(
        actor
            .participant_run_evidence_strict(&c)
            .unwrap()
            .forward_outcome
            .is_none()
    );
    journal.fail_once(
        JournalOp::Read,
        FaultTrigger::NthCall(journal.call_count(JournalOp::Read) + 1),
    );
    assert!(actor.participant_run_evidence_strict(&c).is_err());
}
