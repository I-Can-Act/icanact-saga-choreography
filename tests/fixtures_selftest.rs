//! Self-tests for the deterministic fault fixtures in `tests/support`.
mod support;

use icanact_saga_choreography::{
    InMemoryDedupe, InMemoryJournal, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    SagaId,
};
use support::{
    CutPoint, Cuts, EffectError, EffectLedger, FaultDedupe, FaultJournal, FaultTrigger, JournalOp,
    ManualClock,
};

fn event() -> ParticipantEvent {
    ParticipantEvent::SagaRegistered {
        saga_type: "t".into(),
        step_name: "s".into(),
        registered_at_millis: 1,
    }
}

/// A scenario that does not handle journal errors: appends twice, propagating `?`.
fn naive_two_appends(
    j: &impl ParticipantJournal,
) -> Result<(), icanact_saga_choreography::JournalError> {
    j.append(SagaId(1), event())?;
    j.append(SagaId(1), event())?;
    Ok(())
}

#[test]
fn fault_journal_detects_injected_append_failure() {
    // Control: nothing armed, scenario succeeds and both events are stored.
    let control = FaultJournal::new(InMemoryJournal::new());
    assert!(naive_two_appends(&control).is_ok());
    assert_eq!(control.read(SagaId(1)).unwrap().len(), 2);

    // Armed on the 2nd append: the unhandled scenario must surface the failure.
    let armed = FaultJournal::new(InMemoryJournal::new());
    armed.fail_once(JournalOp::Append, FaultTrigger::NthCall(2));
    assert!(
        naive_two_appends(&armed).is_err(),
        "injected append failure must surface"
    );
    assert_eq!(armed.call_count(JournalOp::Append), 2);
    assert_eq!(armed.inner().read(SagaId(1)).unwrap().len(), 1);

    // One-shot: a retry succeeds, and a "reopened" wrapper shares the same store.
    assert!(armed.append(SagaId(1), event()).is_ok());
    let reopened = FaultJournal::shared(armed.inner().clone());
    assert_eq!(reopened.read(SagaId(1)).unwrap().len(), 2);
}

#[test]
fn fault_journal_triggers_on_kind_and_cut() {
    let j = FaultJournal::new(InMemoryJournal::new());
    j.fail_always(JournalOp::Append, FaultTrigger::EventKind("SagaRegistered"));
    assert!(j.append(SagaId(1), event()).is_err());
    j.disarm();

    let cuts = Cuts::new();
    j.fail_always(
        JournalOp::Prune,
        FaultTrigger::AtCut(cuts.clone(), CutPoint::BeforeGc),
    );
    assert!(j.prune(SagaId(1)).is_ok());
    cuts.enter(CutPoint::BeforeGc);
    assert!(j.prune(SagaId(1)).is_err());
}

#[test]
fn fault_dedupe_fails_on_nth_call_and_shares_store() {
    let d = FaultDedupe::new(InMemoryDedupe::new());
    d.fail_check_and_mark(FaultTrigger::NthCall(2));
    assert!(d.check_and_mark(SagaId(1), "a").unwrap());
    assert!(d.check_and_mark(SagaId(1), "b").is_err());
    let reopened = FaultDedupe::shared(d.inner().clone());
    assert!(!reopened.check_and_mark(SagaId(1), "a").unwrap());
}

#[test]
fn effect_ledger_detects_double_effect() {
    // Control: one forward per run key.
    let control = EffectLedger::new();
    control.forward("run-1");
    assert!(control.double_effects().is_empty());

    // A naive retry that re-applies the effect must be flagged.
    let retried = EffectLedger::new();
    retried.forward("run-1");
    retried.forward("run-1");
    assert_eq!(retried.forward_count("run-1"), 2);
    assert_eq!(retried.double_effects(), vec!["run-1".to_owned()]);
}

#[test]
fn effect_ledger_enforces_undo_order_and_records_calls() {
    let l = EffectLedger::new();
    l.require_undo_order("A", "B");
    assert!(matches!(l.undo("A"), Err(EffectError::UndoBlocked { .. })));
    assert_eq!(l.undo("B"), Ok(1));
    assert_eq!(l.undo("A"), Ok(1));
    assert_eq!(l.calls().len(), 3);
}

#[test]
fn manual_clock_advances_deterministically() {
    let c = ManualClock::new(100);
    assert_eq!(c.advance(50), 150);
    c.set(7);
    assert_eq!(c.now(), 7);
}
