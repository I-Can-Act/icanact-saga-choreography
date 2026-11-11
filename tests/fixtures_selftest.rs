//! Self-tests for the deterministic fault fixtures in `tests/support`.
mod support;

use icanact_saga_choreography::{
    InMemoryParticipantStore, ParticipantEvent, ParticipantStore, SagaId,
};
use support::{
    CutPoint, Cuts, EffectError, EffectLedger, FaultStore, FaultTrigger, JournalOp, ManualClock,
};

fn context() -> icanact_saga_choreography::SagaContext {
    icanact_saga_choreography::SagaContext {
        saga_id: SagaId(1),
        saga_type: "t".into(),
        step_name: "s".into(),
        correlation_id: 1,
        causation_id: 1,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: Default::default(),
        saga_started_at_millis: 1,
        event_timestamp_millis: 1,
    }
}

fn event() -> ParticipantEvent {
    ParticipantEvent::ParticipantRunRecorded {
        saga_type: "t".into(),
        saga_started_at_millis: 1,
        recorded_at_millis: 1,
    }
}

/// A scenario that does not handle journal errors: appends twice, propagating `?`.
fn naive_two_appends(
    j: &impl ParticipantStore,
) -> Result<(), icanact_saga_choreography::JournalError> {
    j.append(SagaId(1), event())?;
    j.append(SagaId(1), event())?;
    Ok(())
}

#[test]
fn fault_store_detects_injected_append_failure() {
    // Control: nothing armed, scenario succeeds and both events are stored.
    let control = FaultStore::new(InMemoryParticipantStore::new());
    assert!(naive_two_appends(&control).is_ok());
    assert_eq!(control.read(SagaId(1)).unwrap().len(), 2);

    // Armed on the 2nd append: the unhandled scenario must surface the failure.
    let armed = FaultStore::new(InMemoryParticipantStore::new());
    armed.fail_once(JournalOp::Append, FaultTrigger::NthCall(2));
    assert!(
        naive_two_appends(&armed).is_err(),
        "injected append failure must surface"
    );
    assert_eq!(armed.call_count(JournalOp::Append), 2);
    assert_eq!(armed.inner().read(SagaId(1)).unwrap().len(), 1);

    // One-shot: a retry succeeds, and a "reopened" wrapper shares the same store.
    assert!(armed.append(SagaId(1), event()).is_ok());
    let reopened = FaultStore::shared(armed.inner().clone());
    assert_eq!(reopened.read(SagaId(1)).unwrap().len(), 2);
}

#[test]
fn fault_store_triggers_on_kind_and_cut() {
    let j = FaultStore::new(InMemoryParticipantStore::new());
    j.fail_always(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantRunRecorded"),
    );
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
fn fault_store_dedupe_fails_on_nth_call_and_shares_store() {
    let d = FaultStore::new(InMemoryParticipantStore::new());
    d.append(
        SagaId(1),
        icanact_saga_choreography::ParticipantEvent::ParticipantRunRecorded {
            saga_type: "t".into(),
            saga_started_at_millis: 1,
            recorded_at_millis: 1,
        },
    )
    .unwrap();
    d.fail_once(JournalOp::CheckAndMark, FaultTrigger::NthCall(2));
    assert!(d.check_and_mark(SagaId(1), "a").unwrap());
    assert!(d.check_and_mark(SagaId(1), "b").is_err());
    let reopened = FaultStore::shared(d.inner().clone());
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

#[test]
fn every_store_operation_has_an_independent_shared_one_shot_fault() {
    for op in [
        JournalOp::Append,
        JournalOp::AppendBatch,
        JournalOp::AdmitAndRecordIntent,
        JournalOp::Read,
        JournalOp::ListSagas,
        JournalOp::UnresolvedSagas,
        JournalOp::CheckAndMark,
        JournalOp::RemoveProcessed,
        JournalOp::Prune,
        JournalOp::CompactTerminalDetail,
        JournalOp::PruneResolvedBefore,
    ] {
        let store = FaultStore::new(InMemoryParticipantStore::new());
        store.inner().append(SagaId(1), event()).unwrap();
        store.inner().mark_processed(SagaId(1), "old").unwrap();
        let before = format!("{:?}", store.inner().read(SagaId(1)).unwrap());
        store.fail_once(op, FaultTrigger::NthCall(1));
        let cloned = store.clone();
        let invoke = || match op {
            JournalOp::Append => cloned.append(SagaId(1), event()).map(|_| ()),
            JournalOp::AppendBatch => cloned.append_batch(SagaId(1), vec![event()]).map(|_| ()),
            JournalOp::AdmitAndRecordIntent => cloned
                .admit_and_record_intent(&context(), "forward", "saga_started")
                .map(|_| ()),
            JournalOp::Read => cloned.read(SagaId(1)).map(|_| ()),
            JournalOp::ListSagas => cloned.list_sagas().map(|_| ()),
            JournalOp::UnresolvedSagas => cloned.unresolved_saga_ids().map(|_| ()),
            JournalOp::CheckAndMark => cloned.check_and_mark(SagaId(1), "new").map(|_| ()),
            JournalOp::RemoveProcessed => cloned.remove_processed(SagaId(1), "old"),
            JournalOp::Prune => cloned.prune(SagaId(1)),
            JournalOp::CompactTerminalDetail => cloned.compact_terminal_detail(256).map(|_| ()),
            JournalOp::PruneResolvedBefore => {
                cloned.prune_resolved_before(u64::MAX, 256).map(|_| ())
            }
        };
        let error = invoke().unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("injected {op:?} failure")),
            "{error}"
        );
        assert_eq!(store.call_count(op), 1, "clone shares {op:?} counter");
        assert_eq!(
            format!("{:?}", store.inner().read(SagaId(1)).unwrap()),
            before
        );
        assert!(!store.inner().check_and_mark(SagaId(1), "old").unwrap());
        assert!(store.inner().check_and_mark(SagaId(1), "new").unwrap());
        store.inner().remove_processed(SagaId(1), "new").unwrap();
        invoke().unwrap();
        assert_eq!(store.call_count(op), 2, "{op:?} one-shot retry");
    }
}

#[test]
fn batch_event_kind_fault_matches_every_position_without_partial_writes() {
    for target_index in 0..3 {
        let store = FaultStore::new(InMemoryParticipantStore::new());
        let mut batch = vec![event(); 3];
        batch[target_index] = ParticipantEvent::ParticipantTerminalRecorded {
            saga_type: "t".into(),
            saga_started_at_millis: 1,
            outcome: icanact_saga_choreography::ParticipantTerminalKind::Completed,
            reason: "done".into(),
            recorded_at_millis: 2,
        };
        store.fail_once(
            JournalOp::AppendBatch,
            FaultTrigger::EventKind("ParticipantTerminalRecorded"),
        );
        assert!(
            store
                .append_batch(SagaId(1), batch.clone())
                .unwrap_err()
                .to_string()
                .contains("injected AppendBatch")
        );
        assert!(store.inner().read(SagaId(1)).unwrap().is_empty());
        assert!(store.inner().list_sagas().unwrap().is_empty());
        assert_eq!(store.call_count(JournalOp::AppendBatch), 1);
        assert_eq!(store.call_count(JournalOp::Append), 0);
        assert_eq!(store.append_batch(SagaId(1), batch).unwrap().len(), 3);
    }
}

#[test]
fn cut_before_atomic_intent_leaves_neither_rows_nor_dedupe_marker() {
    use icanact_saga_choreography::StoreAdmission;
    let store = FaultStore::new(InMemoryParticipantStore::new());
    let cuts = Cuts::new();
    cuts.enter(CutPoint::BeforeIntent);
    store.fail_always(
        JournalOp::AdmitAndRecordIntent,
        FaultTrigger::AtCut(cuts.clone(), CutPoint::BeforeIntent),
    );
    for _ in 0..2 {
        assert!(
            store
                .admit_and_record_intent(&context(), "forward", "saga_started")
                .is_err()
        );
        assert!(store.inner().read(SagaId(1)).unwrap().is_empty());
    }
    cuts.clear();
    assert_eq!(
        store
            .admit_and_record_intent(&context(), "forward", "saga_started")
            .unwrap(),
        StoreAdmission::Admitted
    );
    assert_eq!(store.inner().read(SagaId(1)).unwrap().len(), 3);
    assert_eq!(
        store
            .admit_and_record_intent(&context(), "forward", "saga_started")
            .unwrap(),
        StoreAdmission::Duplicate
    );
    assert_eq!(store.inner().read(SagaId(1)).unwrap().len(), 3);
}
