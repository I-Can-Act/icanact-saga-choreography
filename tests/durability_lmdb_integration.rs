#![cfg(feature = "lmdb")]

use std::path::PathBuf;

use icanact_saga_choreography::durability::lmdb::{
    open_lmdb_participant_support, open_lmdb_participant_support_for_saga_type, LmdbDedupe,
    LmdbJournal,
};
use icanact_saga_choreography::durability::{panic_quarantine_reason, ActiveSagaExecutionPhase};
use icanact_saga_choreography::{
    ParticipantDedupeStore, ParticipantEvent, ParticipantJournal, SagaChoreographyEvent,
    SagaContext, SagaId,
};

#[test]
fn lmdb_journal_and_dedupe_roundtrip() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().join("journal");
    let dedupe_path = temp.path().join("dedupe");

    let journal = LmdbJournal::open(&journal_path).expect("journal should open");
    let dedupe = LmdbDedupe::open(&dedupe_path).expect("dedupe should open");

    let saga_a = SagaId::new(100);
    let saga_b = SagaId::new(101);

    journal
        .append(
            saga_a,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1000,
            },
        )
        .expect("append for saga_a should succeed");
    journal
        .append(
            saga_b,
            ParticipantEvent::StepExecutionCompleted {
                output: b"ok".to_vec(),
                compensation_data: vec![],
                completed_at_millis: 1100,
            },
        )
        .expect("append for saga_b should succeed");

    let read_a = journal.read(saga_a).expect("read saga_a should succeed");
    assert_eq!(read_a.len(), 1);
    assert!(matches!(
        read_a[0].event,
        ParticipantEvent::StepExecutionStarted { .. }
    ));

    let mut saga_ids = journal.list_sagas().expect("list_sagas should succeed");
    saga_ids.sort_by_key(|id| id.get());
    assert_eq!(saga_ids, vec![saga_a, saga_b]);

    assert!(dedupe
        .check_and_mark(saga_a, "probe")
        .expect("first check_and_mark should succeed"));
    assert!(!dedupe
        .check_and_mark(saga_a, "probe")
        .expect("second check_and_mark should succeed"));
    assert!(dedupe.contains(saga_a, "probe"));

    dedupe
        .mark_processed(saga_b, "manual")
        .expect("mark_processed should succeed");
    assert!(dedupe.contains(saga_b, "manual"));

    dedupe.prune(saga_a).expect("prune should succeed");
    assert!(!dedupe.contains(saga_a, "probe"));
}

#[test]
fn open_support_replays_panic_quarantine_once() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let base: PathBuf = temp.path().join("support");

    let journal = LmdbJournal::open(&base.join("journal")).expect("journal should open");
    let saga_id = SagaId::new(202);
    journal
        .append(
            saga_id,
            ParticipantEvent::Quarantined {
                reason: panic_quarantine_reason(ActiveSagaExecutionPhase::StepExecution, "boom"),
                quarantined_at_millis: SagaContext::now_millis(),
            },
        )
        .expect("append quarantined event should succeed");

    let mut first =
        open_lmdb_participant_support_for_saga_type(&base, "risk_gate", "mature_pool_refresh")
            .expect("support should open");
    let first_events = first.take_startup_recovery_events();
    assert_eq!(first_events.len(), 1);
    assert!(matches!(
        &first_events[0],
        SagaChoreographyEvent::SagaQuarantined { context, .. }
            if context.saga_type.as_ref() == "mature_pool_refresh" && context.saga_id == saga_id
    ));

    let mut second =
        open_lmdb_participant_support_for_saga_type(&base, "risk_gate", "mature_pool_refresh")
            .expect("support should reopen");
    let second_events = second.take_startup_recovery_events();
    assert!(
        second_events.is_empty(),
        "dedupe should prevent replaying panic quarantine more than once"
    );

    let mut default_support =
        open_lmdb_participant_support(&base, "risk_gate").expect("default support should open");
    let default_events = default_support.take_startup_recovery_events();
    assert!(
        default_events.is_empty(),
        "panic replay should remain deduped through default open helper"
    );
}

#[test]
fn lmdb_open_fails_for_file_paths() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_file = temp.path().join("journal-file");
    let dedupe_file = temp.path().join("dedupe-file");
    std::fs::write(&journal_file, b"not-a-directory").expect("journal file should be created");
    std::fs::write(&dedupe_file, b"not-a-directory").expect("dedupe file should be created");

    let journal_err = LmdbJournal::open(&journal_file).expect_err("file path must fail");
    assert!(
        !journal_err.to_string().is_empty(),
        "journal open error should include a storage message"
    );

    let dedupe_err = LmdbDedupe::open(&dedupe_file).expect_err("file path must fail");
    assert!(
        !dedupe_err.to_string().is_empty(),
        "dedupe open error should include a storage message"
    );
}
