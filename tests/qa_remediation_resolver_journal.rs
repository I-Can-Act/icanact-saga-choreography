#![cfg(feature = "lmdb")]

use std::path::Path;

use heed::EnvOpenOptions;
use heed::types::{Bytes, Str};
use icanact_saga_choreography::{
    InMemoryTerminalResolverJournal, LmdbTerminalResolverJournal, SagaChoreographyEvent,
    SagaContext, SagaId, TerminalResolverJournal, TerminalResolverJournalEntry,
    TerminalResolverJournalError,
};

fn ctx(saga_id: u64, saga_type: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: saga_type.into(),
        step_name: "step".into(),
        correlation_id: 1,
        causation_id: 1,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn started(saga_id: u64, saga_type: &str, started_at: u64, len: usize) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx(saga_id, saga_type, started_at),
        payload: vec![9; len],
    }
}

fn step(saga_id: u64, saga_type: &str, started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepStarted {
        context: ctx(saga_id, saga_type, started_at),
    }
}

fn completed(saga_id: u64, saga_type: &str, started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaCompleted {
        context: ctx(saga_id, saga_type, started_at),
    }
}

fn quarantined(saga_id: u64, saga_type: &str, started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaQuarantined {
        context: ctx(saga_id, saga_type, started_at),
        reason: "r".into(),
        step: "s".into(),
        participant_id: "p".into(),
    }
}

fn tamper_rows(
    path: &Path,
    map_size: usize,
    edit: impl FnOnce(&heed::Database<Str, Bytes>, &mut heed::RwTxn<'_>),
) {
    let env = unsafe {
        EnvOpenOptions::new()
            .max_dbs(4)
            .map_size(map_size)
            .open(path)
    }
    .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let rows = env
        .open_database::<Str, Bytes>(&wtxn, Some("resolver_events"))
        .unwrap()
        .unwrap();
    edit(&rows, &mut wtxn);
    wtxn.commit().unwrap();
}

#[test]
fn varied_payload_lengths_survive_live_read_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    for len in 0..8usize {
        let seq = journal
            .append(started(100 + len as u64, "a", 1, len))
            .unwrap();
        assert_eq!(seq, len as u64 + 1);
    }
    let check = |entries: Vec<TerminalResolverJournalEntry>| {
        assert_eq!(entries.len(), 8);
        for (len, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, len as u64 + 1);
            assert_eq!(entry.event, started(100 + len as u64, "a", 1, len));
        }
    };
    check(journal.read_all().unwrap());
    drop(journal);
    let reopened = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    check(reopened.read_all().unwrap());
    // Appending after reopen keeps the sequence monotonic.
    assert_eq!(reopened.append(started(1, "a", 1, 3)).unwrap(), 9);
}

#[test]
fn corrupted_rows_remain_errors() {
    for corrupt in [
        |bytes: &[u8]| bytes[..bytes.len() - 5].to_vec(),
        |bytes: &[u8]| vec![0xff; bytes.len()],
        |_: &[u8]| Vec::new(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        journal.append(started(1, "a", 1, 3)).unwrap();
        journal.append(started(2, "a", 1, 4)).unwrap();
        drop(journal);
        tamper_rows(dir.path(), 256 * 1024 * 1024, |rows, wtxn| {
            let key = format!("{:020}", 2);
            let original = rows.get(wtxn, &key).unwrap().unwrap().to_vec();
            rows.put(wtxn, &key, &corrupt(&original)).unwrap();
        });
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        assert!(matches!(
            journal.read_all(),
            Err(TerminalResolverJournalError::Storage(_))
        ));
        let before = journal.compact_terminal_detail();
        assert!(before.is_err(), "compaction must not trust corrupt rows");
    }
}

#[test]
fn compaction_keeps_unresolved_runs_and_terminal_fences() {
    let dir = tempfile::tempdir().unwrap();
    let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    // Run 1: completed. Run 2: unresolved. Run 3: quarantined.
    // Run 4: same saga id/type as run 1 but a later start, unresolved.
    journal.append(started(1, "a", 10, 1)).unwrap(); // 1
    journal.append(started(2, "a", 10, 2)).unwrap(); // 2
    journal.append(started(3, "a", 10, 3)).unwrap(); // 3
    journal.append(step(1, "a", 10)).unwrap(); // 4
    journal.append(step(2, "a", 10)).unwrap(); // 5
    journal.append(completed(1, "a", 10)).unwrap(); // 6
    journal.append(quarantined(3, "a", 10)).unwrap(); // 7
    journal.append(started(1, "a", 20, 4)).unwrap(); // 8
    journal.append(started(1, "b", 10, 5)).unwrap(); // 9: other type, unresolved

    let removed = journal.compact_terminal_detail().unwrap();
    assert_eq!(
        removed, 2,
        "quarantined runs are unresolved: all their evidence must remain"
    );

    let kept: Vec<u64> = journal
        .read_all()
        .unwrap()
        .iter()
        .map(|entry| entry.sequence)
        .collect();
    assert_eq!(kept, vec![2, 3, 5, 6, 7, 8, 9]);

    // Idempotent, and sequence numbering stays monotonic and never reused.
    assert_eq!(journal.compact_terminal_detail().unwrap(), 0);
    assert_eq!(journal.append(completed(2, "a", 10)).unwrap(), 10);
    drop(journal);
    let reopened = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    let seqs: Vec<u64> = reopened
        .read_all()
        .unwrap()
        .iter()
        .map(|entry| entry.sequence)
        .collect();
    assert_eq!(seqs, vec![2, 3, 5, 6, 7, 8, 9, 10]);
    assert_eq!(reopened.append(step(9, "a", 1)).unwrap(), 11);
}

#[test]
fn custom_journals_report_unsupported_maintenance() {
    let journal = InMemoryTerminalResolverJournal::default();
    journal.append(completed(1, "a", 1)).unwrap();
    let result = journal.compact_terminal_detail();
    assert!(
        matches!(result, Err(TerminalResolverJournalError::Unsupported(_))),
        "{result:?}"
    );
    assert_eq!(journal.read_all().unwrap().len(), 1);
}

#[test]
fn small_map_size_fails_visibly_without_losing_old_records() {
    let dir = tempfile::tempdir().unwrap();
    let map_size = 256 * 1024;
    let journal = LmdbTerminalResolverJournal::open_with_map_size(dir.path(), map_size).unwrap();
    let mut accepted = Vec::new();
    let mut failure = None;
    for id in 0..10_000u64 {
        match journal.append(started(id, "a", 1, 2048)) {
            Ok(sequence) => accepted.push(sequence),
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    let failure = failure.expect("journal must report capacity exhaustion");
    assert!(matches!(failure, TerminalResolverJournalError::Storage(_)));
    assert!(!accepted.is_empty());
    let entries = journal.read_all().unwrap();
    let seqs: Vec<u64> = entries.iter().map(|entry| entry.sequence).collect();
    assert_eq!(seqs, accepted, "failed append must not lose or add rows");
}

fn failed(saga_id: u64, saga_type: &str, started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaFailed {
        context: ctx(saga_id, saga_type, started_at),
        reason: "ordinary".into(),
        failure: None,
    }
}

fn effect(saga_id: u64, saga_type: &str, started_at: u64, trace: u64) -> SagaChoreographyEvent {
    let mut context = ctx(saga_id, saga_type, started_at);
    context.trace_id = trace;
    SagaChoreographyEvent::StepCompleted {
        context,
        output: vec![1],
        saga_input: vec![1],
        compensation_available: true,
    }
}

fn sequences(entries: &[TerminalResolverJournalEntry]) -> Vec<u64> {
    entries.iter().map(|entry| entry.sequence).collect()
}

#[test]
fn in_memory_journal_reads_one_saga_in_sequence_order() {
    let journal = InMemoryTerminalResolverJournal::default();
    assert!(journal.supports_saga_lookup());
    journal.append(started(1, "a", 1, 1)).unwrap(); // 0
    journal.append(started(2, "a", 1, 1)).unwrap(); // 1
    journal.append(step(1, "a", 1)).unwrap(); // 2
    journal.append(completed(1, "a", 1)).unwrap(); // 3
    assert_eq!(
        sequences(&journal.read_saga(SagaId::new(1)).unwrap()),
        vec![0, 2, 3]
    );
    assert_eq!(
        sequences(&journal.read_saga(SagaId::new(2)).unwrap()),
        vec![1]
    );
    assert!(journal.read_saga(SagaId::new(3)).unwrap().is_empty());
}

#[test]
fn custom_journals_default_to_honest_unsupported_lookup() {
    struct Custom;
    impl TerminalResolverJournal for Custom {
        fn append(&self, _: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError> {
            Ok(0)
        }
        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            Ok(Vec::new())
        }
    }
    assert!(!Custom.supports_saga_lookup());
    assert!(matches!(
        Custom.read_saga(SagaId::new(1)),
        Err(TerminalResolverJournalError::Unsupported(_))
    ));
}

#[test]
fn lmdb_per_saga_lookup_is_bounded_ordered_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    assert!(journal.supports_saga_lookup());
    // Ids that share a decimal prefix must not bleed into each other.
    for id in [1u64, 10, 100, 1] {
        journal.append(started(id, "a", id, 1)).unwrap();
    }
    journal.append(completed(1, "a", 1)).unwrap();
    let check = |journal: &LmdbTerminalResolverJournal| {
        assert_eq!(
            sequences(&journal.read_saga(SagaId::new(1)).unwrap()),
            vec![1, 4, 5]
        );
        assert_eq!(
            sequences(&journal.read_saga(SagaId::new(10)).unwrap()),
            vec![2]
        );
        assert_eq!(
            sequences(&journal.read_saga(SagaId::new(100)).unwrap()),
            vec![3]
        );
        assert!(journal.read_saga(SagaId::new(u64::MAX)).unwrap().is_empty());
    };
    check(&journal);
    drop(journal);
    check(&LmdbTerminalResolverJournal::open(dir.path()).unwrap());
}

#[test]
fn lmdb_schema_one_journal_is_backfilled_transactionally_on_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        journal.append(started(7, "a", 1, 1)).unwrap();
        journal.append(started(8, "a", 1, 1)).unwrap();
        journal.append(completed(7, "a", 1)).unwrap();
    }
    // Rewind to the pre-index schema: version 1 and no index rows.
    let env = unsafe {
        EnvOpenOptions::new()
            .max_dbs(4)
            .map_size(256 * 1024 * 1024)
            .open(dir.path())
    }
    .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let meta = env
        .open_database::<Str, Str>(&wtxn, Some("resolver_meta"))
        .unwrap()
        .unwrap();
    meta.put(&mut wtxn, "schema_version", "1").unwrap();
    if let Some(index) = env
        .open_database::<Str, Bytes>(&wtxn, Some("resolver_saga_index"))
        .unwrap()
    {
        index.clear(&mut wtxn).unwrap();
    }
    wtxn.commit().unwrap();
    drop(env);

    let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    assert_eq!(
        sequences(&journal.read_saga(SagaId::new(7)).unwrap()),
        vec![1, 3]
    );
    assert_eq!(
        sequences(&journal.read_saga(SagaId::new(8)).unwrap()),
        vec![2]
    );
    // Appends after the migration stay indexed.
    journal.append(step(8, "a", 1)).unwrap();
    assert_eq!(
        sequences(&journal.read_saga(SagaId::new(8)).unwrap()),
        vec![2, 4]
    );
    drop(journal);
    // The migrated store is now a current-schema store that reopens unchanged.
    let reopened = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    assert_eq!(
        sequences(&reopened.read_saga(SagaId::new(8)).unwrap()),
        vec![2, 4]
    );
}

#[test]
fn lmdb_compaction_keeps_lookup_consistent_and_retains_failed_run_effect_fingerprints() {
    let dir = tempfile::tempdir().unwrap();
    let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    // Run 1 succeeded (detail removable); run 2 failed after a compensable effect.
    journal.append(started(1, "a", 10, 1)).unwrap(); // 1
    journal.append(effect(1, "a", 10, 5)).unwrap(); // 2
    journal.append(completed(1, "a", 10)).unwrap(); // 3
    journal.append(started(2, "a", 10, 1)).unwrap(); // 4
    journal.append(step(2, "a", 10)).unwrap(); // 5
    journal.append(effect(2, "a", 10, 6)).unwrap(); // 6
    journal.append(failed(2, "a", 10)).unwrap(); // 7

    assert_eq!(journal.compact_terminal_detail().unwrap(), 4);
    let kept = |id| sequences(&journal.read_saga(SagaId::new(id)).unwrap());
    assert_eq!(kept(1), vec![3], "only the completion fence remains");
    assert_eq!(
        kept(2),
        vec![6, 7],
        "a failed run keeps its compensable-effect fingerprint and fence"
    );
    // The lookup index and the rows agree after compaction and reopen.
    let all = sequences(&journal.read_all().unwrap());
    assert_eq!(all, vec![3, 6, 7]);
    drop(journal);
    let reopened = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    assert_eq!(
        sequences(&reopened.read_saga(SagaId::new(2)).unwrap()),
        vec![6, 7]
    );
    assert_eq!(reopened.compact_terminal_detail().unwrap(), 0);
}

#[test]
fn per_saga_lookup_refuses_excess_detail_without_returning_a_partial_history() {
    let memory = InMemoryTerminalResolverJournal::default();
    let dir = tempfile::tempdir().unwrap();
    let lmdb = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
    for journal in [&memory as &dyn TerminalResolverJournal, &lmdb] {
        journal.append(started(901, "a", 1, 1)).unwrap();
        journal.append(completed(901, "a", 1)).unwrap();
        let error = journal.read_saga_bounded(SagaId::new(901), 1).unwrap_err();
        assert!(error.to_string().contains("capacity"), "{error}");
        assert_eq!(
            journal
                .read_saga_bounded(SagaId::new(901), 2)
                .unwrap()
                .len(),
            2
        );
        assert!(
            journal
                .read_saga_bounded(SagaId::new(902), 0)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn lmdb_failed_append_leaves_rows_and_lookup_index_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let journal = LmdbTerminalResolverJournal::open_with_map_size(dir.path(), 256 * 1024).unwrap();
    let mut failed_at = None;
    for id in 0..10_000u64 {
        if journal.append(started(id % 5, "a", id, 2048)).is_err() {
            failed_at = Some(id);
            break;
        }
    }
    assert!(failed_at.is_some(), "the small map must fill up visibly");
    let all = journal.read_all().unwrap();
    for id in 0..5u64 {
        let expected: Vec<u64> = all
            .iter()
            .filter(|entry| entry.event.context().saga_id == SagaId::new(id))
            .map(|entry| entry.sequence)
            .collect();
        assert_eq!(
            sequences(&journal.read_saga(SagaId::new(id)).unwrap()),
            expected,
            "index and rows must commit or fail together (id {id})"
        );
    }
}
