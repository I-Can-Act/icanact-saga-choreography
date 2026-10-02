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
