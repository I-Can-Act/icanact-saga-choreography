#![cfg(feature = "lmdb")]

use std::path::Path;

use heed::EnvOpenOptions;
use heed::types::{Bytes, Str};
use icanact_saga_choreography::durability::lmdb::LmdbJournal;
use icanact_saga_choreography::{ParticipantEvent, ParticipantJournal, SagaId};

fn event() -> ParticipantEvent {
    ParticipantEvent::StepExecutionStarted {
        attempt: 1,
        started_at_millis: 1000,
    }
}

/// Applies `edit` to the meta db and returns a snapshot of all journal rows.
fn tamper_meta(
    path: &Path,
    edit: impl FnOnce(&heed::Database<Str, Str>, &mut heed::RwTxn<'_>),
) -> Vec<(String, Vec<u8>)> {
    let env = unsafe {
        EnvOpenOptions::new()
            .max_dbs(16)
            .map_size(1024 * 1024 * 1024)
            .open(path)
    }
    .expect("raw env should open");
    let mut wtxn = env.write_txn().expect("write txn");
    let meta = env
        .create_database::<Str, Str>(&mut wtxn, Some("journal_meta"))
        .expect("meta db");
    edit(&meta, &mut wtxn);
    wtxn.commit().expect("commit");
    snapshot_rows(&env)
}

fn snapshot_rows(env: &heed::Env) -> Vec<(String, Vec<u8>)> {
    let mut wtxn = env.write_txn().expect("write txn");
    let rows = env
        .create_database::<Str, Bytes>(&mut wtxn, Some("journal_rows"))
        .expect("rows db");
    let out = rows
        .iter(&wtxn)
        .expect("iter")
        .map(|r| {
            let (k, v) = r.expect("row");
            (k.to_owned(), v.to_vec())
        })
        .collect();
    wtxn.commit().expect("commit");
    out
}

fn seeded(path: &Path) {
    let journal = LmdbJournal::open(path).expect("open");
    let saga = SagaId::new(7);
    assert_eq!(journal.append(saga, event()).expect("append 1"), 1);
    assert_eq!(journal.append(saga, event()).expect("append 2"), 2);
}

#[test]
fn journal_rejects_bad_sequence_metadata_without_overwriting_existing_row() {
    for corruption in ["missing", "non-numeric"] {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("journal");
        seeded(&path);
        let before = tamper_meta(&path, |meta, wtxn| match corruption {
            "missing" => {
                meta.delete(wtxn, "next_sequence").expect("delete");
            }
            _ => meta.put(wtxn, "next_sequence", "garbage").expect("put"),
        });
        assert_eq!(before.len(), 2);

        let journal = LmdbJournal::open(&path).expect("reopen");
        let result = journal.append(SagaId::new(7), event());
        assert!(
            result.is_err(),
            "{corruption}: append must fail, got {result:?}"
        );
        drop(journal);

        let after = tamper_meta(&path, |_, _| {});
        assert_eq!(
            before, after,
            "{corruption}: existing rows must be unchanged"
        );
    }
}

#[test]
fn journal_rejects_exhausted_sequence_space() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("journal");
    seeded(&path);
    let before = tamper_meta(&path, |meta, wtxn| {
        meta.put(wtxn, "next_sequence", &u64::MAX.to_string())
            .expect("put")
    });

    let journal = LmdbJournal::open(&path).expect("reopen");
    let result = journal.append(SagaId::new(7), event());
    assert!(
        result.is_err(),
        "u64::MAX allocator must fail, got {result:?}"
    );
    drop(journal);
    assert_eq!(before, tamper_meta(&path, |_, _| {}));
}

#[test]
fn fresh_empty_journal_starts_at_one() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal = LmdbJournal::open(&temp.path().join("journal")).expect("open");
    assert_eq!(journal.append(SagaId::new(1), event()).expect("append"), 1);
}
