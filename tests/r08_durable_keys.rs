//! R08 / ADR-0001 §2.4–§2.6: run-scoped durable keys for the in-crate journal and dedupe stores.

use icanact_saga_choreography::{
    InMemoryDedupe, InMemoryJournal, JournalError, ParticipantDedupeStore, ParticipantEvent,
    ParticipantJournal, RunIncarnation, RunKey, RunTerminalOutcome, RunTombstone, SagaId,
};

fn run(saga_type: &str, id: u64, incarnation: u64) -> RunKey {
    RunKey::new(saga_type, SagaId::new(id), RunIncarnation::new(incarnation))
}

fn started(marker: u64) -> ParticipantEvent {
    ParticipantEvent::StepExecutionStarted {
        attempt: 1,
        started_at_millis: marker,
    }
}

fn markers(rows: &[icanact_saga_choreography::JournalEntry]) -> Vec<u64> {
    rows.iter()
        .map(|row| match &row.event {
            ParticipantEvent::StepExecutionStarted {
                started_at_millis, ..
            } => *started_at_millis,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

fn tombstone(run: &RunKey) -> RunTombstone {
    RunTombstone::new(run.clone(), RunTerminalOutcome::Completed, 5_000)
}

fn assert_runs_are_isolated(journal: &dyn ParticipantJournal) {
    let order = run("order", 7, 100);
    let payment = run("payment", 7, 100);
    journal
        .append_run(&order, started(1))
        .expect("append order");
    journal
        .append_run(&payment, started(2))
        .expect("append payment");
    journal
        .append_run(&order, started(3))
        .expect("append order");
    assert_eq!(markers(&journal.read_run(&order).expect("read")), [1, 3]);
    assert_eq!(markers(&journal.read_run(&payment).expect("read")), [2]);
    let mut expected = vec![order, payment];
    expected.sort();
    assert_eq!(journal.list_runs().expect("list_runs"), expected);
}

fn assert_finalize_is_scoped(journal: &dyn ParticipantJournal) {
    let old = run("order", 9, 100);
    let live = run("order", 9, 200);
    let other_type = run("payment", 9, 100);
    for (key, marker) in [(&old, 1), (&live, 2), (&other_type, 3)] {
        journal.append_run(key, started(marker)).expect("append");
    }
    // Pre-existing tombstones: one expired (incarnation < cutoff), one retained.
    let expired = run("order", 8, 10);
    let retained = run("order", 8, 150);
    journal
        .finalize_run(&tombstone(&expired), RunIncarnation::new(0))
        .expect("seed expired tombstone");
    journal
        .finalize_run(&tombstone(&retained), RunIncarnation::new(0))
        .expect("seed retained tombstone");

    journal
        .finalize_run(&tombstone(&old), RunIncarnation::new(50))
        .expect("finalize");

    assert!(journal.read_run(&old).expect("read").is_empty());
    assert_eq!(markers(&journal.read_run(&live).expect("read")), [2]);
    assert_eq!(markers(&journal.read_run(&other_type).expect("read")), [3]);
    let nine: Vec<RunKey> = journal
        .run_tombstones("order", SagaId::new(9))
        .expect("tombstones")
        .iter()
        .map(|t| t.run().clone())
        .collect();
    assert_eq!(nine, std::slice::from_ref(&old));
    let eight: Vec<RunKey> = journal
        .run_tombstones("order", SagaId::new(8))
        .expect("tombstones")
        .iter()
        .map(|t| t.run().clone())
        .collect();
    assert_eq!(
        eight,
        [retained],
        "expired tombstone deleted in the same txn"
    );
    // Remaining expired tombstones are pruned by the explicit sweep.
    assert_eq!(
        journal
            .prune_expired_tombstones(RunIncarnation::new(1_000))
            .expect("prune"),
        2
    );
    assert!(
        journal
            .run_tombstones("order", SagaId::new(9))
            .expect("tombstones")
            .is_empty()
    );
}

fn assert_legacy_union(journal: &dyn ParticipantJournal) {
    let id = SagaId::new(3);
    let a = run("order", 3, 100);
    let b = run("payment", 3, 100);
    journal.append(id, started(1)).expect("legacy append");
    journal.append_run(&a, started(2)).expect("append");
    journal.append_run(&b, started(3)).expect("append");
    journal
        .append_run(&run("order", 4, 100), started(4))
        .expect("append");
    assert_eq!(markers(&journal.read(id).expect("read")), [1, 2, 3]);
    let ids: Vec<u64> = journal
        .list_sagas()
        .expect("list_sagas")
        .into_iter()
        .map(|id| id.get())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, [3, 4]);
    assert_eq!(ids.len(), 2, "union must not duplicate ids");
    journal.prune(id).expect("prune");
    assert!(journal.read(id).expect("read").is_empty());
    assert!(journal.read_run(&a).expect("read").is_empty());
    assert!(journal.read_run(&b).expect("read").is_empty());
    assert_eq!(
        markers(&journal.read_run(&run("order", 4, 100)).expect("read")),
        [4]
    );
}

fn assert_rejects_nested_transition(journal: &dyn ParticipantJournal) {
    let key = run("order", 5, 100);
    let nested = ParticipantEvent::TransitionCommitted {
        transition: Box::new(ParticipantEvent::TransitionCommitted {
            transition: Box::new(started(1)),
            outbox: Vec::new(),
        }),
        outbox: Vec::new(),
    };
    assert!(matches!(
        journal.append_run(&key, nested),
        Err(JournalError::Storage(_))
    ));
    assert!(journal.read_run(&key).expect("read").is_empty());
}

fn assert_dedupe_isolated(dedupe: &dyn ParticipantDedupeStore) {
    let order = run("order", 7, 100);
    let payment = run("payment", 7, 100);
    let newer = run("order", 7, 200);
    assert!(dedupe.check_and_mark_run(&order, "k1").expect("mark"));
    assert!(!dedupe.check_and_mark_run(&order, "k1").expect("mark"));
    assert!(dedupe.check_and_mark_run(&payment, "k1").expect("mark"));
    assert!(dedupe.check_and_mark_run(&newer, "k1").expect("mark"));
    dedupe.mark_processed_run(&order, "k2").expect("mark");
    assert!(dedupe.contains_run(&order, "k2").expect("contains"));
    assert!(!dedupe.contains_run(&payment, "k2").expect("contains"));
    assert_eq!(
        dedupe.keys_run(&order).expect("keys"),
        vec!["k1".into(), "k2".into()]
    );
    let mut expected = vec![order.clone(), payment.clone(), newer.clone()];
    expected.sort();
    assert_eq!(dedupe.list_runs().expect("list_runs"), expected);

    dedupe.remove_processed_run(&order, "k1").expect("remove");
    assert!(!dedupe.contains_run(&order, "k1").expect("contains"));
    assert!(dedupe.contains_run(&payment, "k1").expect("contains"));

    // Expiry sweep: only runs with incarnation < cutoff, counted per run.
    assert_eq!(
        dedupe
            .prune_expired(RunIncarnation::new(150))
            .expect("prune_expired"),
        2
    );
    assert_eq!(dedupe.list_runs().expect("list_runs"), vec![newer.clone()]);
    dedupe.prune_run(&newer).expect("prune_run");
    assert!(dedupe.list_runs().expect("list_runs").is_empty());
    assert!(dedupe.keys_run(&newer).expect("keys").is_empty());
}

fn assert_dedupe_legacy_prune_is_union(dedupe: &dyn ParticipantDedupeStore) {
    let id = SagaId::new(11);
    let a = run("order", 11, 100);
    dedupe.mark_processed(id, "legacy").expect("mark");
    dedupe.mark_processed_run(&a, "run").expect("mark");
    let keep = run("order", 12, 100);
    dedupe.mark_processed_run(&keep, "run").expect("mark");
    dedupe.prune(id).expect("prune");
    assert!(!dedupe.contains(id, "legacy").expect("contains"));
    assert!(!dedupe.contains_run(&a, "run").expect("contains"));
    assert!(dedupe.contains_run(&keep, "run").expect("contains"));
}

#[test]
fn in_memory_journal_is_run_scoped() {
    assert_runs_are_isolated(&InMemoryJournal::new());
    assert_finalize_is_scoped(&InMemoryJournal::new());
    assert_legacy_union(&InMemoryJournal::new());
    assert_rejects_nested_transition(&InMemoryJournal::new());
}

#[test]
fn in_memory_dedupe_is_run_scoped() {
    assert_dedupe_isolated(&InMemoryDedupe::new());
    assert_dedupe_legacy_prune_is_union(&InMemoryDedupe::new());
}

#[cfg(feature = "lmdb")]
mod lmdb {
    use super::*;
    use heed::EnvOpenOptions;
    use heed::types::Str;
    use icanact_saga_choreography::durability::lmdb::{LmdbDedupe, LmdbJournal};
    use std::path::Path;

    fn set_meta(path: &Path, version: &str) {
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(16)
                .map_size(1024 * 1024 * 1024)
                .open(path)
        }
        .expect("raw env");
        let mut wtxn = env.write_txn().expect("write txn");
        let meta = env
            .create_database::<Str, Str>(&mut wtxn, Some("journal_meta"))
            .expect("meta db");
        meta.put(&mut wtxn, "journal_schema_version", version)
            .expect("put");
        wtxn.commit().expect("commit");
    }

    fn schema(path: &Path) -> String {
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(16)
                .map_size(1024 * 1024 * 1024)
                .open(path)
        }
        .expect("raw env");
        let mut wtxn = env.write_txn().expect("write txn");
        let meta = env
            .create_database::<Str, Str>(&mut wtxn, Some("journal_meta"))
            .expect("meta db");
        let version = meta
            .get(&wtxn, "journal_schema_version")
            .expect("get")
            .expect("stamped")
            .to_owned();
        wtxn.commit().expect("commit");
        version
    }

    #[test]
    fn lmdb_journal_is_run_scoped_and_survives_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("journal");
        {
            let journal = LmdbJournal::open(&path).expect("open");
            assert_runs_are_isolated(&journal);
        }
        let journal = LmdbJournal::open(&path).expect("reopen");
        let order = run("order", 7, 100);
        let payment = run("payment", 7, 100);
        assert_eq!(markers(&journal.read_run(&order).expect("read")), [1, 3]);
        assert_eq!(markers(&journal.read_run(&payment).expect("read")), [2]);
        // Sequence allocator is shared and stays monotonic after reopen.
        let next = journal.append_run(&order, started(9)).expect("append");
        assert_eq!(next, 4);
    }

    #[test]
    fn lmdb_finalize_union_and_nested_rejection() {
        let temp = tempfile::tempdir().expect("tempdir");
        assert_finalize_is_scoped(&LmdbJournal::open(&temp.path().join("a")).expect("open"));
        assert_legacy_union(&LmdbJournal::open(&temp.path().join("b")).expect("open"));
        assert_rejects_nested_transition(&LmdbJournal::open(&temp.path().join("c")).expect("open"));
    }

    #[test]
    fn lmdb_schema_2_with_rows_is_refused_with_drain_message() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("journal");
        {
            let journal = LmdbJournal::open(&path).expect("open");
            journal.append(SagaId::new(1), started(1)).expect("append");
            journal.append(SagaId::new(1), started(2)).expect("append");
        }
        set_meta(&path, "2");
        let err = LmdbJournal::open(&path).expect_err("legacy rows must be refused");
        assert!(
            matches!(err, JournalError::LegacyRunIdentity { legacy_rows: 2 }),
            "unexpected error: {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("drain"), "{message}");
        assert!(
            message.contains("docs/upgrade.md#run-identity"),
            "{message}"
        );
    }

    #[test]
    fn lmdb_schema_2_without_rows_upgrades_in_place() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("journal");
        drop(LmdbJournal::open(&path).expect("open"));
        set_meta(&path, "2");
        drop(LmdbJournal::open(&path).expect("empty schema 2 upgrades"));
        assert_eq!(schema(&path), "3");
    }

    #[test]
    fn lmdb_dedupe_is_run_scoped_and_survives_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("dedupe");
        {
            let dedupe = LmdbDedupe::open(&path).expect("open");
            let order = run("order", 7, 100);
            let payment = run("payment", 7, 100);
            assert!(dedupe.check_and_mark_run(&order, "k").expect("mark"));
            assert!(dedupe.check_and_mark_run(&payment, "k").expect("mark"));
        }
        let dedupe = LmdbDedupe::open(&path).expect("reopen");
        assert!(
            !dedupe
                .check_and_mark_run(&run("order", 7, 100), "k")
                .expect("mark")
        );
        assert!(
            !dedupe
                .check_and_mark_run(&run("payment", 7, 100), "k")
                .expect("mark")
        );
        drop(dedupe);
        assert_dedupe_isolated(&LmdbDedupe::open(&temp.path().join("d2")).expect("open"));
        assert_dedupe_legacy_prune_is_union(
            &LmdbDedupe::open(&temp.path().join("d3")).expect("open"),
        );
    }
}
