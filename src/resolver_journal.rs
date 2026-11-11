use std::sync::Mutex;

use crate::journal::{MemoryRuns, RunSummary, context_key};
use crate::{SagaChoreographyEvent, SagaId};

#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TerminalResolverJournalEntry {
    pub sequence: u64,
    pub event: SagaChoreographyEvent,
}

#[derive(Debug, thiserror::Error)]
pub enum TerminalResolverJournalError {
    #[error("terminal resolver journal storage error: {0}")]
    Storage(Box<str>),
    #[error("terminal resolver journal sequence space exhausted")]
    SequenceExhausted,
    #[error("terminal resolver journal maintenance unsupported: {0}")]
    Unsupported(Box<str>),
}

pub trait TerminalResolverJournal: Send + Sync + 'static {
    fn append(&self, event: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError>;
    fn read_all(&self) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>;
    fn supports_saga_lookup(&self) -> bool {
        false
    }
    fn read_saga(
        &self,
        _saga_id: SagaId,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "per-saga lookup".into(),
        ))
    }
    fn read_saga_bounded(
        &self,
        _saga_id: SagaId,
        _max_entries: usize,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "bounded per-saga lookup".into(),
        ))
    }
    /// Streams unresolved/quarantined evidence without a read_all fallback.
    /// Visitors must not reenter a writing method on this journal.
    fn visit_unresolved_runs(
        &self,
        _visitor: &mut dyn FnMut(
            &TerminalResolverJournalEntry,
        ) -> Result<(), TerminalResolverJournalError>,
    ) -> Result<(), TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "streaming unresolved runs".into(),
        ))
    }
    /// At most 256 runs and 256 detail deletions in a write transaction.
    /// Failed-run effect fingerprints and the first terminal fence survive.
    fn compact_terminal_detail_batched(
        &self,
        _batch_limit: usize,
    ) -> Result<usize, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "compaction".into(),
        ))
    }
    /// Compatibility entry point. Each iteration commits a bounded batch.
    fn compact_terminal_detail(&self) -> Result<usize, TerminalResolverJournalError> {
        let mut total = 0;
        loop {
            let changed = self.compact_terminal_detail_batched(256)?;
            total += changed;
            if changed == 0 {
                return Ok(total);
            }
        }
    }
    /// Explicitly ends replay protection for ordinary runs older than cutoff.
    fn prune_resolved_before(
        &self,
        _cutoff_millis: u64,
        _batch_limit: usize,
    ) -> Result<usize, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported("pruning".into()))
    }
}

fn phase(event: &SagaChoreographyEvent) -> u8 {
    match event {
        SagaChoreographyEvent::SagaCompleted { .. } => 1,
        SagaChoreographyEvent::SagaFailed { .. } => 2,
        SagaChoreographyEvent::SagaQuarantined { .. } => 3,
        _ => 0,
    }
}
fn keep_effect(entry: &TerminalResolverJournalEntry, summary: &RunSummary) -> bool {
    summary.phase == 2
        && matches!(
            entry.event,
            SagaChoreographyEvent::StepCompleted {
                compensation_available: true,
                ..
            } | SagaChoreographyEvent::StepAccepted { .. }
        )
}
fn storage(error: impl std::fmt::Display) -> TerminalResolverJournalError {
    TerminalResolverJournalError::Storage(error.to_string().into())
}

pub struct InMemoryTerminalResolverJournal {
    state: Mutex<MemoryRuns<TerminalResolverJournalEntry>>,
}
impl Default for InMemoryTerminalResolverJournal {
    fn default() -> Self {
        Self::with_capacity(4096, 4096)
    }
}
impl InMemoryTerminalResolverJournal {
    pub fn with_capacity(terminal_capacity: usize, protected_capacity: usize) -> Self {
        Self {
            state: Mutex::new(MemoryRuns::new(terminal_capacity, protected_capacity)),
        }
    }
    fn state(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, MemoryRuns<TerminalResolverJournalEntry>>,
        TerminalResolverJournalError,
    > {
        self.state
            .lock()
            .map_err(|_| storage("in-memory resolver journal lock poisoned"))
    }
}
impl TerminalResolverJournal for InMemoryTerminalResolverJournal {
    fn append(&self, event: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError> {
        let mut state = self.state()?;
        let key = context_key(event.context());
        state.check_new(&key).map_err(storage)?;
        let sequence = state.next;
        let next = sequence
            .checked_add(1)
            .ok_or(TerminalResolverJournalError::SequenceExhausted)?;
        let kind = phase(&event);
        let at = event.context().event_timestamp_millis;
        state.insert(
            key,
            sequence,
            TerminalResolverJournalEntry { sequence, event },
            kind,
            at,
        );
        state.next = next;
        Ok(sequence)
    }
    fn supports_saga_lookup(&self) -> bool {
        true
    }
    fn read_saga(
        &self,
        saga_id: SagaId,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        self.read_saga_bounded(saga_id, usize::MAX)
    }
    fn read_saga_bounded(
        &self,
        saga_id: SagaId,
        max_entries: usize,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        let state = self.state()?;
        let keys = state.by_saga.get(&saga_id.get());
        if keys
            .into_iter()
            .flatten()
            .map(|k| state.runs[k].rows.len())
            .sum::<usize>()
            > max_entries
        {
            return Err(storage("per-saga lookup capacity exceeded"));
        }
        let mut rows: Vec<_> = keys
            .into_iter()
            .flatten()
            .flat_map(|key| state.runs[key].rows.values().cloned())
            .collect();
        rows.sort_by_key(|e| e.sequence);
        Ok(rows)
    }
    fn read_all(&self) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        let mut rows: Vec<_> = self
            .state()?
            .runs
            .values()
            .flat_map(|r| r.rows.values().cloned())
            .collect();
        rows.sort_by_key(|e| e.sequence);
        Ok(rows)
    }
    fn visit_unresolved_runs(
        &self,
        visitor: &mut dyn FnMut(
            &TerminalResolverJournalEntry,
        ) -> Result<(), TerminalResolverJournalError>,
    ) -> Result<(), TerminalResolverJournalError> {
        let state = self.state()?;
        for key in &state.unresolved {
            for entry in state.runs[key].rows.values() {
                visitor(entry)?;
            }
        }
        Ok(())
    }
    fn compact_terminal_detail_batched(
        &self,
        batch_limit: usize,
    ) -> Result<usize, TerminalResolverJournalError> {
        Ok(self.state()?.maintain(None, batch_limit, keep_effect))
    }
    fn prune_resolved_before(
        &self,
        cutoff_millis: u64,
        batch_limit: usize,
    ) -> Result<usize, TerminalResolverJournalError> {
        Ok(self
            .state()?
            .maintain(Some(cutoff_millis), batch_limit, keep_effect))
    }
}

#[cfg(feature = "lmdb")]
pub mod lmdb {
    use super::*;
    use crate::lmdb_owner::{IndexedStore, LmdbEnvOwner, run_key};
    use std::path::Path;

    #[derive(Debug)]
    pub struct LmdbTerminalResolverJournal {
        db: IndexedStore,
    }
    impl LmdbTerminalResolverJournal {
        pub fn open(path: &Path) -> Result<Self, TerminalResolverJournalError> {
            let owner = LmdbEnvOwner::acquire(path).map_err(storage)?;
            Self::with_owner(path, owner)
        }
        pub fn open_with_map_size(
            path: &Path,
            map_size_bytes: usize,
        ) -> Result<Self, TerminalResolverJournalError> {
            let owner = LmdbEnvOwner::open(
                path,
                heed::EnvOpenOptions::new()
                    .max_dbs(16)
                    .map_size(map_size_bytes),
            )
            .map_err(storage)?;
            Self::with_owner(path, owner)
        }
        fn with_owner(
            path: &Path,
            owner: LmdbEnvOwner,
        ) -> Result<Self, TerminalResolverJournalError> {
            Ok(Self {
                db: IndexedStore::open(owner, path, "resolver", "r:".into()).map_err(storage)?,
            })
        }
        #[cfg(test)]
        pub(super) fn seed_next_sequence_for_test(&self, value: u64) {
            let mut txn = self.db.env.write_txn().unwrap();
            self.db
                .meta
                .put(&mut txn, &self.db.counter, &value.to_string())
                .unwrap();
            txn.commit().unwrap();
        }
        #[cfg(test)]
        pub(super) fn next_sequence_for_test(&self) -> String {
            let txn = self.db.env.read_txn().unwrap();
            self.db
                .meta
                .get(&txn, &self.db.counter)
                .unwrap()
                .unwrap()
                .to_owned()
        }
        #[cfg(test)]
        pub(super) fn raw_rows_for_test(&self) -> Vec<(String, Vec<u8>)> {
            let txn = self.db.env.read_txn().unwrap();
            self.db
                .rows
                .iter(&txn)
                .unwrap()
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (k.to_owned(), v.to_vec())
                })
                .collect()
        }
    }
    impl TerminalResolverJournal for LmdbTerminalResolverJournal {
        fn append(
            &self,
            event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            let mut txn = self.db.env.write_txn().map_err(storage)?;
            let key =
                run_key(&self.db.namespace, &context_key(event.context())).map_err(storage)?;
            let sequence = self.db.next(&mut txn, 1).map_err(|e| {
                if e == "sequence space exhausted" {
                    TerminalResolverJournalError::SequenceExhausted
                } else {
                    storage(e)
                }
            })?;
            let kind = phase(&event);
            let at = event.context().event_timestamp_millis;
            let entry = TerminalResolverJournalEntry { sequence, event };
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&entry).map_err(storage)?;
            self.db
                .put_row(&mut txn, &key, sequence, &bytes, kind, at)
                .map_err(storage)?;
            txn.commit().map_err(storage)?;
            Ok(sequence)
        }
        fn supports_saga_lookup(&self) -> bool {
            true
        }
        fn read_saga(
            &self,
            saga_id: SagaId,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            self.read_saga_bounded(saga_id, usize::MAX)
        }
        fn read_saga_bounded(
            &self,
            saga_id: SagaId,
            max_entries: usize,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            let txn = self.db.env.read_txn().map_err(storage)?;
            let prefix = format!("{}{:020}:", self.db.namespace, saga_id.get());
            let mut rows = Vec::new();
            for row in self.db.rows.prefix_iter(&txn, &prefix).map_err(storage)? {
                let (key, bytes) = row.map_err(storage)?;
                if rows.len() == max_entries {
                    return Err(storage("per-saga lookup capacity exceeded"));
                }
                rows.push(decode_indexed_entry(key, bytes)?);
            }
            rows.sort_by_key(|e| e.sequence);
            Ok(rows)
        }
        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            let txn = self.db.env.read_txn().map_err(storage)?;
            let mut rows = Vec::new();
            for row in self.db.rows.iter(&txn).map_err(storage)? {
                let (key, bytes) = row.map_err(storage)?;
                rows.push(decode_indexed_entry(key, bytes)?);
            }
            rows.sort_by_key(|e| e.sequence);
            Ok(rows)
        }
        fn visit_unresolved_runs(
            &self,
            visitor: &mut dyn FnMut(
                &TerminalResolverJournalEntry,
            ) -> Result<(), TerminalResolverJournalError>,
        ) -> Result<(), TerminalResolverJournalError> {
            let txn = self.db.env.read_txn().map_err(storage)?;
            for run in self.db.unresolved.iter(&txn).map_err(storage)? {
                let (key, _) = run.map_err(storage)?;
                for row in self.db.rows.prefix_iter(&txn, key).map_err(storage)? {
                    let (row_key, bytes) = row.map_err(storage)?;
                    visitor(&decode_indexed_entry(row_key, bytes)?)?;
                }
            }
            Ok(())
        }
        fn compact_terminal_detail_batched(
            &self,
            batch_limit: usize,
        ) -> Result<usize, TerminalResolverJournalError> {
            self.db
                .maintain(
                    None,
                    batch_limit,
                    |key, bytes, summary| {
                        Ok(keep_effect(
                            &decode_indexed_entry(key, bytes).map_err(|e| e.to_string())?,
                            summary,
                        ))
                    },
                    None,
                )
                .map_err(storage)
        }
        fn prune_resolved_before(
            &self,
            cutoff_millis: u64,
            batch_limit: usize,
        ) -> Result<usize, TerminalResolverJournalError> {
            self.db
                .maintain(Some(cutoff_millis), batch_limit, |_, _, _| Ok(false), None)
                .map_err(storage)
        }
    }
    fn decode_indexed_entry(
        key: &str,
        bytes: &[u8],
    ) -> Result<TerminalResolverJournalEntry, TerminalResolverJournalError> {
        let entry = decode_entry(bytes)?;
        let run = run_key("r:", &context_key(entry.event.context())).map_err(storage)?;
        if key != format!("{run}{:020}", entry.sequence) {
            return Err(storage("resolver index row identity mismatch"));
        }
        Ok(entry)
    }
    fn decode_entry(
        bytes: &[u8],
    ) -> Result<TerminalResolverJournalEntry, TerminalResolverJournalError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<TerminalResolverJournalEntry, rkyv::rancor::Error>(&aligned)
            .map_err(storage)
    }
}

#[cfg(all(test, feature = "lmdb"))]
mod tests {
    use super::lmdb::LmdbTerminalResolverJournal;
    use super::{TerminalResolverJournal, TerminalResolverJournalError};
    use crate::{DeterministicContextBuilder, saga_started};

    fn event(saga_id: u64) -> crate::SagaChoreographyEvent {
        saga_started(
            DeterministicContextBuilder::default()
                .with_saga_id(saga_id)
                .build(),
            Vec::new(),
        )
    }

    #[test]
    fn resolver_journal_envs_close_on_drop() {
        let dir = tempfile::tempdir().expect("tempdir should open");
        for iteration in 0..600 {
            let journal = LmdbTerminalResolverJournal::open(
                &dir.path().join(format!("resolver-{iteration}")),
            )
            .unwrap_or_else(|err| panic!("resolver open failed at iteration {iteration}: {err}"));
            drop(journal);
        }
    }

    #[test]
    fn resolver_journal_preserves_rows_after_drop_and_reopen() {
        let dir = tempfile::tempdir().expect("tempdir should open");
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let sequence = journal.append(event(100)).unwrap();
        drop(journal);

        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let rows = journal.read_all().expect("rows should read back");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sequence, sequence);
        assert!(matches!(
            &rows[0].event,
            crate::SagaChoreographyEvent::SagaStarted { context, .. }
                if context.saga_id == crate::SagaId::new(100)
        ));
        let saga_rows = journal.read_saga(crate::SagaId::new(100)).unwrap();
        assert_eq!(saga_rows.len(), 1);
        assert_eq!(saga_rows[0].sequence, sequence);
        assert_eq!(journal.append(event(101)).unwrap(), sequence + 1);
    }

    #[test]
    fn resolver_journal_double_open_drop_and_reopen() {
        let dir = tempfile::tempdir().expect("tempdir should open");
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let second_journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        drop(journal);
        drop(second_journal);

        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        assert!(journal.read_all().unwrap().is_empty());
        assert_eq!(journal.append(event(100)).unwrap(), 1);
    }

    #[test]
    fn resolver_journal_drop_preserves_other_same_path_owners() {
        let dir = tempfile::tempdir().expect("tempdir should open");
        let first = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let second = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        drop(first);

        assert_eq!(second.append(event(100)).unwrap(), 1);
        assert_eq!(second.read_all().unwrap().len(), 1);
        let third = LmdbTerminalResolverJournal::open(dir.path())
            .expect("third open should succeed while the second owner is alive");
        assert_eq!(third.read_all().unwrap().len(), 1);
        drop(second);
        assert_eq!(third.append(event(101)).unwrap(), 2);
        drop(third);

        let reopened = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        assert_eq!(reopened.read_all().unwrap().len(), 2);
    }

    #[test]
    fn resolver_journal_concurrent_open_and_drop() {
        let dir = tempfile::tempdir().expect("tempdir should open");
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let path = dir.path();
                scope.spawn(move || {
                    for iteration in 0..100 {
                        let journal = LmdbTerminalResolverJournal::open(path).unwrap();
                        let saga_id = worker * 100 + iteration;
                        journal.append(event(saga_id)).unwrap();
                        assert_eq!(
                            journal
                                .read_saga(crate::SagaId::new(saga_id))
                                .unwrap()
                                .len(),
                            1
                        );
                    }
                });
            }
        });
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        assert_eq!(journal.read_all().unwrap().len(), 400);
    }

    #[test]
    fn resolver_journal_failed_open_releases_env_ownership() {
        let dir = tempfile::tempdir().expect("tempdir should open");
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .max_dbs(4)
                .map_size(256 * 1024 * 1024)
                .open(dir.path())
        }
        .unwrap();
        let mut wtxn = env.write_txn().unwrap();
        let meta = env
            .create_database::<heed::types::Str, heed::types::Str>(&mut wtxn, Some("resolver_meta"))
            .unwrap();
        meta.put(&mut wtxn, "schema_version", "unsupported")
            .unwrap();
        wtxn.commit().unwrap();
        let _ = env.prepare_for_closing();

        for _ in 0..2 {
            let err = LmdbTerminalResolverJournal::open(dir.path())
                .expect_err("incompatible schema must fail");
            assert!(
                err.to_string()
                    .contains("incompatible terminal resolver journal schema")
            );
            assert!(heed::env_closing_event(dir.path().canonicalize().unwrap()).is_none());
        }
    }

    #[test]
    fn resolver_journal_sequence_exhaustion_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        journal.seed_next_sequence_for_test(u64::MAX);
        // Land a row at the maximum key so an overwrite would be observable.
        let before = journal.raw_rows_for_test();

        let result = journal.append(event(2));
        assert!(
            matches!(result, Err(TerminalResolverJournalError::SequenceExhausted)),
            "append at u64::MAX must fail, got {result:?}"
        );
        assert_eq!(before, journal.raw_rows_for_test());
        assert_eq!(
            journal.next_sequence_for_test(),
            u64::MAX.to_string(),
            "metadata must be left untouched"
        );
    }

    #[test]
    fn resolver_journal_refuses_to_overwrite_existing_sequence_row() {
        let dir = tempfile::tempdir().unwrap();
        let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        assert_eq!(journal.append(event(1)).unwrap(), 1);
        let before = journal.raw_rows_for_test();
        journal.seed_next_sequence_for_test(1);

        let result = journal.append(event(2));
        assert!(result.is_err(), "existing key must not be overwritten");
        assert_eq!(before, journal.raw_rows_for_test());
    }
}

#[cfg(test)]
mod wave1c_tests {
    use super::*;
    use crate::DeterministicContextBuilder;
    fn start(id: u64) -> SagaChoreographyEvent {
        crate::saga_started(
            DeterministicContextBuilder::default()
                .with_saga_id(id)
                .build(),
            vec![],
        )
    }
    fn exercise(journal: &impl TerminalResolverJournal) {
        for id in 0..5 {
            let e = start(id);
            let context = e.context().clone();
            journal.append(e).unwrap();
            if id > 1 {
                journal
                    .append(SagaChoreographyEvent::SagaCompleted { context })
                    .unwrap();
            }
        }
        let mut count = 0;
        journal
            .visit_unresolved_runs(&mut |_entry| {
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(journal.compact_terminal_detail_batched(1).unwrap(), 1);
        assert_eq!(journal.prune_resolved_before(u64::MAX, 1).unwrap(), 1);
        assert!(!journal.read_saga(SagaId::new(0)).unwrap().is_empty());
    }
    fn effect_and_quarantine_preservation(journal: &impl TerminalResolverJournal) {
        for id in 0..3 {
            let e = start(id);
            let context = e.context().clone();
            journal.append(e).unwrap();
            journal
                .append(SagaChoreographyEvent::StepCompleted {
                    context: context.clone(),
                    output: vec![1],
                    saga_input: vec![2],
                    compensation_available: true,
                })
                .unwrap();
            if id == 0 {
                journal
                    .append(SagaChoreographyEvent::SagaFailed {
                        context,
                        reason: "failed".into(),
                        failure: None,
                    })
                    .unwrap();
            } else {
                journal
                    .append(SagaChoreographyEvent::SagaQuarantined {
                        context: context.clone(),
                        reason: "q".into(),
                        step: "step".into(),
                        participant_id: "p".into(),
                    })
                    .unwrap();
                journal
                    .append(SagaChoreographyEvent::SagaCompleted { context })
                    .unwrap();
            }
        }
        while journal.compact_terminal_detail_batched(1).unwrap() != 0 {}
        let failed = journal.read_saga(SagaId::new(0)).unwrap();
        assert_eq!(failed.len(), 2);
        assert!(
            failed
                .iter()
                .any(|e| matches!(e.event, SagaChoreographyEvent::StepCompleted { .. }))
        );
        assert_eq!(journal.read_saga(SagaId::new(1)).unwrap().len(), 4);
        let mut visits = 0;
        journal
            .visit_unresolved_runs(&mut |_| {
                visits += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visits, 8);
        assert_eq!(journal.prune_resolved_before(u64::MAX, 1).unwrap(), 1);
        assert_eq!(journal.read_saga(SagaId::new(2)).unwrap().len(), 4);
        assert!(
            journal
                .visit_unresolved_runs(&mut |_| Err(storage("visitor stop")))
                .is_err()
        );
    }
    #[test]
    fn memory_failed_effects_and_quarantine_survive_compaction() {
        effect_and_quarantine_preservation(&InMemoryTerminalResolverJournal::default());
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_failed_effects_and_quarantine_survive_compaction() {
        let dir = tempfile::tempdir().unwrap();
        effect_and_quarantine_preservation(
            &lmdb::LmdbTerminalResolverJournal::open(dir.path()).unwrap(),
        );
    }
    #[test]
    fn memory_default_fifo_caps_4096_ordinary_runs() {
        let journal = InMemoryTerminalResolverJournal::default();
        for id in 0..4098 {
            journal
                .append(SagaChoreographyEvent::SagaCompleted {
                    context: start(id).context().clone(),
                })
                .unwrap();
        }
        assert_eq!(journal.read_all().unwrap().len(), 4096);
        assert!(journal.read_saga(SagaId::new(1)).unwrap().is_empty());
    }

    fn run_batch_bound(journal: &impl TerminalResolverJournal) {
        for id in 0..300 {
            let e = start(id);
            let context = e.context().clone();
            journal.append(e).unwrap();
            journal
                .append(SagaChoreographyEvent::SagaCompleted { context })
                .unwrap();
        }
        assert_eq!(
            journal.compact_terminal_detail_batched(usize::MAX).unwrap(),
            256
        );
        assert_eq!(journal.read_all().unwrap().len(), 344);
        assert_eq!(journal.compact_terminal_detail_batched(256).unwrap(), 44);
        assert_eq!(journal.compact_terminal_detail_batched(256).unwrap(), 0);
        assert_eq!(
            journal.prune_resolved_before(u64::MAX, usize::MAX).unwrap(),
            256
        );
        assert_eq!(journal.read_all().unwrap().len(), 44);
        assert_eq!(journal.prune_resolved_before(u64::MAX, 256).unwrap(), 44);
        assert_eq!(journal.append(start(500)).unwrap(), 601);
    }
    #[test]
    fn memory_maintenance_limits_runs_per_batch() {
        run_batch_bound(&InMemoryTerminalResolverJournal::default());
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_maintenance_limits_runs_per_batch() {
        let dir = tempfile::tempdir().unwrap();
        run_batch_bound(&lmdb::LmdbTerminalResolverJournal::open(dir.path()).unwrap());
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_partial_compaction_resumes_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let journal = lmdb::LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let e = start(1);
        let context = e.context().clone();
        for _ in 0..600 {
            journal.append(e.clone()).unwrap();
        }
        journal
            .append(SagaChoreographyEvent::SagaCompleted { context })
            .unwrap();
        assert_eq!(journal.compact_terminal_detail_batched(256).unwrap(), 1);
        assert_eq!(journal.read_all().unwrap().len(), 345);
        drop(journal);
        let journal = lmdb::LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        assert_eq!(journal.compact_terminal_detail_batched(256).unwrap(), 1);
        assert_eq!(journal.read_all().unwrap().len(), 89);
        assert_eq!(journal.compact_terminal_detail_batched(256).unwrap(), 1);
        assert_eq!(journal.read_all().unwrap().len(), 1);
        assert_eq!(journal.append(start(2)).unwrap(), 602);
    }
    #[test]
    fn custom_streaming_reports_unsupported_without_read_all_fallback() {
        struct Custom;
        impl TerminalResolverJournal for Custom {
            fn append(
                &self,
                _: SagaChoreographyEvent,
            ) -> Result<u64, TerminalResolverJournalError> {
                unreachable!()
            }
            fn read_all(
                &self,
            ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>
            {
                panic!("must not load history")
            }
        }
        let journal: &dyn TerminalResolverJournal = &Custom;
        assert!(matches!(
            journal.visit_unresolved_runs(&mut |_| Ok(())),
            Err(TerminalResolverJournalError::Unsupported(_))
        ));
        assert!(matches!(
            journal.compact_terminal_detail(),
            Err(TerminalResolverJournalError::Unsupported(_))
        ));
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_index_identity_mismatch_is_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let journal = lmdb::LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let sequence = journal.append(start(1)).unwrap();
        let owner = crate::lmdb_owner::LmdbEnvOwner::acquire(dir.path()).unwrap();
        let mut txn = owner.env().write_txn().unwrap();
        let rows = owner
            .env()
            .open_database::<heed::types::Str, heed::types::Bytes>(&txn, Some("resolver_events"))
            .unwrap()
            .unwrap();
        let key = rows
            .iter(&txn)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0
            .to_owned();
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&TerminalResolverJournalEntry {
            sequence,
            event: start(2),
        })
        .unwrap();
        rows.put(&mut txn, &key, &encoded).unwrap();
        txn.commit().unwrap();
        assert!(journal.read_saga(SagaId::new(1)).is_err());
        assert!(journal.visit_unresolved_runs(&mut |_| Ok(())).is_err());
    }

    #[test]
    fn memory_streaming_and_bounded_maintenance() {
        exercise(&InMemoryTerminalResolverJournal::default());
    }
    #[test]
    fn memory_ordinary_fifo_cap() {
        let journal = InMemoryTerminalResolverJournal::with_capacity(2, 8);
        for id in 0..5 {
            let e = start(id);
            let context = e.context().clone();
            journal.append(e).unwrap();
            if id > 0 {
                journal
                    .append(SagaChoreographyEvent::SagaCompleted { context })
                    .unwrap();
            }
        }
        assert!(journal.read_saga(SagaId::new(1)).unwrap().is_empty());
        assert_eq!(journal.read_saga(SagaId::new(0)).unwrap().len(), 1);
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_streaming_and_bounded_maintenance() {
        let dir = tempfile::tempdir().unwrap();
        exercise(&lmdb::LmdbTerminalResolverJournal::open(dir.path()).unwrap());
    }
}
