use std::sync::Mutex;

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

    /// Whether [`Self::read_saga`] is a bounded, indexed lookup. The bus only
    /// evicts its in-memory admission fences when the journal can reload them;
    /// otherwise every fence stays resident and admission is refused at capacity.
    fn supports_saga_lookup(&self) -> bool {
        false
    }

    /// Every retained row of one saga id, in sequence order. Implementations
    /// that return `true` from [`Self::supports_saga_lookup`] must do so
    /// without scanning unrelated history.
    ///
    /// The default reports [`TerminalResolverJournalError::Unsupported`]; it
    /// never silently scans the whole journal.
    fn read_saga(
        &self,
        _saga_id: SagaId,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "this journal does not implement per-saga lookup".into(),
        ))
    }

    /// Indexed per-id read capped before cloning/decoding more than `max_entries`.
    /// Admission uses this method so one repeatedly reused id cannot turn a cache
    /// miss into an unbounded history allocation. Implementations advertising
    /// [`Self::supports_saga_lookup`] must implement it; unsupported is fail-closed,
    /// not permission to fall back to a global or unbounded read.
    fn read_saga_bounded(
        &self,
        _saga_id: SagaId,
        _max_entries: usize,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "this journal does not implement bounded per-saga lookup".into(),
        ))
    }

    /// Removes ordinarily resolved run detail while retaining its terminal replay
    /// fence. Every unresolved or quarantined run keeps all evidence. Returns the
    /// number of rows removed.
    ///
    /// The default reports [`TerminalResolverJournalError::Unsupported`]; it
    /// never pretends to compact.
    fn compact_terminal_detail(&self) -> Result<u64, TerminalResolverJournalError> {
        Err(TerminalResolverJournalError::Unsupported(
            "this journal does not implement compaction".into(),
        ))
    }
}

#[derive(Default)]
pub struct InMemoryTerminalResolverJournal {
    state: Mutex<InMemoryTerminalResolverJournalState>,
}

#[derive(Default)]
struct InMemoryTerminalResolverJournalState {
    next_sequence: u64,
    entries: Vec<TerminalResolverJournalEntry>,
    /// Positions in `entries` per saga id, in append order.
    by_saga: std::collections::HashMap<u64, Vec<usize>>,
}

impl TerminalResolverJournal for InMemoryTerminalResolverJournal {
    fn append(&self, event: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError> {
        let mut state = self.state.lock().map_err(|_| {
            TerminalResolverJournalError::Storage(
                "in-memory terminal resolver journal lock poisoned".into(),
            )
        })?;
        let sequence = state.next_sequence;
        let next = sequence
            .checked_add(1)
            .ok_or(TerminalResolverJournalError::SequenceExhausted)?;
        state.next_sequence = next;
        let position = state.entries.len();
        state
            .by_saga
            .entry(event.context().saga_id.get())
            .or_default()
            .push(position);
        state
            .entries
            .push(TerminalResolverJournalEntry { sequence, event });
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
        let state = self.state.lock().map_err(|_| {
            TerminalResolverJournalError::Storage(
                "in-memory terminal resolver journal lock poisoned".into(),
            )
        })?;
        let positions = state
            .by_saga
            .get(&saga_id.get())
            .map(Vec::as_slice)
            .unwrap_or_default();
        if positions.len() > max_entries {
            return Err(TerminalResolverJournalError::Storage(
                format!(
                    "per-saga lookup capacity exceeded; saga_id={} max_entries={max_entries}",
                    saga_id.get()
                )
                .into(),
            ));
        }
        Ok(positions
            .iter()
            .map(|position| state.entries[*position].clone())
            .collect())
    }

    fn read_all(&self) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        let state = self.state.lock().map_err(|_| {
            TerminalResolverJournalError::Storage(
                "in-memory terminal resolver journal lock poisoned".into(),
            )
        })?;
        Ok(state.entries.clone())
    }
}

#[cfg(feature = "lmdb")]
pub mod lmdb {
    use std::collections::{HashMap, HashSet};
    use std::path::Path;

    use heed::types::{Bytes, Str};
    use heed::{Database, Env, EnvOpenOptions};

    use super::{
        SagaChoreographyEvent, SagaId, TerminalResolverJournal, TerminalResolverJournalEntry,
        TerminalResolverJournalError,
    };

    const SCHEMA_KEY: &str = "schema_version";
    /// Version 2 adds the per-saga lookup index. Version 1 stores are migrated
    /// (backfilled) on open; version 1 binaries refuse a version 2 store.
    const SCHEMA_VERSION: &str = "2";
    const LEGACY_SCHEMA_VERSION: &str = "1";
    const DEFAULT_MAP_SIZE_BYTES: usize = 256 * 1024 * 1024;

    #[derive(Debug)]
    pub struct LmdbTerminalResolverJournal {
        env: Env,
        rows: Database<Str, Bytes>,
        /// `{saga_id:020}:{sequence:020}` -> empty. Maintained in the same write
        /// transaction as every row insert or delete.
        saga_index: Database<Str, Bytes>,
        meta: Database<Str, Str>,
    }

    impl LmdbTerminalResolverJournal {
        pub fn open(path: &Path) -> Result<Self, TerminalResolverJournalError> {
            Self::open_with_map_size(path, DEFAULT_MAP_SIZE_BYTES)
        }

        pub fn open_with_map_size(
            path: &Path,
            map_size_bytes: usize,
        ) -> Result<Self, TerminalResolverJournalError> {
            std::fs::create_dir_all(path).map_err(storage_error)?;
            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(4)
                    .map_size(map_size_bytes)
                    .open(path)
            }
            .map_err(storage_error)?;
            let mut wtxn = env.write_txn().map_err(storage_error)?;
            let rows = env
                .create_database::<Str, Bytes>(&mut wtxn, Some("resolver_events"))
                .map_err(storage_error)?;
            let saga_index = env
                .create_database::<Str, Bytes>(&mut wtxn, Some("resolver_saga_index"))
                .map_err(storage_error)?;
            let meta = env
                .create_database::<Str, Str>(&mut wtxn, Some("resolver_meta"))
                .map_err(storage_error)?;
            match meta.get(&wtxn, SCHEMA_KEY).map_err(storage_error)? {
                Some(version) if version == SCHEMA_VERSION => {}
                Some(version) if version == LEGACY_SCHEMA_VERSION => {
                    // One transaction: the rebuilt index and the new version
                    // commit together or not at all.
                    saga_index.clear(&mut wtxn).map_err(storage_error)?;
                    let mut keys = Vec::new();
                    for row in rows.iter(&wtxn).map_err(storage_error)? {
                        let (_, bytes) = row.map_err(storage_error)?;
                        let entry = decode_entry(bytes)?;
                        keys.push(index_key(entry.event.context().saga_id, entry.sequence));
                    }
                    for key in &keys {
                        saga_index.put(&mut wtxn, key, &[]).map_err(storage_error)?;
                    }
                    meta.put(&mut wtxn, SCHEMA_KEY, SCHEMA_VERSION)
                        .map_err(storage_error)?;
                }
                Some(version) => {
                    return Err(TerminalResolverJournalError::Storage(
                        format!(
                            "incompatible terminal resolver journal schema: stored={version} required={SCHEMA_VERSION}"
                        )
                        .into(),
                    ));
                }
                None => {
                    meta.put(&mut wtxn, SCHEMA_KEY, SCHEMA_VERSION)
                        .map_err(storage_error)?;
                    meta.put(&mut wtxn, "next_sequence", "1")
                        .map_err(storage_error)?;
                }
            }
            wtxn.commit().map_err(storage_error)?;
            Ok(Self {
                env,
                rows,
                saga_index,
                meta,
            })
        }

        #[cfg(test)]
        pub(super) fn seed_next_sequence_for_test(&self, value: u64) {
            let mut wtxn = self.env.write_txn().unwrap();
            self.meta
                .put(&mut wtxn, "next_sequence", &value.to_string())
                .unwrap();
            wtxn.commit().unwrap();
        }

        #[cfg(test)]
        pub(super) fn next_sequence_for_test(&self) -> String {
            let rtxn = self.env.read_txn().unwrap();
            self.meta
                .get(&rtxn, "next_sequence")
                .unwrap()
                .unwrap()
                .to_owned()
        }

        #[cfg(test)]
        pub(super) fn raw_rows_for_test(&self) -> Vec<(String, Vec<u8>)> {
            let rtxn = self.env.read_txn().unwrap();
            self.rows
                .iter(&rtxn)
                .unwrap()
                .map(|row| {
                    let (k, v) = row.unwrap();
                    (k.to_owned(), v.to_vec())
                })
                .collect()
        }

        fn next_sequence(
            meta: &Database<Str, Str>,
            wtxn: &mut heed::RwTxn<'_>,
        ) -> Result<u64, TerminalResolverJournalError> {
            let Some(raw) = meta.get(wtxn, "next_sequence").map_err(storage_error)? else {
                return Err(TerminalResolverJournalError::Storage(
                    "terminal resolver journal next_sequence is missing".into(),
                ));
            };
            let sequence = raw.parse::<u64>().map_err(storage_error)?;
            let next = sequence
                .checked_add(1)
                .ok_or(TerminalResolverJournalError::SequenceExhausted)?
                .to_string();
            meta.put(wtxn, "next_sequence", &next)
                .map_err(storage_error)?;
            Ok(sequence)
        }
    }

    impl TerminalResolverJournal for LmdbTerminalResolverJournal {
        fn append(
            &self,
            event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            let mut wtxn = self.env.write_txn().map_err(storage_error)?;
            let sequence = Self::next_sequence(&self.meta, &mut wtxn)?;
            let entry = TerminalResolverJournalEntry { sequence, event };
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&entry).map_err(storage_error)?;
            let key = format!("{sequence:020}");
            if self.rows.get(&wtxn, &key).map_err(storage_error)?.is_some() {
                return Err(TerminalResolverJournalError::Storage(
                    format!("terminal resolver journal row already exists for sequence {sequence}")
                        .into(),
                ));
            }
            self.rows
                .put(&mut wtxn, &key, encoded.as_ref())
                .map_err(storage_error)?;
            self.saga_index
                .put(
                    &mut wtxn,
                    &index_key(entry.event.context().saga_id, sequence),
                    &[],
                )
                .map_err(storage_error)?;
            wtxn.commit().map_err(storage_error)?;
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
            let rtxn = self.env.read_txn().map_err(storage_error)?;
            let prefix = format!("{:020}:", saga_id.get());
            let mut entries = Vec::new();
            for row in self
                .saga_index
                .prefix_iter(&rtxn, &prefix)
                .map_err(storage_error)?
            {
                let (key, _) = row.map_err(storage_error)?;
                if entries.len() == max_entries {
                    return Err(TerminalResolverJournalError::Storage(
                        format!("per-saga lookup capacity exceeded; saga_id={} max_entries={max_entries}", saga_id.get()).into(),
                    ));
                }
                let sequence = key
                    .strip_prefix(prefix.as_str())
                    .ok_or_else(|| storage_error("malformed saga index key"))?;
                let Some(bytes) = self.rows.get(&rtxn, sequence).map_err(storage_error)? else {
                    return Err(TerminalResolverJournalError::Storage(
                        format!("saga index references a missing row: {key}").into(),
                    ));
                };
                let entry = decode_entry(bytes)?;
                let indexed_sequence: u64 = sequence.parse().map_err(storage_error)?;
                if entry.sequence != indexed_sequence || entry.event.context().saga_id != saga_id {
                    return Err(storage_error("saga index row identity mismatch"));
                }
                entries.push(entry);
            }
            Ok(entries)
        }

        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            let rtxn = self.env.read_txn().map_err(storage_error)?;
            let iter = self.rows.iter(&rtxn).map_err(storage_error)?;
            let mut entries = Vec::new();
            for row in iter {
                let (_, bytes) = row.map_err(storage_error)?;
                entries.push(decode_entry(bytes)?);
            }
            entries.sort_by_key(|entry| entry.sequence);
            Ok(entries)
        }

        fn compact_terminal_detail(&self) -> Result<u64, TerminalResolverJournalError> {
            let mut wtxn = self.env.write_txn().map_err(storage_error)?;
            let mut decoded = Vec::new();
            for row in self.rows.iter(&wtxn).map_err(storage_error)? {
                let (key, bytes) = row.map_err(storage_error)?;
                decoded.push((key.to_owned(), decode_entry(bytes)?));
            }
            // Quarantine remains unresolved even if contradictory ordinary
            // terminal evidence also exists. Maintenance is not reconciliation.
            let mut quarantined = HashSet::new();
            let mut fences: HashMap<RunKey, (u64, bool)> = HashMap::new();
            for (_, entry) in &decoded {
                if matches!(entry.event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                    quarantined.insert(run_key(&entry.event));
                } else if is_terminal(&entry.event) {
                    fences.entry(run_key(&entry.event)).or_insert((
                        entry.sequence,
                        matches!(entry.event, SagaChoreographyEvent::SagaFailed { .. }),
                    ));
                }
            }
            let mut removed = 0u64;
            for (key, entry) in &decoded {
                let run = run_key(&entry.event);
                if quarantined.contains(&run) {
                    continue;
                }
                let Some((fence, failed)) = fences.get(&run) else {
                    continue; // unresolved run: keep everything
                };
                // A failed run may still see a replay of an effect it already
                // knew; keeping those fingerprints lets that replay be told apart
                // from genuinely new uncertainty. A successful run needs none.
                if *failed && is_effect_evidence(&entry.event) {
                    continue;
                }
                if *fence != entry.sequence {
                    self.rows.delete(&mut wtxn, key).map_err(storage_error)?;
                    self.saga_index
                        .delete(
                            &mut wtxn,
                            &index_key(entry.event.context().saga_id, entry.sequence),
                        )
                        .map_err(storage_error)?;
                    removed += 1;
                }
            }
            // next_sequence is untouched so removed sequences are never reused.
            wtxn.commit().map_err(storage_error)?;
            Ok(removed)
        }
    }

    type RunKey = (u64, Box<str>, u64);

    fn run_key(event: &SagaChoreographyEvent) -> RunKey {
        let context = event.context();
        (
            context.saga_id.get(),
            context.saga_type.clone(),
            context.saga_started_at_millis,
        )
    }

    fn index_key(saga_id: SagaId, sequence: u64) -> String {
        format!("{:020}:{sequence:020}", saga_id.get())
    }

    /// Compensable completions and accepted steps: the effect fingerprints.
    fn is_effect_evidence(event: &SagaChoreographyEvent) -> bool {
        matches!(
            event,
            SagaChoreographyEvent::StepCompleted {
                compensation_available: true,
                ..
            } | SagaChoreographyEvent::StepAccepted { .. }
        )
    }

    fn is_terminal(event: &SagaChoreographyEvent) -> bool {
        matches!(
            event,
            SagaChoreographyEvent::SagaCompleted { .. }
                | SagaChoreographyEvent::SagaFailed { .. }
                | SagaChoreographyEvent::SagaQuarantined { .. }
        )
    }

    /// Decodes a validated archive from an explicitly aligned owned copy;
    /// LMDB value slices carry no alignment guarantee.
    fn decode_entry(
        bytes: &[u8],
    ) -> Result<TerminalResolverJournalEntry, TerminalResolverJournalError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<TerminalResolverJournalEntry, rkyv::rancor::Error>(&aligned)
            .map_err(storage_error)
    }

    fn storage_error(error: impl std::fmt::Display) -> TerminalResolverJournalError {
        TerminalResolverJournalError::Storage(error.to_string().into())
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
