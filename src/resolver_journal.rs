use std::sync::Mutex;

use crate::SagaChoreographyEvent;

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
        state
            .entries
            .push(TerminalResolverJournalEntry { sequence, event });
        Ok(sequence)
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
        SagaChoreographyEvent, TerminalResolverJournal, TerminalResolverJournalEntry,
        TerminalResolverJournalError,
    };

    const SCHEMA_KEY: &str = "schema_version";
    const SCHEMA_VERSION: &str = "1";
    const DEFAULT_MAP_SIZE_BYTES: usize = 256 * 1024 * 1024;

    #[derive(Debug)]
    pub struct LmdbTerminalResolverJournal {
        env: Env,
        rows: Database<Str, Bytes>,
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
            let meta = env
                .create_database::<Str, Str>(&mut wtxn, Some("resolver_meta"))
                .map_err(storage_error)?;
            match meta.get(&wtxn, SCHEMA_KEY).map_err(storage_error)? {
                Some(version) if version == SCHEMA_VERSION => {}
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
            Ok(Self { env, rows, meta })
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
            wtxn.commit().map_err(storage_error)?;
            Ok(sequence)
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
            let mut fences: HashMap<RunKey, u64> = HashMap::new();
            for (_, entry) in &decoded {
                if matches!(entry.event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                    quarantined.insert(run_key(&entry.event));
                } else if is_terminal(&entry.event) {
                    fences
                        .entry(run_key(&entry.event))
                        .or_insert(entry.sequence);
                }
            }
            let mut removed = 0u64;
            for (key, entry) in &decoded {
                let run = run_key(&entry.event);
                if quarantined.contains(&run) {
                    continue;
                }
                let Some(fence) = fences.get(&run) else {
                    continue; // unresolved run: keep everything
                };
                if *fence != entry.sequence {
                    self.rows.delete(&mut wtxn, key).map_err(storage_error)?;
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
