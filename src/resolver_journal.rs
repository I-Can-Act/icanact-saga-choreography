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
}

pub trait TerminalResolverJournal: Send + Sync + 'static {
    fn append(&self, event: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError>;

    fn read_all(&self) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>;
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
        state.next_sequence = state.next_sequence.saturating_add(1);
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
            std::fs::create_dir_all(path).map_err(storage_error)?;
            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(4)
                    .map_size(DEFAULT_MAP_SIZE_BYTES)
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
            let next = sequence.saturating_add(1).to_string();
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
                let entry =
                    rkyv::from_bytes::<TerminalResolverJournalEntry, rkyv::rancor::Error>(bytes)
                        .map_err(storage_error)?;
                entries.push(entry);
            }
            entries.sort_by_key(|entry| entry.sequence);
            Ok(entries)
        }
    }

    fn storage_error(error: impl std::fmt::Display) -> TerminalResolverJournalError {
        TerminalResolverJournalError::Storage(error.to_string().into())
    }
}
