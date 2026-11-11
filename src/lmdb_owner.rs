//! Shared ownership of heed's process-global, path-keyed environment cache.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use heed::{Env, EnvOpenOptions};

static OWNERS: LazyLock<Mutex<HashMap<PathBuf, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub(crate) struct LmdbEnvOwner {
    key: PathBuf,
    env: Option<Env>,
}

impl LmdbEnvOwner {
    pub(crate) fn acquire(path: &Path) -> heed::Result<Self> {
        Self::open(
            path,
            EnvOpenOptions::new()
                .max_dbs(16)
                .map_size(map_size_bytes()?),
        )
    }

    pub(crate) fn open(path: &Path, options: &EnvOpenOptions) -> heed::Result<Self> {
        for legacy in ["journal", "dedupe"] {
            if path.join(legacy).join("data.mdb").exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("incompatible split LMDB layout: {legacy}"),
                )
                .into());
            }
        }
        std::fs::create_dir_all(path)?;
        let key = path.canonicalize()?;
        // Serialize open + registration with last-owner release + prepare, so
        // no opener can acquire an env between its last release and closing.
        let mut owners = OWNERS.lock().unwrap_or_else(|poison| poison.into_inner());
        let env = unsafe { options.open(&key) }?;
        *owners.entry(key.clone()).or_default() += 1;
        Ok(Self {
            key,
            env: Some(env),
        })
    }

    pub(crate) fn env(&self) -> &Env {
        self.env.as_ref().expect("live LMDB owner has an env")
    }
}

fn release(owners: &mut HashMap<PathBuf, usize>, key: &Path) -> bool {
    let Some(count) = owners.get_mut(key) else {
        return false;
    };
    if *count > 1 {
        *count -= 1;
        false
    } else {
        owners.remove(key);
        true
    }
}

impl Drop for LmdbEnvOwner {
    fn drop(&mut self) {
        let mut owners = OWNERS.lock().unwrap_or_else(|poison| poison.into_inner());
        let env = self.env.take();
        if release(&mut owners, &self.key) {
            // The owner must outlive all other env handles in its struct. Keep
            // the lock until prepare drops the final env; never wait here.
            // External heed users can have prepared this path already, so Drop
            // must absorb prepare_for_closing's panic in that case.
            if let Some(env) = env {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _ = env.prepare_for_closing();
                }));
            }
        } else {
            drop(env);
        }
    }
}

#[cfg(test)]
mod wave1c_tests {
    use super::*;

    #[test]
    fn v3_schema_has_eight_tables_and_indexed_terminal_maintenance() {
        use crate::journal::{ParticipantStore, lmdb::LmdbParticipantStore};
        use crate::{
            DeterministicContextBuilder, ParticipantEvent, ParticipantJournal,
            ParticipantTerminalKind,
        };
        let dir = tempfile::tempdir().unwrap();
        let store = LmdbParticipantStore::open(dir.path(), "a").unwrap();
        let c = DeterministicContextBuilder::default()
            .with_saga_id(1)
            .build();
        store.admit_and_record_intent(&c, "x", "start").unwrap();
        store
            .append(
                c.saga_id,
                ParticipantEvent::ParticipantTerminalRecorded {
                    saga_type: c.saga_type,
                    saga_started_at_millis: c.saga_started_at_millis,
                    outcome: ParticipantTerminalKind::Completed,
                    reason: "done".into(),
                    recorded_at_millis: 10,
                },
            )
            .unwrap();
        let owner = LmdbEnvOwner::acquire(dir.path()).unwrap();
        let index_count = |prefix: &str| {
            let txn = owner.env().read_txn().unwrap();
            let runs = owner
                .env()
                .open_database::<Str, Bytes>(&txn, Some("participant_runs"))
                .unwrap()
                .unwrap();
            runs.prefix_iter(&txn, prefix).unwrap().count()
        };
        let txn = owner.env().read_txn().unwrap();
        let root = owner
            .env()
            .open_database::<Str, Bytes>(&txn, None)
            .unwrap()
            .unwrap();
        let names: Vec<_> = root
            .iter(&txn)
            .unwrap()
            .map(|r| r.unwrap().0.to_owned())
            .collect();
        assert_eq!(
            names,
            [
                "meta",
                "participant_dedupe",
                "participant_events",
                "participant_runs",
                "participant_unresolved",
                "resolver_events",
                "resolver_runs",
                "resolver_unresolved"
            ]
        );
        drop(txn);
        assert_eq!(index_count("~age:61:"), 1);
        assert_eq!(index_count("~detail:61:"), 1);
        assert_eq!(store.compact_terminal_detail(1).unwrap(), 1);
        assert_eq!(index_count("~detail:61:"), 0);
        assert_eq!(index_count("~age:61:"), 1);
        assert_eq!(store.prune_resolved_before(11, 1).unwrap(), 1);
        assert_eq!(index_count("~age:61:"), 0);
    }

    #[test]
    fn acquire_configures_sixteen_named_databases() {
        let dir = tempfile::tempdir().unwrap();
        let owner = LmdbEnvOwner::acquire(dir.path()).unwrap();
        let mut txn = owner.env().write_txn().unwrap();
        for i in 0..16 {
            owner
                .env()
                .create_database::<Str, Bytes>(&mut txn, Some(&format!("test-{i}")))
                .unwrap();
        }
        assert!(
            owner
                .env()
                .create_database::<Str, Bytes>(&mut txn, Some("seventeenth"))
                .is_err()
        );
    }

    #[test]
    fn old_and_unversioned_schemas_fail_closed_without_migration() {
        use crate::LmdbTerminalResolverJournal;
        use crate::journal::lmdb::LmdbParticipantStore;
        for version in [None, Some("1"), Some("2"), Some("3")] {
            let dir = tempfile::tempdir().unwrap();
            let owner = LmdbEnvOwner::acquire(dir.path()).unwrap();
            let mut txn = owner.env().write_txn().unwrap();
            let meta = owner
                .env()
                .create_database::<Str, Str>(&mut txn, Some("meta"))
                .unwrap();
            if let Some(version) = version {
                meta.put(&mut txn, "schema_version", version).unwrap();
            }
            meta.put(&mut txn, "evidence", "must survive").unwrap();
            txn.commit().unwrap();
            drop(owner);
            assert!(LmdbParticipantStore::open(dir.path(), "a").is_err());
            assert!(LmdbTerminalResolverJournal::open(dir.path()).is_err());
            let owner = LmdbEnvOwner::acquire(dir.path()).unwrap();
            let txn = owner.env().read_txn().unwrap();
            let meta = owner
                .env()
                .open_database::<Str, Str>(&txn, Some("meta"))
                .unwrap()
                .unwrap();
            assert_eq!(meta.get(&txn, "evidence").unwrap(), Some("must survive"));
            assert_eq!(meta.get(&txn, "schema_version").unwrap(), version);
        }
    }

    #[test]
    fn map_size_parser_refuses_invalid_overrides() {
        assert_eq!(parse_map_size(None).unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_map_size(Some("10485760")).unwrap(), 10485760);
        for invalid in ["0", "-1", "bad", "184467440737095516160"] {
            assert!(parse_map_size(Some(invalid)).is_err());
        }
    }

    #[test]
    fn split_layout_refused_before_creating_replacement_env() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("journal")).unwrap();
        std::fs::write(dir.path().join("journal/data.mdb"), []).unwrap();
        assert!(LmdbEnvOwner::acquire(dir.path()).is_err());
        assert!(!dir.path().join("data.mdb").exists());
    }

    #[test]
    fn acquire_creates_canonical_shared_environment() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("missing");
        let first = LmdbEnvOwner::acquire(&base).unwrap();
        let second = LmdbEnvOwner::acquire(&base.join(".")).unwrap();
        assert_eq!(first.key, base.canonicalize().unwrap());
        assert_eq!(first.env().info().map_size, 1024 * 1024 * 1024);
        drop(first);
        second.env().write_txn().unwrap().commit().unwrap();
        assert!(base.join("data.mdb").exists());
    }
}

/// Every production opener uses the same options; malformed overrides fail closed.
fn parse_map_size(raw: Option<&str>) -> std::io::Result<usize> {
    match raw {
        None => Ok(1024 * 1024 * 1024),
        Some(raw) => raw
            .parse::<usize>()
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "SAGA_LMDB_MAP_SIZE_BYTES must be a positive usize",
                )
            }),
    }
}
pub(crate) fn map_size_bytes() -> std::io::Result<usize> {
    match std::env::var("SAGA_LMDB_MAP_SIZE_BYTES") {
        Ok(raw) => parse_map_size(Some(&raw)),
        Err(std::env::VarError::NotPresent) => parse_map_size(None),
        Err(error) => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error)),
    }
}

/// Shared v3 schema. Database creation/version validation is one transaction.
pub(crate) fn initialize_schema(env: &Env, path: &Path) -> Result<(), String> {
    use heed::types::{Bytes, Str};
    for legacy in ["journal", "dedupe"] {
        if path.join(legacy).join("data.mdb").exists() {
            return Err(format!("incompatible split LMDB layout: {legacy}"));
        }
    }
    let mut txn = env.write_txn().map_err(|e| e.to_string())?;
    // The unnamed database lists named databases. Never create a fresh schema
    // over an old or unversioned recovery store.
    let root = env
        .open_database::<Str, Bytes>(&txn, None)
        .map_err(|e| e.to_string())?;
    let meta = env
        .open_database::<Str, Str>(&txn, Some("meta"))
        .map_err(|e| e.to_string())?;
    if let Some(meta) = meta {
        if meta
            .get(&txn, "schema_version")
            .map_err(|e| e.to_string())?
            != Some("3")
        {
            return Err(
                "incompatible terminal resolver journal schema / participant schema: required=3"
                    .into(),
            );
        }
        let names = root
            .ok_or("missing schema directory")?
            .iter(&txn)
            .map_err(|e| e.to_string())?
            .map(|row| row.map(|(name, _)| name.to_owned()))
            .collect::<heed::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        if names
            != [
                "meta",
                "participant_dedupe",
                "participant_events",
                "participant_runs",
                "participant_unresolved",
                "resolver_events",
                "resolver_runs",
                "resolver_unresolved",
            ]
        {
            return Err("incompatible or incomplete v3 schema".into());
        }
    } else if root.is_some_and(|db| db.len(&txn).unwrap_or(1) != 0) {
        return Err(
            "incompatible terminal resolver journal schema / participant schema: required=3".into(),
        );
    }
    let meta = env
        .create_database::<Str, Str>(&mut txn, Some("meta"))
        .map_err(|e| e.to_string())?;
    meta.put(&mut txn, "schema_version", "3")
        .map_err(|e| e.to_string())?;
    for name in [
        "participant_dedupe",
        "participant_runs",
        "participant_unresolved",
        "participant_events",
        "resolver_runs",
        "resolver_unresolved",
        "resolver_events",
    ] {
        env.create_database::<Str, Bytes>(&mut txn, Some(name))
            .map_err(|e| e.to_string())?;
    }
    txn.commit().map_err(|e| e.to_string())
}

use crate::journal::{RunKey, RunSummary};
use heed::Database;
use heed::types::{Bytes, Str};

pub(crate) fn hex(value: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        write!(out, "{byte:02x}").unwrap();
    }
    out
}
pub(crate) fn run_key(namespace: &str, key: &RunKey) -> Result<String, String> {
    let out = format!("{namespace}{:020}:{:020}:{}:", key.0, key.2, hex(&key.1));
    if out.len() + 32 > 511 {
        return Err("run identity exceeds LMDB key capacity".into());
    }
    Ok(out)
}
pub(crate) fn parse_run(namespace: &str, key: &str) -> Result<RunKey, String> {
    let mut parts = key
        .strip_prefix(namespace)
        .ok_or("foreign run namespace")?
        .split(':');
    let id = parts
        .next()
        .ok_or("missing saga id")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    let start = parts
        .next()
        .ok_or("missing run start")?
        .parse::<u64>()
        .map_err(|e| e.to_string())?;
    let encoded = parts.next().ok_or("missing saga type")?;
    if encoded.len() % 2 != 0 {
        return Err("invalid run identity".into());
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).map_err(|e| e.to_string())?;
            u8::from_str_radix(pair, 16).map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        id,
        String::from_utf8(bytes).map_err(|e| e.to_string())?.into(),
        start,
    ))
}
pub(crate) fn decode_summary(bytes: &[u8]) -> Result<RunSummary, String> {
    let mut aligned = rkyv::util::AlignedVec::<16>::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<RunSummary, rkyv::rancor::Error>(&aligned).map_err(|e| e.to_string())
}

/// Indexed raw operations shared by participant and resolver stores. No history
/// decoding on open, and maintenance stages at most 256 keys/rows per txn.
pub(crate) struct IndexedStore {
    pub env: Env,
    pub rows: Database<Str, Bytes>,
    pub runs: Database<Str, Bytes>,
    pub unresolved: Database<Str, Bytes>,
    pub meta: Database<Str, Str>,
    pub namespace: String,
    pub counter: String,
    _owner: LmdbEnvOwner,
}
impl std::fmt::Debug for IndexedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedStore")
            .field("namespace", &self.namespace)
            .finish()
    }
}
impl IndexedStore {
    pub fn open(
        owner: LmdbEnvOwner,
        path: &Path,
        kind: &str,
        namespace: String,
    ) -> Result<Self, String> {
        initialize_schema(owner.env(), path)?;
        let env = owner.env();
        let mut txn = env.write_txn().map_err(|e| e.to_string())?;
        let rows = env
            .create_database::<Str, Bytes>(&mut txn, Some(&format!("{kind}_events")))
            .map_err(|e| e.to_string())?;
        let runs = env
            .create_database::<Str, Bytes>(&mut txn, Some(&format!("{kind}_runs")))
            .map_err(|e| e.to_string())?;
        let unresolved = env
            .create_database::<Str, Bytes>(&mut txn, Some(&format!("{kind}_unresolved")))
            .map_err(|e| e.to_string())?;
        let meta = env
            .create_database::<Str, Str>(&mut txn, Some("meta"))
            .map_err(|e| e.to_string())?;
        let counter = format!("{kind}:{namespace}next_sequence");
        if meta
            .get(&txn, &counter)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            if runs
                .prefix_iter(&txn, &namespace)
                .map_err(|e| e.to_string())?
                .next()
                .is_some()
            {
                return Err("next_sequence is missing".into());
            }
            meta.put(&mut txn, &counter, "1")
                .map_err(|e| e.to_string())?;
            meta.put(&mut txn, &format!("{counter}:last_allocated"), "0")
                .map_err(|e| e.to_string())?;
        }
        txn.commit().map_err(|e| e.to_string())?;
        Ok(Self {
            env: env.clone(),
            rows,
            runs,
            unresolved,
            meta,
            namespace,
            counter,
            _owner: owner,
        })
    }
    pub fn next(&self, txn: &mut heed::RwTxn<'_>, count: usize) -> Result<u64, String> {
        let raw = self
            .meta
            .get(txn, &self.counter)
            .map_err(|e| e.to_string())?
            .ok_or("next_sequence is missing")?;
        let start = raw.parse::<u64>().map_err(|e| e.to_string())?;
        let next = start
            .checked_add(count as u64)
            .ok_or("sequence space exhausted")?;
        let guard_key = format!("{}:last_allocated", self.counter);
        let last = self
            .meta
            .get(txn, &guard_key)
            .map_err(|e| e.to_string())?
            .ok_or("sequence guard is missing")?
            .parse::<u64>()
            .map_err(|e| e.to_string())?;
        if start <= last {
            return Err("sequence already allocated".into());
        }
        if count > 0 {
            self.meta
                .put(txn, &guard_key, &(next - 1).to_string())
                .map_err(|e| e.to_string())?;
        }
        self.meta
            .put(txn, &self.counter, &next.to_string())
            .map_err(|e| e.to_string())?;
        Ok(start)
    }
    pub fn summary(&self, txn: &heed::RoTxn<'_>, key: &str) -> Result<RunSummary, String> {
        self.runs
            .get(txn, key)
            .map_err(|e| e.to_string())?
            .map(decode_summary)
            .transpose()
            .map(|s| s.unwrap_or_default())
    }
    fn age_key(&self, run: &str, at: u64) -> Result<String, String> {
        let identity = run
            .strip_prefix(&self.namespace)
            .ok_or("foreign run namespace")?;
        Ok(format!("~age:{}{at:020}:{identity}", self.namespace))
    }
    fn remove_indexes(
        &self,
        txn: &mut heed::RwTxn<'_>,
        run: &str,
        summary: &RunSummary,
    ) -> Result<(), String> {
        if summary.ordinary() {
            self.runs
                .delete(txn, &self.age_key(run, summary.fence_at)?)
                .map_err(|e| e.to_string())?;
            self.runs
                .delete(txn, &format!("~detail:{run}"))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn remove_summary(&self, txn: &mut heed::RwTxn<'_>, run: &str) -> Result<(), String> {
        self.remove_indexes(txn, run, &self.summary(txn, run)?)?;
        self.runs.delete(txn, run).map_err(|e| e.to_string())?;
        self.unresolved
            .delete(txn, run)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub fn put_summary(
        &self,
        txn: &mut heed::RwTxn<'_>,
        key: &str,
        summary: &RunSummary,
    ) -> Result<(), String> {
        self.remove_indexes(txn, key, &self.summary(txn, key)?)?;
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(summary).map_err(|e| e.to_string())?;
        self.runs
            .put(txn, key, &encoded)
            .map_err(|e| e.to_string())?;
        if summary.ordinary() {
            self.unresolved
                .delete(txn, key)
                .map_err(|e| e.to_string())?;
            self.runs
                .put(txn, &self.age_key(key, summary.fence_at)?, key.as_bytes())
                .map_err(|e| e.to_string())?;
            if !summary.compacted {
                self.runs
                    .put(txn, &format!("~detail:{key}"), key.as_bytes())
                    .map_err(|e| e.to_string())?;
            }
        } else {
            self.unresolved
                .put(txn, key, &[])
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn put_row(
        &self,
        txn: &mut heed::RwTxn<'_>,
        run: &str,
        sequence: u64,
        bytes: &[u8],
        phase: u8,
        at: u64,
    ) -> Result<(), String> {
        let key = format!("{run}{sequence:020}");
        // The transactional allocation guard prevents cross-run sequence reuse;
        // this check also refuses overwrites of a corrupt existing row.
        if self
            .rows
            .get(txn, &key)
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Err("row already exists".into());
        }
        self.rows.put(txn, &key, bytes).map_err(|e| e.to_string())?;
        let mut summary = self.summary(txn, run)?;
        summary.observe(phase, sequence, at);
        self.put_summary(txn, run, &summary)
    }
    pub fn maintain(
        &self,
        cutoff: Option<u64>,
        limit: usize,
        keep: impl Fn(&str, &[u8], &RunSummary) -> Result<bool, String>,
        dedupe: Option<Database<Str, Bytes>>,
    ) -> Result<usize, String> {
        if limit == 0 {
            return Ok(0);
        }
        let mut txn = self.env.write_txn().map_err(|e| e.to_string())?;
        let mut selected = Vec::new();
        let prefix = format!(
            "~{}:{}",
            if cutoff.is_some() { "age" } else { "detail" },
            self.namespace
        );
        // Secondary keys inside the runs table avoid scanning active histories
        // or already compacted summaries under the writer lock.
        for row in self
            .runs
            .prefix_iter(&txn, &prefix)
            .map_err(|e| e.to_string())?
            .take(limit.min(256))
        {
            let (_, bytes) = row.map_err(|e| e.to_string())?;
            let run = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            if !run.starts_with(&self.namespace) {
                return Err("foreign maintenance index".into());
            }
            let summary = self.summary(&txn, run)?;
            if !summary.ordinary()
                || self
                    .unresolved
                    .get(&txn, run)
                    .map_err(|e| e.to_string())?
                    .is_some()
            {
                return Err("maintenance index references protected run".into());
            }
            if cutoff.is_some_and(|c| summary.fence_at >= c) {
                break;
            }
            selected.push((run.to_owned(), summary));
        }
        let mut changed = 0;
        let mut budget = 256;
        for (run, mut summary) in selected {
            if budget == 0 {
                break;
            }
            let fence_key = format!(
                "{run}{:020}",
                summary.fence.ok_or("ordinary run missing fence")?
            );
            if self
                .rows
                .get(&txn, &fence_key)
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Err("ordinary run terminal fence is missing".into());
            }
            let mut deletes = Vec::new();
            let mut remaining = false;
            for row in self
                .rows
                .prefix_iter(&txn, &run)
                .map_err(|e| e.to_string())?
            {
                let (key, bytes) = row.map_err(|e| e.to_string())?;
                if key == fence_key || (cutoff.is_none() && keep(key, bytes, &summary)?) {
                    continue;
                }
                if deletes.len() == budget {
                    remaining = true;
                    break;
                }
                deletes.push(key.to_owned());
            }
            for key in &deletes {
                self.rows.delete(&mut txn, key).map_err(|e| e.to_string())?;
            }
            budget -= deletes.len();
            let mut marker_progress = false;
            if cutoff.is_some() && !remaining && budget > 0 {
                // Delete run-scoped markers in the same bounded transaction.
                // If marker count exhausts the budget, retain the fence until
                // a later call has removed the remaining markers.
                let mut markers = Vec::new();
                let mut markers_remain = false;
                if let Some(db) = dedupe {
                    for row in db.prefix_iter(&txn, &run).map_err(|e| e.to_string())? {
                        let (key, _) = row.map_err(|e| e.to_string())?;
                        if markers.len() == budget {
                            markers_remain = true;
                            break;
                        }
                        markers.push(key.to_owned());
                    }
                    for key in &markers {
                        db.delete(&mut txn, key).map_err(|e| e.to_string())?;
                    }
                }
                budget -= markers.len();
                if !markers_remain && budget > 0 {
                    self.rows
                        .delete(&mut txn, &fence_key)
                        .map_err(|e| e.to_string())?;
                    self.remove_summary(&mut txn, &run)?;
                    budget -= 1;
                    changed += 1;
                    continue;
                }
                marker_progress = !markers.is_empty();
            }
            let summary_changed = !remaining && !summary.compacted;
            if !remaining {
                summary.compacted = true;
            }
            self.put_summary(&mut txn, &run, &summary)?;
            if !deletes.is_empty() || summary_changed || marker_progress {
                changed += 1;
            }
        }
        txn.commit().map_err(|e| e.to_string())?;
        Ok(changed)
    }
}
