//! Participant journal storage for SAGA event persistence.
//!
//! This module provides the journaling infrastructure that enables participant
//! services to durably record events related to SAGA orchestrations. Journaling
//! is essential for:
//!
//! - **Recovery**: Reconstructing participant state after failures
//! - **Audit**: Maintaining a complete history of actions taken
//! - **Compensation**: Enabling proper rollback by tracking what was done
//!
//! In the choreography-based SAGA pattern, each participant maintains its own
//! journal of events, allowing for independent recovery and replay.

use super::{ParticipantEvent, SagaId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Mutex;

/// A single entry in the participant's journal.
///
/// Each entry captures an event along with metadata about when and in what
/// order it was recorded. This information is essential for:
///
/// - Ordering events during replay
/// - Debugging and auditing
/// - Time-based analysis of SAGA execution
#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct JournalEntry {
    /// The monotonically increasing sequence number assigned to this entry.
    ///
    /// Sequence numbers provide a total ordering of all events across
    /// all SAGAs for this participant.
    pub sequence: u64,

    /// The Unix timestamp in milliseconds when this entry was recorded.
    ///
    /// This represents wall-clock time at the moment the event was
    /// persisted to the journal.
    pub recorded_at_millis: u64,

    /// The participant event that was recorded.
    ///
    /// This captures what action or state change occurred in the SAGA.
    pub event: ParticipantEvent,
}

/// Errors that can occur during journal operations.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// A storage-layer error occurred.
    ///
    /// The contained string describes the specific error from the
    /// underlying storage mechanism.
    #[error("Storage error: {0}")]
    Storage(Box<str>),

    /// The requested SAGA was not found in the journal.
    #[error("Not found: {0}")]
    NotFound(SagaId),
}

#[cfg(test)]
mod wave1c_tests {
    use super::*;
    use crate::{DeterministicContextBuilder, ParticipantTerminalKind, SagaContext};

    fn context(id: u64) -> SagaContext {
        DeterministicContextBuilder::default()
            .with_saga_id(id)
            .build()
    }
    fn terminal(c: &SagaContext, outcome: ParticipantTerminalKind) -> ParticipantEvent {
        ParticipantEvent::ParticipantTerminalRecorded {
            saga_type: c.saga_type.clone(),
            saga_started_at_millis: c.saga_started_at_millis,
            outcome,
            reason: "test".into(),
            recorded_at_millis: 10,
        }
    }
    fn admit(store: &impl ParticipantStore, c: &SagaContext) -> StoreAdmission {
        store
            .admit_and_record_intent(c, "trigger", "SagaStarted")
            .unwrap()
    }

    fn concurrent_one_winner(store: &impl ParticipantStore) {
        let c = context(99);
        let wins = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (c, wins) = (&c, &wins);
                scope.spawn(move || {
                    if admit(store, c) == StoreAdmission::Admitted {
                        wins.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                });
            }
        });
        assert_eq!(wins.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(store.read(c.saga_id).unwrap().len(), 3);
    }
    #[test]
    fn memory_concurrent_admission_has_one_winner() {
        concurrent_one_winner(&InMemoryParticipantStore::new());
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_concurrent_admission_has_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        concurrent_one_winner(&lmdb::LmdbParticipantStore::open(dir.path(), "a").unwrap());
    }
    #[test]
    fn memory_sequence_exhaustion_is_atomic() {
        let store = InMemoryParticipantStore::new();
        store.state().unwrap().next = u64::MAX - 1;
        assert!(
            store
                .admit_and_record_intent(&context(1), "x", "start")
                .is_err()
        );
        assert!(store.list_sagas().unwrap().is_empty());
        assert!(store.unresolved_saga_ids().unwrap().is_empty());
        store.state().unwrap().next = 1;
        assert_eq!(
            store
                .admit_and_record_intent(&context(1), "x", "start")
                .unwrap(),
            StoreAdmission::Admitted
        );
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_map_full_rolls_back_marker_rows_and_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            lmdb::LmdbParticipantStore::open_with_map_size(dir.path(), "a", 1024 * 1024).unwrap();
        let c = context(1);
        assert!(
            store
                .admit_and_record_intent(&c, "x", &"x".repeat(2 * 1024 * 1024))
                .is_err()
        );
        assert!(store.read(c.saga_id).unwrap().is_empty());
        assert!(store.unresolved_saga_ids().unwrap().is_empty());
        assert_eq!(
            store.admit_and_record_intent(&c, "x", "start").unwrap(),
            StoreAdmission::Admitted
        );
        assert_eq!(store.read(c.saga_id).unwrap()[0].sequence, 1);
    }
    #[test]
    fn memory_fifo_default_caps_4096_runs_and_quarantine_dominates() {
        let store = InMemoryParticipantStore::new();
        let pinned = context(10000);
        admit(&store, &pinned);
        store
            .append(
                pinned.saga_id,
                terminal(&pinned, ParticipantTerminalKind::Completed),
            )
            .unwrap();
        store
            .append(
                pinned.saga_id,
                terminal(&pinned, ParticipantTerminalKind::Quarantined),
            )
            .unwrap();
        store
            .append(
                pinned.saga_id,
                terminal(&pinned, ParticipantTerminalKind::Failed),
            )
            .unwrap();
        for id in 0..4098 {
            let c = context(id);
            admit(&store, &c);
            store
                .append(c.saga_id, terminal(&c, ParticipantTerminalKind::Completed))
                .unwrap();
        }
        assert_eq!(store.list_sagas().unwrap().len(), 4097);
        assert!(store.read(SagaId::new(0)).unwrap().is_empty());
        assert!(store.read(SagaId::new(1)).unwrap().is_empty());
        assert_eq!(store.unresolved_saga_ids().unwrap(), vec![pinned.saga_id]);
        assert!(matches!(
            admit(&store, &pinned),
            StoreAdmission::Rejected(ParticipantAdmission::TerminalReplay {
                outcome: ParticipantTerminalKind::Quarantined
            })
        ));
    }

    #[test]
    fn memory_atomic_admission_and_unresolved_index() {
        let store = InMemoryParticipantStore::new();
        let c = context(1);
        assert_eq!(admit(&store, &c), StoreAdmission::Admitted);
        assert_eq!(admit(&store, &c), StoreAdmission::Duplicate);
        assert_eq!(store.read(c.saga_id).unwrap().len(), 3);
        assert_eq!(store.unresolved_saga_ids().unwrap(), vec![c.saga_id]);
        store
            .append(c.saga_id, terminal(&c, ParticipantTerminalKind::Completed))
            .unwrap();
        assert!(store.unresolved_saga_ids().unwrap().is_empty());
        assert!(matches!(admit(&store, &c), StoreAdmission::Rejected(_)));
    }

    #[test]
    fn memory_fifo_pins_quarantine_and_active_exact_runs() {
        let store = InMemoryParticipantStore::with_capacity(2, 8);
        for id in 0..6 {
            let c = context(id);
            admit(&store, &c);
            if id != 0 {
                store
                    .append(
                        c.saga_id,
                        terminal(
                            &c,
                            if id == 1 {
                                ParticipantTerminalKind::Quarantined
                            } else {
                                ParticipantTerminalKind::Completed
                            },
                        ),
                    )
                    .unwrap();
            }
        }
        assert!(!store.read(SagaId::new(0)).unwrap().is_empty());
        assert!(!store.read(SagaId::new(1)).unwrap().is_empty());
        assert!(store.read(SagaId::new(2)).unwrap().is_empty());
        assert_eq!(store.prune_resolved_before(11, 1).unwrap(), 1);
        assert_eq!(store.compact_terminal_detail(256).unwrap(), 1);
    }

    #[test]
    fn protected_capacity_and_sequence_failure_leave_no_intent() {
        let store = InMemoryParticipantStore::with_capacity(2, 1);
        admit(&store, &context(1));
        assert!(
            store
                .admit_and_record_intent(&context(2), "x", "start")
                .is_err()
        );
        assert!(store.read(SagaId::new(2)).unwrap().is_empty());
    }

    #[test]
    fn compaction_advances_past_fence_only_runs() {
        let store = InMemoryParticipantStore::new();
        for id in 0..257 {
            let c = context(id);
            store
                .append(c.saga_id, terminal(&c, ParticipantTerminalKind::Completed))
                .unwrap();
        }
        let c = context(300);
        admit(&store, &c);
        store
            .append(c.saga_id, terminal(&c, ParticipantTerminalKind::Completed))
            .unwrap();
        while store.compact_terminal_detail(256).unwrap() != 0 {}
        assert_eq!(store.read(c.saga_id).unwrap().len(), 1);
    }

    fn maintenance_preserves_exact_run(store: &impl ParticipantStore) {
        let c = context(50);
        admit(store, &c);
        for _ in 0..600 {
            store
                .append(
                    c.saga_id,
                    ParticipantEvent::StepExecutionStarted {
                        attempt: 1,
                        started_at_millis: 1,
                    },
                )
                .unwrap();
        }
        store
            .append(c.saga_id, terminal(&c, ParticipantTerminalKind::Failed))
            .unwrap();
        let mut newer = c.clone();
        newer.saga_started_at_millis += 1;
        assert_eq!(admit(store, &newer), StoreAdmission::Admitted);
        let before = store.read(c.saga_id).unwrap().len();
        assert_eq!(store.compact_terminal_detail(usize::MAX).unwrap(), 1);
        let after = store.read(c.saga_id).unwrap().len();
        assert_eq!(before - after, 256);
        assert_eq!(store.prune_resolved_before(10, 256).unwrap(), 0);
        while store.prune_resolved_before(11, 1).unwrap() != 0 {}
        assert_eq!(store.read(c.saga_id).unwrap().len(), 3);
        assert_eq!(admit(store, &newer), StoreAdmission::Duplicate);
        assert_eq!(store.unresolved_saga_ids().unwrap(), vec![c.saga_id]);
    }
    #[test]
    fn memory_row_budget_preserves_newer_run_and_dedupe() {
        maintenance_preserves_exact_run(&InMemoryParticipantStore::new());
    }
    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_row_budget_preserves_newer_run_and_dedupe() {
        let dir = tempfile::tempdir().unwrap();
        maintenance_preserves_exact_run(
            &lmdb::LmdbParticipantStore::open(dir.path(), "worker").unwrap(),
        );
    }

    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_shared_v3_atomic_reopen_and_rollback() {
        use super::lmdb::LmdbParticipantStore;
        use crate::{LmdbTerminalResolverJournal, TerminalResolverJournal};
        let dir = tempfile::tempdir().unwrap();
        let a = LmdbParticipantStore::open(dir.path(), "a").unwrap();
        let b = LmdbParticipantStore::open(dir.path(), "b").unwrap();
        let resolver = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
        let c = context(1);
        assert_eq!(admit(&a, &c), StoreAdmission::Admitted);
        assert_eq!(admit(&b, &c), StoreAdmission::Admitted);
        assert!(resolver.read_all().unwrap().is_empty());
        a.seed_next_sequence_for_test(u64::MAX - 1);
        assert!(
            a.admit_and_record_intent(&context(2), "x", "start")
                .is_err()
        );
        assert!(a.read(SagaId::new(2)).unwrap().is_empty());
        a.seed_next_sequence_for_test(4);
        assert_eq!(
            a.admit_and_record_intent(&context(2), "x", "start")
                .unwrap(),
            StoreAdmission::Admitted
        );
        drop((a, b, resolver));
        let a = LmdbParticipantStore::open(dir.path(), "a").unwrap();
        assert_eq!(admit(&a, &c), StoreAdmission::Duplicate);
        assert_eq!(
            a.unresolved_saga_ids().unwrap(),
            vec![c.saga_id, SagaId::new(2)]
        );
        assert!(!dir.path().join("journal").exists());
    }
}

use crate::{ParticipantAdmission, ParticipantTerminalKind, SagaContext};

/// Admission and execution intent are committed together, or not at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreAdmission {
    Admitted,
    Duplicate,
    Rejected(ParticipantAdmission),
}

pub trait ParticipantStore: Send + Sync + 'static {
    /// Appends a new event to the journal for the specified SAGA.
    ///
    /// Events are assigned monotonically increasing sequence numbers
    /// and timestamped with the current system time.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the SAGA this event belongs to
    /// * `event` - The participant event to record
    ///
    /// # Returns
    ///
    /// The sequence number assigned to this event on success, or a
    /// [`JournalError`] on failure.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Storage`] if the underlying storage fails
    /// to persist the event.
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError>;

    /// Reads all journal entries for a specific SAGA.
    ///
    /// Entries are returned in the order they were recorded (by sequence number).
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the SAGA to read events for
    ///
    /// # Returns
    ///
    /// A vector of [`JournalEntry`] instances representing all recorded events,
    /// or an empty vector if no events exist for this SAGA.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Storage`] if the underlying storage fails
    /// to read the events.
    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError>;

    /// Lists all SAGA IDs that have at least one journal entry.
    ///
    /// This is useful for recovery scenarios where you need to identify
    /// all SAGAs that may need to be resumed.
    ///
    /// # Returns
    ///
    /// A vector of [`SagaId`] instances for all SAGAs with recorded events.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Storage`] if the underlying storage fails.
    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError>;

    /// Deletes all journal entries for a specific SAGA.
    ///
    /// Destructive administrative primitive: deletion removes replay protection
    /// and unresolved evidence. Never use it for automatic terminal cleanup;
    /// preserve an audited replacement fence before deliberate archival/deletion.
    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError>;

    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, JournalError>;
    fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), JournalError>;
    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), JournalError> {
        self.check_and_mark(saga_id, key).map(|_| ())
    }

    fn append_batch(
        &self,
        saga_id: SagaId,
        events: Vec<ParticipantEvent>,
    ) -> Result<Vec<u64>, JournalError>;
    fn admit_and_record_intent(
        &self,
        context: &SagaContext,
        dedupe_key: &str,
        triggering_event: &str,
    ) -> Result<StoreAdmission, JournalError>;
    fn unresolved_saga_ids(&self) -> Result<Vec<SagaId>, JournalError>;
    /// At most 256 runs and 256 detail rows per call/transaction. Returns runs changed.
    fn compact_terminal_detail(&self, batch_limit: usize) -> Result<usize, JournalError>;
    /// Explicit replay-retention policy; cutoff is exclusive. Returns runs changed.
    fn prune_resolved_before(
        &self,
        cutoff_millis: u64,
        batch_limit: usize,
    ) -> Result<usize, JournalError>;
}

impl<T: ParticipantStore + ?Sized> ParticipantStore for std::sync::Arc<T> {
    fn append(&self, id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        (**self).append(id, event)
    }
    fn read(&self, id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        (**self).read(id)
    }
    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        (**self).list_sagas()
    }
    fn prune(&self, id: SagaId) -> Result<(), JournalError> {
        (**self).prune(id)
    }
    fn append_batch(
        &self,
        id: SagaId,
        events: Vec<ParticipantEvent>,
    ) -> Result<Vec<u64>, JournalError> {
        (**self).append_batch(id, events)
    }
    fn admit_and_record_intent(
        &self,
        c: &SagaContext,
        key: &str,
        trigger: &str,
    ) -> Result<StoreAdmission, JournalError> {
        (**self).admit_and_record_intent(c, key, trigger)
    }
    fn unresolved_saga_ids(&self) -> Result<Vec<SagaId>, JournalError> {
        (**self).unresolved_saga_ids()
    }
    fn compact_terminal_detail(&self, limit: usize) -> Result<usize, JournalError> {
        (**self).compact_terminal_detail(limit)
    }
    fn prune_resolved_before(&self, cutoff: u64, limit: usize) -> Result<usize, JournalError> {
        (**self).prune_resolved_before(cutoff, limit)
    }
    fn check_and_mark(&self, id: SagaId, key: &str) -> Result<bool, JournalError> {
        (**self).check_and_mark(id, key)
    }
    fn remove_processed(&self, id: SagaId, key: &str) -> Result<(), JournalError> {
        (**self).remove_processed(id, key)
    }
}

pub(crate) type RunKey = (u64, Box<str>, u64);

#[derive(Clone, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct RunSummary {
    pub phase: u8, // 0 active, 1 completed, 2 failed, 3 quarantined (absorbing)
    pub fence: Option<u64>,
    pub fence_at: u64,
    pub compacted: bool,
}
impl RunSummary {
    pub fn ordinary(&self) -> bool {
        self.phase == 1 || self.phase == 2
    }
    pub fn observe(&mut self, phase: u8, sequence: u64, at: u64) {
        if phase == 3 {
            self.phase = 3;
        } else if phase != 0 && self.phase == 0 {
            self.phase = phase;
            self.fence = Some(sequence);
            self.fence_at = at;
        }
        self.compacted = false;
    }
}

pub(crate) struct MemoryRun<E> {
    pub summary: RunSummary,
    pub rows: BTreeMap<u64, E>,
    pub dedupe: HashSet<Box<str>>,
}
impl<E> Default for MemoryRun<E> {
    fn default() -> Self {
        Self {
            summary: RunSummary::default(),
            rows: BTreeMap::new(),
            dedupe: HashSet::new(),
        }
    }
}

pub(crate) struct MemoryRuns<E> {
    pub runs: BTreeMap<RunKey, MemoryRun<E>>,
    pub by_saga: HashMap<u64, BTreeSet<RunKey>>,
    pub unresolved: BTreeSet<RunKey>,
    fifo: VecDeque<RunKey>,
    pub next: u64,
    terminal_capacity: usize,
    protected_capacity: usize,
}
impl<E> MemoryRuns<E> {
    pub fn new(terminal_capacity: usize, protected_capacity: usize) -> Self {
        Self {
            runs: BTreeMap::new(),
            by_saga: HashMap::new(),
            unresolved: BTreeSet::new(),
            fifo: VecDeque::new(),
            next: 1,
            terminal_capacity,
            protected_capacity,
        }
    }
    pub fn check_new(&self, key: &RunKey) -> Result<(), JournalError> {
        if !self.runs.contains_key(key) && self.unresolved.len() >= self.protected_capacity {
            return Err(storage("protected run capacity exceeded"));
        }
        Ok(())
    }
    pub fn insert(&mut self, key: RunKey, sequence: u64, entry: E, phase: u8, at: u64) {
        self.by_saga.entry(key.0).or_default().insert(key.clone());
        let run = self.runs.entry(key.clone()).or_default();
        let ordinary_before = run.summary.ordinary();
        run.summary.observe(phase, sequence, at);
        run.rows.insert(sequence, entry);
        if run.summary.ordinary() {
            self.unresolved.remove(&key);
            if !ordinary_before {
                self.fifo.push_back(key);
            }
        } else {
            self.unresolved.insert(key.clone());
            if ordinary_before {
                self.fifo.retain(|queued| queued != &key);
            }
        }
        while self.fifo.len() > self.terminal_capacity {
            let oldest = self.fifo.pop_front().unwrap();
            if self.runs.get(&oldest).is_some_and(|r| r.summary.ordinary()) {
                self.remove(&oldest);
            }
        }
    }
    pub fn remove(&mut self, key: &RunKey) {
        self.runs.remove(key);
        self.unresolved.remove(key);
        self.fifo.retain(|queued| queued != key);
        if let Some(keys) = self.by_saga.get_mut(&key.0) {
            keys.remove(key);
            if keys.is_empty() {
                self.by_saga.remove(&key.0);
            }
        }
    }
    pub fn maintain(
        &mut self,
        cutoff: Option<u64>,
        limit: usize,
        keep: impl Fn(&E, &RunSummary) -> bool,
    ) -> usize {
        let mut changed = 0;
        let mut rows_left = 256;
        let keys: Vec<_> = self
            .runs
            .iter()
            .filter(|(_, r)| {
                r.summary.ordinary()
                    && match cutoff {
                        Some(c) => r.summary.fence_at < c,
                        None => !r.summary.compacted,
                    }
            })
            .take(limit.min(256))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if rows_left == 0 {
                break;
            }
            let run = self.runs.get_mut(&key).unwrap();
            let deletes: Vec<_> = run
                .rows
                .iter()
                .filter(|(seq, e)| {
                    Some(**seq) != run.summary.fence && (cutoff.is_some() || !keep(e, &run.summary))
                })
                .take(rows_left)
                .map(|(seq, _)| *seq)
                .collect();
            for seq in &deletes {
                run.rows.remove(seq);
            }
            rows_left -= deletes.len();
            let remaining = run.rows.iter().any(|(seq, e)| {
                Some(*seq) != run.summary.fence && (cutoff.is_some() || !keep(e, &run.summary))
            });
            if cutoff.is_some() && !remaining && rows_left > 0 {
                rows_left -= run.rows.len();
                self.remove(&key); // fence and dedupe disappear last
                changed += 1;
            } else {
                let summary_changed = !remaining && !run.summary.compacted;
                if !remaining {
                    run.summary.compacted = true;
                }
                if !deletes.is_empty() || summary_changed {
                    changed += 1;
                }
            }
        }
        changed
    }
}

pub(crate) fn storage(error: impl std::fmt::Display) -> JournalError {
    JournalError::Storage(error.to_string().into())
}
pub(crate) fn context_key(c: &SagaContext) -> RunKey {
    (
        c.saga_id.get(),
        c.saga_type.clone(),
        c.saga_started_at_millis,
    )
}
fn event_key(saga_id: SagaId, event: &ParticipantEvent) -> Option<RunKey> {
    match event {
        ParticipantEvent::ParticipantRunRecorded {
            saga_type,
            saga_started_at_millis,
            ..
        }
        | ParticipantEvent::ParticipantTerminalRecorded {
            saga_type,
            saga_started_at_millis,
            ..
        } => Some((saga_id.get(), saga_type.clone(), *saga_started_at_millis)),
        ParticipantEvent::AcceptedDeadlineRenewed { context, .. }
        | ParticipantEvent::AcceptedStepRecorded { context, .. }
        | ParticipantEvent::AcceptedCompensationRecorded { context, .. }
        | ParticipantEvent::CompensationRequestRecorded { context, .. }
        | ParticipantEvent::ParticipantReconciliationEvidence { context, .. }
        | ParticipantEvent::ParticipantDependencyCompletedRecorded { context, .. } => {
            Some(context_key(context))
        }
        ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
            Some(context_key(&outcome.context))
        }
        _ => None,
    }
}
fn participant_phase(event: &ParticipantEvent) -> (u8, u64) {
    match event {
        ParticipantEvent::ParticipantTerminalRecorded {
            outcome,
            recorded_at_millis,
            ..
        } => (
            match outcome {
                ParticipantTerminalKind::Completed => 1,
                ParticipantTerminalKind::Failed => 2,
                ParticipantTerminalKind::Quarantined => 3,
            },
            *recorded_at_millis,
        ),
        ParticipantEvent::Quarantined {
            quarantined_at_millis,
            ..
        } => (3, *quarantined_at_millis),
        _ => (0, 0),
    }
}
fn admission<'a>(
    context: &SagaContext,
    runs: impl Iterator<Item = (&'a RunKey, &'a RunSummary)>,
) -> Option<ParticipantAdmission> {
    let key = context_key(context);
    let mut latest = 0;
    let mut terminal = None;
    let mut quarantine = None;
    let mut active = None;
    let mut legacy = false;
    for (other, summary) in runs {
        legacy |= other.1.is_empty();
        latest = latest.max(other.2);
        if other == &key && summary.phase != 0 {
            terminal = Some(ParticipantAdmission::TerminalReplay {
                outcome: match summary.phase {
                    1 => ParticipantTerminalKind::Completed,
                    2 => ParticipantTerminalKind::Failed,
                    _ => ParticipantTerminalKind::Quarantined,
                },
            });
        }
        if summary.phase == 3 {
            quarantine = Some(ParticipantAdmission::QuarantinedReuse {
                quarantined_started_at_millis: other.2,
                reason: "quarantined durable run".into(),
            });
        }
        if other != &key && !summary.ordinary() {
            active = Some(ParticipantAdmission::ActiveRunReuse {
                active_started_at_millis: other.2,
            });
        }
    }
    if legacy {
        return Some(ParticipantAdmission::LegacyHistory);
    }
    terminal
        .or_else(|| {
            (latest > key.2).then_some(ParticipantAdmission::StaleRun {
                latest_started_at_millis: latest,
            })
        })
        .or(quarantine)
        .or(active)
}
fn intent(context: &SagaContext, triggering_event: &str, new_run: bool) -> Vec<ParticipantEvent> {
    let at = SagaContext::now_millis();
    let mut events = Vec::new();
    if new_run {
        events.push(ParticipantEvent::ParticipantRunRecorded {
            saga_type: context.saga_type.clone(),
            saga_started_at_millis: context.saga_started_at_millis,
            recorded_at_millis: at,
        });
    }
    events.push(ParticipantEvent::StepTriggered {
        triggering_event: triggering_event.into(),
        triggered_at_millis: at,
    });
    events.push(ParticipantEvent::StepExecutionStarted {
        attempt: context.attempt.saturating_add(1),
        started_at_millis: at,
    });
    events
}

/// Compatibility journal name and the unified store have identical backing state.
pub struct InMemoryParticipantStore {
    state: Mutex<MemoryRuns<JournalEntry>>,
}
impl Default for InMemoryParticipantStore {
    fn default() -> Self {
        Self::new()
    }
}
impl InMemoryParticipantStore {
    pub fn new() -> Self {
        Self::with_capacity(4096, 4096)
    }
    pub fn with_capacity(terminal_capacity: usize, protected_capacity: usize) -> Self {
        Self {
            state: Mutex::new(MemoryRuns::new(terminal_capacity, protected_capacity)),
        }
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, MemoryRuns<JournalEntry>>, JournalError> {
        self.state
            .lock()
            .map_err(|_| storage("in-memory participant store lock poisoned"))
    }
    fn batch(
        state: &mut MemoryRuns<JournalEntry>,
        saga_id: SagaId,
        events: Vec<ParticipantEvent>,
    ) -> Result<Vec<u64>, JournalError> {
        let next = state
            .next
            .checked_add(events.len() as u64)
            .ok_or_else(|| storage("sequence space exhausted"))?;
        let mut current = state
            .by_saga
            .get(&saga_id.get())
            .and_then(|keys| keys.iter().max_by_key(|k| k.2))
            .cloned()
            .unwrap_or((saga_id.get(), "".into(), 0));
        // Validate the entire batch before any mutation.
        let mut staged = Vec::new();
        let mut new_keys = BTreeSet::new();
        for event in events {
            if let Some(key) = event_key(saga_id, &event) {
                current = key;
            }
            state.check_new(&current)?;
            if !state.runs.contains_key(&current) {
                new_keys.insert(current.clone());
            }
            staged.push((current.clone(), event));
        }
        if state.unresolved.len() + new_keys.len() > state.protected_capacity {
            return Err(storage("protected run capacity exceeded"));
        }
        let mut sequences = Vec::new();
        for (key, event) in staged {
            let sequence = state.next;
            state.next += 1;
            let (mut phase, at) = participant_phase(&event);
            if key.1.is_empty() && phase != 3 {
                phase = 0;
            }
            state.insert(
                key,
                sequence,
                JournalEntry {
                    sequence,
                    recorded_at_millis: SagaContext::now_millis(),
                    event,
                },
                phase,
                at,
            );
            sequences.push(sequence);
        }
        debug_assert_eq!(state.next, next);
        Ok(sequences)
    }
}
impl ParticipantStore for InMemoryParticipantStore {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        Ok(self.append_batch(saga_id, vec![event])?[0])
    }
    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        let state = self.state()?;
        let mut rows: Vec<_> = state
            .by_saga
            .get(&saga_id.get())
            .into_iter()
            .flatten()
            .flat_map(|key| state.runs[key].rows.values().cloned())
            .collect();
        rows.sort_by_key(|e| e.sequence);
        Ok(rows)
    }
    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        Ok(self
            .state()?
            .by_saga
            .keys()
            .copied()
            .map(SagaId::new)
            .collect())
    }
    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        let mut state = self.state()?;
        let keys = state
            .by_saga
            .get(&saga_id.get())
            .cloned()
            .unwrap_or_default();
        for key in keys {
            state.remove(&key);
        }
        Ok(())
    }

    fn check_and_mark(&self, saga_id: SagaId, marker: &str) -> Result<bool, JournalError> {
        let mut state = self.state()?;
        let key = state
            .by_saga
            .get(&saga_id.get())
            .and_then(|keys| keys.iter().max_by_key(|k| k.2))
            .cloned()
            .ok_or_else(|| storage("marker requires durable run evidence"))?;
        Ok(state
            .runs
            .get_mut(&key)
            .unwrap()
            .dedupe
            .insert(marker.into()))
    }
    fn remove_processed(&self, saga_id: SagaId, marker: &str) -> Result<(), JournalError> {
        let mut state = self.state()?;
        let keys = state
            .by_saga
            .get(&saga_id.get())
            .cloned()
            .unwrap_or_default();
        for key in keys {
            state.runs.get_mut(&key).unwrap().dedupe.remove(marker);
        }
        Ok(())
    }
    fn append_batch(
        &self,
        saga_id: SagaId,
        events: Vec<ParticipantEvent>,
    ) -> Result<Vec<u64>, JournalError> {
        Self::batch(&mut *self.state()?, saga_id, events)
    }
    fn admit_and_record_intent(
        &self,
        c: &SagaContext,
        dedupe_key: &str,
        triggering_event: &str,
    ) -> Result<StoreAdmission, JournalError> {
        if c.saga_type.is_empty() {
            return Err(storage("empty saga type"));
        }
        let mut state = self.state()?;
        if let Some(reason) = admission(
            c,
            state
                .by_saga
                .get(&c.saga_id.get())
                .into_iter()
                .flatten()
                .map(|k| (k, &state.runs[k].summary)),
        ) {
            return Ok(StoreAdmission::Rejected(reason));
        }
        let key = context_key(c);
        if state
            .runs
            .get(&key)
            .is_some_and(|r| r.dedupe.contains(dedupe_key))
        {
            return Ok(StoreAdmission::Duplicate);
        }
        let new_run = !state.runs.contains_key(&key);
        Self::batch(&mut state, c.saga_id, intent(c, triggering_event, new_run))?;
        state
            .runs
            .get_mut(&key)
            .unwrap()
            .dedupe
            .insert(dedupe_key.into());
        Ok(StoreAdmission::Admitted)
    }
    fn unresolved_saga_ids(&self) -> Result<Vec<SagaId>, JournalError> {
        {
            let mut ids: Vec<_> = self
                .state()?
                .unresolved
                .iter()
                .map(|k| SagaId::new(k.0))
                .collect();
            ids.dedup(); // Ordered index: O(U), no per-saga scans or tree inserts.
            Ok(ids)
        }
    }
    fn compact_terminal_detail(&self, batch_limit: usize) -> Result<usize, JournalError> {
        Ok(self.state()?.maintain(None, batch_limit, |_e, _s| false))
    }
    fn prune_resolved_before(
        &self,
        cutoff_millis: u64,
        batch_limit: usize,
    ) -> Result<usize, JournalError> {
        Ok(self
            .state()?
            .maintain(Some(cutoff_millis), batch_limit, |_e, _s| false))
    }
}

#[cfg(feature = "lmdb")]
pub mod lmdb {
    use super::*;
    use crate::lmdb_owner::{IndexedStore, LmdbEnvOwner, decode_summary, hex, parse_run, run_key};
    use heed::{
        Database,
        types::{Bytes, Str},
    };
    use std::path::Path;

    #[derive(Debug)]
    pub struct LmdbParticipantStore {
        db: IndexedStore,
        dedupe: Database<Str, Bytes>,
    }
    impl LmdbParticipantStore {
        pub fn open(path: &Path, participant_key: &str) -> Result<Self, JournalError> {
            Self::with_owner(
                path,
                participant_key,
                LmdbEnvOwner::acquire(path).map_err(storage)?,
            )
        }
        pub fn open_with_map_size(
            path: &Path,
            participant_key: &str,
            map_size_bytes: usize,
        ) -> Result<Self, JournalError> {
            let owner = LmdbEnvOwner::open(
                path,
                heed::EnvOpenOptions::new()
                    .max_dbs(16)
                    .map_size(map_size_bytes),
            )
            .map_err(storage)?;
            Self::with_owner(path, participant_key, owner)
        }
        fn with_owner(
            path: &Path,
            participant_key: &str,
            owner: LmdbEnvOwner,
        ) -> Result<Self, JournalError> {
            if participant_key.is_empty() || participant_key.len() > 100 {
                return Err(storage("invalid participant key"));
            }
            let db = IndexedStore::open(
                owner,
                path,
                "participant",
                format!("{}:", hex(participant_key)),
            )
            .map_err(storage)?;
            let txn = db.env.read_txn().map_err(storage)?;
            let dedupe = db
                .env
                .open_database::<Str, Bytes>(&txn, Some("participant_dedupe"))
                .map_err(storage)?
                .ok_or_else(|| storage("missing participant_dedupe"))?;
            drop(txn);
            Ok(Self { db, dedupe })
        }
        fn saga_prefix(&self, saga_id: SagaId) -> String {
            format!("{}{:020}:", self.db.namespace, saga_id.get())
        }
        fn batch(
            &self,
            txn: &mut heed::RwTxn<'_>,
            saga_id: SagaId,
            events: Vec<ParticipantEvent>,
        ) -> Result<Vec<u64>, JournalError> {
            let first = self.db.next(txn, events.len()).map_err(storage)?;
            let mut current = (saga_id.get(), "".into(), 0);
            let mut latest = None;
            for row in self
                .db
                .runs
                .prefix_iter(txn, &self.saga_prefix(saga_id))
                .map_err(storage)?
            {
                let (key, _) = row.map_err(storage)?;
                let key = parse_run(&self.db.namespace, key).map_err(storage)?;
                if latest.is_none_or(|start| start <= key.2) {
                    latest = Some(key.2);
                    current = key;
                }
            }
            let mut sequences = Vec::new();
            for (offset, event) in events.into_iter().enumerate() {
                if let Some(key) = event_key(saga_id, &event) {
                    current = key;
                }
                let key = run_key(&self.db.namespace, &current).map_err(storage)?;
                let sequence = first + offset as u64;
                let (mut phase, at) = participant_phase(&event);
                if current.1.is_empty() && phase != 3 {
                    phase = 0;
                }
                let entry = JournalEntry {
                    sequence,
                    recorded_at_millis: SagaContext::now_millis(),
                    event,
                };
                let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&entry).map_err(storage)?;
                self.db
                    .put_row(txn, &key, sequence, &encoded, phase, at)
                    .map_err(storage)?;
                sequences.push(sequence);
            }
            Ok(sequences)
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
    }
    impl ParticipantStore for LmdbParticipantStore {
        fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
            Ok(self.append_batch(saga_id, vec![event])?[0])
        }
        fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
            let txn = self.db.env.read_txn().map_err(storage)?;
            let mut rows = Vec::new();
            for row in self
                .db
                .rows
                .prefix_iter(&txn, &self.saga_prefix(saga_id))
                .map_err(storage)?
            {
                let (_, bytes) = row.map_err(storage)?;
                rows.push(decode_entry(bytes)?);
            }
            rows.sort_by_key(|e| e.sequence);
            Ok(rows)
        }
        fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
            let txn = self.db.env.read_txn().map_err(storage)?;
            let mut ids = BTreeSet::new();
            for row in self
                .db
                .runs
                .prefix_iter(&txn, &self.db.namespace)
                .map_err(storage)?
            {
                let (key, _) = row.map_err(storage)?;
                ids.insert(parse_run(&self.db.namespace, key).map_err(storage)?.0);
            }
            Ok(ids.into_iter().map(SagaId::new).collect())
        }
        fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
            let mut txn = self.db.env.write_txn().map_err(storage)?;
            let prefix = self.saga_prefix(saga_id);
            let runs = self
                .db
                .runs
                .prefix_iter(&txn, &prefix)
                .map_err(storage)?
                .map(|r| r.map(|(k, _)| k.to_owned()))
                .collect::<heed::Result<Vec<_>>>()
                .map_err(storage)?;
            for run in runs {
                self.db.remove_summary(&mut txn, &run).map_err(storage)?;
            }
            for db in [self.db.rows, self.dedupe] {
                let keys = db
                    .prefix_iter(&txn, &prefix)
                    .map_err(storage)?
                    .map(|r| r.map(|(k, _)| k.to_owned()))
                    .collect::<heed::Result<Vec<_>>>()
                    .map_err(storage)?;
                for key in keys {
                    db.delete(&mut txn, &key).map_err(storage)?;
                }
            }
            txn.commit().map_err(storage)
        }

        fn check_and_mark(&self, saga_id: SagaId, marker: &str) -> Result<bool, JournalError> {
            let mut txn = self.db.env.write_txn().map_err(storage)?;
            let mut latest = None;
            for row in self
                .db
                .runs
                .prefix_iter(&txn, &self.saga_prefix(saga_id))
                .map_err(storage)?
            {
                let (key, _) = row.map_err(storage)?;
                let run = parse_run(&self.db.namespace, key).map_err(storage)?;
                if latest.as_ref().is_none_or(|(at, _)| run.2 > *at) {
                    latest = Some((run.2, key.to_owned()));
                }
            }
            let (_, run) = latest.ok_or_else(|| storage("marker requires durable run evidence"))?;
            let key = format!("{run}{}", hex(marker));
            if key.len() > 511 {
                return Err(storage("dedupe key exceeds LMDB key capacity"));
            }
            if self.dedupe.get(&txn, &key).map_err(storage)?.is_some() {
                return Ok(false);
            }
            self.dedupe.put(&mut txn, &key, &[]).map_err(storage)?;
            txn.commit().map_err(storage)?;
            Ok(true)
        }
        fn remove_processed(&self, saga_id: SagaId, marker: &str) -> Result<(), JournalError> {
            let mut txn = self.db.env.write_txn().map_err(storage)?;
            let runs = self
                .db
                .runs
                .prefix_iter(&txn, &self.saga_prefix(saga_id))
                .map_err(storage)?
                .map(|row| row.map(|(key, _)| key.to_owned()))
                .collect::<heed::Result<Vec<_>>>()
                .map_err(storage)?;
            for run in runs {
                self.dedupe
                    .delete(&mut txn, &format!("{run}{}", hex(marker)))
                    .map_err(storage)?;
            }
            txn.commit().map_err(storage)
        }
        fn append_batch(
            &self,
            saga_id: SagaId,
            events: Vec<ParticipantEvent>,
        ) -> Result<Vec<u64>, JournalError> {
            let mut txn = self.db.env.write_txn().map_err(storage)?;
            let sequences = self.batch(&mut txn, saga_id, events)?;
            txn.commit().map_err(storage)?;
            Ok(sequences)
        }
        fn admit_and_record_intent(
            &self,
            c: &SagaContext,
            dedupe_key: &str,
            triggering_event: &str,
        ) -> Result<StoreAdmission, JournalError> {
            if c.saga_type.is_empty() {
                return Err(storage("empty saga type"));
            }
            let mut txn = self.db.env.write_txn().map_err(storage)?;
            let key = run_key(&self.db.namespace, &context_key(c)).map_err(storage)?;
            let marker = format!("{key}{}", hex(dedupe_key));
            if marker.len() > 511 {
                return Err(storage("dedupe key exceeds LMDB key capacity"));
            }
            // Indexed per-id summaries only, never event-history reconstruction.
            let mut summaries = Vec::new();
            for row in self
                .db
                .runs
                .prefix_iter(&txn, &self.saga_prefix(c.saga_id))
                .map_err(storage)?
            {
                let (k, bytes) = row.map_err(storage)?;
                summaries.push((
                    parse_run(&self.db.namespace, k).map_err(storage)?,
                    decode_summary(bytes).map_err(storage)?,
                ));
            }
            if let Some(reason) = admission(c, summaries.iter().map(|(k, s)| (k, s))) {
                return Ok(StoreAdmission::Rejected(reason));
            }
            if self.dedupe.get(&txn, &marker).map_err(storage)?.is_some() {
                return Ok(StoreAdmission::Duplicate);
            }
            let new_run = self.db.runs.get(&txn, &key).map_err(storage)?.is_none();
            // Stage marker first: any later serialization/map-full/sequence error
            // aborts it along with rows, summaries, indexes and the sequence.
            self.dedupe.put(&mut txn, &marker, &[]).map_err(storage)?;
            self.batch(&mut txn, c.saga_id, intent(c, triggering_event, new_run))?;
            txn.commit().map_err(storage)?;
            Ok(StoreAdmission::Admitted)
        }
        fn unresolved_saga_ids(&self) -> Result<Vec<SagaId>, JournalError> {
            let txn = self.db.env.read_txn().map_err(storage)?;
            let mut ids = Vec::new();
            for row in self
                .db
                .unresolved
                .prefix_iter(&txn, &self.db.namespace)
                .map_err(storage)?
            {
                let (key, _) = row.map_err(storage)?;
                let id = SagaId::new(parse_run(&self.db.namespace, key).map_err(storage)?.0);
                if ids.last() != Some(&id) {
                    ids.push(id);
                }
            }
            Ok(ids)
        }
        fn compact_terminal_detail(&self, batch_limit: usize) -> Result<usize, JournalError> {
            self.db
                .maintain(None, batch_limit, |_, _, _| Ok(false), Some(self.dedupe))
                .map_err(storage)
        }
        fn prune_resolved_before(
            &self,
            cutoff_millis: u64,
            batch_limit: usize,
        ) -> Result<usize, JournalError> {
            self.db
                .maintain(
                    Some(cutoff_millis),
                    batch_limit,
                    |_, _, _| Ok(false),
                    Some(self.dedupe),
                )
                .map_err(storage)
        }
    }
    fn decode_entry(bytes: &[u8]) -> Result<JournalEntry, JournalError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<JournalEntry, rkyv::rancor::Error>(&aligned).map_err(storage)
    }
}
