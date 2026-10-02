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
use crate::{
    InboxTxn, OutboxId, OutboxRecord, RunIncarnation, RunKey, RunTombstone, SagaChoreographyEvent,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// A trait for participant journal storage implementations.
///
/// The journal provides durable, append-only storage for events that occur
/// during SAGA execution. This enables participants to:
///
/// - Record events as they happen for recovery purposes
/// - Replay events to reconstruct state after a crash
/// - Query which SAGAs have been processed by this participant
///
/// Implementations should ensure atomicity of append operations and durability
/// of stored events. For production use, consider implementations backed by
/// databases or persistent message queues.
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync + 'static` as journals are typically
/// shared across async tasks.
///
/// # Example
///
/// ```
/// use icanact_saga_choreography::{
///     InMemoryJournal, ParticipantEvent, ParticipantJournal, SagaId,
/// };
///
/// let journal = InMemoryJournal::new();
/// let saga_id = SagaId::new(1);
/// journal.append(
///     saga_id,
///     ParticipantEvent::StepExecutionStarted {
///         attempt: 1,
///         started_at_millis: 42,
///     },
/// )?;
/// let entries = journal.read(saga_id)?;
/// assert_eq!(entries.len(), 1);
/// # Ok::<(), icanact_saga_choreography::JournalError>(())
/// ```
pub trait ParticipantJournal: Send + Sync + 'static {
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
    /// Terminal saga cleanup uses this to keep durable participant journals
    /// bounded. Active, non-terminal SAGAs remain journaled for startup
    /// recovery until they reach a terminal event.
    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError>;

    /// Run-scoped append; returns the row sequence (ADR-0001).
    fn append_run(&self, run: &RunKey, event: ParticipantEvent) -> Result<u64, JournalError>;

    /// Rows of one run in sequence order (ADR-0001).
    fn read_run(&self, run: &RunKey) -> Result<Vec<JournalEntry>, JournalError>;

    /// Runs that currently have rows, sorted (ADR-0001).
    fn list_runs(&self) -> Result<Vec<RunKey>, JournalError>;

    /// One txn: write `tombstone`, delete the run's rows, delete tombstones older than `cutoff` (ADR-0001).
    fn finalize_run(
        &self,
        tombstone: &RunTombstone,
        cutoff: RunIncarnation,
    ) -> Result<(), JournalError>;

    /// Unexpired tombstones of `(saga_type, saga_id)` (ADR-0001).
    fn run_tombstones(
        &self,
        saga_type: &str,
        saga_id: SagaId,
    ) -> Result<Vec<RunTombstone>, JournalError>;

    /// Delete tombstones with `incarnation < cutoff`; returns how many (ADR-0001).
    fn prune_expired_tombstones(&self, cutoff: RunIncarnation) -> Result<u64, JournalError>;

    /// Atomic inbox commit as one row (ADR-0005).
    fn commit_inbox(&self, run: &RunKey, txn: InboxTxn) -> Result<u64, JournalError> {
        self.append_run(run, txn.into_event())
    }

    /// Append `event` with the outbound events it obliges in one row (ADR-0003).
    fn commit_with_outbox(
        &self,
        run: &RunKey,
        event: ParticipantEvent,
        outbox: Vec<SagaChoreographyEvent>,
    ) -> Result<u64, JournalError> {
        if matches!(event, ParticipantEvent::TransitionCommitted { .. }) {
            return Err(JournalError::Storage(
                "commit_with_outbox: transition is already an outbox row (ADR-0003)".into(),
            ));
        }
        if let Some(foreign) = outbox
            .iter()
            .find(|obligation| !run.is_run_of(obligation.context()))
        {
            return Err(JournalError::Storage(
                format!(
                    "commit_with_outbox: obligation {} belongs to another run than {run}",
                    foreign.event_type()
                )
                .into(),
            ));
        }
        if outbox.is_empty() {
            return self.append_run(run, event);
        }
        self.append_run(
            run,
            ParticipantEvent::TransitionCommitted {
                transition: Box::new(event),
                outbox,
            },
        )
    }

    /// Obligations of every run with `incarnation >= cutoff`, in (run, sequence, index) order (ADR-0003).
    fn outbox_for_replay(&self, cutoff: RunIncarnation) -> Result<Vec<OutboxRecord>, JournalError> {
        let mut records = Vec::new();
        for run in self.list_runs()? {
            if run.incarnation() < cutoff {
                continue;
            }
            for entry in self.read_run(&run)? {
                for (index, event) in entry.event.outbox().iter().enumerate() {
                    let index = u32::try_from(index).map_err(|_| {
                        JournalError::Storage("outbox row index exceeds u32".into())
                    })?;
                    records.push(OutboxRecord {
                        id: OutboxId::new(run.clone(), entry.sequence, index),
                        event: event.clone(),
                    });
                }
            }
        }
        Ok(records)
    }
}

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

    /// A persistent journal still holds rows written before run identity (ADR-0001 §2.6).
    #[error(
        "participant journal holds {legacy_rows} rows written before run identity (schema 2); drain in-flight sagas on the previous binary until the journal is empty, then start this version (docs/upgrade.md#run-identity)"
    )]
    LegacyRunIdentity { legacy_rows: u64 },
}

/// An in-memory implementation of [`ParticipantJournal`].
///
/// This implementation stores journal entries in memory using a `HashMap`
/// and is suitable for testing and development. Data is not persisted
/// across restarts.
///
/// # Warning
///
/// This implementation should NOT be used in production as all data
/// is lost when the process terminates.
///
/// An in-memory implementation [`ParticipantJournal`].
///
/// This implementation stores journal entries in process-local memory
/// and is suitable for testing and development. Data is not persisted across
/// restarts.
///
/// # Warning
///
/// This implementation should NOT be used in production as all data is lost
/// when the process terminates.
///
/// The backing map uses a short critical section. It does not start a worker
/// thread or perform a blocking actor ask, so it is safe to call from either
/// sync scheduler workers or async participants.
pub struct InMemoryJournal {
    state: Mutex<InMemoryJournalState>,
}

struct InMemoryJournalState {
    /// Legacy `SagaId` partition (`append`).
    entries: HashMap<u64, Vec<JournalEntry>>,
    /// Run-scoped rows (`append_run`).
    runs: BTreeMap<RunKey, Vec<JournalEntry>>,
    tombstones: BTreeMap<RunKey, RunTombstone>,
    next_sequence: u64,
}

impl InMemoryJournalState {
    fn row(&mut self, event: ParticipantEvent) -> JournalEntry {
        let recorded_at_millis =
            match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => duration.as_millis() as u64,
                Err(err) => {
                    tracing::error!(
                        target: "core::saga",
                        event = "in_memory_journal_now_millis_failed",
                        error = %err
                    );
                    0
                }
            };
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        JournalEntry {
            sequence,
            recorded_at_millis,
            event,
        }
    }
}

impl InMemoryJournal {
    /// Creates a new empty in-memory journal.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(InMemoryJournalState {
                entries: HashMap::new(),
                runs: BTreeMap::new(),
                tombstones: BTreeMap::new(),
                next_sequence: 1,
            }),
        }
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, InMemoryJournalState>, JournalError> {
        self.state
            .lock()
            .map_err(|_| JournalError::Storage("in-memory journal lock poisoned".into()))
    }
}

/// Rejects a `TransitionCommitted` row that wraps another `TransitionCommitted` (ADR-0003).
pub(crate) fn reject_nested_transition(
    run: &RunKey,
    event: &ParticipantEvent,
) -> Result<(), JournalError> {
    if let ParticipantEvent::TransitionCommitted { transition, .. } = event
        && matches!(
            transition.as_ref(),
            ParticipantEvent::TransitionCommitted { .. }
        )
    {
        tracing::error!(
            target: "core::saga",
            event = "journal_nested_transition_rejected",
            run = %run
        );
        return Err(JournalError::Storage(
            format!("append_run: nested TransitionCommitted rejected for {run} (ADR-0003)").into(),
        ));
    }
    Ok(())
}

impl ParticipantJournal for InMemoryJournal {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        let mut state = self.state()?;
        let entry = state.row(event);
        let sequence = entry.sequence;
        state.entries.entry(saga_id.0).or_default().push(entry);
        Ok(sequence)
    }

    /// Union of the legacy partition and every run with this id, by sequence (ADR-0001 §2.4).
    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        let state = self.state()?;
        let mut rows = state.entries.get(&saga_id.0).cloned().unwrap_or_default();
        for (run, run_rows) in &state.runs {
            if run.saga_id() == saga_id {
                rows.extend(run_rows.iter().cloned());
            }
        }
        rows.sort_by_key(|row| row.sequence);
        Ok(rows)
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        let state = self.state()?;
        let mut ids: Vec<u64> = state.entries.keys().copied().collect();
        ids.extend(state.runs.keys().map(|run| run.saga_id().get()));
        ids.sort_unstable();
        ids.dedup();
        Ok(ids.into_iter().map(SagaId::new).collect())
    }

    /// Deletes the legacy partition and every run with this id; tombstones are kept (ADR-0001 §2.4).
    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        let mut state = self.state()?;
        state.entries.remove(&saga_id.0);
        state.runs.retain(|run, _| run.saga_id() != saga_id);
        Ok(())
    }

    fn append_run(&self, run: &RunKey, event: ParticipantEvent) -> Result<u64, JournalError> {
        reject_nested_transition(run, &event)?;
        let mut state = self.state()?;
        let entry = state.row(event);
        let sequence = entry.sequence;
        state.runs.entry(run.clone()).or_default().push(entry);
        Ok(sequence)
    }

    fn read_run(&self, run: &RunKey) -> Result<Vec<JournalEntry>, JournalError> {
        Ok(self.state()?.runs.get(run).cloned().unwrap_or_default())
    }

    fn list_runs(&self) -> Result<Vec<RunKey>, JournalError> {
        Ok(self.state()?.runs.keys().cloned().collect())
    }

    fn finalize_run(
        &self,
        tombstone: &RunTombstone,
        cutoff: RunIncarnation,
    ) -> Result<(), JournalError> {
        let mut state = self.state()?;
        state.runs.remove(tombstone.run());
        state
            .tombstones
            .insert(tombstone.run().clone(), tombstone.clone());
        state
            .tombstones
            .retain(|run, _| run.incarnation() >= cutoff);
        Ok(())
    }

    fn run_tombstones(
        &self,
        saga_type: &str,
        saga_id: SagaId,
    ) -> Result<Vec<RunTombstone>, JournalError> {
        Ok(self
            .state()?
            .tombstones
            .iter()
            .filter(|(run, _)| run.saga_id() == saga_id && run.saga_type() == saga_type)
            .map(|(_, tombstone)| tombstone.clone())
            .collect())
    }

    fn prune_expired_tombstones(&self, cutoff: RunIncarnation) -> Result<u64, JournalError> {
        let mut state = self.state()?;
        let before = state.tombstones.len();
        state
            .tombstones
            .retain(|run, _| run.incarnation() >= cutoff);
        Ok((before - state.tombstones.len()) as u64)
    }
}

impl Default for InMemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ParticipantJournal for std::sync::Arc<T>
where
    T: ParticipantJournal + ?Sized,
{
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        (**self).append(saga_id, event)
    }

    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        (**self).read(saga_id)
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        (**self).list_sagas()
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        (**self).prune(saga_id)
    }

    fn append_run(&self, run: &RunKey, event: ParticipantEvent) -> Result<u64, JournalError> {
        (**self).append_run(run, event)
    }

    fn read_run(&self, run: &RunKey) -> Result<Vec<JournalEntry>, JournalError> {
        (**self).read_run(run)
    }

    fn list_runs(&self) -> Result<Vec<RunKey>, JournalError> {
        (**self).list_runs()
    }

    fn finalize_run(
        &self,
        tombstone: &RunTombstone,
        cutoff: RunIncarnation,
    ) -> Result<(), JournalError> {
        (**self).finalize_run(tombstone, cutoff)
    }

    fn run_tombstones(
        &self,
        saga_type: &str,
        saga_id: SagaId,
    ) -> Result<Vec<RunTombstone>, JournalError> {
        (**self).run_tombstones(saga_type, saga_id)
    }

    fn prune_expired_tombstones(&self, cutoff: RunIncarnation) -> Result<u64, JournalError> {
        (**self).prune_expired_tombstones(cutoff)
    }

    fn commit_inbox(&self, run: &RunKey, txn: InboxTxn) -> Result<u64, JournalError> {
        (**self).commit_inbox(run, txn)
    }

    fn commit_with_outbox(
        &self,
        run: &RunKey,
        event: ParticipantEvent,
        outbox: Vec<SagaChoreographyEvent>,
    ) -> Result<u64, JournalError> {
        (**self).commit_with_outbox(run, event, outbox)
    }

    fn outbox_for_replay(&self, cutoff: RunIncarnation) -> Result<Vec<OutboxRecord>, JournalError> {
        (**self).outbox_for_replay(cutoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeterministicContextBuilder, RunTerminalOutcome};

    #[test]
    fn w1_run_methods_delegate_and_outbox_is_one_row() {
        let journal = InMemoryJournal::new();
        let ctx = DeterministicContextBuilder::default().build();
        let run = ctx.run_key();
        let saga_id = run.saga_id();

        journal
            .append_run(
                &run,
                ParticipantEvent::StepExecutionStarted {
                    attempt: 0,
                    started_at_millis: 1,
                },
            )
            .expect("append_run");
        assert_eq!(journal.read(saga_id).expect("read").len(), 1);
        assert_eq!(journal.read_run(&run).expect("read_run").len(), 1);
        assert_eq!(journal.list_runs().expect("list_runs"), vec![run.clone()]);

        let completed = || ParticipantEvent::StepExecutionCompleted {
            output: vec![1],
            compensation_data: Vec::new(),
            completed_at_millis: 2,
        };
        let obligation = SagaChoreographyEvent::StepCompleted {
            context: ctx.clone(),
            output: vec![1],
            saga_input: Vec::new(),
            compensation_available: false,
        };
        journal
            .commit_with_outbox(&run, completed(), vec![obligation])
            .expect("commit_with_outbox");
        let rows = journal.read_run(&run).expect("read_run");
        assert_eq!(rows.len(), 2, "event and obligation share exactly one row");
        let row = &rows[1].event;
        assert!(matches!(
            row.transition(),
            ParticipantEvent::StepExecutionCompleted { .. }
        ));
        assert_eq!(row.outbox().len(), 1);

        let mut foreign_ctx = ctx.clone();
        foreign_ctx.saga_started_at_millis += 1;
        let foreign = SagaChoreographyEvent::SagaCompleted {
            context: foreign_ctx,
        };
        assert!(
            journal
                .commit_with_outbox(&run, completed(), vec![foreign])
                .is_err()
        );
        assert_eq!(journal.read_run(&run).expect("read_run").len(), 2);

        let tombstone = RunTombstone::new(run.clone(), RunTerminalOutcome::Completed, 3);
        journal
            .finalize_run(&tombstone, RunIncarnation::new(0))
            .expect("finalize_run");
        assert!(journal.read(saga_id).expect("read").is_empty());
    }
}
