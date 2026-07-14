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
use std::collections::HashMap;
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
    entries: HashMap<u64, Vec<JournalEntry>>,
    next_sequence: u64,
}

impl InMemoryJournal {
    /// Creates a new empty in-memory journal.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(InMemoryJournalState {
                entries: HashMap::new(),
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

impl ParticipantJournal for InMemoryJournal {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
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
        let mut state = self.state()?;
        let sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.saturating_add(1);
        state
            .entries
            .entry(saga_id.0)
            .or_default()
            .push(JournalEntry {
                sequence,
                recorded_at_millis,
                event,
            });
        Ok(sequence)
    }

    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        Ok(self
            .state()?
            .entries
            .get(&saga_id.0)
            .cloned()
            .unwrap_or_default())
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        Ok(self
            .state()?
            .entries
            .keys()
            .copied()
            .map(SagaId::new)
            .collect())
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        self.state()?.entries.remove(&saga_id.0);
        Ok(())
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
}
