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
/// ```ignore
/// let journal = InMemoryJournal::new();
/// let saga_id = SagaId::new(1);
///
/// // Record an event
/// journal.append(saga_id, ParticipantEvent::Started)?;
///
/// // Read back all events for a saga
/// let entries = journal.read(saga_id)?;
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
/// # Thread Safety
///
/// Uses `RwLock` internally to provide thread-safe access to the journal.
pub struct InMemoryJournal {
    /// The backing store mapping SAGA IDs to their journal entries.
    data: std::sync::RwLock<std::collections::HashMap<u64, Vec<JournalEntry>>>,
    /// Atomic counter for generating monotonically increasing sequence numbers.
    counter: std::sync::atomic::AtomicU64,
}

impl InMemoryJournal {
    /// Creates a new empty in-memory journal.
    pub fn new() -> Self {
        Self {
            data: std::sync::RwLock::new(std::collections::HashMap::new()),
            counter: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl ParticipantJournal for InMemoryJournal {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        let seq = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = JournalEntry {
            sequence: seq,
            recorded_at_millis: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            event,
        };

        let mut data = self
            .data
            .write()
            .map_err(|e| JournalError::Storage(e.to_string().into()))?;
        data.entry(saga_id.0).or_default().push(entry);

        Ok(seq)
    }

    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        let data = self
            .data
            .read()
            .map_err(|e| JournalError::Storage(e.to_string().into()))?;
        Ok(data.get(&saga_id.0).cloned().unwrap_or_default())
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        let data = self
            .data
            .read()
            .map_err(|e| JournalError::Storage(e.to_string().into()))?;
        Ok(data.keys().map(|&id| SagaId::new(id)).collect())
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
}
