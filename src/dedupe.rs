//! Participant deduplication storage for idempotent SAGA processing.
//!
//! This module provides deduplication infrastructure that enables participant
//! services to safely process the same message multiple times without
//! side effects. This is critical in distributed systems because:
//!
//! - Messages may be redelivered due to network issues
//! - At-least-once delivery semantics require idempotent handlers
//! - Retry logic may cause duplicate attempts
//!
//! In the choreography-based SAGA pattern, each participant must be able to
//! determine if it has already processed a given request to maintain exactly-once
//! semantics despite the possibility of duplicate message delivery.

use super::SagaId;

/// A trait for participant deduplication storage implementations.
///
/// The deduplication store tracks which operations have already been processed
/// for each SAGA, enabling idempotent message handling. This is essential for:
///
/// - Preventing duplicate transaction execution
/// - Ensuring compensation actions aren't applied multiple times
/// - Maintaining exactly-once processing semantics
///
/// Implementations should provide persistent storage with appropriate TTLs
/// for production use. The store should survive process restarts to handle
/// redelivered messages after crashes.
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync + 'static` as stores are typically
/// shared across async tasks.
///
/// # Example
///
/// ```ignore
/// let dedupe = InMemoryDedupe::new();
/// let saga_id = SagaId::new(1);
/// let operation_key = "reserve_inventory";
///
/// // Check and mark atomically - returns true if this is a new operation
/// if dedupe.check_and_mark(saga_id, operation_key)? {
///     // First time processing - execute the operation
///     execute_operation()?;
/// } else {
///     // Already processed - skip or return cached result
/// }
/// ```
pub trait ParticipantDedupeStore: Send + Sync + 'static {
    /// Atomically checks if an operation has been processed and marks it if not.
    ///
    /// This is the preferred method for deduplication as it provides atomic
    /// check-and-set semantics, avoiding race conditions between concurrent
    /// checks.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the SAGA
    /// * `key` - A unique key identifying the specific operation within the SAGA
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if this is the first time the operation is being processed
    ///   (the operation was marked as processed)
    /// - `Ok(false)` if the operation was already processed previously
    /// - `Err(DedupeError)` if the storage operation failed
    ///
    /// # Errors
    ///
    /// Returns [`DedupeError::Storage`] if the underlying storage fails.
    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError>;

    /// Checks if an operation has already been processed without modifying state.
    ///
    /// Use this when you need to query state without the side effect of marking
    /// the operation as processed.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the SAGA
    /// * `key` - A unique key identifying the specific operation within the SAGA
    ///
    /// # Returns
    ///
    /// `true` if the operation has been marked as processed, `false` otherwise.
    fn contains(&self, saga_id: SagaId, key: &str) -> bool;

    /// Marks an operation as processed without checking first.
    ///
    /// Use this when you need to explicitly record that an operation was
    /// completed, such as after successfully executing an operation.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the SAGA
    /// * `key` - A unique key identifying the specific operation within the SAGA
    ///
    /// # Errors
    ///
    /// Returns [`DedupeError::Storage`] if the underlying storage fails.
    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError>;

    /// Removes all deduplication records for a completed SAGA.
    ///
    /// Call this when a SAGA has completed (successfully or with compensation)
    /// to free up storage. This is particularly important for long-running
    /// systems to prevent unbounded memory/disk growth.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the completed SAGA
    ///
    /// # Errors
    ///
    /// Returns [`DedupeError::Storage`] if the underlying storage fails.
    fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError>;
}

/// Errors that can occur during deduplication operations.
#[derive(Debug, thiserror::Error)]
pub enum DedupeError {
    /// A storage-layer error occurred.
    ///
    /// The contained string describes the specific error from the
    /// underlying storage mechanism.
    #[error("Storage error: {0}")]
    Storage(Box<str>),
}

/// An in-memory implementation of [`ParticipantDedupeStore`].
///
/// This implementation stores deduplication records in memory using a `HashSet`
/// and is suitable for testing and development. Records are not persisted
/// across restarts.
///
/// # Warning
///
/// This implementation should NOT be used in production as all deduplication
/// state is lost when the process terminates, which could lead to duplicate
/// processing of redelivered messages after a crash.
///
/// # Thread Safety
///
/// Uses `RwLock` internally to provide thread-safe access to the store.
pub struct InMemoryDedupe {
    /// The backing store containing tuples of (SAGA ID, operation key).
    data: std::sync::RwLock<std::collections::HashSet<(u64, Box<str>)>>,
}

impl InMemoryDedupe {
    /// Creates a new empty in-memory deduplication store.
    pub fn new() -> Self {
        Self {
            data: std::sync::RwLock::new(std::collections::HashSet::new()),
        }
    }
}

impl ParticipantDedupeStore for InMemoryDedupe {
    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        let entry = (saga_id.0, key.into());
        let mut data = self
            .data
            .write()
            .map_err(|e| DedupeError::Storage(e.to_string().into()))?;
        Ok(data.insert(entry))
    }

    fn contains(&self, saga_id: SagaId, key: &str) -> bool {
        let data = self.data.read().ok();
        data.map(|d| d.contains(&(saga_id.0, key.into())))
            .unwrap_or(false)
    }

    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        let mut data = self
            .data
            .write()
            .map_err(|e| DedupeError::Storage(e.to_string().into()))?;
        data.insert((saga_id.0, key.into()));
        Ok(())
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError> {
        let mut data = self
            .data
            .write()
            .map_err(|e| DedupeError::Storage(e.to_string().into()))?;
        data.retain(|(id, _)| *id != saga_id.0);
        Ok(())
    }
}

impl Default for InMemoryDedupe {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ParticipantDedupeStore for std::sync::Arc<T>
where
    T: ParticipantDedupeStore + ?Sized,
{
    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        (**self).check_and_mark(saga_id, key)
    }

    fn contains(&self, saga_id: SagaId, key: &str) -> bool {
        (**self).contains(saga_id, key)
    }

    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        (**self).mark_processed(saga_id, key)
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError> {
        (**self).prune(saga_id)
    }
}
