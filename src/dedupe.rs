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
use std::collections::HashSet;
use std::sync::Mutex;

type DedupeKey = (u64, Box<str>);
type DedupeSet = HashSet<DedupeKey>;

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
/// ```
/// use icanact_saga_choreography::{
///     InMemoryDedupe, ParticipantDedupeStore, SagaId,
/// };
///
/// let dedupe = InMemoryDedupe::new();
/// let saga_id = SagaId::new(1);
/// let operation_key = "reserve_inventory";
///
/// assert!(dedupe.check_and_mark(saga_id, operation_key)?);
/// assert!(!dedupe.check_and_mark(saga_id, operation_key)?);
/// assert!(dedupe.contains(saga_id, operation_key)?);
/// # Ok::<(), icanact_saga_choreography::DedupeError>(())
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
    /// `Ok(true)` if operation marked processed, `Ok(false)` otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`DedupeError::Storage`] when backing storage cannot answer query.
    fn contains(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError>;

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

    /// Removes one processed marker when durable journal evidence proves that
    /// the corresponding operation was recorded but never started.
    fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError>;

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
/// An in-memory implementation [`ParticipantDedupeStore`].
///
/// This implementation stores deduplication records in process-local memory and
/// is suitable for testing and development. Records are not persisted across
/// restarts.
///
/// # Warning
///
/// This implementation should NOT be used in production as all deduplication
/// state is lost when the process terminates, which could lead to duplicate
/// processing of redelivered messages after a crash.
///
/// The backing set uses a short critical section. It does not start a worker
/// thread or perform a blocking actor ask, so it is safe to call from either
/// sync scheduler workers or async participants.
pub struct InMemoryDedupe {
    seen: Mutex<DedupeSet>,
}

impl InMemoryDedupe {
    /// Creates a new empty in-memory deduplication store.
    pub fn new() -> Self {
        Self {
            seen: Mutex::new(HashSet::new()),
        }
    }

    fn seen(&self) -> Result<std::sync::MutexGuard<'_, DedupeSet>, DedupeError> {
        self.seen
            .lock()
            .map_err(|_| DedupeError::Storage("in-memory dedupe lock poisoned".into()))
    }
}

impl ParticipantDedupeStore for InMemoryDedupe {
    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        Ok(self.seen()?.insert((saga_id.0, key.into())))
    }

    fn contains(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        Ok(self
            .seen()?
            .iter()
            .any(|(id, stored_key)| *id == saga_id.0 && stored_key.as_ref() == key))
    }

    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        self.seen()?.insert((saga_id.0, key.into()));
        Ok(())
    }

    fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        self.seen()?
            .retain(|(id, stored_key)| *id != saga_id.0 || stored_key.as_ref() != key);
        Ok(())
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError> {
        self.seen()?.retain(|(id, _)| *id != saga_id.0);
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

    fn contains(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        (**self).contains(saga_id, key)
    }

    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        (**self).mark_processed(saga_id, key)
    }

    fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        (**self).remove_processed(saga_id, key)
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError> {
        (**self).prune(saga_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn concurrent_check_and_mark_has_exactly_one_winner() {
        let store = Arc::new(InMemoryDedupe::new());
        let saga_id = SagaId::new(7);
        let workers = (0..16)
            .map(|_| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || store.check_and_mark(saga_id, "reserve").unwrap())
            })
            .collect::<Vec<_>>();

        let winners = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count();

        assert_eq!(winners, 1);
        assert!(store.contains(saga_id, "reserve").unwrap());
        store.prune(saga_id).unwrap();
        assert!(!store.contains(saga_id, "reserve").unwrap());
    }
}
