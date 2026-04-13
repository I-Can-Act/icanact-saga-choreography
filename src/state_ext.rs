//! Extension trait for saga state management.
//!
//! This module provides the [`SagaStateExt`] trait, which supplies common
//! state-management methods for saga participants.
//!
//! Preferred integration: embed [`crate::SagaParticipantSupport`] in your actor
//! and implement [`crate::HasSagaParticipantSupport`]. This crate will then
//! provide `SagaStateExt` automatically.

use crate::{
    HasSagaParticipantSupport, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    SagaId, SagaStateEntry,
};
use std::collections::{HashMap, HashSet};

/// Extension trait providing common saga state management operations.
///
/// This trait defines the core interface for types that manage saga lifecycle
/// state, including access to state storage, journaling, and deduplication.
/// This trait is only available to types that implement
/// [`crate::HasSagaParticipantSupport`]. Manual `SagaStateExt` implementations
/// are intentionally not supported.
///
/// # Implementation Requirements
///
/// Implementors must provide:
/// - Mutable and immutable access to the saga state map
/// - Access to the participant journal for event persistence
/// - Access to the deduplication store for idempotency tracking
/// - A monotonic timestamp source for time-based operations
///
/// # Example
///
/// ```ignore
/// struct MyActor {
///     saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
/// }
///
/// impl HasSagaParticipantSupport for MyActor {
///     type Journal = InMemoryJournal;
///     type Dedupe = InMemoryDedupe;
///
///     fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
///         &self.saga
///     }
///
///     fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
///         &mut self.saga
///     }
/// }
/// ```
pub trait SagaStateExt: HasSagaParticipantSupport {
    /// Returns mutable access to the saga state map.
    ///
    /// This provides direct access to the underlying storage for saga state entries,
    /// allowing modifications such as inserting new sagas or updating existing ones.
    fn saga_states(&mut self) -> &mut HashMap<SagaId, SagaStateEntry> {
        &mut self.saga_support_mut().saga_states
    }

    /// Returns immutable access to the saga state map.
    ///
    /// Use this for read-only operations that need to inspect saga state
    /// without modifying it.
    fn saga_states_ref(&self) -> &HashMap<SagaId, SagaStateEntry> {
        &self.saga_support().saga_states
    }

    /// Returns mutable access to per-saga dependency completion tracking.
    fn dependency_completions(&mut self) -> &mut HashMap<SagaId, HashSet<Box<str>>> {
        &mut self.saga_support_mut().dependency_completions
    }

    /// Returns mutable access to per-saga dependency fire tracking.
    fn dependency_fired(&mut self) -> &mut HashSet<SagaId> {
        &mut self.saga_support_mut().dependency_fired
    }

    /// Returns mutable access to terminal saga latches.
    fn terminal_sagas(&mut self) -> &mut HashSet<SagaId> {
        &mut self.saga_support_mut().terminal_sagas
    }

    /// Returns true when this participant has already observed terminal saga state
    /// for the given saga id and should ignore late replays until a new SagaStarted resets it.
    fn is_terminal_saga_latched(&self, saga_id: SagaId) -> bool {
        self.saga_support().terminal_sagas.contains(&saga_id)
    }

    /// Returns the participant journal for event persistence.
    ///
    /// The journal is used to durably record saga events for recovery
    /// and audit purposes.
    fn saga_journal(&self) -> &<Self as HasSagaParticipantSupport>::Journal {
        &self.saga_support().journal
    }

    /// Returns the deduplication store for idempotency tracking.
    ///
    /// The dedupe store tracks which operations have already been processed
    /// to prevent duplicate execution of side effects.
    fn saga_dedupe(&self) -> &<Self as HasSagaParticipantSupport>::Dedupe {
        &self.saga_support().dedupe
    }

    /// Returns the current timestamp in milliseconds.
    ///
    /// This should return a monotonically increasing value suitable for
    /// time-based operations such as timeouts and expiration checks.
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Checks and marks a deduplication key for the given saga.
    ///
    /// Returns `true` if this is the first time the key has been seen for
    /// this saga (indicating the operation should proceed), or `false` if
    /// the key has already been processed (indicating the operation should
    /// be skipped as a duplicate).
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga
    /// * `key` - The deduplication key to check
    ///
    /// # Returns
    ///
    /// `true` if the operation is new and should proceed, `false` if it
    /// has already been processed.
    fn check_dedupe(&self, saga_id: SagaId, key: &str) -> bool {
        self.saga_dedupe()
            .check_and_mark(saga_id, key)
            .unwrap_or(false)
    }

    /// Records an event to the saga journal.
    ///
    /// Appends the given event to the durable journal for the specified saga.
    /// Errors during journaling are silently ignored; use this for best-effort
    /// event recording where durability is desired but not strictly required.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga
    /// * `event` - The participant event to record
    fn record_event(&self, saga_id: SagaId, event: ParticipantEvent) {
        let _ = self.saga_journal().append(saga_id, event);
    }

    /// Removes all state associated with a saga.
    ///
    /// This removes the saga from the state map and prunes its deduplication
    /// entries. Use this when a saga has completed and its state is no longer
    /// needed.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga to prune
    fn prune_saga(&mut self, saga_id: SagaId) {
        self.saga_states().remove(&saga_id);
        self.dependency_completions().remove(&saga_id);
        self.dependency_fired().remove(&saga_id);
        let _ = self.saga_dedupe().prune(saga_id);
    }

    /// Checks whether a saga is still actively running.
    ///
    /// Returns `true` if the saga exists and has not reached a terminal state,
    /// `false` otherwise (including if the saga does not exist).
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga to check
    ///
    /// # Returns
    ///
    /// `true` if the saga is active, `false` if completed, failed, or not found.
    fn is_saga_active(&self, saga_id: SagaId) -> bool {
        self.saga_states_ref()
            .get(&saga_id)
            .map(|e| !e.is_terminal())
            .unwrap_or(false)
    }

    /// Returns a list of all active saga identifiers.
    ///
    /// Collects and returns the IDs of all sagas that have not yet reached
    /// a terminal state. Useful for monitoring and cleanup operations.
    ///
    /// # Returns
    ///
    /// A vector containing the IDs of all active sagas.
    fn active_saga_ids(&self) -> Vec<SagaId> {
        self.saga_states_ref()
            .iter()
            .filter(|(_, entry)| !entry.is_terminal())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Returns the count of currently active sagas.
    ///
    /// This is a convenience method that counts sagas that have not yet
    /// reached a terminal state.
    ///
    /// # Returns
    ///
    /// The number of active (non-terminal) sagas.
    fn active_saga_count(&self) -> usize {
        self.saga_states_ref()
            .values()
            .filter(|e| !e.is_terminal())
            .count()
    }
}

impl<T> SagaStateExt for T where T: HasSagaParticipantSupport {}

#[cfg(test)]
mod tests {
    use crate::{
        HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, ParticipantEvent,
        ParticipantJournal, SagaId, SagaParticipantSupport,
    };

    use super::SagaStateExt;

    struct DummyParticipant {
        saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    }

    impl DummyParticipant {
        fn new() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            }
        }
    }

    impl HasSagaParticipantSupport for DummyParticipant {
        type Journal = InMemoryJournal;
        type Dedupe = InMemoryDedupe;

        fn saga_support(&self) -> &crate::SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &self.saga
        }

        fn saga_support_mut(
            &mut self,
        ) -> &mut crate::SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &mut self.saga
        }
    }

    #[test]
    fn blanket_impl_routes_state_and_storage_through_embedded_support() {
        let participant = DummyParticipant::new();
        let saga_id = SagaId::new(42);

        participant.record_event(
            saga_id,
            ParticipantEvent::StepTriggered {
                triggering_event: "saga_started".into(),
                triggered_at_millis: 10,
            },
        );
        assert_eq!(
            participant
                .saga_journal()
                .read(saga_id)
                .expect("journal should read")
                .len(),
            1
        );

        assert!(participant.check_dedupe(saga_id, "step_started"));
        assert!(!participant.check_dedupe(saga_id, "step_started"));
        assert_eq!(participant.active_saga_count(), 0);
    }
}
