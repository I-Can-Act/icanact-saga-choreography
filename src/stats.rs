//! Participant statistics

use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe statistics tracker for a saga participant.
///
/// Tracks counters for various saga lifecycle events, enabling monitoring
/// of participant activity, success rates, and error conditions.
/// All counters use atomic operations for safe concurrent access.
///
/// # Example
///
/// ```
/// use std::sync::atomic::Ordering;
/// use icanact_saga_choreography::ParticipantStats;
///
/// let stats = ParticipantStats::new();
/// stats.events_received.fetch_add(1, Ordering::Relaxed);
/// stats.steps_completed.fetch_add(1, Ordering::Relaxed);
///
/// let snapshot = stats.snapshot();
/// assert_eq!(snapshot.events_received, 1);
/// assert_eq!(snapshot.steps_completed, 1);
/// ```
pub struct ParticipantStats {
    /// Total number of events received by this participant from the message broker.
    /// Includes all events regardless of whether they are relevant to this participant.
    pub events_received: AtomicU64,

    /// Number of events that were relevant to this participant.
    /// An event is relevant if it matches the participant's subscription criteria
    /// and triggers step execution or compensation.
    pub events_relevant: AtomicU64,

    /// Number of duplicate events detected and ignored.
    /// Duplicates can occur due to message broker redelivery or network retries.
    pub duplicate_events: AtomicU64,

    /// Number of saga steps that have started execution.
    /// Increments when a participant begins processing a step handler.
    pub steps_started: AtomicU64,

    /// Number of saga steps that completed successfully.
    /// A step is completed when its handler returns without error.
    pub steps_completed: AtomicU64,

    /// Number of saga steps that failed during execution.
    /// Step failures may trigger compensation in the saga.
    pub steps_failed: AtomicU64,

    /// Number of compensation handlers that have started execution.
    /// Compensation runs in reverse order when a saga needs to rollback.
    pub compensations_started: AtomicU64,

    /// Number of compensation handlers that completed successfully.
    pub compensations_completed: AtomicU64,

    /// Number of sagas that have been quarantined by this participant.
    /// Quarantined sagas are paused and require manual intervention.
    pub quarantined_sagas: AtomicU64,
}

impl ParticipantStats {
    /// Creates a new `ParticipantStats` instance with all counters initialized to zero.
    pub fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_relevant: AtomicU64::new(0),
            duplicate_events: AtomicU64::new(0),
            steps_started: AtomicU64::new(0),
            steps_completed: AtomicU64::new(0),
            steps_failed: AtomicU64::new(0),
            compensations_started: AtomicU64::new(0),
            compensations_completed: AtomicU64::new(0),
            quarantined_sagas: AtomicU64::new(0),
        }
    }

    /// Creates an immutable snapshot of all current statistics.
    ///
    /// The snapshot captures consistent values across all counters at a point in time.
    /// Uses `Ordering::Relaxed` for atomic loads, which is sufficient for monitoring
    /// purposes where exact consistency with concurrent updates is not critical.
    pub fn snapshot(&self) -> ParticipantStatsSnapshot {
        ParticipantStatsSnapshot {
            events_received: self.events_received.load(Ordering::Relaxed),
            events_relevant: self.events_relevant.load(Ordering::Relaxed),
            duplicate_events: self.duplicate_events.load(Ordering::Relaxed),
            steps_started: self.steps_started.load(Ordering::Relaxed),
            steps_completed: self.steps_completed.load(Ordering::Relaxed),
            steps_failed: self.steps_failed.load(Ordering::Relaxed),
            compensations_started: self.compensations_started.load(Ordering::Relaxed),
            compensations_completed: self.compensations_completed.load(Ordering::Relaxed),
            quarantined_sagas: self.quarantined_sagas.load(Ordering::Relaxed),
        }
    }
}

impl Default for ParticipantStats {
    fn default() -> Self {
        Self::new()
    }
}

/// An immutable snapshot of participant statistics at a point in time.
///
/// This struct provides a copy of all counter values that can be used
/// for reporting, logging, or comparison without holding references
/// to the live statistics.
#[derive(Clone, Debug)]
pub struct ParticipantStatsSnapshot {
    /// Total number of events received by this participant.
    pub events_received: u64,

    /// Number of events that were relevant to this participant.
    pub events_relevant: u64,

    /// Number of duplicate events detected and ignored.
    pub duplicate_events: u64,

    /// Number of saga steps that have started execution.
    pub steps_started: u64,

    /// Number of saga steps that completed successfully.
    pub steps_completed: u64,

    /// Number of saga steps that failed during execution.
    pub steps_failed: u64,

    /// Number of compensation handlers that have started execution.
    pub compensations_started: u64,

    /// Number of compensation handlers that completed successfully.
    pub compensations_completed: u64,

    /// Number of sagas that have been quarantined.
    pub quarantined_sagas: u64,
}
