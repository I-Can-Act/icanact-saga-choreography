//! Saga context and identity types

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static LAST_RUN_START: AtomicU64 = AtomicU64::new(0);

fn allocate_run_start(counter: &AtomicU64, now_millis: u64) -> u64 {
    let mut previous = counter.load(Ordering::Relaxed);
    loop {
        assert_ne!(previous, u64::MAX, "run start token exhausted");
        let next = now_millis.max(previous.saturating_add(1));
        match counter.compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(observed) => previous = observed,
        }
    }
}

/// Unique identifier for a saga execution
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct SagaId(pub u64);

impl SagaId {
    /// Create a new saga ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value
    pub fn get(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for SagaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SagaId({})", self.0)
    }
}

impl std::fmt::Display for SagaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a step within a saga
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StepId {
    /// The saga this step belongs to
    pub saga_id: SagaId,
    /// Index of the step in the workflow
    pub step_index: usize,
}

/// Peer ID type (matches icanact-core)
pub type PeerId = [u8; 32];

/// Correlation context passed with every saga event
#[derive(Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SagaContext {
    /// Unique saga execution identifier
    pub saga_id: SagaId,
    /// Type of saga (e.g., "order_workflow")
    pub saga_type: Box<str>,
    /// Name of the current step
    pub step_name: Box<str>,
    /// Correlation ID linking all events in this saga
    pub correlation_id: u64,
    /// ID of the event that caused this one
    pub causation_id: u64,
    /// Per-event trace/span ID. `correlation_id` is stable for the saga.
    pub trace_id: u64,
    /// Index of this step in the workflow
    pub step_index: usize,
    /// Retry attempt number (0 = first attempt)
    pub attempt: u32,
    /// Peer ID of the saga initiator
    pub initiator_peer_id: PeerId,
    /// Logical run identity allocated by `next_run_start_millis`, not a timeout anchor.
    pub saga_started_at_millis: u64,
    /// Timestamp of this event (millis since UNIX epoch)
    pub event_timestamp_millis: u64,
}

impl SagaContext {
    /// Epoch milliseconds advanced only by monotonic elapsed time after initialization.
    pub fn now_millis() -> u64 {
        static ANCHOR: OnceLock<(u64, Instant)> = OnceLock::new();
        let (epoch_millis, instant) = ANCHOR.get_or_init(|| {
            let instant = Instant::now();
            let epoch_millis = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
                Err(err) => {
                    tracing::error!(
                        target: "core::saga",
                        event = "saga_now_millis_failed",
                        error = %err
                    );
                    0
                }
            };
            (epoch_millis, instant)
        });
        epoch_millis.saturating_add(instant.elapsed().as_millis().min(u64::MAX as u128) as u64)
    }

    /// Allocate a process-unique, increasing run identity. Not an elapsed-time clock.
    ///
    /// Panics on exhaustion rather than wrapping or reusing an identity.
    pub fn next_run_start_millis() -> u64 {
        allocate_run_start(&LAST_RUN_START, Self::now_millis())
    }

    /// Retained identities must precede all subsequently allocated run tokens.
    pub(crate) fn observe_run_start_millis(retained: u64) {
        LAST_RUN_START.fetch_max(retained, Ordering::Relaxed);
    }

    /// Create a context for the next step in sequence
    pub fn next_step(&self, step_name: Box<str>) -> Self {
        Self {
            step_name,
            causation_id: self.trace_id,
            trace_id: Self::next_trace_id(),
            step_index: self.step_index + 1,
            attempt: 0,
            event_timestamp_millis: Self::now_millis(),
            ..self.clone()
        }
    }

    /// Create a context for compensation
    pub fn for_compensation(&self) -> Self {
        Self {
            causation_id: self.trace_id,
            trace_id: Self::next_trace_id(),
            event_timestamp_millis: Self::now_millis(),
            ..self.clone()
        }
    }

    /// Event time minus the logical run token, saturating if allocation led the clock.
    /// This is not a timeout budget; use observed `now_millis` for elapsed timing.
    pub fn elapsed_millis(&self) -> u64 {
        self.event_timestamp_millis
            .saturating_sub(self.saga_started_at_millis)
    }

    fn next_trace_id() -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }
}

impl std::fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SagaContext")
            .field("saga_id", &self.saga_id)
            .field("saga_type", &self.saga_type)
            .field("step_name", &self.step_name)
            .field("step_index", &self.step_index)
            .field("attempt", &self.attempt)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;

    #[test]
    fn clock_does_not_regress_or_inflate_during_run_token_burst() {
        let elapsed = Instant::now();
        let before = SagaContext::now_millis();
        let mut previous_clock = before;
        let mut previous_token = SagaContext::next_run_start_millis();
        for _ in 0..50_000 {
            let token = SagaContext::next_run_start_millis();
            let clock = SagaContext::now_millis();
            assert!(token > previous_token, "run tokens must strictly increase");
            assert!(clock >= previous_clock, "elapsed clock must not regress");
            assert!(
                clock <= before + elapsed.elapsed().as_millis() as u64 + 1,
                "run identity allocation must not fabricate elapsed time"
            );
            previous_token = token;
            previous_clock = clock;
        }
    }

    #[test]
    fn run_tokens_increase_at_identical_and_regressing_clock_samples() {
        let counter = AtomicU64::new(0);
        assert_eq!(allocate_run_start(&counter, 1_000), 1_000);
        assert_eq!(allocate_run_start(&counter, 1_000), 1_001);
        assert_eq!(allocate_run_start(&counter, 900), 1_002);
        assert_eq!(allocate_run_start(&counter, 2_000), 2_000);
    }

    #[test]
    fn concurrent_run_tokens_are_unique() {
        let counter = AtomicU64::new(0);
        let mut tokens = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..1_000)
                            .map(|_| allocate_run_start(&counter, 1_000))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        tokens.sort_unstable();
        assert_eq!(tokens.len(), 8_000);
        assert_eq!(tokens.first(), Some(&1_000));
        assert_eq!(tokens.last(), Some(&8_999));
        assert!(tokens.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    #[should_panic(expected = "run start token exhausted")]
    fn exhausted_run_tokens_fail_closed_instead_of_reusing_identity() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(allocate_run_start(&counter, 0), u64::MAX);
        allocate_run_start(&counter, 0);
    }
}
