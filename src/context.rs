//! Saga context and identity types

/// Unique identifier for a saga execution
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Clone)]
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
    /// Distributed tracing ID
    pub trace_id: u64,
    /// Index of this step in the workflow
    pub step_index: usize,
    /// Retry attempt number (0 = first attempt)
    pub attempt: u32,
    /// Peer ID of the saga initiator
    pub initiator_peer_id: PeerId,
    /// When the saga started (millis since UNIX epoch)
    pub saga_started_at_millis: u64,
    /// Timestamp of this event (millis since UNIX epoch)
    pub event_timestamp_millis: u64,
}

impl SagaContext {
    /// Get current time in milliseconds since UNIX epoch
    pub fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
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

    /// Create a context for a retry attempt
    pub fn retry(&self) -> Self {
        Self {
            attempt: self.attempt + 1,
            trace_id: Self::next_trace_id(),
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

    /// Calculate elapsed time since saga started
    pub fn elapsed_millis(&self) -> u64 {
        self.event_timestamp_millis
            .saturating_sub(self.saga_started_at_millis)
    }

    fn next_trace_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
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
