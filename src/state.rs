//! Typestate states for saga participants

pub mod markers {
    pub trait StepState: Send + 'static {}
}

// State types
pub struct Idle;
pub struct Triggered {
    pub triggered_at_millis: u64,
    pub triggering_event: Box<str>,
}
pub struct Executing {
    pub started_at_millis: u64,
    pub attempt: u32,
}
pub struct Completed {
    pub completed_at_millis: u64,
    pub output: Vec<u8>,
    pub compensation_data: Vec<u8>,
}
pub struct Failed {
    pub failed_at_millis: u64,
    pub error: Box<str>,
    pub requires_compensation: bool,
}
pub struct Compensating {
    pub started_at_millis: u64,
    pub attempt: u32,
}
pub struct Compensated {
    pub completed_at_millis: u64,
}
pub struct Quarantined {
    pub quarantined_at_millis: u64,
    pub reason: Box<str>,
}

impl markers::StepState for Idle {}
impl markers::StepState for Triggered {}
impl markers::StepState for Executing {}
impl markers::StepState for Completed {}
impl markers::StepState for Failed {}
impl markers::StepState for Compensating {}
impl markers::StepState for Compensated {}
impl markers::StepState for Quarantined {}

use super::ParticipantEvent;

/// Timestamped event for journal
pub struct TimestampedEvent {
    pub recorded_at_millis: u64,
    pub event: ParticipantEvent,
}

/// State container with typestate
pub struct SagaParticipantState<S: markers::StepState> {
    pub saga_id: super::SagaId,
    pub saga_type: Box<str>,
    pub step_name: Box<str>,
    pub correlation_id: u64,
    pub trace_id: u64,
    pub initiator_peer_id: super::PeerId,
    pub saga_started_at_millis: u64,
    pub last_updated_at_millis: u64,
    pub state: S,
    pub events: Vec<TimestampedEvent>,
}

impl SagaParticipantState<Idle> {
    pub fn new(
        saga_id: super::SagaId,
        saga_type: Box<str>,
        step_name: Box<str>,
        correlation_id: u64,
        trace_id: u64,
        initiator_peer_id: super::PeerId,
        saga_started_at_millis: u64,
    ) -> Self {
        Self {
            saga_id,
            saga_type,
            step_name,
            correlation_id,
            trace_id,
            initiator_peer_id,
            saga_started_at_millis,
            last_updated_at_millis: saga_started_at_millis,
            state: Idle,
            events: Vec::new(),
        }
    }

    pub fn trigger(
        self,
        triggering_event: &str,
        now_millis: u64,
    ) -> SagaParticipantState<Triggered> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Triggered {
                triggered_at_millis: now_millis,
                triggering_event: triggering_event.into(),
            },
            events: self.events,
        }
    }
}

impl SagaParticipantState<Triggered> {
    pub fn start_execution(self, now_millis: u64) -> SagaParticipantState<Executing> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Executing {
                started_at_millis: now_millis,
                attempt: 1,
            },
            events: self.events,
        }
    }
}

impl SagaParticipantState<Executing> {
    pub fn complete(
        self,
        output: Vec<u8>,
        compensation_data: Vec<u8>,
        now_millis: u64,
    ) -> SagaParticipantState<Completed> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Completed {
                completed_at_millis: now_millis,
                output,
                compensation_data,
            },
            events: self.events,
        }
    }

    pub fn fail(
        self,
        error: Box<str>,
        requires_compensation: bool,
        now_millis: u64,
    ) -> SagaParticipantState<Failed> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Failed {
                failed_at_millis: now_millis,
                error,
                requires_compensation,
            },
            events: self.events,
        }
    }
}

impl SagaParticipantState<Completed> {
    pub fn start_compensation(self, now_millis: u64) -> SagaParticipantState<Compensating> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Compensating {
                started_at_millis: now_millis,
                attempt: 1,
            },
            events: self.events,
        }
    }
}

impl SagaParticipantState<Compensating> {
    pub fn complete_compensation(self, now_millis: u64) -> SagaParticipantState<Compensated> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Compensated {
                completed_at_millis: now_millis,
            },
            events: self.events,
        }
    }

    pub fn quarantine(
        self,
        reason: Box<str>,
        now_millis: u64,
    ) -> SagaParticipantState<Quarantined> {
        SagaParticipantState {
            saga_id: self.saga_id,
            saga_type: self.saga_type,
            step_name: self.step_name,
            correlation_id: self.correlation_id,
            trace_id: self.trace_id,
            initiator_peer_id: self.initiator_peer_id,
            saga_started_at_millis: self.saga_started_at_millis,
            last_updated_at_millis: now_millis,
            state: Quarantined {
                quarantined_at_millis: now_millis,
                reason,
            },
            events: self.events,
        }
    }
}

/// Type-erased state entry for HashMap storage
pub enum SagaStateEntry {
    Idle(SagaParticipantState<Idle>),
    Triggered(SagaParticipantState<Triggered>),
    Executing(SagaParticipantState<Executing>),
    Completed(SagaParticipantState<Completed>),
    Failed(SagaParticipantState<Failed>),
    Compensating(SagaParticipantState<Compensating>),
    Compensated(SagaParticipantState<Compensated>),
    Quarantined(SagaParticipantState<Quarantined>),
}

impl SagaStateEntry {
    pub fn saga_id(&self) -> super::SagaId {
        match self {
            Self::Idle(s) => s.saga_id,
            Self::Triggered(s) => s.saga_id,
            Self::Executing(s) => s.saga_id,
            Self::Completed(s) => s.saga_id,
            Self::Failed(s) => s.saga_id,
            Self::Compensating(s) => s.saga_id,
            Self::Compensated(s) => s.saga_id,
            Self::Quarantined(s) => s.saga_id,
        }
    }

    pub fn last_updated_at_millis(&self) -> u64 {
        match self {
            Self::Idle(s) => s.last_updated_at_millis,
            Self::Triggered(s) => s.last_updated_at_millis,
            Self::Executing(s) => s.last_updated_at_millis,
            Self::Completed(s) => s.last_updated_at_millis,
            Self::Failed(s) => s.last_updated_at_millis,
            Self::Compensating(s) => s.last_updated_at_millis,
            Self::Compensated(s) => s.last_updated_at_millis,
            Self::Quarantined(s) => s.last_updated_at_millis,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Compensated(_) | Self::Quarantined(_))
    }

    pub fn step_name(&self) -> &str {
        match self {
            Self::Idle(s) => &s.step_name,
            Self::Triggered(s) => &s.step_name,
            Self::Executing(s) => &s.step_name,
            Self::Completed(s) => &s.step_name,
            Self::Failed(s) => &s.step_name,
            Self::Compensating(s) => &s.step_name,
            Self::Compensated(s) => &s.step_name,
            Self::Quarantined(s) => &s.step_name,
        }
    }
}
