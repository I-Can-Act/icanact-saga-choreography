//! First-class embedded saga support for participants.

use std::collections::{HashMap, HashSet, VecDeque};

use icanact_core::local::PublishStats;

use crate::{
    AcceptedStepPolicy, ParticipantDedupeStore, ParticipantJournal, ParticipantStats,
    SagaChoreographyBus, SagaChoreographyEvent, SagaContext, SagaId, SagaStateEntry,
    StepExecutionId,
};

#[derive(Clone, Debug)]
pub struct AcceptedWorkflowStep {
    pub context: SagaContext,
    pub participant_id: Box<str>,
    pub execution_id: StepExecutionId,
    pub policy: AcceptedStepPolicy,
    pub accepted_at_millis: u64,
    pub deadline_at_millis: u64,
    pub hard_deadline_at_millis: u64,
}

/// Embedded choreography capability owned by a saga-enabled participant.
///
/// This groups all choreography plumbing into one field so application actors
/// can own `business_state + saga_support` instead of repeating multiple raw
/// storage fields in every actor.
pub struct SagaParticipantSupport<J, D>
where
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
{
    pub saga_states: HashMap<SagaId, SagaStateEntry>,
    pub dependency_completions: HashMap<SagaId, HashSet<Box<str>>>,
    pub dependency_fired: HashSet<SagaId>,
    pub terminal_sagas: HashSet<SagaId>,
    pub terminal_saga_order: VecDeque<SagaId>,
    pub saga_run_started_at: HashMap<SagaId, u64>,
    pub accepted_workflow_steps: HashMap<SagaId, AcceptedWorkflowStep>,
    pub resolved_workflow_steps: HashSet<(SagaId, StepExecutionId)>,
    pub journal: J,
    pub dedupe: D,
    pub stats: ParticipantStats,
    pub startup_recovery_events: Vec<SagaChoreographyEvent>,
    pub bus: Option<SagaChoreographyBus>,
}

impl<J, D> SagaParticipantSupport<J, D>
where
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
{
    pub fn new(journal: J, dedupe: D) -> Self {
        Self {
            saga_states: HashMap::new(),
            dependency_completions: HashMap::new(),
            dependency_fired: HashSet::new(),
            terminal_sagas: HashSet::new(),
            terminal_saga_order: VecDeque::new(),
            saga_run_started_at: HashMap::new(),
            accepted_workflow_steps: HashMap::new(),
            resolved_workflow_steps: HashSet::new(),
            journal,
            dedupe,
            stats: ParticipantStats::new(),
            startup_recovery_events: Vec::new(),
            bus: None,
        }
    }

    pub fn with_startup_recovery_events(mut self, events: Vec<SagaChoreographyEvent>) -> Self {
        self.startup_recovery_events = events;
        self
    }

    pub fn take_startup_recovery_events(&mut self) -> Vec<SagaChoreographyEvent> {
        std::mem::take(&mut self.startup_recovery_events)
    }

    pub fn attach_bus(&mut self, bus: SagaChoreographyBus) {
        self.bus = Some(bus);
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> Result<PublishStats, String> {
        if let Some(bus) = &self.bus {
            bus.publish_strict(event)
                .map_err(|err| format!("saga bus strict publish failed: {err:?}"))
        } else {
            Err("saga bus is not attached".to_string())
        }
    }

    pub fn has_accepted_workflow_step(&self, saga_id: SagaId) -> bool {
        self.accepted_workflow_steps.contains_key(&saga_id)
    }

    pub fn accepted_workflow_step_count(&self) -> usize {
        self.accepted_workflow_steps.len()
    }

    pub fn has_resolved_workflow_step(
        &self,
        saga_id: SagaId,
        execution_id: &StepExecutionId,
    ) -> bool {
        self.resolved_workflow_steps
            .contains(&(saga_id, execution_id.clone()))
    }

    pub fn resolved_workflow_step_count(&self) -> usize {
        self.resolved_workflow_steps.len()
    }
}

impl<J, D> std::fmt::Debug for SagaParticipantSupport<J, D>
where
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SagaParticipantSupport")
            .field("saga_states_len", &self.saga_states.len())
            .field(
                "dependency_completions_len",
                &self.dependency_completions.len(),
            )
            .field("dependency_fired_len", &self.dependency_fired.len())
            .field("terminal_sagas_len", &self.terminal_sagas.len())
            .field("terminal_saga_order_len", &self.terminal_saga_order.len())
            .field("saga_run_started_at_len", &self.saga_run_started_at.len())
            .field(
                "accepted_workflow_steps_len",
                &self.accepted_workflow_steps.len(),
            )
            .field(
                "resolved_workflow_steps_len",
                &self.resolved_workflow_steps.len(),
            )
            .field(
                "startup_recovery_events_len",
                &self.startup_recovery_events.len(),
            )
            .field("bus_attached", &self.bus.is_some())
            .field("stats", &self.stats.snapshot())
            .finish()
    }
}

/// Access trait for embedded first-class saga support.
pub trait HasSagaParticipantSupport: Send + 'static {
    type Journal: ParticipantJournal;
    type Dedupe: ParticipantDedupeStore;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe>;
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe>;
}

/// Convenience methods for participants that embed [`SagaParticipantSupport`].
pub trait SagaParticipantSupportExt: HasSagaParticipantSupport {
    fn attach_saga_bus(&mut self, bus: SagaChoreographyBus) {
        self.saga_support_mut().attach_bus(bus);
    }

    fn publish_saga_event(&self, event: SagaChoreographyEvent) -> Result<PublishStats, String> {
        self.saga_support().publish(event)
    }

    fn take_startup_recovery_events(&mut self) -> Vec<SagaChoreographyEvent> {
        self.saga_support_mut().take_startup_recovery_events()
    }
}

impl<T> SagaParticipantSupportExt for T where T: HasSagaParticipantSupport {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{InMemoryDedupe, InMemoryJournal, PeerId, SagaContext, SagaId};

    use super::*;

    #[test]
    fn support_starts_empty_and_drains_recovery_events() {
        let event = SagaChoreographyEvent::SagaFailed {
            context: SagaContext {
                saga_id: SagaId::new(7),
                saga_type: "order_lifecycle".into(),
                step_name: "risk_check".into(),
                correlation_id: 7,
                causation_id: 7,
                trace_id: 7,
                step_index: 0,
                attempt: 0,
                initiator_peer_id: PeerId::default(),
                saga_started_at_millis: 100,
                event_timestamp_millis: 100,
            },
            reason: "startup quarantine".into(),
            failure: None,
        };

        let mut support =
            SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new())
                .with_startup_recovery_events(vec![event.clone()]);

        assert!(support.saga_states.is_empty());
        assert!(support.dependency_completions.is_empty());
        assert!(support.dependency_fired.is_empty());
        assert_eq!(support.take_startup_recovery_events().len(), 1);
        assert!(support.take_startup_recovery_events().is_empty());
    }

    #[test]
    fn support_publishes_to_attached_bus() {
        let bus = SagaChoreographyBus::new();
        let delivered = Arc::new(AtomicUsize::new(0));
        let delivered_clone = Arc::clone(&delivered);
        let _sub = bus.subscribe_saga_type_fn("order_lifecycle", move |_event| {
            delivered_clone.fetch_add(1, Ordering::Relaxed);
            true
        });

        let mut support =
            SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new());
        support.attach_bus(bus);
        let published = support.publish(SagaChoreographyEvent::SagaCompleted {
            context: SagaContext {
                saga_id: SagaId::new(11),
                saga_type: "order_lifecycle".into(),
                step_name: "risk_check".into(),
                correlation_id: 11,
                causation_id: 11,
                trace_id: 11,
                step_index: 1,
                attempt: 0,
                initiator_peer_id: PeerId::default(),
                saga_started_at_millis: 200,
                event_timestamp_millis: 300,
            },
        });
        assert!(published.is_ok(), "publish should succeed: {published:?}");

        assert_eq!(delivered.load(Ordering::Relaxed), 1);
    }
}
