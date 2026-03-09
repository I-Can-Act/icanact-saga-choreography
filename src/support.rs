//! First-class embedded saga support for participants.

use std::collections::HashMap;

use icanact_core::local_direct::PubSub;

use crate::{
    ParticipantDedupeStore, ParticipantJournal, ParticipantStats, SagaChoreographyEvent, SagaId,
    SagaStateEntry,
};

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
    pub journal: J,
    pub dedupe: D,
    pub stats: ParticipantStats,
    pub startup_recovery_events: Vec<SagaChoreographyEvent>,
    pub bus: Option<PubSub<SagaChoreographyEvent>>,
}

impl<J, D> SagaParticipantSupport<J, D>
where
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
{
    pub fn new(journal: J, dedupe: D) -> Self {
        Self {
            saga_states: HashMap::new(),
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

    pub fn attach_bus(&mut self, bus: PubSub<SagaChoreographyEvent>) {
        self.bus = Some(bus);
    }

    pub fn publish(&self, event: SagaChoreographyEvent) {
        if let Some(bus) = &self.bus {
            let topic = SagaChoreographyEvent::topic(event.context().saga_type.as_ref());
            let _ = bus.publish(topic.as_str(), event);
        }
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
    fn attach_saga_bus(&mut self, bus: PubSub<SagaChoreographyEvent>) {
        self.saga_support_mut().attach_bus(bus);
    }

    fn publish_saga_event(&self, event: SagaChoreographyEvent) {
        self.saga_support().publish(event);
    }

    fn take_startup_recovery_events(&mut self) -> Vec<SagaChoreographyEvent> {
        self.saga_support_mut().take_startup_recovery_events()
    }
}

impl<T> SagaParticipantSupportExt for T where T: HasSagaParticipantSupport {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

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
        };

        let mut support =
            SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new())
                .with_startup_recovery_events(vec![event.clone()]);

        assert!(support.saga_states.is_empty());
        assert_eq!(support.take_startup_recovery_events().len(), 1);
        assert!(support.take_startup_recovery_events().is_empty());
    }

    #[test]
    fn support_publishes_to_attached_bus() {
        let bus = PubSub::<SagaChoreographyEvent>::new();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let delivered_clone = Arc::clone(&delivered);
        let _sub = bus.subscribe_fn("saga:order_lifecycle", move |event| {
            delivered_clone.lock().expect("lock").push(event);
            true
        });

        let mut support =
            SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new());
        support.attach_bus(bus);
        support.publish(SagaChoreographyEvent::SagaCompleted {
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

        assert_eq!(delivered.lock().expect("lock").len(), 1);
    }
}
