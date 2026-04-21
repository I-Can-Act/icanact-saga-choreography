use icanact_core::local::EventSubscription;

use crate::{
    HasSagaWorkflowParticipants, SagaChoreographyBus, SagaChoreographyEvent,
    SagaWorkflowParticipant,
};

#[derive(Clone, Debug)]
pub enum SagaParticipantChannel<C> {
    Saga(SagaChoreographyEvent),
    Business(C),
}

impl<C> From<C> for SagaParticipantChannel<C> {
    fn from(value: C) -> Self {
        Self::Business(value)
    }
}

pub fn workflow_saga_types<A>(
    workflows: &[&'static dyn SagaWorkflowParticipant<A>],
) -> Vec<&'static str> {
    let mut saga_types = Vec::new();
    for workflow in workflows {
        for saga_type in workflow.saga_types() {
            if !saga_types.contains(saga_type) {
                saga_types.push(*saga_type);
            }
        }
    }
    saga_types
}

pub fn checked_workflow_saga_types<A>() -> Result<Vec<&'static str>, String>
where
    A: HasSagaWorkflowParticipants,
{
    let workflows = A::saga_workflows();
    let mut saga_types = Vec::new();
    for workflow in workflows {
        for saga_type in workflow.saga_types() {
            if saga_types.contains(saga_type) {
                return Err(format!(
                    "duplicate workflow registration for saga_type `{saga_type}` on actor type `{}`",
                    std::any::type_name::<A>()
                ));
            }
            saga_types.push(*saga_type);
        }
    }
    Ok(saga_types)
}

pub fn bind_sync_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    saga_types: &[&'static str],
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let sender: std::sync::Arc<
        std::sync::Mutex<
            icanact_core::local_sync::ChannelSender<
                <A as icanact_core::local_sync::SyncActor>::Channel,
            >,
        >,
    > = std::sync::Arc::new(std::sync::Mutex::new(
        actor_ref
            .add_channel(channel_name.to_string(), capacity.max(1))
            .ok_or_else(|| format!("failed to register saga channel `{channel_name}`"))?,
    ));

    Ok(saga_types
        .iter()
        .map(|saga_type| {
            let sender = std::sync::Arc::clone(&sender);
            bus.subscribe_saga_type_fn(saga_type, move |event| {
                sender
                    .lock()
                    .expect("saga sync channel sender lock poisoned")
                    .try_send(SagaParticipantChannel::Saga(event.clone()).into())
                    .is_ok()
            })
        })
        .collect())
}

pub fn bind_sync_workflow_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let saga_types = checked_workflow_saga_types::<A>()?;
    bind_sync_participant_channel::<A, C>(bus, actor_ref, &saga_types, channel_name, capacity)
}

pub fn bind_sync_participant_tell<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    saga_types: &[&'static str],
    map_event: F,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + Send + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let map_event = std::sync::Arc::new(map_event);
    Ok(saga_types
        .iter()
        .map(|saga_type| {
            let actor_ref = actor_ref.clone();
            let map_event = std::sync::Arc::clone(&map_event);
            bus.subscribe_saga_type_fn(saga_type, move |event| {
                actor_ref.try_tell(map_event(event.clone())).is_ok()
            })
        })
        .collect())
}

pub fn bind_sync_workflow_participant_tell<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    map_event: F,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let saga_types = checked_workflow_saga_types::<A>()?;
    bind_sync_participant_tell(bus, actor_ref, &saga_types, map_event)
}

pub fn bind_async_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    saga_types: &[&'static str],
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let sender: std::sync::Arc<
        std::sync::Mutex<
            icanact_core::local_async::ChannelSender<
                <A as icanact_core::local_async::AsyncActor>::Channel,
            >,
        >,
    > = std::sync::Arc::new(std::sync::Mutex::new(
        actor_ref
            .add_channel(channel_name.to_string(), capacity.max(1))
            .ok_or_else(|| format!("failed to register saga channel `{channel_name}`"))?,
    ));

    Ok(saga_types
        .iter()
        .map(|saga_type| {
            let sender = std::sync::Arc::clone(&sender);
            bus.subscribe_saga_type_fn(saga_type, move |event| {
                sender
                    .lock()
                    .expect("saga async channel sender lock poisoned")
                    .try_send(SagaParticipantChannel::Saga(event.clone()).into())
                    .is_ok()
            })
        })
        .collect())
}

pub fn bind_async_participant_tell<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    saga_types: &[&'static str],
    map_event: F,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + Send + 'static,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let map_event = std::sync::Arc::new(map_event);
    Ok(saga_types
        .iter()
        .map(|saga_type| {
            let actor_ref = actor_ref.clone();
            let map_event = std::sync::Arc::clone(&map_event);
            bus.subscribe_saga_type_fn(saga_type, move |event| {
                actor_ref.try_tell(map_event(event.clone())).is_ok()
            })
        })
        .collect())
}
