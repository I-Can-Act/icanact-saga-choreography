use icanact_core::local::EventSubscription;

use crate::{SagaChoreographyBus, SagaChoreographyEvent};

#[derive(Clone, Debug)]
pub enum SagaParticipantChannel<C> {
    Saga(SagaChoreographyEvent),
    Business(C),
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
