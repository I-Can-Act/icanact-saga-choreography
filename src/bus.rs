use icanact_core::local_direct::{PubSub, PublishStats};

use crate::SagaChoreographyEvent;

#[derive(Clone)]
pub struct SagaChoreographyBus {
    inner: PubSub<SagaChoreographyEvent>,
}

impl SagaChoreographyBus {
    pub fn new() -> Self {
        Self {
            inner: PubSub::new(),
        }
    }

    pub fn subscribe_fn<F>(
        &self,
        topic: &str,
        f: F,
    ) -> icanact_core::local_sync::DirectSubscription
    where
        F: Fn(SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.inner.subscribe_fn(topic, f)
    }

    pub fn unsubscribe(&self, sub: icanact_core::local_sync::DirectSubscription) -> bool {
        self.inner.unsubscribe(sub)
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        let topic = SagaChoreographyEvent::topic(event.context().saga_type.as_ref());
        self.inner.publish(topic.as_str(), event)
    }

    pub fn publish_to_saga_type(
        &self,
        saga_type: &str,
        event: SagaChoreographyEvent,
    ) -> PublishStats {
        let topic = SagaChoreographyEvent::topic(saga_type);
        self.inner.publish(topic.as_str(), event)
    }

    pub fn subscribe_saga_type_fn<F>(
        &self,
        saga_type: &str,
        f: F,
    ) -> icanact_core::local_sync::DirectSubscription
    where
        F: Fn(SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        let topic = SagaChoreographyEvent::topic(saga_type);
        self.subscribe_fn(topic.as_str(), f)
    }
}

impl Default for SagaChoreographyBus {
    fn default() -> Self {
        Self::new()
    }
}
