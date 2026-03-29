use std::collections::HashMap;

use icanact_core::local_sync::{self, PubSub, PublishStats, Subscription};
use icanact_core::{ActorMessage, Ask, Tell};

use crate::reply_registry::{SagaReplyToHandle, SagaReplyToResult};
use crate::{
    SagaChoreographyEvent, SagaId, SagaReplyTo, TERMINAL_RESOLVER_STEP, TerminalPolicy,
    TerminalResolver,
};

pub struct SagaChoreographyBus {
    pubsub: PubSub<SagaChoreographyEvent>,
    control_ref: icanact_core::SyncActorRef<SagaBusControlActor>,
    control_handle: Option<local_sync::ActorHandle>,
}

impl SagaChoreographyBus {
    pub fn new() -> Self {
        let pubsub = PubSub::new();
        let (control_ref, control_handle) = local_sync::spawn(SagaBusControlActor::default());
        control_handle.wait_for_startup();
        Self {
            pubsub,
            control_ref,
            control_handle: Some(control_handle),
        }
    }

    pub fn subscribe_fn<F>(&self, topic: &str, f: F) -> Subscription
    where
        F: Fn(SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.pubsub.subscribe_direct_fn(topic, f)
    }

    pub fn unsubscribe(&self, sub: Subscription) -> bool {
        let subscription_id = sub.id().as_u64();
        let removed = self.pubsub.unsubscribe(sub);
        if removed {
            let _ = self
                .control_ref
                .ask(SagaBusControlAsk::UnregisterResolver { subscription_id });
        }
        removed
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        let topic = SagaChoreographyEvent::topic(event.context().saga_type.as_ref());
        self.pubsub.publish(topic.as_str(), event)
    }

    pub fn publish_to_saga_type(
        &self,
        saga_type: &str,
        event: SagaChoreographyEvent,
    ) -> PublishStats {
        let topic = SagaChoreographyEvent::topic(saga_type);
        self.pubsub.publish(topic.as_str(), event)
    }

    pub fn subscribe_saga_type_fn<F>(&self, saga_type: &str, f: F) -> Subscription
    where
        F: Fn(SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        let topic = SagaChoreographyEvent::topic(saga_type);
        self.subscribe_fn(topic.as_str(), f)
    }

    pub fn register_terminal_reply(
        &self,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    ) -> Result<(), Box<str>> {
        match self
            .control_ref
            .ask(SagaBusControlAsk::RegisterTerminalReply { saga_id, reply })
        {
            Ok(SagaBusControlAskReply::RegisterTerminalReply(result)) => result,
            Ok(_) => unreachable!("control actor returned unexpected register reply variant"),
            Err(err) => Err(format!("control actor ask failed: {err:?}").into_boxed_str()),
        }
    }

    pub fn complete_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) -> bool {
        match self
            .control_ref
            .ask(SagaBusControlAsk::CompleteTerminalReply { saga_id, reply })
        {
            Ok(SagaBusControlAskReply::CompleteTerminalReply(delivered)) => delivered,
            Ok(_) => unreachable!("control actor returned unexpected complete reply variant"),
            Err(_) => false,
        }
    }

    pub fn reject_terminal_reply(&self, saga_id: SagaId, reason: impl Into<String>) -> bool {
        match self.control_ref.ask(SagaBusControlAsk::RejectTerminalReply {
            saga_id,
            reason: reason.into().into_boxed_str(),
        }) {
            Ok(SagaBusControlAskReply::RejectTerminalReply(delivered)) => delivered,
            Ok(_) => unreachable!("control actor returned unexpected reject reply variant"),
            Err(_) => false,
        }
    }

    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> Subscription {
        let topic = SagaChoreographyEvent::topic(policy.saga_type.as_ref());
        let (resolver_ref, resolver_handle) = local_sync::spawn(TerminalResolverActor::new(
            TerminalResolver::new(policy),
            self.clone(),
            responder.into(),
        ));
        resolver_handle.wait_for_startup();
        let subscription = self.pubsub.subscribe_direct_fn(topic.as_str(), move |event| {
            resolver_ref.tell(event)
        });
        let _ = self.control_ref.ask(SagaBusControlAsk::RegisterResolver {
            subscription_id: subscription.id().as_u64(),
            handle: resolver_handle,
        });
        subscription
    }

    pub fn attach_order_lifecycle_terminal_resolver(
        &self,
        responder: &'static str,
    ) -> Subscription {
        self.attach_terminal_resolver(TerminalPolicy::order_lifecycle_default(), responder)
    }

    fn complete_terminal_reply_from_event(
        &self,
        event: &SagaChoreographyEvent,
        responder: impl Into<Box<str>>,
    ) -> bool {
        let is_resolver_terminal = match event {
            SagaChoreographyEvent::SagaCompleted { context }
            | SagaChoreographyEvent::SagaFailed { context, .. }
            | SagaChoreographyEvent::SagaQuarantined { context, .. } => {
                context.step_name.as_ref() == TERMINAL_RESOLVER_STEP
            }
            _ => false,
        };
        if !is_resolver_terminal {
            return false;
        }
        let Some(outcome) = event.terminal_outcome() else {
            return false;
        };
        self.complete_terminal_reply(
            event.context().saga_id,
            SagaReplyTo {
                responder: responder.into(),
                outcome,
            },
        )
    }
}

impl Clone for SagaChoreographyBus {
    fn clone(&self) -> Self {
        Self {
            pubsub: self.pubsub.clone(),
            control_ref: self.control_ref.clone(),
            control_handle: None,
        }
    }
}

impl Drop for SagaChoreographyBus {
    fn drop(&mut self) {
        if let Some(handle) = self.control_handle.take() {
            handle.shutdown();
        }
    }
}

impl Default for SagaChoreographyBus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct SagaBusControlActor {
    pending_replies: HashMap<u64, SagaReplyToHandle>,
    resolver_handles: HashMap<u64, local_sync::ActorHandle>,
}

enum SagaBusControlMsg {
    Ask {
        msg: SagaBusControlAsk,
        reply: local_sync::ReplyTo<SagaBusControlAskReply>,
    },
}

enum SagaBusControlAsk {
    RegisterTerminalReply {
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    },
    CompleteTerminalReply {
        saga_id: SagaId,
        reply: SagaReplyTo,
    },
    RejectTerminalReply {
        saga_id: SagaId,
        reason: Box<str>,
    },
    RegisterResolver {
        subscription_id: u64,
        handle: local_sync::ActorHandle,
    },
    UnregisterResolver {
        subscription_id: u64,
    },
}

enum SagaBusControlAskReply {
    RegisterTerminalReply(Result<(), Box<str>>),
    CompleteTerminalReply(bool),
    RejectTerminalReply(bool),
    RegisterResolver,
    UnregisterResolver(bool),
}

impl Tell for SagaBusControlActor {
    type Msg = SagaBusControlMsg;

    fn handle_tell(&mut self, msg: ActorMessage<Self::Msg>) {
        let SagaBusControlMsg::Ask { msg, reply } = msg.payload;
        let _ = reply.reply(handle_control_ask(self, msg));
    }
}

impl Ask for SagaBusControlActor {
    type Msg = SagaBusControlAsk;
    type Reply = SagaBusControlAskReply;

    fn handle_ask(&mut self, msg: ActorMessage<Self::Msg>) -> Self::Reply {
        handle_control_ask(self, msg.payload)
    }
}

impl icanact_core::local_sync::Actor for SagaBusControlActor {
    type ActorMailboxContract = icanact_core::local_sync::contract::TellAsk;
    type Tell = SagaBusControlMsg;
    type Ask = SagaBusControlAsk;
    type Reply = SagaBusControlAskReply;
}

impl icanact_core::local_sync::SyncEndpoint for SagaBusControlActor {
    type RuntimeMsg = icanact_core::local_sync::SyncTellAskMessage<
        SagaBusControlMsg,
        SagaBusControlAsk,
        SagaBusControlAskReply,
    >;

    fn handle_runtime(&mut self, msg: ActorMessage<Self::RuntimeMsg>) {
        match msg.payload {
            icanact_core::local_sync::SyncTellAskMessage::Tell(inner) => {
                icanact_core::Tell::handle_tell(self, ActorMessage::tell(inner));
            }
            icanact_core::local_sync::SyncTellAskMessage::Ask { msg, reply } => {
                let out = icanact_core::Ask::handle_ask(self, ActorMessage::ask(msg));
                let _ = reply.reply(out);
            }
        }
    }
}

impl icanact_core::local_sync::SyncTellEndpoint for SagaBusControlActor {
    fn runtime_tell(msg: SagaBusControlMsg) -> Self::RuntimeMsg {
        icanact_core::local_sync::SyncTellAskMessage::Tell(msg)
    }

    fn recover_tell(msg: Self::RuntimeMsg) -> Option<SagaBusControlMsg> {
        match msg {
            icanact_core::local_sync::SyncTellAskMessage::Tell(inner) => Some(inner),
            _ => None,
        }
    }
}

impl icanact_core::local_sync::SyncAskEndpoint for SagaBusControlActor {
    fn runtime_ask(
        msg: SagaBusControlAsk,
        reply: icanact_core::local_sync::ReplyTo<SagaBusControlAskReply>,
    ) -> Self::RuntimeMsg {
        icanact_core::local_sync::SyncTellAskMessage::Ask { msg, reply }
    }
}

fn handle_control_ask(
    actor: &mut SagaBusControlActor,
    ask: SagaBusControlAsk,
) -> SagaBusControlAskReply {
    match ask {
        SagaBusControlAsk::RegisterTerminalReply { saga_id, reply } => {
            if actor.pending_replies.contains_key(&saga_id.get()) {
                let _ = reply.reply(Err(
                    "terminal reply already registered for saga id".to_string(),
                ));
                SagaBusControlAskReply::RegisterTerminalReply(Err(
                    "terminal reply already registered for saga id".into(),
                ))
            } else {
                actor.pending_replies.insert(saga_id.get(), reply);
                SagaBusControlAskReply::RegisterTerminalReply(Ok(()))
            }
        }
        SagaBusControlAsk::CompleteTerminalReply { saga_id, reply } => {
            let delivered = actor
                .pending_replies
                .remove(&saga_id.get())
                .map(|reply_to| reply_to.reply(Ok(reply)).is_ok())
                .unwrap_or(false);
            SagaBusControlAskReply::CompleteTerminalReply(delivered)
        }
        SagaBusControlAsk::RejectTerminalReply { saga_id, reason } => {
            let delivered = actor
                .pending_replies
                .remove(&saga_id.get())
                .map(|reply_to| reply_to.reply(Err(reason.into())).is_ok())
                .unwrap_or(false);
            SagaBusControlAskReply::RejectTerminalReply(delivered)
        }
        SagaBusControlAsk::RegisterResolver {
            subscription_id,
            handle,
        } => {
            actor.resolver_handles.insert(subscription_id, handle);
            SagaBusControlAskReply::RegisterResolver
        }
        SagaBusControlAsk::UnregisterResolver { subscription_id } => {
            let removed = actor
                .resolver_handles
                .remove(&subscription_id)
                .map(|handle| {
                    handle.shutdown();
                    true
                })
                .unwrap_or(false);
            SagaBusControlAskReply::UnregisterResolver(removed)
        }
    }
}

impl SagaBusControlActor {
    fn cleanup_owned_resources(&mut self, reason: &str) {
        for (_, reply) in self.pending_replies.drain() {
            let _ = reply.reply(Err(reason.to_string()));
        }
        for (_, handle) in self.resolver_handles.drain() {
            handle.shutdown();
        }
    }
}

impl icanact_core::local_sync::actor::SyncActor for SagaBusControlActor {
    fn on_stop(&mut self) {
        self.cleanup_owned_resources("saga bus control stopped");
    }

    fn on_panic(&mut self, message: &str) {
        self.cleanup_owned_resources(&format!("saga bus control panicked: {message}"));
    }
}

#[derive(Default, icanact_core::SyncActor)]
struct TerminalResolverActor {
    resolver: Option<TerminalResolver>,
    bus: Option<SagaChoreographyBus>,
    responder: Option<Box<str>>,
}

impl TerminalResolverActor {
    fn new(resolver: TerminalResolver, bus: SagaChoreographyBus, responder: Box<str>) -> Self {
        Self {
            resolver: Some(resolver),
            bus: Some(bus),
            responder: Some(responder),
        }
    }
}

impl Tell for TerminalResolverActor {
    type Msg = SagaChoreographyEvent;

    fn handle_tell(&mut self, msg: ActorMessage<Self::Msg>) {
        let resolver = self
            .resolver
            .as_mut()
            .expect("terminal resolver actor must be initialized");
        let bus = self
            .bus
            .as_ref()
            .expect("terminal resolver actor bus must be initialized");
        let responder = self
            .responder
            .as_ref()
            .expect("terminal resolver actor responder must be initialized")
            .clone();

        let terminal_events = resolver.ingest(&msg.payload);
        for terminal_event in terminal_events {
            let _ = bus.complete_terminal_reply_from_event(&terminal_event, responder.clone());
            let _ = bus.publish(terminal_event);
        }
    }
}

impl icanact_core::local_sync::Actor for TerminalResolverActor {
    type ActorMailboxContract = icanact_core::local_sync::contract::TellOnly;
    type Tell = SagaChoreographyEvent;
    type Ask = icanact_core::local_sync::contract::NoAsk;
    type Reply = icanact_core::local_sync::contract::NoReply;
}

impl icanact_core::local_sync::SyncEndpoint for TerminalResolverActor {
    type RuntimeMsg = icanact_core::local_sync::SyncTellMessage<SagaChoreographyEvent>;

    fn handle_runtime(&mut self, msg: ActorMessage<Self::RuntimeMsg>) {
        match msg.payload {
            icanact_core::local_sync::SyncTellMessage::Tell(inner) => {
                icanact_core::Tell::handle_tell(self, ActorMessage::tell(inner));
            }
        }
    }
}

impl icanact_core::local_sync::SyncTellEndpoint for TerminalResolverActor {
    fn runtime_tell(msg: SagaChoreographyEvent) -> Self::RuntimeMsg {
        icanact_core::local_sync::SyncTellMessage::Tell(msg)
    }

    fn recover_tell(msg: Self::RuntimeMsg) -> Option<SagaChoreographyEvent> {
        match msg {
            icanact_core::local_sync::SyncTellMessage::Tell(inner) => Some(inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use icanact_core::{ActorMessage, Ask};
    use icanact_core::local_sync;

    use crate::{
        SagaChoreographyEvent, SagaContext, SagaId, SagaReplyToResult,
        TERMINAL_RESOLVER_STEP,
    };

    use super::SagaChoreographyBus;

    fn context(step_name: &str, saga_id: u64) -> SagaContext {
        let now = SagaContext::now_millis();
        SagaContext {
            saga_id: SagaId::new(saga_id),
            saga_type: "order_lifecycle".into(),
            step_name: step_name.into(),
            correlation_id: saga_id,
            causation_id: saga_id,
            trace_id: saga_id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: now,
            event_timestamp_millis: now,
        }
    }

    fn wait_until(deadline: Instant, mut pred: impl FnMut() -> bool) {
        while Instant::now() < deadline {
            if pred() {
                return;
            }
            std::hint::spin_loop();
            thread::yield_now();
        }
        assert!(pred(), "condition not met before deadline");
    }

    enum ProbeMsg {
        Register {
            bus: SagaChoreographyBus,
            saga_id: SagaId,
            reply: super::SagaReplyToHandle,
        },
    }

    fn register_pending_reply(
        bus: SagaChoreographyBus,
        saga_id: SagaId,
    ) -> (
        local_sync::PendingAsk<SagaReplyToResult>,
        local_sync::mpsc::ActorHandle,
    ) {
        let (probe_addr, probe_handle) = local_sync::mpsc::spawn(8, |msg: ProbeMsg| match msg {
            ProbeMsg::Register {
                bus,
                saga_id,
                reply,
            } => {
                let _ = bus.register_terminal_reply(saga_id, reply);
            }
        });
        let pending = probe_addr
            .ask_deferred(|reply| ProbeMsg::Register {
                bus,
                saga_id,
                reply,
            })
            .expect("pending reply should be registered");
        (pending, probe_handle)
    }

    #[test]
    fn attached_resolver_emits_single_terminal_completion() {
        let bus = SagaChoreographyBus::new();
        let _resolver_sub = bus.attach_order_lifecycle_terminal_resolver("terminal-resolver");

        let delivered = Arc::new(AtomicUsize::new(0));
        let _capture_sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event: SagaChoreographyEvent| {
                if matches!(
                    event,
                    SagaChoreographyEvent::SagaCompleted { .. }
                        | SagaChoreographyEvent::SagaFailed { .. }
                        | SagaChoreographyEvent::SagaQuarantined { .. }
                ) {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
        });

        let step = SagaChoreographyEvent::StepCompleted {
            context: context("create_order", 42),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        };
        let _ = bus.publish(step.clone());
        let _ = bus.publish(step);

        wait_until(Instant::now() + Duration::from_secs(1), || {
            delivered.load(Ordering::Relaxed) == 1
        });
    }

    #[test]
    fn terminal_reply_state_is_bus_scoped() {
        let bus_a = SagaChoreographyBus::new();
        let bus_b = SagaChoreographyBus::new();
        let saga_id = SagaId::new(7);
        let (pending_a, probe_a) = register_pending_reply(bus_a.clone(), saga_id);
        let (pending_b, probe_b) = register_pending_reply(bus_b.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        let completed_a = SagaChoreographyEvent::SagaCompleted {
            context: SagaContext {
                step_name: TERMINAL_RESOLVER_STEP.into(),
                ..context("create_order", saga_id.get())
            },
        };
        assert!(bus_a.complete_terminal_reply_from_event(&completed_a, "resolver-a"));
        assert!(bus_b.complete_terminal_reply_from_event(&completed_a, "resolver-b"));

        let got_a = pending_a.wait().expect("bus A reply should arrive");
        let got_b = pending_b.wait().expect("bus B reply should arrive");
        assert!(got_a.is_ok());
        assert!(got_b.is_ok());
        probe_a.shutdown();
        probe_b.shutdown();
    }

    #[test]
    fn unsubscribe_stops_future_resolver_processing() {
        let bus = SagaChoreographyBus::new();
        let resolver_sub = bus.attach_order_lifecycle_terminal_resolver("terminal-resolver");
        let delivered = Arc::new(AtomicUsize::new(0));
        let _capture_sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event: SagaChoreographyEvent| {
                if matches!(event, SagaChoreographyEvent::SagaCompleted { .. }) {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
        });

        assert!(bus.unsubscribe(resolver_sub));

        let step = SagaChoreographyEvent::StepCompleted {
            context: context("create_order", 99),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        };
        let _ = bus.publish(step);

        assert_eq!(delivered.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn duplicate_terminal_reply_registration_is_rejected_on_same_bus() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(123);
        let (_pending, probe_a) = register_pending_reply(bus.clone(), saga_id);
        let (pending_dup, probe_b) = register_pending_reply(bus, saga_id);

        let dup_result = pending_dup
            .wait()
            .expect("duplicate registration reply should arrive");
        assert!(dup_result.is_err());
        probe_a.shutdown();
        probe_b.shutdown();
    }

    #[test]
    fn repeated_unsubscribe_is_safe() {
        let bus = SagaChoreographyBus::new();
        let resolver_sub = bus.attach_order_lifecycle_terminal_resolver("terminal-resolver");

        assert!(bus.unsubscribe(resolver_sub.clone()));
        assert!(!bus.unsubscribe(resolver_sub));
    }

    #[test]
    fn dropping_owner_bus_rejects_pending_terminal_replies() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(456);
        let (pending, probe) = register_pending_reply(bus.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        drop(bus);

        let result = pending.wait().expect("pending terminal reply should resolve on bus drop");
        assert!(result.is_err());
        probe.shutdown();
    }

    #[derive(Default, icanact_core::SyncActor)]
    struct ProbeActor;

    impl Ask for ProbeActor {
        type Msg = &'static str;
        type Reply = &'static str;

        fn handle_ask(&mut self, msg: ActorMessage<Self::Msg>) -> Self::Reply {
            msg.payload
        }
    }

    impl icanact_core::local_sync::SyncEndpoint for ProbeActor {
        type RuntimeMsg = icanact_core::local_sync::SyncAskMessage<&'static str, &'static str>;

        fn handle_runtime(&mut self, msg: ActorMessage<Self::RuntimeMsg>) {
            let icanact_core::local_sync::SyncAskMessage::Ask { msg, reply } = msg.payload;
            let _ = reply.reply(msg);
        }
    }

    impl icanact_core::local_sync::SyncAskEndpoint for ProbeActor {
        fn runtime_ask(
            msg: &'static str,
            reply: local_sync::ReplyTo<&'static str>,
        ) -> <Self as icanact_core::local_sync::SyncEndpoint>::RuntimeMsg {
            icanact_core::local_sync::SyncAskMessage::Ask { msg, reply }
        }
    }

    #[test]
    fn control_actor_on_panic_shuts_down_owned_resolvers() {
        let (resolver_ref, resolver_handle) = local_sync::spawn(ProbeActor);
        resolver_handle.wait_for_startup();

        let mut actor = super::SagaBusControlActor::default();
        actor.resolver_handles.insert(1, resolver_handle);

        <super::SagaBusControlActor as icanact_core::local_sync::actor::SyncActor>::on_panic(
            &mut actor,
            "panic-for-test",
        );

        let resolver_result = resolver_ref.ask_timeout("ping", Duration::from_millis(50));
        assert!(resolver_result.is_err());
    }
}
