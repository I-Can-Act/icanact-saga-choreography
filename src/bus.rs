use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use icanact_core::local::{EventBus, EventSubscription, PublishStats};
use icanact_core::CorrelationRegistry;

use crate::reply_registry::{SagaReplyToHandle, SagaReplyToResult};
use crate::{
    SagaChoreographyEvent, SagaId, SagaReplyTo, SagaTerminalOutcome, TerminalPolicy, TerminalResolver,
    TERMINAL_RESOLVER_STEP,
};

pub struct SagaChoreographyBus {
    bus: EventBus<SagaChoreographyEvent>,
    pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    terminal_replies: Arc<Mutex<HashMap<SagaId, SagaReplyTo>>>,
    terminal_outcomes: Arc<Mutex<HashMap<SagaId, SagaTerminalOutcome>>>,
    owned: bool,
}

impl SagaChoreographyBus {
    pub fn new() -> Self {
        Self {
            bus: EventBus::new(),
            pending_replies: CorrelationRegistry::new(),
            terminal_replies: Arc::new(Mutex::new(HashMap::new())),
            terminal_outcomes: Arc::new(Mutex::new(HashMap::new())),
            owned: true,
        }
    }

    pub fn subscribe_fn<F>(&self, topic: &str, f: F) -> EventSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.bus.subscribe_fn(topic, f)
    }

    pub fn unsubscribe(&self, sub: EventSubscription) -> bool {
        self.bus.unsubscribe(sub)
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        if let Some(outcome) = event.terminal_outcome() {
            self.terminal_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(event.context().saga_id, outcome);
        }
        self.bus.publish(event)
    }

    pub fn publish_to_saga_type(
        &self,
        saga_type: &str,
        event: SagaChoreographyEvent,
    ) -> PublishStats {
        self.bus.publish_to(saga_type, event)
    }

    pub fn subscribe_saga_type_fn<F>(&self, saga_type: &str, f: F) -> EventSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.subscribe_fn(saga_type, f)
    }

    pub fn register_terminal_reply(
        &self,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    ) -> Result<(), Box<str>> {
        match self.pending_replies.register(saga_id, reply) {
            Ok(()) => Ok(()),
            Err(err) => {
                let reply = err.into_reply();
                let _ = reply.reply(Err("terminal reply already registered for saga id".into()));
                Err("terminal reply already registered for saga id".into())
            }
        }
    }

    pub fn complete_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) -> bool {
        self.terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(saga_id, reply.clone());
        self.terminal_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(saga_id, reply.outcome.clone());
        self.pending_replies.resolve(&saga_id, Ok(reply)).is_ok()
    }

    pub fn reject_terminal_reply(&self, saga_id: SagaId, reason: impl Into<String>) -> bool {
        self.pending_replies
            .resolve(&saga_id, Err(reason.into()))
            .is_ok()
    }

    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> EventSubscription {
        let resolver = Arc::new(Mutex::new(TerminalResolver::new(policy.clone())));
        let bus = self.clone();
        let responder: Arc<str> = Arc::from(responder);
        self.bus
            .subscribe_fn(policy.saga_type.as_ref(), move |event| {
                let terminal_events = {
                    let mut resolver = match resolver.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    resolver.ingest(event)
                };

                for terminal_event in terminal_events {
                    let _ =
                        bus.complete_terminal_reply_from_event(&terminal_event, responder.as_ref());
                    let _ = bus.publish(terminal_event);
                }

                true
            })
    }

    pub fn attach_order_lifecycle_terminal_resolver(
        &self,
        responder: &'static str,
    ) -> EventSubscription {
        self.attach_terminal_resolver(TerminalPolicy::order_lifecycle_default(), responder)
    }

    pub fn take_terminal_reply(&self, saga_id: SagaId) -> Option<SagaReplyTo> {
        self.terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id)
    }

    pub fn take_terminal_outcome(
        &self,
        saga_id: SagaId,
    ) -> Option<crate::SagaTerminalOutcome> {
        let reply = self.take_terminal_reply(saga_id).map(|reply| reply.outcome);
        let direct = self
            .terminal_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id);
        reply.or(direct)
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

pub fn global_saga_choreography_bus() -> SagaChoreographyBus {
    static BUS: OnceLock<SagaChoreographyBus> = OnceLock::new();
    BUS.get_or_init(SagaChoreographyBus::new).clone()
}

impl Clone for SagaChoreographyBus {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            pending_replies: self.pending_replies.clone(),
            terminal_replies: Arc::clone(&self.terminal_replies),
            terminal_outcomes: Arc::clone(&self.terminal_outcomes),
            owned: false,
        }
    }
}

impl Drop for SagaChoreographyBus {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }

        for (_, reply) in self.pending_replies.drain() {
            let _ = reply.reply(Err("saga bus dropped".to_string()));
        }
    }
}

impl Default for SagaChoreographyBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use icanact_core::local_sync;

    use crate::{
        SagaChoreographyEvent, SagaContext, SagaId, SagaReplyToResult, SagaTerminalOutcome,
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
            .ask_delegated(|reply| ProbeMsg::Register {
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
            move |event: &SagaChoreographyEvent| {
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
    fn take_terminal_outcome_returns_and_consumes_resolved_reply() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(77);
        let (pending, probe) = register_pending_reply(bus.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        let completed = SagaChoreographyEvent::SagaCompleted {
            context: SagaContext {
                step_name: TERMINAL_RESOLVER_STEP.into(),
                ..context("create_order", saga_id.get())
            },
        };
        assert!(bus.complete_terminal_reply_from_event(&completed, "resolver"));

        let reply = pending
            .wait()
            .expect("terminal reply should resolve")
            .expect("terminal reply should be successful");
        assert!(matches!(reply.outcome, SagaTerminalOutcome::Completed { .. }));
        assert!(matches!(
            bus.take_terminal_outcome(saga_id),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "terminal outcome should be consumed after the first read"
        );
        probe.shutdown();
    }

    #[test]
    fn unsubscribe_stops_future_resolver_processing() {
        let bus = SagaChoreographyBus::new();
        let resolver_sub = bus.attach_order_lifecycle_terminal_resolver("terminal-resolver");
        let delivered = Arc::new(AtomicUsize::new(0));
        let _capture_sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event: &SagaChoreographyEvent| {
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

        let result = pending
            .wait()
            .expect("pending terminal reply should resolve on bus drop");
        assert!(result.is_err());
        probe.shutdown();
    }
}
