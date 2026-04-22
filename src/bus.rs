use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use icanact_core::local::{EventBus, EventSubscription, PublishStats};
use icanact_core::CorrelationRegistry;

use crate::reply_registry::{SagaReplyToHandle, SagaReplyToResult};
use crate::{
    SagaChoreographyEvent, SagaId, SagaReplyTo, SagaTerminalOutcome, SagaTerminalPolicyProvider,
    TerminalPolicy, TerminalResolver, TERMINAL_RESOLVER_STEP,
};

pub struct SagaChoreographyBus {
    bus: EventBus<SagaChoreographyEvent>,
    pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    terminal_replies: Arc<Mutex<HashMap<SagaId, SagaReplyTo>>>,
    terminal_outcomes: Arc<Mutex<HashMap<SagaId, SagaTerminalOutcome>>>,
    terminal_order: Arc<Mutex<VecDeque<SagaId>>>,
    terminal_policies_by_saga_type: Arc<Mutex<HashMap<Box<str>, Box<str>>>>,
    owned: bool,
}

const DEFAULT_TERMINAL_RETENTION_LIMIT: usize = 1024;
const DEFAULT_TERMINAL_WATCHDOG_TICK_MS: u64 = 100;

impl SagaChoreographyBus {
    pub fn new() -> Self {
        Self {
            bus: EventBus::new(),
            pending_replies: CorrelationRegistry::new(),
            terminal_replies: Arc::new(Mutex::new(HashMap::new())),
            terminal_outcomes: Arc::new(Mutex::new(HashMap::new())),
            terminal_order: Arc::new(Mutex::new(VecDeque::new())),
            terminal_policies_by_saga_type: Arc::new(Mutex::new(HashMap::new())),
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
        if let SagaChoreographyEvent::SagaStarted { context, .. } = &event {
            if !self.has_terminal_policy_for_saga_type(context.saga_type.as_ref()) {
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: format!(
                        "terminal policy is required before saga start; saga_type={} saga_id={}",
                        context.saga_type,
                        context.saga_id.get()
                    )
                    .into(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context().saga_id, outcome);
                }
                return self.bus.publish(terminal);
            }
        }
        if let Some(outcome) = event.terminal_outcome() {
            self.store_terminal_outcome(event.context().saga_id, outcome);
        }
        self.bus.publish(event)
    }

    /// Registers a terminal policy for a saga type, enabling `SagaStarted`
    /// acceptance for that workflow.
    pub fn register_terminal_policy(&self, policy: &TerminalPolicy) {
        self.terminal_policies_by_saga_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(policy.saga_type.clone(), policy.policy_id.clone());
    }

    /// Registers terminal policy via a static provider contract.
    pub fn register_terminal_policy_provider<P: SagaTerminalPolicyProvider>(&self) {
        let policy = P::terminal_policy();
        self.register_terminal_policy(&policy);
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
        self.store_terminal_reply(saga_id, reply.clone());
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
        self.register_terminal_policy(&policy);
        let resolver = Arc::new(Mutex::new(TerminalResolver::new(policy.clone())));
        let bus = self.clone();
        let responder: Arc<str> = Arc::from(responder);
        spawn_terminal_watchdog_if_needed(
            &policy,
            Arc::clone(&resolver),
            bus.clone(),
            Arc::clone(&responder),
        );
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

    /// Attaches terminal resolver via a static policy provider contract.
    pub fn attach_terminal_resolver_for<P: SagaTerminalPolicyProvider>(
        &self,
        responder: &'static str,
    ) -> EventSubscription {
        self.attach_terminal_resolver(P::terminal_policy(), responder)
    }

    pub fn take_terminal_reply(&self, saga_id: SagaId) -> Option<SagaReplyTo> {
        let reply = self
            .terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id);
        if reply.is_some() {
            self.terminal_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&saga_id);
        }
        reply
    }

    pub fn take_terminal_outcome(&self, saga_id: SagaId) -> Option<crate::SagaTerminalOutcome> {
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

    fn terminal_retention_limit(&self) -> usize {
        std::env::var("SAGA_TERMINAL_RETENTION_LIMIT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_TERMINAL_RETENTION_LIMIT)
    }

    fn has_terminal_policy_for_saga_type(&self, saga_type: &str) -> bool {
        self.terminal_policies_by_saga_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(saga_type)
    }

    fn store_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) {
        let outcome = reply.outcome.clone();
        self.terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(saga_id, reply);
        self.store_terminal_outcome(saga_id, outcome);
    }

    fn store_terminal_outcome(&self, saga_id: SagaId, outcome: SagaTerminalOutcome) {
        let inserted_new = {
            let mut terminal_outcomes = self
                .terminal_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            terminal_outcomes.insert(saga_id, outcome).is_none()
        };

        if inserted_new {
            let limit = self.terminal_retention_limit();
            let evicted_id = {
                let mut order = self
                    .terminal_order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut evicted_id = None;
                order.push_back(saga_id);
                while order.len() > limit {
                    let Some(candidate) = order.pop_front() else {
                        break;
                    };
                    if candidate != saga_id {
                        evicted_id = Some(candidate);
                        break;
                    }
                }
                evicted_id
            };
            if let Some(evicted_id) = evicted_id {
                self.remove_terminal_state(evicted_id);
            }
        }
    }

    fn remove_terminal_state(&self, saga_id: SagaId) -> Option<SagaTerminalOutcome> {
        self.terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id);
        self.terminal_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id)
    }
}

fn terminal_watchdog_tick_interval() -> Duration {
    std::env::var("SAGA_TERMINAL_WATCHDOG_TICK_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(DEFAULT_TERMINAL_WATCHDOG_TICK_MS))
}

fn spawn_terminal_watchdog_if_needed(
    policy: &TerminalPolicy,
    resolver: Arc<Mutex<TerminalResolver>>,
    bus: SagaChoreographyBus,
    responder: Arc<str>,
) {
    if policy.timeout.is_none() && policy.progress_timeout.is_none() {
        return;
    }

    let saga_type = policy.saga_type.clone();
    let watchdog_name = format!("saga-terminal-watchdog:{saga_type}");
    let spawn_result = thread::Builder::new()
        .name(watchdog_name)
        .spawn(move || loop {
            thread::sleep(terminal_watchdog_tick_interval());
            let terminal_events = {
                let mut guard = match resolver.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.poll_timeouts()
            };
            for terminal_event in terminal_events {
                let _ = bus.complete_terminal_reply_from_event(&terminal_event, responder.as_ref());
                let _ = bus.publish(terminal_event);
            }
        });
    if let Err(err) = spawn_result {
        tracing::warn!(
            target: "core::saga",
            event = "terminal_watchdog_spawn_failed",
            saga_type = policy.saga_type.as_ref(),
            error = %err
        );
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
            terminal_order: Arc::clone(&self.terminal_order),
            terminal_policies_by_saga_type: Arc::clone(&self.terminal_policies_by_saga_type),
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
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use icanact_core::local_sync;

    use crate::{
        FailureAuthority, SagaChoreographyEvent, SagaContext, SagaId, SagaReplyToResult,
        SagaTerminalOutcome, SagaTerminalPolicyProvider, SuccessCriteria, TerminalPolicy,
        TERMINAL_RESOLVER_STEP,
    };

    use super::{SagaChoreographyBus, DEFAULT_TERMINAL_RETENTION_LIMIT};

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
        assert!(matches!(
            reply.outcome,
            SagaTerminalOutcome::Completed { .. }
        ));
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
    fn terminal_outcome_retention_is_bounded() {
        let bus = SagaChoreographyBus::new();
        let total = DEFAULT_TERMINAL_RETENTION_LIMIT + 8;

        for raw_id in 1..=total as u64 {
            let _ = bus.publish(SagaChoreographyEvent::SagaCompleted {
                context: SagaContext {
                    step_name: TERMINAL_RESOLVER_STEP.into(),
                    ..context("create_order", raw_id)
                },
            });
        }

        assert!(
            bus.take_terminal_outcome(SagaId::new(1)).is_none(),
            "oldest terminal outcome should be evicted once retention limit is exceeded"
        );
        assert!(matches!(
            bus.take_terminal_outcome(SagaId::new(total as u64)),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
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

    #[test]
    fn saga_started_without_terminal_policy_is_failed_immediately() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(9001);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for unregistered terminal policy");
        };
        assert!(
            reason.contains("terminal policy is required before saga start"),
            "unexpected reason: {reason}"
        );
    }

    struct OrderLifecycleProvider;

    impl SagaTerminalPolicyProvider for OrderLifecycleProvider {
        fn terminal_policy() -> TerminalPolicy {
            TerminalPolicy::order_lifecycle_default()
        }
    }

    #[test]
    fn policy_provider_registration_allows_saga_started() {
        let bus = SagaChoreographyBus::new();
        bus.register_terminal_policy_provider::<OrderLifecycleProvider>();
        let saga_id = SagaId::new(9002);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "policy-registered saga start should not fail immediately"
        );
    }

    #[test]
    fn watchdog_times_out_stalled_saga_without_new_events() {
        let bus = SagaChoreographyBus::new();
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "watchdog/stall".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            timeout: Some(Duration::from_secs(5)),
            progress_timeout: Some(Duration::from_millis(120)),
        };
        let _resolver_sub = bus.attach_terminal_resolver(policy, "terminal-resolver");
        let saga_id = SagaId::new(8_001);
        let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut outcome = None;
        while Instant::now() < deadline {
            if let Some(found) = bus.take_terminal_outcome(saga_id) {
                outcome = Some(found);
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = outcome else {
            panic!("expected stalled terminal failure, got: {outcome:?}");
        };
        assert!(
            reason.as_ref().contains("stalled_timeout"),
            "expected stalled_timeout reason, got: {reason}"
        );
    }
}
