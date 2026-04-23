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
                let guard = match sender.lock() {
                    Ok(guard) => guard,
                    Err(err) => {
                        tracing::error!(
                            target: "core::saga",
                            event = "sync_channel_sender_lock_poisoned",
                            error = %err
                        );
                        return false;
                    }
                };
                guard
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

pub fn bind_sync_workflow_participant_channel_strict<A, C>(
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
    let subs =
        bind_sync_workflow_participant_channel::<A, C>(bus, actor_ref, channel_name, capacity)?;
    bus.register_bound_workflow_participants_for_actor::<A>()?;
    Ok(subs)
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

pub fn bind_sync_workflow_participant_tell_strict<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    map_event: F,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let subs = bind_sync_workflow_participant_tell::<A, F>(bus, actor_ref, map_event)?;
    bus.register_bound_workflow_participants_for_actor::<A>()?;
    Ok(subs)
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
                let guard = match sender.lock() {
                    Ok(guard) => guard,
                    Err(err) => {
                        tracing::error!(
                            target: "core::saga",
                            event = "async_channel_sender_lock_poisoned",
                            error = %err
                        );
                        return false;
                    }
                };
                guard
                    .try_send(SagaParticipantChannel::Saga(event.clone()).into())
                    .is_ok()
            })
        })
        .collect())
}

pub fn bind_async_workflow_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let saga_types = checked_workflow_saga_types::<A>()?;
    bind_async_participant_channel::<A, C>(bus, actor_ref, &saga_types, channel_name, capacity)
}

pub fn bind_async_workflow_participant_channel_strict<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<EventSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let subs =
        bind_async_workflow_participant_channel::<A, C>(bus, actor_ref, channel_name, capacity)?;
    bus.register_bound_workflow_participants_for_actor::<A>()?;
    Ok(subs)
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

#[cfg(test)]
mod tests {
    use crate::{
        CompensationError, HasSagaWorkflowParticipants, SagaChoreographyBus, SagaChoreographyEvent,
        SagaContext, SagaId, SagaTerminalOutcome, SagaWorkflowParticipant, StepError, StepOutput,
        define_saga_workflow_contract,
    };
    use icanact_core::local_sync::{self, SyncActor};

    use super::{
        SagaParticipantChannel, bind_sync_workflow_participant_channel,
        bind_sync_workflow_participant_channel_strict,
    };

    #[derive(Clone, Debug)]
    struct TestTell;

    impl icanact_core::TellAskTell for TestTell {}

    struct BindingActor;

    struct DuplicateBindingActor;

    impl SyncActor for BindingActor {
        type Contract = local_sync::contract::TellAsk;
        type Tell = TestTell;
        type Ask = ();
        type Reply = ();
        type Channel = SagaParticipantChannel<()>;
        type PubSub = ();
        type Broadcast = ();

        fn handle_tell(&mut self, _msg: Self::Tell) {}

        fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {}

        fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, _msg: Self::Channel) {}
    }

    impl SyncActor for DuplicateBindingActor {
        type Contract = local_sync::contract::TellAsk;
        type Tell = TestTell;
        type Ask = ();
        type Reply = ();
        type Channel = SagaParticipantChannel<()>;
        type PubSub = ();
        type Broadcast = ();

        fn handle_tell(&mut self, _msg: Self::Tell) {}

        fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {}

        fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, _msg: Self::Channel) {}
    }

    struct BindingWorkflow;
    struct DuplicateWorkflowOne;
    struct DuplicateWorkflowTwo;

    static BINDING_WORKFLOW: BindingWorkflow = BindingWorkflow;
    static DUPLICATE_WORKFLOW_ONE: DuplicateWorkflowOne = DuplicateWorkflowOne;
    static DUPLICATE_WORKFLOW_TWO: DuplicateWorkflowTwo = DuplicateWorkflowTwo;
    static BINDING_WORKFLOWS: [&'static dyn SagaWorkflowParticipant<BindingActor>; 1] =
        [&BINDING_WORKFLOW];
    static DUPLICATE_WORKFLOWS: [&'static dyn SagaWorkflowParticipant<DuplicateBindingActor>; 2] =
        [&DUPLICATE_WORKFLOW_ONE, &DUPLICATE_WORKFLOW_TWO];

    impl SagaWorkflowParticipant<BindingActor> for BindingWorkflow {
        fn step_name(&self) -> &'static str {
            "gate_step"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["binding_test"]
        }

        fn execute_step(
            &self,
            _actor: &mut BindingActor,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut BindingActor,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), CompensationError> {
            Ok(())
        }
    }

    impl SagaWorkflowParticipant<DuplicateBindingActor> for DuplicateWorkflowOne {
        fn step_name(&self) -> &'static str {
            "dup_one"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["binding_test"]
        }

        fn execute_step(
            &self,
            _actor: &mut DuplicateBindingActor,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut DuplicateBindingActor,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), CompensationError> {
            Ok(())
        }
    }

    impl SagaWorkflowParticipant<DuplicateBindingActor> for DuplicateWorkflowTwo {
        fn step_name(&self) -> &'static str {
            "dup_two"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["binding_test"]
        }

        fn execute_step(
            &self,
            _actor: &mut DuplicateBindingActor,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut DuplicateBindingActor,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), CompensationError> {
            Ok(())
        }
    }

    impl HasSagaWorkflowParticipants for BindingActor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &BINDING_WORKFLOWS
        }
    }

    impl HasSagaWorkflowParticipants for DuplicateBindingActor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &DUPLICATE_WORKFLOWS
        }
    }

    define_saga_workflow_contract! {
        struct BindingWorkflowContract {
            saga_type: "binding_test",
            first_step: gate_step,
            failure_authority: any (),
            required_steps: [gate_step],
            overall_timeout_ms: 30_000,
            stalled_timeout_ms: 10_000,
            steps: {
                gate_step => {
                    participant: "binding-actor",
                    depends_on: on_start ()
                }
            }
        }
    }

    fn context(step_name: &str, saga_id: u64) -> SagaContext {
        let now = SagaContext::now_millis();
        SagaContext {
            saga_id: SagaId::new(saga_id),
            saga_type: "binding_test".into(),
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

    #[test]
    fn non_strict_workflow_binding_leaves_steps_unbound_and_saga_start_fails() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<BindingWorkflowContract>()
            .expect("workflow contract registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<BindingWorkflowContract>("binding-resolver")
            .expect("terminal resolver should attach");
        let (actor_ref, handle) = local_sync::spawn(BindingActor);
        let subs =
            bind_sync_workflow_participant_channel::<BindingActor, ()>(&bus, &actor_ref, "saga", 8)
                .expect("non-strict workflow binding should succeed");

        let saga_id = SagaId::new(7001);
        let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("gate_step", saga_id.get()),
            payload: Vec::new(),
        });

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate failure for unbound workflow steps");
        };
        assert!(
            reason.contains("workflow contract violation: unbound participant steps"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("missing_steps=gate_step"),
            "unexpected reason: {reason}"
        );

        for sub in subs {
            let _ = bus.unsubscribe(sub);
        }
        handle.shutdown();
    }

    #[test]
    fn strict_workflow_binding_registers_steps_and_saga_start_is_accepted() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<BindingWorkflowContract>()
            .expect("workflow contract registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<BindingWorkflowContract>("binding-resolver")
            .expect("terminal resolver should attach");
        let (actor_ref, handle) = local_sync::spawn(BindingActor);
        let subs = bind_sync_workflow_participant_channel_strict::<BindingActor, ()>(
            &bus, &actor_ref, "saga", 8,
        )
        .expect("strict workflow binding should succeed");

        let saga_id = SagaId::new(7002);
        let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("gate_step", saga_id.get()),
            payload: Vec::new(),
        });

        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "strict binding should avoid immediate unbound-step failure"
        );

        for sub in subs {
            let _ = bus.unsubscribe(sub);
        }
        handle.shutdown();
    }

    #[test]
    fn strict_workflow_binding_rejects_duplicate_saga_type_registration() {
        let bus = SagaChoreographyBus::new();
        let (actor_ref, handle) = local_sync::spawn(DuplicateBindingActor);
        let err = bind_sync_workflow_participant_channel_strict::<DuplicateBindingActor, ()>(
            &bus, &actor_ref, "saga", 8,
        )
        .expect_err("duplicate workflow saga type must be rejected");
        assert!(
            err.contains("duplicate workflow registration for saga_type"),
            "unexpected error: {err}"
        );
        handle.shutdown();
    }
}
