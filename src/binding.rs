use icanact_core::local::FirehoseSubscription;
use std::sync::{Arc, Mutex};

use crate::{
    AllowsSagaTellIngress, HasSagaWorkflowParticipants, SagaChoreographyBus, SagaChoreographyEvent,
    SagaWorkflowParticipant,
};

fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone, Debug)]
// Keep the public channel shape direct so actor channel conversions stay simple.
#[allow(clippy::large_enum_variant)]
pub enum SagaParticipantChannel<C> {
    Saga(SagaChoreographyEvent),
    Business(C),
}

// The sender is shared by all subscriptions. Failed lazy registration is not
// cached, so a later publication retries; eager registration fails at bind time.
fn channel_forwarder<S, R, D>(
    channel_name: &str,
    lazy: bool,
    mut register: R,
    deliver: D,
) -> Result<impl Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static, String>
where
    S: Send + 'static,
    R: FnMut() -> Option<S> + Send + 'static,
    D: Fn(&S, SagaChoreographyEvent) -> bool + Send + Sync + 'static,
{
    let sender = if lazy {
        None
    } else {
        Some(
            register()
                .ok_or_else(|| format!("failed to register saga channel `{channel_name}`"))?,
        )
    };
    let state = Mutex::new((sender, register));
    let channel_name = channel_name.to_string();
    Ok(move |event: &SagaChoreographyEvent| {
        let mut state = lock_or_recover(&state);
        if state.0.is_none() {
            state.0 = (state.1)();
        }
        let delivered = state
            .0
            .as_ref()
            .is_some_and(|sender| deliver(sender, event.clone()));
        if !delivered {
            let context = event.context();
            tracing::error!(
                target: "core::saga",
                event = "saga_participant_forward_failed",
                saga_type = context.saga_type.as_ref(),
                saga_id = context.saga_id.get(),
                step = context.step_name.as_ref(),
                channel_name = %channel_name
            );
        }
        delivered
    })
}

fn sync_channel<A, C>(
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
    lazy: bool,
) -> Result<impl Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static, String>
where
    A: icanact_core::local_sync::SyncActor + Send + 'static,
    A::Channel: From<SagaParticipantChannel<C>> + Send + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let actor_ref = actor_ref.clone();
    let name = channel_name.to_string();
    channel_forwarder(
        channel_name,
        lazy,
        move || actor_ref.add_channel(name.clone(), capacity.max(1)),
        |sender, event| {
            sender
                .try_send(SagaParticipantChannel::<C>::Saga(event).into())
                .is_ok()
        },
    )
}

fn async_channel<A, C>(
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
    lazy: bool,
) -> Result<impl Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static, String>
where
    A: icanact_core::local_async::AsyncActor + Send + 'static,
    A::Channel: From<SagaParticipantChannel<C>> + Send + 'static,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let actor_ref = actor_ref.clone();
    let name = channel_name.to_string();
    channel_forwarder(
        channel_name,
        lazy,
        move || actor_ref.add_channel(name.clone(), capacity.max(1)),
        |sender, event| {
            sender
                .try_send(SagaParticipantChannel::<C>::Saga(event).into())
                .is_ok()
        },
    )
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

pub fn bind_sync_participant_channel_lazy<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    saga_types: &[&'static str],
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let forward = sync_channel::<A, C>(actor_ref, channel_name, capacity, true)?;
    Ok(subscribe_saga_types(bus, saga_types, forward))
}

pub fn bind_sync_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    saga_types: &[&'static str],
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let forward = sync_channel::<A, C>(actor_ref, channel_name, capacity, false)?;
    Ok(subscribe_saga_types(bus, saga_types, forward))
}

fn subscribe_saga_types<F>(
    bus: &SagaChoreographyBus,
    saga_types: &[&'static str],
    forward: F,
) -> Vec<FirehoseSubscription>
where
    F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
{
    let forward = Arc::new(forward);
    saga_types
        .iter()
        .map(|saga_type| {
            let forward = Arc::clone(&forward);
            bus.subscribe_saga_type_fn(saga_type, move |event| forward(event))
        })
        .collect()
}

fn subscribe_workflows<A, F>(
    bus: &SagaChoreographyBus,
    forward: F,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: HasSagaWorkflowParticipants,
    F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
{
    checked_workflow_saga_types::<A>()?;
    let forward = Arc::new(forward);
    let mut subscriptions = Vec::new();
    for workflow in A::saga_workflows() {
        for saga_type in workflow.saga_types() {
            let forward = Arc::clone(&forward);
            match bus.subscribe_workflow_step_fn(saga_type, workflow.step_name(), move |event| {
                forward(event)
            }) {
                Ok(sub) => subscriptions.push(sub),
                Err(error) => {
                    for sub in subscriptions {
                        bus.unsubscribe(sub);
                    }
                    return Err(error);
                }
            }
        }
    }
    Ok(subscriptions)
}

pub fn bind_sync_workflow_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let forward = sync_channel::<A, C>(actor_ref, channel_name, capacity, false)?;
    subscribe_workflows::<A, _>(bus, forward)
}

pub fn bind_sync_workflow_participant_channel_strict<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    bind_sync_workflow_participant_channel::<A, C>(bus, actor_ref, channel_name, capacity)
}

pub fn bind_sync_workflow_participant_channel_lazy<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let forward = sync_channel::<A, C>(actor_ref, channel_name, capacity, true)?;
    subscribe_workflows::<A, _>(bus, forward)
}

pub fn bind_sync_workflow_participant_channel_lazy_strict<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + HasSagaWorkflowParticipants + Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    C: Send + 'static,
{
    bind_sync_workflow_participant_channel_lazy::<A, C>(bus, actor_ref, channel_name, capacity)
}

pub fn bind_sync_participant_tell<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    saga_types: &[&'static str],
    map_event: F,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor + AllowsSagaTellIngress + Send + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let actor_ref = actor_ref.clone();
    Ok(subscribe_saga_types(bus, saga_types, move |event| {
        actor_ref.try_tell(map_event(event.clone())).is_ok()
    }))
}

pub fn bind_sync_workflow_participant_tell<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    map_event: F,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor
        + HasSagaWorkflowParticipants
        + AllowsSagaTellIngress
        + Send
        + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let actor_ref = actor_ref.clone();
    subscribe_workflows::<A, _>(bus, move |event| {
        actor_ref.try_tell(map_event(event.clone())).is_ok()
    })
}

pub fn bind_sync_workflow_participant_tell_strict<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    map_event: F,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_sync::SyncActor
        + HasSagaWorkflowParticipants
        + AllowsSagaTellIngress
        + Send
        + 'static,
    A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    bind_sync_workflow_participant_tell::<A, F>(bus, actor_ref, map_event)
}

pub fn bind_async_participant_channel_lazy<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    saga_types: &[&'static str],
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let forward = async_channel::<A, C>(actor_ref, channel_name, capacity, true)?;
    Ok(subscribe_saga_types(bus, saga_types, forward))
}

pub fn bind_async_participant_channel<A, C>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    saga_types: &[&'static str],
    channel_name: &str,
    capacity: usize,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: Send + 'static,
    <A as icanact_core::local_async::AsyncActor>::Channel: From<SagaParticipantChannel<C>>,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    C: Send + 'static,
{
    let forward = async_channel::<A, C>(actor_ref, channel_name, capacity, false)?;
    Ok(subscribe_saga_types(bus, saga_types, forward))
}

pub fn bind_async_participant_tell<A, F>(
    bus: &SagaChoreographyBus,
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    saga_types: &[&'static str],
    map_event: F,
) -> Result<Vec<FirehoseSubscription>, String>
where
    A: icanact_core::local_async::AsyncActor + AllowsSagaTellIngress + Send + 'static,
    A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
    F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
{
    let actor_ref = actor_ref.clone();
    Ok(subscribe_saga_types(bus, saga_types, move |event| {
        actor_ref.try_tell(map_event(event.clone())).is_ok()
    }))
}

#[cfg(test)]
mod tests {
    use crate::{
        CompensationError, HasSagaWorkflowParticipants, SagaChoreographyBus, SagaChoreographyEvent,
        SagaContext, SagaId, SagaWorkflowParticipant, StepError, StepOutput,
        define_saga_workflow_contract,
    };
    use icanact_core::local_sync::{self, SyncActor};

    use super::{
        SagaParticipantChannel, bind_sync_workflow_participant_channel,
        bind_sync_workflow_participant_channel_lazy_strict,
        bind_sync_workflow_participant_channel_strict,
    };

    #[derive(Clone, Debug)]
    struct TestTell;

    impl icanact_core::TellAskTell for TestTell {}

    #[derive(Default)]
    struct BindingActor(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl crate::AllowsSagaTellIngress for BindingActor {}

    struct DuplicateBindingActor;

    impl SyncActor for BindingActor {
        type Contract = local_sync::contract::TellAsk;
        type Tell = TestTell;
        type Ask = ();
        type Reply = ();
        type Channel = SagaParticipantChannel<()>;
        type PubSub = ();
        type Broadcast = ();

        fn handle_tell(&mut self, _msg: Self::Tell) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {}

        fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, _msg: Self::Channel) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
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
        ) -> Result<crate::CompensationOutput, CompensationError> {
            Ok(crate::CompensationOutput::Completed)
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
        ) -> Result<crate::CompensationOutput, CompensationError> {
            Ok(crate::CompensationOutput::Completed)
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
        ) -> Result<crate::CompensationOutput, CompensationError> {
            Ok(crate::CompensationOutput::Completed)
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
    fn workflow_binding_registers_live_step_routes() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<BindingWorkflowContract>()
            .expect("workflow contract registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<BindingWorkflowContract>("binding-resolver")
            .expect("terminal resolver should attach");
        let (actor_ref, handle) = local_sync::spawn(BindingActor::default());
        let subs =
            bind_sync_workflow_participant_channel::<BindingActor, ()>(&bus, &actor_ref, "saga", 8)
                .expect("non-strict workflow binding should succeed");

        let saga_id = SagaId::new(7001);
        let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("gate_step", saga_id.get()),
            payload: Vec::new(),
        });

        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "workflow bindings now supply live step routes"
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
        let (actor_ref, handle) = local_sync::spawn(BindingActor::default());
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
    fn lazy_strict_workflow_binding_does_not_require_eager_channel_registration() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<BindingWorkflowContract>()
            .expect("workflow contract registration should succeed");
        let actor_ref = icanact_core::SyncActorRef::<BindingActor>::new_unset();

        let subs = bind_sync_workflow_participant_channel_lazy_strict::<BindingActor, ()>(
            &bus, &actor_ref, "saga", 8,
        )
        .expect("lazy strict workflow binding should not require the channel lane yet");

        assert_eq!(subs.len(), 1);

        for sub in subs {
            let _ = bus.unsubscribe(sub);
        }
    }

    #[test]
    fn lazy_channel_delivery_failure_is_reported_to_strict_publish() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<BindingWorkflowContract>()
            .expect("workflow contract registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<BindingWorkflowContract>("binding-resolver")
            .expect("terminal resolver should attach");
        let actor_ref = icanact_core::SyncActorRef::<BindingActor>::new_unset();
        let subs = bind_sync_workflow_participant_channel_lazy_strict::<BindingActor, ()>(
            &bus, &actor_ref, "saga", 1,
        )
        .expect("lazy strict workflow binding should not require channel lane yet");

        let err = bus
            .publish_strict(SagaChoreographyEvent::SagaStarted {
                context: context("gate_step", 7004),
                payload: Vec::new(),
            })
            .expect_err("unset lazy channel must not count as delivered");

        assert!(
            matches!(
                err,
                // The terminal resolver (needed for start admission) is the one
                // delivery; the unset lazy participant channel must not count.
                crate::SagaBusPublishError::RequiredPathDeliveryShortfall {
                    delivered: 1,
                    required_min_delivered: 2,
                    ..
                }
            ),
            "unexpected strict publish error: {err:?}"
        );
        for sub in subs {
            let _ = bus.unsubscribe(sub);
        }
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
    #[test]
    fn workflow_tell_and_channel_variants_report_truthful_delivery() {
        for variant in 0..6 {
            let bus = SagaChoreographyBus::new();
            bus.register_workflow_contract_provider::<BindingWorkflowContract>()
                .unwrap();
            let _resolver = bus
                .attach_terminal_resolver_for_contract::<BindingWorkflowContract>("receipts")
                .unwrap();
            let actor = BindingActor::default();
            let received = actor.0.clone();
            let (actor_ref, handle) = local_sync::spawn(actor);
            let subs = match variant {
                0 => super::bind_sync_workflow_participant_channel::<_, ()>(
                    &bus, &actor_ref, "saga", 0,
                ),
                1 => super::bind_sync_workflow_participant_channel_strict::<_, ()>(
                    &bus, &actor_ref, "saga", 0,
                ),
                2 => super::bind_sync_workflow_participant_channel_lazy::<_, ()>(
                    &bus, &actor_ref, "saga", 0,
                ),
                3 => super::bind_sync_workflow_participant_channel_lazy_strict::<_, ()>(
                    &bus, &actor_ref, "saga", 0,
                ),
                4 => super::bind_sync_workflow_participant_tell(&bus, &actor_ref, |_| TestTell),
                _ => super::bind_sync_workflow_participant_tell_strict(&bus, &actor_ref, |_| {
                    TestTell
                }),
            }
            .unwrap();
            let stats = bus
                .publish_strict(SagaChoreographyEvent::SagaStarted {
                    context: context("gate_step", 7100 + variant * 2),
                    payload: vec![],
                })
                .expect("live participant supplies required role receipt");
            assert_eq!(
                stats.delivered, 2,
                "participant + resolver; variant={variant}"
            );
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while received.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "actor never received event; variant={variant}"
                );
                std::thread::yield_now();
            }
            handle.shutdown();
            let id = SagaId::new(7101 + variant * 2);
            let err = bus
                .publish_strict(SagaChoreographyEvent::SagaStarted {
                    context: context("gate_step", id.get()),
                    payload: vec![],
                })
                .expect_err("stopped participant must return false");
            assert!(
                matches!(
                    err,
                    crate::SagaBusPublishError::RequiredPathDeliveryShortfall {
                        delivered: 1,
                        required_min_delivered: 2,
                        ..
                    }
                ),
                "variant={variant}: {err:?}"
            );
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if let Some(outcome) = bus.take_terminal_outcome(id) {
                    assert!(matches!(
                        outcome,
                        crate::SagaTerminalOutcome::Quarantined { .. }
                    ));
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "shortfall not quarantined"
                );
                std::thread::yield_now();
            }
            for sub in subs {
                bus.unsubscribe(sub);
            }
        }
    }

    #[test]
    fn eager_channel_registration_failure_is_immediate() {
        let bus = SagaChoreographyBus::new();
        let actor_ref = icanact_core::SyncActorRef::<BindingActor>::new_unset();
        assert!(
            super::bind_sync_participant_channel::<_, ()>(
                &bus,
                &actor_ref,
                &["binding_test"],
                "saga",
                0
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn async_tell_and_channel_bindings_report_delivery_after_shutdown() {
        use icanact_core::local_async::{self, AsyncActor, AsyncContext};
        struct Actor(std::sync::Arc<std::sync::atomic::AtomicUsize>);
        impl crate::AllowsSagaTellIngress for Actor {}
        impl AsyncActor for Actor {
            type Contract = local_async::contract::TellAsk;
            type Tell = TestTell;
            type Ask = ();
            type Reply = ();
            type Channel = SagaParticipantChannel<()>;
            type PubSub = ();
            type Broadcast = ();
            fn supports_channels() -> bool {
                true
            }
            async fn handle_tell(&mut self, _: TestTell, _: &mut AsyncContext) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            async fn handle_ask(&mut self, _: ()) {}
            async fn handle_channel(&mut self, _: local_async::ChannelId, _: Self::Channel) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        for variant in 0..3 {
            let bus = SagaChoreographyBus::new();
            let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let (actor_ref, handle) = local_async::spawn(Actor(received.clone())).await;
            let subs = match variant {
                0 => super::bind_async_participant_channel::<_, ()>(
                    &bus,
                    &actor_ref,
                    &["binding_test"],
                    "saga",
                    0,
                ),
                1 => super::bind_async_participant_channel_lazy::<_, ()>(
                    &bus,
                    &actor_ref,
                    &["binding_test"],
                    "saga",
                    0,
                ),
                _ => {
                    super::bind_async_participant_tell(&bus, &actor_ref, &["binding_test"], |_| {
                        TestTell
                    })
                }
            }
            .unwrap();
            let event = SagaChoreographyEvent::SagaCompleted {
                context: context("gate_step", 7200 + variant),
            };
            assert_eq!(bus.publish(event.clone()).delivered, 1);
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while received.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("actor received event");
            handle.shutdown().await;
            assert_eq!(
                bus.publish(event).delivered,
                0,
                "stopped actor; variant={variant}"
            );
            for sub in subs {
                bus.unsubscribe(sub);
            }
        }
    }

    #[test]
    fn lazy_channel_registration_retries_and_shares_sender() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registrations = attempts.clone();
        let forward = super::channel_forwarder(
            "retry",
            true,
            move || {
                (registrations.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0).then_some(())
            },
            |_, _| true,
        )
        .unwrap();
        let event = SagaChoreographyEvent::SagaCompleted {
            context: context("gate_step", 7300),
        };
        assert!(!forward(&event));
        assert!(forward(&event));
        assert!(forward(&event));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
