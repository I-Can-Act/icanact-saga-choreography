//! Test helpers for participant choreography tests.

use crate::{
    HasSagaWorkflowParticipants, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant,
    SagaStateExt, apply_sync_workflow_participant_saga_ingress, handle_saga_event_with_emit,
};

/// Small deterministic builder for saga test contexts.
#[derive(Debug, Clone)]
pub struct DeterministicContextBuilder {
    saga_id: u64,
    saga_type: String,
    step_name: String,
    correlation_id: u64,
    causation_id: u64,
    trace_id: u64,
    started_at_millis: u64,
    event_at_millis: u64,
}

impl Default for DeterministicContextBuilder {
    fn default() -> Self {
        Self {
            saga_id: 1,
            saga_type: "order_lifecycle".to_string(),
            step_name: "risk_check".to_string(),
            correlation_id: 1,
            causation_id: 1,
            trace_id: 1,
            started_at_millis: 1_700_000_000_000,
            event_at_millis: 1_700_000_000_000,
        }
    }
}

impl DeterministicContextBuilder {
    pub fn with_saga_id(mut self, saga_id: u64) -> Self {
        self.saga_id = saga_id;
        self
    }

    pub fn with_saga_type(mut self, saga_type: impl Into<String>) -> Self {
        self.saga_type = saga_type.into();
        self
    }

    pub fn with_step_name(mut self, step_name: impl Into<String>) -> Self {
        self.step_name = step_name.into();
        self
    }

    pub fn with_trace_id(mut self, trace_id: u64) -> Self {
        self.trace_id = trace_id;
        self
    }

    pub fn build(self) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(self.saga_id),
            saga_type: self.saga_type.into_boxed_str(),
            step_name: self.step_name.into_boxed_str(),
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            trace_id: self.trace_id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: self.started_at_millis,
            event_timestamp_millis: self.event_at_millis,
        }
    }
}

pub fn saga_started(context: SagaContext, payload: Vec<u8>) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted { context, payload }
}

pub fn step_completed(
    context: SagaContext,
    output: Vec<u8>,
    saga_input: Vec<u8>,
    compensation_available: bool,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context,
        output,
        saga_input,
        compensation_available,
    }
}

pub fn step_failed(
    context: SagaContext,
    error: impl Into<String>,
    requires_compensation: bool,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepFailed {
        context,
        participant_id: "testkit".into(),
        error_code: None,
        error: error.into().into_boxed_str(),
        requires_compensation,
    }
}

pub fn compensation_requested(
    context: SagaContext,
    failed_step: impl Into<String>,
    reason: impl Into<String>,
    steps_to_compensate: Vec<String>,
) -> SagaChoreographyEvent {
    let failed_step = failed_step.into().into_boxed_str();
    let reason = reason.into().into_boxed_str();
    SagaChoreographyEvent::CompensationRequested {
        context,
        failed_step: failed_step.clone(),
        reason: reason.clone(),
        failure: crate::SagaFailureDetails {
            step_name: failed_step,
            participant_id: "testkit".into(),
            error_code: None,
            error_message: reason,
            at_millis: 0,
        },
        steps_to_compensate: steps_to_compensate
            .into_iter()
            .map(|step| step.into_boxed_str())
            .collect(),
    }
}

/// Replay a deterministic event sequence against one participant.
pub fn drive_scenario<P>(
    participant: &mut P,
    events: impl IntoIterator<Item = SagaChoreographyEvent>,
) where
    P: SagaParticipant + SagaStateExt,
{
    for event in events {
        handle_saga_event_with_emit(participant, event, |_| {});
    }
}

/// Replay a deterministic event sequence against a workflow-scoped participant actor.
pub fn drive_workflow_scenario<A>(
    actor: &mut A,
    events: impl IntoIterator<Item = SagaChoreographyEvent>,
) where
    A: crate::HasSagaParticipantSupport + HasSagaWorkflowParticipants + Send + 'static,
{
    for event in events {
        apply_sync_workflow_participant_saga_ingress(actor, event, |_actor, _event| {}, |_| {});
    }
}

#[cfg(any(test, feature = "test-harness"))]
use crate::{
    AcceptedCompensationCompletion, AcceptedCompensationFailure, AcceptedStepCompletion,
    AcceptedStepError, AcceptedStepFailure, AcceptedStepPolicy, StepExecutionId,
    accept_workflow_step, complete_accepted_workflow_compensation, complete_accepted_workflow_step,
    fail_accepted_workflow_compensation, fail_accepted_workflow_step,
    record_accepted_workflow_step_progress,
};
#[cfg(any(test, feature = "test-harness"))]
use std::collections::HashSet;
#[cfg(any(test, feature = "test-harness"))]
use std::sync::{Arc, Once};
#[cfg(any(test, feature = "test-harness"))]
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "test-harness"))]
use crate::{AsyncSagaParticipant, HasSagaParticipantSupport, SagaParticipantSupportExt};
#[cfg(any(test, feature = "test-harness"))]
use crate::{
    SagaChoreographyBus, SagaParticipantChannel, SagaTerminalOutcome, TerminalPolicy,
    bind_async_participant_channel, bind_sync_participant_channel,
    bind_sync_workflow_participant_channel_strict, checked_workflow_saga_types,
};
#[cfg(any(test, feature = "test-harness"))]
use icanact_core::local::{FirehoseSubscription, PublishStats};

#[cfg(any(test, feature = "test-harness"))]
#[derive(Clone, Debug)]
enum TranscriptTell {
    Record(SagaChoreographyEvent),
}

#[cfg(any(test, feature = "test-harness"))]
impl icanact_core::TellAskTell for TranscriptTell {}

#[cfg(any(test, feature = "test-harness"))]
#[derive(Clone, Debug)]
enum TranscriptAsk {
    Snapshot,
    EnsureCapture(Box<str>),
}

#[cfg(any(test, feature = "test-harness"))]
#[derive(Clone, Debug)]
enum TranscriptReply {
    Snapshot(Vec<SagaChoreographyEvent>),
    CaptureWasNew(bool),
}

#[cfg(any(test, feature = "test-harness"))]
#[derive(Default)]
struct TranscriptActor {
    events: Vec<SagaChoreographyEvent>,
    captured_saga_types: HashSet<Box<str>>,
}

#[cfg(any(test, feature = "test-harness"))]
impl icanact_core::local_sync::SyncActor for TranscriptActor {
    type Contract = icanact_core::local_sync::contract::TellAsk;
    type Tell = TranscriptTell;
    type Ask = TranscriptAsk;
    type Reply = TranscriptReply;
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            TranscriptTell::Record(event) => self.events.push(event),
        }
    }

    fn handle_ask(&mut self, msg: Self::Ask) -> Self::Reply {
        match msg {
            TranscriptAsk::Snapshot => TranscriptReply::Snapshot(self.events.clone()),
            TranscriptAsk::EnsureCapture(saga_type) => {
                TranscriptReply::CaptureWasNew(self.captured_saga_types.insert(saga_type))
            }
        }
    }
}

/// Actor-ref-centric harness for exercising real saga participants over the real runtime path.
#[cfg(any(test, feature = "test-harness"))]
pub struct SagaTestWorld {
    bus: SagaChoreographyBus,
    transcript_ref: icanact_core::local_sync::SyncActorRef<TranscriptActor>,
    transcript_handle: Option<icanact_core::local_sync::ActorHandle>,
}

#[cfg(any(test, feature = "test-harness"))]
impl SagaTestWorld {
    pub fn new() -> Self {
        crate::bus::ensure_saga_sync_pool_capacity();
        let (transcript_ref, transcript_handle) =
            icanact_core::local_sync::spawn(TranscriptActor::default());
        Self {
            bus: SagaChoreographyBus::new(),
            transcript_ref,
            transcript_handle: Some(transcript_handle),
        }
    }

    pub fn init_test_logging() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::DEBUG)
                .try_init();
        });
    }

    pub fn bus(&self) -> SagaChoreographyBus {
        self.bus.clone()
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        self.ensure_capture_saga_type(event.context().saga_type.as_ref());
        self.bus.publish(event)
    }

    pub fn start_saga(&self, context: SagaContext, payload: Vec<u8>) -> PublishStats {
        self.publish(saga_started(context, payload))
    }

    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> Result<FirehoseSubscription, String> {
        self.ensure_capture_saga_type(policy.saga_type.as_ref());
        self.bus.attach_terminal_resolver(policy, responder)
    }

    pub fn transcript(&self) -> Vec<SagaChoreographyEvent> {
        match self
            .transcript_ref
            .ask(TranscriptAsk::Snapshot)
            .expect("saga transcript actor should reply")
        {
            TranscriptReply::Snapshot(events) => events,
            TranscriptReply::CaptureWasNew(_) => unreachable!("snapshot ask must return events"),
        }
    }

    pub fn transcript_for_saga(&self, saga_id: SagaId) -> Vec<SagaChoreographyEvent> {
        self.transcript()
            .into_iter()
            .filter(|event| event.context().saga_id == saga_id)
            .collect()
    }

    pub fn wait_for_event<P>(&self, predicate: P, timeout: Duration) -> SagaChoreographyEvent
    where
        P: Fn(&SagaChoreographyEvent) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(found) = self.transcript().into_iter().find(|event| predicate(event)) {
                return found;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for saga testkit event"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn wait_for_terminal(&self, saga_id: SagaId, timeout: Duration) -> SagaTerminalOutcome {
        self.wait_for_event(
            |event| event.context().saga_id == saga_id && event.terminal_outcome().is_some(),
            timeout,
        )
        .terminal_outcome()
        .expect("terminal wait predicate must only match terminal events")
    }

    pub fn wait_for_step_accepted(
        &self,
        saga_id: SagaId,
        step_name: &str,
        timeout: Duration,
    ) -> SagaChoreographyEvent {
        self.wait_for_event(
            {
                let step_name = step_name.to_string();
                move |event| {
                matches!(
                event,
                SagaChoreographyEvent::StepAccepted { context, .. } if context.saga_id == saga_id
                    && context.step_name.as_ref() == step_name
                )
                }
            },
            timeout,
        )
    }

    pub fn complete_accepted_step<A>(
        &self,
        actor: &mut A,
        saga_id: SagaId,
        execution_id: StepExecutionId,
        completion: AcceptedStepCompletion,
    ) -> Result<icanact_core::local::PublishStats, AcceptedStepError>
    where
        A: SagaStateExt,
    {
        let event = complete_accepted_workflow_step(actor, saga_id, execution_id, completion)?;
        Ok(self.publish(event))
    }

    pub fn accept_step<A>(
        &self,
        actor: &mut A,
        context: SagaContext,
        participant_id: Box<str>,
        execution_id: StepExecutionId,
        policy: AcceptedStepPolicy,
        saga_input: Vec<u8>,
        compensation_data: Vec<u8>,
    ) -> Result<icanact_core::local::PublishStats, AcceptedStepError>
    where
        A: SagaStateExt,
    {
        let event = accept_workflow_step(
            actor,
            context,
            participant_id,
            execution_id,
            policy,
            saga_input,
            compensation_data,
        )?;
        Ok(self.publish(event))
    }

    pub fn complete_accepted_compensation<A>(
        &self,
        actor: &mut A,
        saga_id: SagaId,
        execution_id: StepExecutionId,
        completion: AcceptedCompensationCompletion,
    ) -> Result<icanact_core::local::PublishStats, AcceptedStepError>
    where
        A: SagaStateExt,
    {
        let event =
            complete_accepted_workflow_compensation(actor, saga_id, execution_id, completion)?;
        Ok(self.publish(event))
    }

    pub fn fail_accepted_compensation<A>(
        &self,
        actor: &mut A,
        saga_id: SagaId,
        execution_id: StepExecutionId,
        failure: AcceptedCompensationFailure,
    ) -> Result<icanact_core::local::PublishStats, AcceptedStepError>
    where
        A: SagaStateExt,
    {
        let event = fail_accepted_workflow_compensation(actor, saga_id, execution_id, failure)?;
        Ok(self.publish(event))
    }

    pub fn record_accepted_step_progress<A>(
        &self,
        actor: &mut A,
        saga_id: SagaId,
        execution_id: StepExecutionId,
        now_millis: u64,
    ) -> Result<icanact_core::local::PublishStats, AcceptedStepError>
    where
        A: SagaStateExt,
    {
        let event =
            record_accepted_workflow_step_progress(actor, saga_id, execution_id, now_millis)?;
        Ok(self.publish(event))
    }

    pub fn fail_accepted_step<A>(
        &self,
        actor: &mut A,
        saga_id: SagaId,
        execution_id: StepExecutionId,
        failure: AcceptedStepFailure,
    ) -> Result<icanact_core::local::PublishStats, AcceptedStepError>
    where
        A: SagaStateExt,
    {
        let event = fail_accepted_workflow_step(actor, saga_id, execution_id, failure)?;
        Ok(self.publish(event))
    }

    pub async fn wait_for_event_async<P>(
        &self,
        predicate: P,
        timeout: Duration,
    ) -> SagaChoreographyEvent
    where
        P: Fn(&SagaChoreographyEvent) -> bool + Send + 'static,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(found) = self.transcript().into_iter().find(|event| predicate(event)) {
                return found;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for saga testkit event"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn wait_for_terminal_async(
        &self,
        saga_id: SagaId,
        timeout: Duration,
    ) -> SagaTerminalOutcome {
        self.wait_for_event_async(
            move |event| event.context().saga_id == saga_id && event.terminal_outcome().is_some(),
            timeout,
        )
        .await
        .terminal_outcome()
        .expect("terminal wait predicate must only match terminal events")
    }

    pub fn spawn_sync_participant<A, F>(
        &self,
        actor: A,
        map_event: F,
    ) -> SyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_sync::SyncActor + SagaParticipant + HasSagaParticipantSupport,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        self.spawn_sync_participant_with_opts(
            actor,
            icanact_core::local_sync::SpawnOpts::default(),
            map_event,
        )
    }

    pub fn spawn_sync_participant_with_opts<A, F>(
        &self,
        mut actor: A,
        opts: icanact_core::local_sync::SpawnOpts,
        map_event: F,
    ) -> SyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_sync::SyncActor + SagaParticipant + HasSagaParticipantSupport,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        actor.attach_saga_bus(self.bus.clone());
        let saga_types: Vec<&'static str> = actor.saga_types().to_vec();
        let (actor_ref, handle) = icanact_core::local_sync::spawn_with_opts(actor, opts);
        self.register_sync_subscriptions(actor_ref.clone(), &saga_types, map_event);
        SyncSagaParticipantHandle { actor_ref, handle }
    }

    pub fn spawn_sync_channel_participant<A, C>(
        &self,
        mut actor: A,
        channel_name: &str,
        channel_capacity: usize,
    ) -> SyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_sync::SyncActor + SagaParticipant + HasSagaParticipantSupport,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
        C: Send + 'static,
    {
        actor.attach_saga_bus(self.bus.clone());
        let saga_types: Vec<&'static str> = actor.saga_types().to_vec();
        let (actor_ref, handle) = icanact_core::local_sync::spawn(actor);
        let subs = bind_sync_participant_channel::<A, C>(
            &self.bus,
            &actor_ref,
            &saga_types,
            channel_name,
            channel_capacity,
        )
        .expect("sync participant saga channel binding should succeed");
        drop(subs);
        SyncSagaParticipantHandle { actor_ref, handle }
    }

    pub fn spawn_sync_workflow_channel_participant<A, C>(
        &self,
        mut actor: A,
        channel_name: &str,
        channel_capacity: usize,
    ) -> SyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_sync::SyncActor
            + HasSagaParticipantSupport
            + HasSagaWorkflowParticipants,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        <A as icanact_core::local_sync::SyncActor>::Channel: From<SagaParticipantChannel<C>>,
        C: Send + 'static,
    {
        actor.attach_saga_bus(self.bus.clone());
        let saga_types = checked_workflow_saga_types::<A>()
            .expect("workflow participant saga type registration should be valid");
        for saga_type in &saga_types {
            self.ensure_capture_saga_type(saga_type);
        }

        let (actor_ref, handle) = icanact_core::local_sync::spawn(actor);
        let subs = bind_sync_workflow_participant_channel_strict::<A, C>(
            &self.bus,
            &actor_ref,
            channel_name,
            channel_capacity,
        )
        .expect("sync workflow participant saga channel binding should succeed");
        drop(subs);
        SyncSagaParticipantHandle { actor_ref, handle }
    }

    pub fn spawn_sync_participant_args<A, Args, FMake, F>(
        &self,
        args: Args,
        make: FMake,
        map_event: F,
    ) -> SyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_sync::SyncActor + SagaParticipant + HasSagaParticipantSupport,
        Args: Clone + Send + Sync + 'static,
        FMake: Fn(Args) -> A + Send + Sync + 'static,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        self.spawn_sync_participant_with_opts(make(args), Default::default(), map_event)
    }

    pub fn spawn_sync_participant_args_with_opts<A, Args, FMake, F>(
        &self,
        args: Args,
        make: FMake,
        opts: icanact_core::local_sync::SpawnOpts,
        map_event: F,
    ) -> SyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_sync::SyncActor + SagaParticipant + HasSagaParticipantSupport,
        Args: Clone + Send + Sync + 'static,
        FMake: Fn(Args) -> A + Send + Sync + 'static,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        self.spawn_sync_participant_with_opts(make(args), opts, map_event)
    }

    pub async fn spawn_async_participant<A, F>(
        &self,
        actor: A,
        map_event: F,
    ) -> AsyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_async::AsyncActor + AsyncSagaParticipant + HasSagaParticipantSupport,
        A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        self.spawn_async_participant_with_opts(
            actor,
            icanact_core::local_async::SpawnOpts::default(),
            map_event,
        )
        .await
    }

    pub async fn spawn_async_participant_with_opts<A, F>(
        &self,
        mut actor: A,
        opts: icanact_core::local_async::SpawnOpts,
        map_event: F,
    ) -> AsyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_async::AsyncActor + AsyncSagaParticipant + HasSagaParticipantSupport,
        A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        actor.attach_saga_bus(self.bus.clone());
        let saga_types: Vec<&'static str> = actor.saga_types().to_vec();
        let (actor_ref, handle) = icanact_core::local_async::spawn_with_opts(actor, opts).await;
        self.register_async_subscriptions(actor_ref.clone(), &saga_types, map_event);
        AsyncSagaParticipantHandle { actor_ref, handle }
    }

    pub async fn spawn_async_channel_participant<A, C>(
        &self,
        mut actor: A,
        channel_name: &str,
        channel_capacity: usize,
    ) -> AsyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_async::AsyncActor + AsyncSagaParticipant + HasSagaParticipantSupport,
        A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
        <A as icanact_core::local_async::AsyncActor>::Channel: From<SagaParticipantChannel<C>>,
        C: Send + 'static,
    {
        actor.attach_saga_bus(self.bus.clone());
        let saga_types: Vec<&'static str> = actor.saga_types().to_vec();
        let (actor_ref, handle) = icanact_core::local_async::spawn_with_opts(
            actor,
            icanact_core::local_async::SpawnOpts::default(),
        )
        .await;
        let subs = bind_async_participant_channel::<A, C>(
            &self.bus,
            &actor_ref,
            &saga_types,
            channel_name,
            channel_capacity,
        )
        .expect("async participant saga channel binding should succeed");
        drop(subs);
        AsyncSagaParticipantHandle { actor_ref, handle }
    }

    pub async fn spawn_async_participant_args<A, Args, FMake, F>(
        &self,
        args: Args,
        make: FMake,
        map_event: F,
    ) -> AsyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_async::AsyncActor + AsyncSagaParticipant + HasSagaParticipantSupport,
        Args: Clone + Send + Sync + 'static,
        FMake: Fn(Args) -> A + Send + Sync + 'static,
        A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        self.spawn_async_participant_with_opts(make(args), Default::default(), map_event)
            .await
    }

    pub async fn spawn_async_participant_args_with_opts<A, Args, FMake, F>(
        &self,
        args: Args,
        make: FMake,
        opts: icanact_core::local_async::SpawnOpts,
        map_event: F,
    ) -> AsyncSagaParticipantHandle<A>
    where
        A: icanact_core::local_async::AsyncActor + AsyncSagaParticipant + HasSagaParticipantSupport,
        Args: Clone + Send + Sync + 'static,
        FMake: Fn(Args) -> A + Send + Sync + 'static,
        A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        self.spawn_async_participant_with_opts(make(args), opts, map_event)
            .await
    }

    fn ensure_capture_saga_type(&self, saga_type: &str) {
        let capture_was_new = match self
            .transcript_ref
            .ask(TranscriptAsk::EnsureCapture(saga_type.into()))
            .expect("saga transcript actor should reply")
        {
            TranscriptReply::CaptureWasNew(was_new) => was_new,
            TranscriptReply::Snapshot(_) => unreachable!("capture ask must return capture state"),
        };
        if !capture_was_new {
            return;
        }
        let transcript_ref = self.transcript_ref.clone();
        let sub = self.bus.subscribe_saga_type_fn(saga_type, move |event| {
            transcript_ref.tell(TranscriptTell::Record(event.clone()));
            true
        });
        let _ = sub;
    }

    fn register_sync_subscriptions<A, F>(
        &self,
        actor_ref: icanact_core::local_sync::SyncActorRef<A>,
        saga_types: &[&'static str],
        map_event: F,
    ) where
        A: icanact_core::local_sync::SyncActor,
        A::Contract: icanact_core::local_sync::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        let map_event = Arc::new(map_event);
        for saga_type in saga_types {
            self.ensure_capture_saga_type(saga_type);
            let actor_ref = actor_ref.clone();
            let map_event = Arc::clone(&map_event);
            let sub = self.bus.subscribe_saga_type_fn(saga_type, move |event| {
                actor_ref.tell(map_event(event.clone()))
            });
            let _ = sub;
        }
    }

    fn register_async_subscriptions<A, F>(
        &self,
        actor_ref: icanact_core::local_async::AsyncActorRef<A>,
        saga_types: &[&'static str],
        map_event: F,
    ) where
        A: icanact_core::local_async::AsyncActor,
        A::Contract: icanact_core::local_async::contract::SupportsTell<A>,
        F: Fn(SagaChoreographyEvent) -> A::Tell + Send + Sync + 'static,
    {
        let map_event = Arc::new(map_event);
        for saga_type in saga_types {
            self.ensure_capture_saga_type(saga_type);
            let actor_ref = actor_ref.clone();
            let map_event = Arc::clone(&map_event);
            let sub = self.bus.subscribe_saga_type_fn(saga_type, move |event| {
                actor_ref.tell(map_event(event.clone()))
            });
            let _ = sub;
        }
    }
}

#[cfg(any(test, feature = "test-harness"))]
impl Drop for SagaTestWorld {
    fn drop(&mut self) {
        if let Some(handle) = self.transcript_handle.take() {
            handle.shutdown();
        }
    }
}

impl Default for SagaTestWorld {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-harness"))]
pub struct SyncSagaParticipantHandle<A>
where
    A: icanact_core::local_sync::SyncActor,
{
    actor_ref: icanact_core::local_sync::SyncActorRef<A>,
    handle: icanact_core::local_sync::ActorHandle,
}

#[cfg(any(test, feature = "test-harness"))]
impl<A> SyncSagaParticipantHandle<A>
where
    A: icanact_core::local_sync::SyncActor,
{
    pub fn actor_ref(&self) -> icanact_core::local_sync::SyncActorRef<A> {
        self.actor_ref.clone()
    }

    pub fn shutdown(self) {
        self.handle.shutdown();
    }
}

#[cfg(any(test, feature = "test-harness"))]
pub struct AsyncSagaParticipantHandle<A>
where
    A: icanact_core::local_async::AsyncActor,
{
    actor_ref: icanact_core::local_async::AsyncActorRef<A>,
    handle: icanact_core::local_async::ActorHandle,
}

#[cfg(any(test, feature = "test-harness"))]
impl<A> AsyncSagaParticipantHandle<A>
where
    A: icanact_core::local_async::AsyncActor,
{
    pub fn actor_ref(&self) -> icanact_core::local_async::AsyncActorRef<A> {
        self.actor_ref.clone()
    }

    pub async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CompensationError, DependencySpec, HasSagaParticipantSupport, InMemoryDedupe,
        InMemoryJournal, SagaParticipantSupport, StepError, StepOutput,
    };

    use super::*;

    struct TestParticipant {
        saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
        called: bool,
    }

    impl Default for TestParticipant {
        fn default() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
                called: false,
            }
        }
    }

    impl HasSagaParticipantSupport for TestParticipant {
        type Journal = InMemoryJournal;
        type Dedupe = InMemoryDedupe;

        fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &self.saga
        }

        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &mut self.saga
        }
    }

    impl SagaParticipant for TestParticipant {
        type Error = String;

        fn step_name(&self) -> &str {
            "risk_check"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["order_lifecycle"]
        }

        fn depends_on(&self) -> DependencySpec {
            DependencySpec::OnSagaStart
        }

        fn execute_step(
            &mut self,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            self.called = true;
            Ok(StepOutput::Completed {
                output: vec![],
                compensation_data: vec![],
            })
        }

        fn compensate_step(
            &mut self,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<crate::CompensationOutput, CompensationError> {
            Ok(crate::CompensationOutput::Completed)
        }
    }

    #[test]
    fn drive_scenario_runs_on_saga_start() {
        let mut participant = TestParticipant::default();
        let ctx = DeterministicContextBuilder::default().build();
        drive_scenario(&mut participant, [saga_started(ctx, vec![1, 2, 3])]);
        assert!(participant.called);
    }
}
