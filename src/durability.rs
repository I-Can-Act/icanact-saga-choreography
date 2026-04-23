use std::path::{Path, PathBuf};

use crate::{
    AsyncSagaParticipant, DedupeError, HasSagaParticipantSupport, HasSagaWorkflowParticipants,
    JournalEntry, JournalError, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport,
    SagaStateEntry, SagaStateExt, SagaWorkflowParticipant, handle_async_saga_event_with_emit,
    handle_saga_event_with_emit,
};

pub const PANIC_QUARANTINE_REASON_PREFIX: &str = "panic_during_active_";
pub const PANIC_QUARANTINE_PUBLISH_KEY: &str = "panic_quarantine_published";
pub const DEFAULT_RECOVERY_SAGA_TYPE: &str = "default_workflow";

#[derive(Debug)]
pub enum RecoveryCollectionError {
    ListSagas(JournalError),
    ReadSaga {
        saga_id: SagaId,
        source: JournalError,
    },
    MarkDedupe {
        saga_id: SagaId,
        source: DedupeError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSagaExecutionPhase {
    StepExecution,
    CompensationExecution,
}

impl ActiveSagaExecutionPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StepExecution => "step_execution",
            Self::CompensationExecution => "compensation_execution",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActiveSagaExecution {
    pub context: SagaContext,
    pub phase: ActiveSagaExecutionPhase,
}

pub trait HasActiveSagaExecution {
    fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution>;
}

pub fn panic_quarantine_reason(phase: ActiveSagaExecutionPhase, message: &str) -> Box<str> {
    format!(
        "{PANIC_QUARANTINE_REASON_PREFIX}{}:{message}",
        phase.as_str()
    )
    .into_boxed_str()
}

pub fn is_panic_quarantine_reason(reason: &str) -> bool {
    reason.starts_with(PANIC_QUARANTINE_REASON_PREFIX)
}

pub fn panic_message_from_payload(payload: &(dyn std::any::Any + Send)) -> Box<str> {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).into()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str().into()
    } else {
        "panic (non-string payload)".into()
    }
}

pub fn panic_quarantine_reason_from_entries(entries: &[JournalEntry]) -> Option<Box<str>> {
    let last = entries.last()?;
    let ParticipantEvent::Quarantined { reason, .. } = &last.event else {
        return None;
    };
    if is_panic_quarantine_reason(reason.as_ref()) {
        Some(reason.clone())
    } else {
        None
    }
}

pub fn is_valid_emitted_transition(
    entry: Option<&SagaStateEntry>,
    event: &SagaChoreographyEvent,
) -> bool {
    match event {
        SagaChoreographyEvent::StepCompleted { .. } => {
            matches!(entry, Some(SagaStateEntry::Completed(_)))
        }
        SagaChoreographyEvent::StepFailed { .. } => {
            matches!(entry, Some(SagaStateEntry::Failed(_)))
        }
        SagaChoreographyEvent::CompensationCompleted { .. } => {
            matches!(entry, Some(SagaStateEntry::Compensated(_)))
        }
        SagaChoreographyEvent::CompensationFailed { .. }
        | SagaChoreographyEvent::SagaQuarantined { .. } => {
            matches!(entry, Some(SagaStateEntry::Quarantined(_)))
        }
        _ => true,
    }
}

pub fn apply_sync_participant_saga_ingress<P, FApplyTerminal, FOnInvalid>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    apply_terminal_side_effects: FApplyTerminal,
    on_invalid_transition: FOnInvalid,
) where
    P: SagaParticipant + SagaStateExt,
    FApplyTerminal: FnMut(&mut P, &SagaChoreographyEvent),
    FOnInvalid: FnMut(&SagaChoreographyEvent),
{
    apply_sync_participant_saga_ingress_with_hooks(
        participant,
        event,
        apply_terminal_side_effects,
        on_invalid_transition,
        |_participant, _event| {},
    );
}

pub fn apply_sync_participant_saga_ingress_with_hooks<P, FApplyTerminal, FOnInvalid, FOnEmitted>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut apply_terminal_side_effects: FApplyTerminal,
    mut on_invalid_transition: FOnInvalid,
    mut on_emitted_transition: FOnEmitted,
) where
    P: SagaParticipant + SagaStateExt,
    FApplyTerminal: FnMut(&mut P, &SagaChoreographyEvent),
    FOnInvalid: FnMut(&SagaChoreographyEvent),
    FOnEmitted: FnMut(&mut P, &SagaChoreographyEvent),
{
    apply_terminal_side_effects(participant, &event);

    let mut emitted = Vec::new();
    handle_saga_event_with_emit(participant, event, |next_event| emitted.push(next_event));

    let saga_bus = participant.saga_support().bus.clone();
    for next_event in emitted {
        if !is_valid_emitted_transition(
            participant
                .saga_states_ref()
                .get(&next_event.context().saga_id),
            &next_event,
        ) {
            on_invalid_transition(&next_event);
            continue;
        }

        on_emitted_transition(participant, &next_event);

        if let Some(bus) = &saga_bus {
            if let Err(err) = bus.publish_strict(next_event) {
                tracing::error!(
                    target: "core::saga",
                    event = "participant_ingress_emit_publish_failed",
                    error = ?err
                );
            }
        }
    }
}

fn workflow_for_event<A>(
    event: &SagaChoreographyEvent,
) -> Result<Option<&'static dyn SagaWorkflowParticipant<A>>, String>
where
    A: HasSagaWorkflowParticipants,
{
    let saga_type = event.context().saga_type.as_ref();
    let mut matches = A::saga_workflows()
        .iter()
        .copied()
        .filter(|workflow| workflow.saga_types().contains(&saga_type));
    let selected = matches.next();
    if matches.next().is_some() {
        return Err(format!(
            "ambiguous workflow registration for saga_type={} on actor type={}",
            saga_type,
            std::any::type_name::<A>()
        ));
    }
    Ok(selected)
}

pub fn apply_sync_workflow_participant_saga_ingress<A, FApplyTerminal, FOnInvalid>(
    actor: &mut A,
    event: SagaChoreographyEvent,
    apply_terminal_side_effects: FApplyTerminal,
    on_invalid_transition: FOnInvalid,
) where
    A: HasSagaParticipantSupport + HasSagaWorkflowParticipants + Send + 'static,
    FApplyTerminal: FnMut(&mut A, &SagaChoreographyEvent),
    FOnInvalid: FnMut(&SagaChoreographyEvent),
{
    apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        apply_terminal_side_effects,
        on_invalid_transition,
        |_actor, _event| {},
    );
}

pub fn apply_sync_workflow_participant_saga_ingress_with_hooks<
    A,
    FApplyTerminal,
    FOnInvalid,
    FOnEmitted,
>(
    actor: &mut A,
    event: SagaChoreographyEvent,
    mut apply_terminal_side_effects: FApplyTerminal,
    mut on_invalid_transition: FOnInvalid,
    mut on_emitted_transition: FOnEmitted,
) where
    A: HasSagaParticipantSupport + HasSagaWorkflowParticipants + Send + 'static,
    FApplyTerminal: FnMut(&mut A, &SagaChoreographyEvent),
    FOnInvalid: FnMut(&SagaChoreographyEvent),
    FOnEmitted: FnMut(&mut A, &SagaChoreographyEvent),
{
    let workflow = match workflow_for_event::<A>(&event) {
        Ok(Some(workflow)) => workflow,
        Ok(None) => return,
        Err(err) => {
            let framework_failure = SagaChoreographyEvent::SagaFailed {
                context: event
                    .context()
                    .next_step(crate::TERMINAL_RESOLVER_STEP.into()),
                reason: format!("workflow_participant_resolution_failed: {err}").into(),
                failure: None,
            };
            on_invalid_transition(&framework_failure);
            if let Some(bus) = actor.saga_support().bus.as_ref() {
                if let Err(publish_err) = bus.publish_strict(framework_failure) {
                    tracing::error!(
                        target: "core::saga",
                        event = "workflow_resolution_failure_publish_failed",
                        error = ?publish_err
                    );
                }
            }
            return;
        }
    };

    apply_terminal_side_effects(actor, &event);

    let mut emitted = Vec::new();
    handle_workflow_saga_event_with_emit(actor, workflow, event, |next_event| {
        emitted.push(next_event)
    });

    let saga_bus = actor.saga_support().bus.clone();
    for next_event in emitted {
        if !is_valid_emitted_transition(
            actor.saga_states_ref().get(&next_event.context().saga_id),
            &next_event,
        ) {
            on_invalid_transition(&next_event);
            continue;
        }

        on_emitted_transition(actor, &next_event);

        if let Some(bus) = &saga_bus {
            if let Err(err) = bus.publish_strict(next_event) {
                tracing::error!(
                    target: "core::saga",
                    event = "workflow_participant_ingress_emit_publish_failed",
                    error = ?err
                );
            }
        }
    }
}

fn handle_workflow_saga_event_with_emit<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    event: SagaChoreographyEvent,
    mut emit: F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let context = event.context().clone();
    let now = actor.now_millis();

    if !workflow
        .saga_types()
        .iter()
        .any(|t| *t == context.saga_type.as_ref())
    {
        return;
    }

    let is_saga_started = matches!(event, SagaChoreographyEvent::SagaStarted { .. });
    if !is_saga_started && actor.is_terminal_saga_latched(context.saga_id) {
        return;
    }

    let dedupe_key = workflow_dedupe_key_for_event(&event);
    if !actor.check_dedupe(context.saga_id, &dedupe_key) {
        return;
    }

    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. }
            if workflow.depends_on().is_on_saga_start() =>
        {
            actor.unlatch_terminal_saga(context.saga_id);
            actor.saga_states().remove(&context.saga_id);
            actor.dependency_completions().remove(&context.saga_id);
            actor.dependency_fired().remove(&context.saga_id);
            execute_workflow_step_with_emit(
                actor,
                workflow,
                context.clone(),
                payload,
                now,
                &mut emit,
            );
        }
        SagaChoreographyEvent::SagaStarted { .. } => {
            actor.unlatch_terminal_saga(context.saga_id);
            actor.saga_states().remove(&context.saga_id);
            actor.dependency_completions().remove(&context.saga_id);
            actor.dependency_fired().remove(&context.saga_id);
        }
        SagaChoreographyEvent::StepCompleted {
            context: step_ctx,
            output,
            saga_input,
            ..
        } => {
            let dependency_spec = workflow.depends_on();
            let should_fire = workflow_dependency_should_fire(
                actor,
                context.saga_id,
                &dependency_spec,
                &step_ctx.step_name,
            );
            if should_fire {
                let next_context = context.next_step(workflow.step_name().into());
                let input = if dependency_spec.prefers_original_saga_input() {
                    saga_input
                } else {
                    output
                };
                execute_workflow_step_with_emit(
                    actor,
                    workflow,
                    next_context,
                    input,
                    now,
                    &mut emit,
                );
            }
        }
        SagaChoreographyEvent::CompensationRequested {
            steps_to_compensate,
            ..
        } => {
            if steps_to_compensate.contains(&workflow.step_name().into()) {
                compensate_workflow_with_emit(actor, workflow, &context, now, &mut emit);
            }
        }
        SagaChoreographyEvent::SagaCompleted { .. } => {
            actor.latch_terminal_saga(context.saga_id);
            workflow.on_saga_completed(actor, &context);
            actor.prune_saga(context.saga_id);
        }
        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            actor.latch_terminal_saga(context.saga_id);
            workflow.on_saga_failed(actor, &context, &reason);
            actor.prune_saga(context.saga_id);
        }
        SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
            actor.latch_terminal_saga(context.saga_id);
            workflow.on_quarantined(actor, &context, &reason);
            actor.prune_saga(context.saga_id);
        }
        _ => {}
    }
}

fn workflow_dependency_should_fire<A>(
    actor: &mut A,
    saga_id: SagaId,
    dependency_spec: &crate::DependencySpec,
    completed_step: &str,
) -> bool
where
    A: HasSagaParticipantSupport,
{
    match dependency_spec {
        crate::DependencySpec::OnSagaStart => false,
        crate::DependencySpec::After(step) => {
            if completed_step != *step {
                return false;
            }
            actor.dependency_fired().insert(saga_id)
        }
        crate::DependencySpec::AnyOf(steps) => {
            if !steps.contains(&completed_step) {
                return false;
            }
            actor.dependency_fired().insert(saga_id)
        }
        crate::DependencySpec::AllOf(steps) => {
            if !steps.contains(&completed_step) {
                return false;
            }
            {
                let seen = actor.dependency_completions().entry(saga_id).or_default();
                seen.insert(completed_step.into());
                if !steps.iter().all(|step| seen.contains(*step)) {
                    return false;
                }
            }
            actor.dependency_fired().insert(saga_id)
        }
    }
}

fn workflow_dedupe_key_for_event(event: &SagaChoreographyEvent) -> String {
    let context = event.context();
    match event {
        SagaChoreographyEvent::CompensationRequested { failed_step, .. } => format!(
            "{}:{}:{}:{}:{}",
            context.trace_id,
            context.saga_started_at_millis,
            event.event_type(),
            context.step_name,
            failed_step
        ),
        _ => format!(
            "{}:{}:{}:{}",
            context.trace_id,
            context.saga_started_at_millis,
            event.event_type(),
            context.step_name
        ),
    }
}

fn execute_workflow_step_with_emit<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: SagaContext,
    input: Vec<u8>,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let state = crate::SagaParticipantState::new(
        saga_id,
        context.saga_type.clone(),
        workflow.step_name().into(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("dependency_satisfied", now)
    .start_execution(now);

    actor.record_event(
        saga_id,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );
    actor
        .saga_states()
        .insert(saga_id, SagaStateEntry::Executing(state));

    match workflow.execute_step(actor, &context, &input) {
        Ok(output) => complete_workflow_step(actor, workflow, &context, input, output, now, emit),
        Err(error) => fail_workflow_step(actor, workflow, &context, error, now, emit),
    }
}

fn complete_workflow_step<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    saga_input: Vec<u8>,
    output: crate::StepOutput,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let (out_data, comp_data, compensation_available) = match output {
        crate::StepOutput::Completed {
            output,
            compensation_data,
        } => {
            let compensation_available = !compensation_data.is_empty();
            (output, compensation_data, compensation_available)
        }
        crate::StepOutput::CompletedWithEffect {
            output,
            compensation_data,
            ..
        } => {
            let compensation_available = !compensation_data.is_empty();
            (output, compensation_data, compensation_available)
        }
    };

    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
        let new_state = state.complete(out_data.clone(), comp_data, now);
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Completed(new_state));
    }

    let emitted_output = out_data.clone();
    actor.record_event(
        saga_id,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data,
            compensation_data: vec![],
            completed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepCompleted {
        context: context.next_step(workflow.step_name().into()),
        output: emitted_output,
        saga_input,
        compensation_available,
    });
}

fn fail_workflow_step<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    error: crate::StepError,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let (reason, requires_comp) = match error {
        crate::StepError::Terminal { reason } => (reason, false),
        crate::StepError::RequireCompensation { reason } => (reason, true),
    };

    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
        let new_state = state.fail(reason.clone(), requires_comp, now);
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Failed(new_state));
    }

    actor.record_event(
        saga_id,
        ParticipantEvent::StepExecutionFailed {
            error: reason.clone(),
            requires_compensation: requires_comp,
            failed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepFailed {
        context: context.next_step(workflow.step_name().into()),
        participant_id: workflow.participant_id_owned(),
        error_code: None,
        error: reason,
        requires_compensation: requires_comp,
    });
}

fn compensate_workflow_with_emit<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;

    if let Some(SagaStateEntry::Completed(state)) = actor.saga_states().remove(&saga_id) {
        let comp_data = state.state.compensation_data.clone();
        let new_state = state.start_compensation(now);
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Compensating(new_state));

        actor.record_event(
            saga_id,
            ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: now,
            },
        );

        match workflow.compensate_step(actor, context, &comp_data) {
            Ok(()) => complete_workflow_compensation(actor, workflow, context, now, emit),
            Err(error) => fail_workflow_compensation(actor, workflow, context, error, now, emit),
        }
    }
}

fn complete_workflow_compensation<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;

    if let Some(SagaStateEntry::Compensating(state)) = actor.saga_states().remove(&saga_id) {
        let new_state = state.complete_compensation(now);
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Compensated(new_state));
    }

    actor.record_event(
        saga_id,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::CompensationCompleted {
        context: context.next_step(workflow.step_name().into()),
    });

    workflow.on_compensation_completed(actor, context);
}

fn fail_workflow_compensation<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    error: crate::CompensationError,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let (reason, is_ambiguous) = match error {
        crate::CompensationError::SafeToRetry { reason } => (reason, false),
        crate::CompensationError::Ambiguous { reason } => (reason, true),
        crate::CompensationError::Terminal { reason } => (reason, false),
    };

    if let Some(SagaStateEntry::Compensating(state)) = actor.saga_states().remove(&saga_id) {
        let new_state = state.quarantine(reason.clone(), now);
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Quarantined(new_state));
    }

    actor.record_event(
        saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );

    let event_context = context.next_step(workflow.step_name().into());
    emit(SagaChoreographyEvent::CompensationFailed {
        context: event_context.clone(),
        participant_id: workflow.participant_id_owned(),
        error: reason.clone(),
        is_ambiguous,
    });
    if is_ambiguous {
        emit(SagaChoreographyEvent::SagaQuarantined {
            context: event_context,
            reason: reason.clone(),
            step: workflow.step_name().into(),
            participant_id: workflow.participant_id_owned(),
        });
    }

    workflow.on_quarantined(actor, context, &reason);
}

pub async fn apply_async_participant_saga_ingress<P, FApplyTerminal, FOnInvalid>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    apply_terminal_side_effects: FApplyTerminal,
    on_invalid_transition: FOnInvalid,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    FApplyTerminal: FnMut(&mut P, &SagaChoreographyEvent),
    FOnInvalid: FnMut(&SagaChoreographyEvent),
{
    apply_async_participant_saga_ingress_with_hooks(
        participant,
        event,
        apply_terminal_side_effects,
        on_invalid_transition,
        |_participant, _event| {},
    )
    .await;
}

pub async fn apply_async_participant_saga_ingress_with_hooks<
    P,
    FApplyTerminal,
    FOnInvalid,
    FOnEmitted,
>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut apply_terminal_side_effects: FApplyTerminal,
    mut on_invalid_transition: FOnInvalid,
    mut on_emitted_transition: FOnEmitted,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    FApplyTerminal: FnMut(&mut P, &SagaChoreographyEvent),
    FOnInvalid: FnMut(&SagaChoreographyEvent),
    FOnEmitted: FnMut(&mut P, &SagaChoreographyEvent),
{
    apply_terminal_side_effects(participant, &event);

    let mut emitted = Vec::new();
    handle_async_saga_event_with_emit(participant, event, |next_event| emitted.push(next_event))
        .await;

    let saga_bus = participant.saga_support().bus.clone();
    for next_event in emitted {
        if !is_valid_emitted_transition(
            participant
                .saga_states_ref()
                .get(&next_event.context().saga_id),
            &next_event,
        ) {
            on_invalid_transition(&next_event);
            continue;
        }

        on_emitted_transition(participant, &next_event);

        if let Some(bus) = &saga_bus {
            if let Err(err) = bus.publish_strict(next_event) {
                tracing::error!(
                    target: "core::saga",
                    event = "async_participant_ingress_emit_publish_failed",
                    error = ?err
                );
            }
        }
    }
}

pub fn run_participant_phase_with_panic_quarantine<A, R, F>(
    actor: &mut A,
    context: &SagaContext,
    phase: ActiveSagaExecutionPhase,
    run: F,
) -> R
where
    A: SagaParticipant + HasSagaParticipantSupport + HasActiveSagaExecution,
    F: FnOnce(&mut A) -> R,
{
    *actor.active_saga_execution_slot() = Some(ActiveSagaExecution {
        context: context.clone(),
        phase,
    });

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(actor)));

    *actor.active_saga_execution_slot() = None;

    match result {
        Ok(out) => out,
        Err(panic_payload) => {
            let step_name = actor.step_name().to_string();
            let participant_id = actor.participant_id_owned();
            publish_active_saga_panic_quarantine(
                actor.saga_support_mut(),
                context,
                phase,
                panic_payload.as_ref(),
                step_name.as_str(),
                participant_id,
            );
            std::panic::resume_unwind(panic_payload);
        }
    }
}

pub fn run_workflow_participant_phase_with_panic_quarantine<A, W, R, F>(
    actor: &mut A,
    workflow: &'static W,
    context: &SagaContext,
    phase: ActiveSagaExecutionPhase,
    run: F,
) -> R
where
    A: HasSagaParticipantSupport + HasActiveSagaExecution,
    W: SagaWorkflowParticipant<A>,
    F: FnOnce(&mut A) -> R,
{
    *actor.active_saga_execution_slot() = Some(ActiveSagaExecution {
        context: context.clone(),
        phase,
    });

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(actor)));

    *actor.active_saga_execution_slot() = None;

    match result {
        Ok(out) => out,
        Err(panic_payload) => {
            publish_active_saga_panic_quarantine(
                actor.saga_support_mut(),
                context,
                phase,
                panic_payload.as_ref(),
                workflow.step_name(),
                workflow.participant_id_owned(),
            );
            std::panic::resume_unwind(panic_payload);
        }
    }
}

pub fn publish_active_saga_panic_quarantine<J, D>(
    saga: &mut SagaParticipantSupport<J, D>,
    context: &SagaContext,
    phase: ActiveSagaExecutionPhase,
    panic_payload: &(dyn std::any::Any + Send),
    step_name: &str,
    participant_id: Box<str>,
) where
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
{
    let message = panic_message_from_payload(panic_payload);
    let reason = panic_quarantine_reason(phase, message.as_ref());
    let now = SagaContext::now_millis();

    if let Err(err) = saga.journal.append(
        context.saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    ) {
        tracing::error!(
            target: "core::saga",
            event = "panic_quarantine_journal_append_failed",
            saga_id = context.saga_id.get(),
            error = %err
        );
    }

    let emitted = SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(step_name.into()),
        reason,
        step: step_name.to_string().into_boxed_str(),
        participant_id,
    };

    if let Some(bus) = &saga.bus {
        match bus.publish_strict(emitted) {
            Ok(stats) => {
                if stats.delivered > 0 {
                    if let Err(err) = saga
                        .dedupe
                        .mark_processed(context.saga_id, PANIC_QUARANTINE_PUBLISH_KEY)
                    {
                        tracing::error!(
                            target: "core::saga",
                            event = "panic_quarantine_dedupe_mark_failed",
                            saga_id = context.saga_id.get(),
                            error = %err
                        );
                    }
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "core::saga",
                    event = "panic_quarantine_publish_failed",
                    saga_id = context.saga_id.get(),
                    error = ?err
                );
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDecision {
    Continue,
    QuarantineStale,
    ReplayPanicQuarantine,
    TerminalNoAction,
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryPolicy {
    pub stale_after_ms: u64,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        let stale_after_ms = match std::env::var("SAGA_RECOVERY_MAX_AGE_MS") {
            Ok(raw) => match raw.parse::<u64>() {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(
                        target: "core::saga",
                        event = "saga_recovery_max_age_parse_failed",
                        env = "SAGA_RECOVERY_MAX_AGE_MS",
                        value = %raw,
                        error = %err
                    );
                    5 * 60 * 1000
                }
            },
            Err(_) => 5 * 60 * 1000,
        };
        Self { stale_after_ms }
    }
}

pub fn classify_recovery(
    entries: &[JournalEntry],
    now_ms: u64,
    policy: RecoveryPolicy,
) -> RecoveryDecision {
    let Some(last) = entries.last() else {
        return RecoveryDecision::TerminalNoAction;
    };
    if matches!(
        &last.event,
        ParticipantEvent::Quarantined { reason, .. } if is_panic_quarantine_reason(reason.as_ref())
    ) {
        return RecoveryDecision::ReplayPanicQuarantine;
    }
    let terminal = matches!(
        last.event,
        ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::StepExecutionFailed {
                requires_compensation: false,
                ..
            }
    );
    if terminal {
        return RecoveryDecision::TerminalNoAction;
    }
    let age = now_ms.saturating_sub(last.recorded_at_millis);
    if age > policy.stale_after_ms {
        RecoveryDecision::QuarantineStale
    } else {
        RecoveryDecision::Continue
    }
}

pub fn collect_startup_recovery_events<J: ParticipantJournal, D: ParticipantDedupeStore>(
    journal: &J,
    dedupe: &D,
    step_name: &'static str,
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    collect_startup_recovery_events_for_saga_type(
        journal,
        dedupe,
        step_name,
        DEFAULT_RECOVERY_SAGA_TYPE,
    )
}

pub fn collect_startup_recovery_events_for_saga_type<
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
>(
    journal: &J,
    dedupe: &D,
    step_name: &'static str,
    saga_type: &'static str,
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    let mut out = Vec::new();
    let policy = RecoveryPolicy::default();
    let now = SagaContext::now_millis();
    let saga_ids = match journal.list_sagas() {
        Ok(ids) => ids,
        Err(err) => return Err(RecoveryCollectionError::ListSagas(err)),
    };
    for saga_id in saga_ids {
        let entries = match journal.read(saga_id) {
            Ok(entries) => entries,
            Err(err) => {
                return Err(RecoveryCollectionError::ReadSaga {
                    saga_id,
                    source: err,
                });
            }
        };
        if entries.is_empty() {
            continue;
        }
        match classify_recovery(&entries, now, policy) {
            RecoveryDecision::QuarantineStale => {
                out.push(SagaChoreographyEvent::saga_failed_default(
                    recovery_context_for_saga_type(saga_id, step_name, saga_type),
                    Box::<str>::from("startup recovery quarantined stale saga"),
                ));
            }
            RecoveryDecision::ReplayPanicQuarantine => {
                let should_emit = match dedupe.check_and_mark(saga_id, PANIC_QUARANTINE_PUBLISH_KEY)
                {
                    Ok(value) => value,
                    Err(err) => {
                        return Err(RecoveryCollectionError::MarkDedupe {
                            saga_id,
                            source: err,
                        });
                    }
                };
                if should_emit {
                    let reason = panic_quarantine_reason_from_entries(&entries)
                        .unwrap_or_else(|| Box::<str>::from("panic quarantined during execution"));
                    out.push(SagaChoreographyEvent::SagaQuarantined {
                        context: recovery_context_for_saga_type(saga_id, step_name, saga_type),
                        reason,
                        step: step_name.into(),
                        participant_id: step_name.into(),
                    });
                }
            }
            RecoveryDecision::Continue | RecoveryDecision::TerminalNoAction => {}
        }
    }
    Ok(out)
}

fn recovery_context_for_saga_type(
    saga_id: SagaId,
    step_name: &'static str,
    saga_type: &'static str,
) -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id,
        saga_type: saga_type.into(),
        step_name: step_name.into(),
        correlation_id: saga_id.get(),
        causation_id: saga_id.get(),
        trace_id: saga_id.get(),
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

pub fn default_runtime_dir(var: &str, fallback: &str) -> PathBuf {
    if let Ok(value) = std::env::var(var) {
        return PathBuf::from(value);
    }
    if cfg!(test) {
        use std::sync::OnceLock;
        static TEST_RUNTIME_DIR: OnceLock<PathBuf> = OnceLock::new();
        return TEST_RUNTIME_DIR
            .get_or_init(|| {
                let nanos = SagaContext::now_millis();
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("target")
                    .join("test-tmp")
                    .join(format!("saga-runtime-{}-{nanos}", std::process::id()))
            })
            .clone();
    }
    PathBuf::from(fallback)
}

pub trait SagaLmdbBackedActor: Sized {
    fn open_with_saga_lmdb_base(
        actor_id: &'static str,
        saga_lmdb_base: &Path,
    ) -> Result<Self, String>;
}

pub fn open_saga_lmdb_actor<A: SagaLmdbBackedActor>(
    actor_id: &'static str,
    saga_lmdb_base: &Path,
) -> Result<A, String> {
    A::open_with_saga_lmdb_base(actor_id, saga_lmdb_base)
}

#[cfg(feature = "lmdb")]
pub mod lmdb {
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use heed::types::{Bytes, Str};
    use heed::{Database, Env, EnvOpenOptions};

    use super::{DEFAULT_RECOVERY_SAGA_TYPE, collect_startup_recovery_events_for_saga_type};
    use crate::{
        DedupeError, JournalEntry, JournalError, ParticipantDedupeStore, ParticipantEvent,
        ParticipantJournal, SagaId, SagaParticipantSupport,
    };

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn key_saga_seq(saga_id: SagaId, seq: u64) -> String {
        format!("{:020}:{:020}", saga_id.get(), seq)
    }

    fn key_saga_prefix(saga_id: SagaId) -> String {
        format!("{:020}:", saga_id.get())
    }

    #[derive(Debug)]
    pub struct LmdbJournal {
        env: Env,
        rows: Database<Str, Bytes>,
        saga_index: Database<Str, Str>,
        meta: Database<Str, Str>,
    }

    impl LmdbJournal {
        pub fn open(path: &Path) -> Result<Self, JournalError> {
            std::fs::create_dir_all(path)
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let env = unsafe { EnvOpenOptions::new().max_dbs(16).open(path) }
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let mut wtxn = env
                .write_txn()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let rows = env
                .create_database::<Str, Bytes>(&mut wtxn, Some("journal_rows"))
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let saga_index = env
                .create_database::<Str, Str>(&mut wtxn, Some("journal_saga_index"))
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let meta = env
                .create_database::<Str, Str>(&mut wtxn, Some("journal_meta"))
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            wtxn.commit()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            Ok(Self {
                env,
                rows,
                saga_index,
                meta,
            })
        }

        fn next_sequence(
            meta: &Database<Str, Str>,
            wtxn: &mut heed::RwTxn<'_>,
        ) -> Result<u64, JournalError> {
            let next = meta
                .get(wtxn, "next_sequence")
                .map_err(|err| JournalError::Storage(err.to_string().into()))?
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(1);
            let after = next.saturating_add(1);
            meta.put(wtxn, "next_sequence", &after.to_string())
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            Ok(next)
        }
    }

    impl ParticipantJournal for LmdbJournal {
        fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let sequence = Self::next_sequence(&self.meta, &mut wtxn)?;
            let entry = JournalEntry {
                sequence,
                recorded_at_millis: now_millis(),
                event,
            };
            let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&entry)
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            self.rows
                .put(
                    &mut wtxn,
                    &key_saga_seq(saga_id, sequence),
                    encoded.as_ref(),
                )
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            self.saga_index
                .put(&mut wtxn, &format!("{:020}", saga_id.get()), "1")
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            wtxn.commit()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            Ok(sequence)
        }

        fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
            let rtxn = self
                .env
                .read_txn()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let prefix = key_saga_prefix(saga_id);
            let mut entries = Vec::new();
            let iter = self
                .rows
                .prefix_iter(&rtxn, &prefix)
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            for row in iter {
                let (_, v) = row.map_err(|err| JournalError::Storage(err.to_string().into()))?;
                let owned = v.to_vec();
                let decoded: JournalEntry =
                    rkyv::from_bytes::<JournalEntry, rkyv::rancor::Error>(&owned)
                        .map_err(|err| JournalError::Storage(err.to_string().into()))?;
                entries.push(decoded);
            }
            entries.sort_by_key(|e| e.sequence);
            Ok(entries)
        }

        fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
            let rtxn = self
                .env
                .read_txn()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let mut out = Vec::new();
            let iter = self
                .saga_index
                .iter(&rtxn)
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            for row in iter {
                let (k, _) = row.map_err(|err| JournalError::Storage(err.to_string().into()))?;
                if let Ok(id) = k.parse::<u64>() {
                    out.push(SagaId::new(id));
                }
            }
            out.sort_by_key(|id| id.get());
            Ok(out)
        }
    }

    #[derive(Debug)]
    pub struct LmdbDedupe {
        env: Env,
        entries: Database<Str, Str>,
    }

    impl LmdbDedupe {
        pub fn open(path: &Path) -> Result<Self, DedupeError> {
            std::fs::create_dir_all(path)
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            let env = unsafe { EnvOpenOptions::new().max_dbs(8).open(path) }
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            let mut wtxn = env
                .write_txn()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            let entries = env
                .create_database::<Str, Str>(&mut wtxn, Some("dedupe_entries"))
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            wtxn.commit()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            Ok(Self { env, entries })
        }

        fn key(saga_id: SagaId, key: &str) -> String {
            format!("{:020}:{key}", saga_id.get())
        }
    }

    impl ParticipantDedupeStore for LmdbDedupe {
        fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
            let full_key = Self::key(saga_id, key);
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            if self
                .entries
                .get(&wtxn, &full_key)
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?
                .is_some()
            {
                return Ok(false);
            }
            self.entries
                .put(&mut wtxn, &full_key, "1")
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            wtxn.commit()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            Ok(true)
        }

        fn contains(&self, saga_id: SagaId, key: &str) -> bool {
            let Ok(rtxn) = self.env.read_txn() else {
                return false;
            };
            self.entries
                .get(&rtxn, &Self::key(saga_id, key))
                .map(|v| v.is_some())
                .unwrap_or(false)
        }

        fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            self.entries
                .put(&mut wtxn, &Self::key(saga_id, key), "1")
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            wtxn.commit()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            Ok(())
        }

        fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError> {
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            let prefix = key_saga_prefix(saga_id);
            let mut iter = self
                .entries
                .prefix_iter_mut(&mut wtxn, &prefix)
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            while iter.next().is_some() {
                unsafe { iter.del_current() }
                    .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            }
            drop(iter);
            wtxn.commit()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            Ok(())
        }
    }

    pub fn open_lmdb_participant_support(
        base: &Path,
        step_name: &'static str,
    ) -> Result<SagaParticipantSupport<LmdbJournal, LmdbDedupe>, String> {
        open_lmdb_participant_support_for_saga_type(base, step_name, DEFAULT_RECOVERY_SAGA_TYPE)
    }

    pub fn open_lmdb_participant_support_for_saga_type(
        base: &Path,
        step_name: &'static str,
        saga_type: &'static str,
    ) -> Result<SagaParticipantSupport<LmdbJournal, LmdbDedupe>, String> {
        let journal = LmdbJournal::open(&base.join("journal")).map_err(|err| err.to_string())?;
        let dedupe = LmdbDedupe::open(&base.join("dedupe")).map_err(|err| err.to_string())?;
        let startup_recovery_events =
            collect_startup_recovery_events_for_saga_type(&journal, &dedupe, step_name, saga_type)
                .map_err(|err| format!("startup recovery collection failed: {err:?}"))?;
        Ok(SagaParticipantSupport::new(journal, dedupe)
            .with_startup_recovery_events(startup_recovery_events))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn contains_returns_false_when_reader_slots_are_exhausted() {
            let temp = tempfile::tempdir().expect("tempdir should open");
            let path = temp.path().join("lmdb-dedupe-readers");
            std::fs::create_dir_all(&path).expect("lmdb path should be created");

            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(8)
                    .max_readers(1)
                    .open(&path)
                    .expect("env should open")
            };
            let mut wtxn = env.write_txn().expect("write txn should open");
            let entries = env
                .create_database::<Str, Str>(&mut wtxn, Some("dedupe_entries"))
                .expect("dedupe database should be created");
            wtxn.commit().expect("setup write txn should commit");

            let dedupe = LmdbDedupe {
                env: env.clone(),
                entries,
            };
            let saga_id = SagaId::new(404);
            dedupe
                .mark_processed(saga_id, "probe")
                .expect("mark_processed should succeed");

            let held_reader = env.read_txn().expect("held reader should open");
            assert!(
                !dedupe.contains(saga_id, "probe"),
                "contains should return false when read_txn cannot reserve a reader slot"
            );
            drop(held_reader);

            assert!(
                dedupe.contains(saga_id, "probe"),
                "contains should recover once reader slot pressure is released"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ActiveSagaExecution, HasActiveSagaExecution, apply_sync_workflow_participant_saga_ingress,
        default_runtime_dir, workflow_for_event,
    };
    use crate::{
        DependencySpec, DeterministicContextBuilder, HasSagaParticipantSupport,
        HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal, SagaParticipantSupport,
        SagaWorkflowParticipant, StepOutput,
    };

    struct WorkflowTestActor {
        saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
        active: Option<ActiveSagaExecution>,
        alpha_calls: usize,
        beta_calls: usize,
    }

    impl Default for WorkflowTestActor {
        fn default() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
                active: None,
                alpha_calls: 0,
                beta_calls: 0,
            }
        }
    }

    impl HasSagaParticipantSupport for WorkflowTestActor {
        type Journal = InMemoryJournal;
        type Dedupe = InMemoryDedupe;

        fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &self.saga
        }

        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &mut self.saga
        }
    }

    impl HasActiveSagaExecution for WorkflowTestActor {
        fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
            &mut self.active
        }
    }

    struct AlphaWorkflow;
    struct BetaWorkflow;

    static ALPHA_WORKFLOW: AlphaWorkflow = AlphaWorkflow;
    static BETA_WORKFLOW: BetaWorkflow = BetaWorkflow;
    static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<WorkflowTestActor>; 2] =
        [&ALPHA_WORKFLOW, &BETA_WORKFLOW];

    impl SagaWorkflowParticipant<WorkflowTestActor> for AlphaWorkflow {
        fn step_name(&self) -> &'static str {
            "alpha_step"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["alpha_workflow"]
        }

        fn execute_step(
            &self,
            actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _input: &[u8],
        ) -> Result<crate::StepOutput, crate::StepError> {
            actor.alpha_calls += 1;
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), crate::CompensationError> {
            Ok(())
        }
    }

    impl SagaWorkflowParticipant<WorkflowTestActor> for BetaWorkflow {
        fn step_name(&self) -> &'static str {
            "beta_step"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["beta_workflow"]
        }

        fn depends_on(&self) -> DependencySpec {
            DependencySpec::OnSagaStart
        }

        fn execute_step(
            &self,
            actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _input: &[u8],
        ) -> Result<crate::StepOutput, crate::StepError> {
            actor.beta_calls += 1;
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), crate::CompensationError> {
            Ok(())
        }
    }

    impl HasSagaWorkflowParticipants for WorkflowTestActor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &WORKFLOWS
        }
    }

    struct DuplicateWorkflowTestActor;

    struct DuplicateWorkflowAlpha;
    struct DuplicateWorkflowBeta;

    static DUPLICATE_ALPHA_WORKFLOW: DuplicateWorkflowAlpha = DuplicateWorkflowAlpha;
    static DUPLICATE_BETA_WORKFLOW: DuplicateWorkflowBeta = DuplicateWorkflowBeta;
    static DUPLICATE_WORKFLOWS: [&'static dyn SagaWorkflowParticipant<DuplicateWorkflowTestActor>;
        2] = [&DUPLICATE_ALPHA_WORKFLOW, &DUPLICATE_BETA_WORKFLOW];

    impl SagaWorkflowParticipant<DuplicateWorkflowTestActor> for DuplicateWorkflowAlpha {
        fn step_name(&self) -> &'static str {
            "alpha"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["shared_workflow"]
        }

        fn execute_step(
            &self,
            _actor: &mut DuplicateWorkflowTestActor,
            _context: &crate::SagaContext,
            _input: &[u8],
        ) -> Result<crate::StepOutput, crate::StepError> {
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut DuplicateWorkflowTestActor,
            _context: &crate::SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), crate::CompensationError> {
            Ok(())
        }
    }

    impl SagaWorkflowParticipant<DuplicateWorkflowTestActor> for DuplicateWorkflowBeta {
        fn step_name(&self) -> &'static str {
            "beta"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["shared_workflow"]
        }

        fn execute_step(
            &self,
            _actor: &mut DuplicateWorkflowTestActor,
            _context: &crate::SagaContext,
            _input: &[u8],
        ) -> Result<crate::StepOutput, crate::StepError> {
            Ok(StepOutput::Completed {
                output: Vec::new(),
                compensation_data: Vec::new(),
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut DuplicateWorkflowTestActor,
            _context: &crate::SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), crate::CompensationError> {
            Ok(())
        }
    }

    impl HasSagaWorkflowParticipants for DuplicateWorkflowTestActor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &DUPLICATE_WORKFLOWS
        }
    }

    #[test]
    fn default_runtime_dir_uses_test_tmp_path_when_env_is_missing() {
        let key = "SAGA_DURABILITY_UNITTEST_RUNTIME_DIR";
        std::env::remove_var(key);
        let runtime_dir = default_runtime_dir(key, "fallback-runtime");
        assert!(
            runtime_dir.to_string_lossy().contains("target/test-tmp"),
            "expected test runtime dir under target/test-tmp, got {:?}",
            runtime_dir
        );
    }

    #[test]
    fn workflow_ingress_routes_to_matching_workflow_spec() {
        let mut actor = WorkflowTestActor::default();
        let event = crate::SagaChoreographyEvent::SagaStarted {
            context: DeterministicContextBuilder::default()
                .with_saga_id(77)
                .with_saga_type("beta_workflow")
                .with_step_name("beta_step")
                .build(),
            payload: Vec::new(),
        };

        apply_sync_workflow_participant_saga_ingress(
            &mut actor,
            event,
            |_actor, _event| {},
            |_| {},
        );

        assert_eq!(actor.alpha_calls, 0);
        assert_eq!(actor.beta_calls, 1);
    }

    #[test]
    fn workflow_lookup_rejects_duplicate_saga_type_registration() {
        let event = crate::SagaChoreographyEvent::SagaStarted {
            context: DeterministicContextBuilder::default()
                .with_saga_type("shared_workflow")
                .build(),
            payload: Vec::new(),
        };

        let err = match workflow_for_event::<DuplicateWorkflowTestActor>(&event) {
            Ok(_) => panic!("duplicate workflow registration should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.contains("ambiguous workflow registration"),
            "unexpected error: {err}"
        );
    }
}
