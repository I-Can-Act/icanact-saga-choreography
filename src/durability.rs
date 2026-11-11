use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::state_ext::SagaStateStoreError;
use crate::support::{AcceptedWorkflowCompensation, AcceptedWorkflowStep};
use crate::{
    AcceptedCompensationCompletion, AcceptedCompensationFailure, AcceptedStepCompletion,
    AcceptedStepError, AcceptedStepFailure, AcceptedStepPolicy, AcceptedStepTimeoutOutcome,
    AsyncSagaParticipant, HasSagaParticipantSupport, HasSagaWorkflowParticipants, JournalEntry,
    JournalError, ParticipantAdmission, ParticipantEvent, ParticipantForwardOutcome,
    ParticipantStore, ParticipantTerminalKind, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt, SagaWorkflowParticipant,
    StepExecutionId,
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
        source: JournalError,
    },
    MissingCompensationState {
        saga_id: SagaId,
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
        SagaChoreographyEvent::StepAccepted { .. } => {
            matches!(
                entry,
                Some(
                    SagaStateEntry::Executing(_)
                        | SagaStateEntry::Completed(_)
                        | SagaStateEntry::Failed(_)
                        | SagaStateEntry::Quarantined(_)
                )
            )
        }
        SagaChoreographyEvent::StepCompleted { .. } => {
            matches!(entry, Some(SagaStateEntry::Completed(_)))
        }
        SagaChoreographyEvent::StepFailed { .. } => {
            matches!(entry, Some(SagaStateEntry::Failed(_)))
        }
        SagaChoreographyEvent::CompensationCompleted { .. } => {
            matches!(entry, Some(SagaStateEntry::Compensated(_)))
        }
        SagaChoreographyEvent::CompensationFailed { .. } => {
            matches!(
                entry,
                Some(SagaStateEntry::Failed(_) | SagaStateEntry::Quarantined(_))
            )
        }
        // Quarantine is the fail-closed signal: it must stay publishable when the
        // in-memory state is absent (admission, dedupe or store outage before any
        // state existed) or still shows retained completed evidence.
        SagaChoreographyEvent::SagaQuarantined { .. } => true,
        _ => true,
    }
}

pub fn accept_workflow_step<A>(
    actor: &mut A,
    context: SagaContext,
    participant_id: Box<str>,
    execution_id: StepExecutionId,
    policy: AcceptedStepPolicy,
    saga_input: Vec<u8>,
    compensation_data: Vec<u8>,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let saga_id = context.saga_id;
    policy
        .validate()
        .map_err(|source| AcceptedStepError::InvalidPolicy {
            execution_id: execution_id.clone(),
            source,
        })?;

    admit_low_level_accept(actor, &context, &execution_id)?;
    if actor.saga_support().terminal_sagas.contains(&saga_id) {
        return Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id,
        });
    }
    if actor
        .saga_support()
        .resolved_workflow_steps
        .get(&saga_id)
        .is_some_and(|steps| steps.contains(&execution_id))
    {
        return Err(AcceptedStepError::AlreadyResolved {
            saga_id,
            execution_id,
        });
    }
    if actor
        .saga_support()
        .accepted_workflow_steps
        .contains_key(&saga_id)
    {
        return Err(AcceptedStepError::AlreadyAccepted {
            saga_id,
            execution_id,
        });
    }
    let accepted_at_millis = actor.now_millis();
    let mut accepted_context = context;
    accepted_context.event_timestamp_millis = accepted_at_millis;
    let deadline_at_millis =
        accepted_at_millis.saturating_add(policy.idle_timeout.as_millis() as u64);
    let hard_deadline_at_millis =
        accepted_at_millis.saturating_add(policy.hard_timeout.as_millis() as u64);

    let state = crate::SagaParticipantState::new(
        saga_id,
        accepted_context.saga_type.clone(),
        accepted_context.step_name.clone(),
        accepted_context.correlation_id,
        accepted_context.trace_id,
        accepted_context.initiator_peer_id,
        accepted_context.saga_started_at_millis,
    )
    .trigger("step_accepted", accepted_at_millis)
    .start_execution(accepted_at_millis);
    let accepted_step = AcceptedWorkflowStep {
        context: accepted_context.clone(),
        participant_id: participant_id.clone(),
        execution_id: execution_id.clone(),
        policy,
        saga_input,
        compensation_data,
        accepted_at_millis,
        deadline_at_millis,
        hard_deadline_at_millis,
    };
    let timeout_outcome = accepted_step.policy.timeout_outcome.clone();
    let compensation_available = !accepted_step.compensation_data.is_empty();
    actor
        .record_event_strict(
            saga_id,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: accepted_at_millis,
            },
        )
        .map_err(|source| AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        })?;
    record_accepted_step_metadata(actor, &accepted_step).map_err(|source| {
        retain_accepted_compensation_evidence(actor, &accepted_step, &source);
        AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        }
    })?;

    actor
        .saga_states()
        .insert(saga_id, SagaStateEntry::Executing(state));
    actor
        .saga_support_mut()
        .accepted_workflow_steps
        .insert(saga_id, accepted_step);

    Ok(SagaChoreographyEvent::StepAccepted {
        context: accepted_context,
        participant_id,
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis,
        timeouts_enabled: true,
        timeout_outcome,
        compensation_available,
    })
}

/// Durable admission for the public low-level accept. Consults the journal for run
/// identity and any existing forward execution evidence for this run.
fn admit_low_level_accept<A>(
    actor: &mut A,
    context: &SagaContext,
    execution_id: &StepExecutionId,
) -> Result<(), AcceptedStepError>
where
    A: SagaStateExt,
{
    let saga_id = context.saga_id;
    let durability = |error: String| AcceptedStepError::Durability {
        saga_id,
        execution_id: execution_id.clone(),
        error: error.into(),
    };
    match actor.admit_participant_event_strict(context) {
        Ok(ParticipantAdmission::Admitted) => {}
        Ok(ParticipantAdmission::TerminalReplay { .. } | ParticipantAdmission::StaleRun { .. }) => {
            return Err(AcceptedStepError::AlreadyTerminal {
                saga_id,
                execution_id: execution_id.clone(),
            });
        }
        Ok(rejected) => return Err(durability(format!("admission rejected: {rejected:?}"))),
        Err(source) => return Err(durability(format!("{source:?}"))),
    }
    match actor.forward_execution_recorded_strict(context) {
        Ok(false) => {
            crate::helpers::reset_in_memory_run_if_changed(actor, context);
            Ok(())
        }
        Ok(true) => Err(durability(
            "forward execution already has durable intent/evidence; reconciliation required".into(),
        )),
        Err(source) => Err(durability(format!("{source:?}"))),
    }
}

/// Keeps compensation bytes durably reachable when the accepted-step metadata write
/// failed after the forward intent was recorded. Recovery never auto-resumes it.
fn retain_accepted_compensation_evidence<A>(
    actor: &A,
    accepted: &AcceptedWorkflowStep,
    cause: &SagaStateStoreError,
) where
    A: SagaStateExt,
{
    if accepted.compensation_data.is_empty() {
        return;
    }
    let reason = format!("accepted step metadata persistence failed: {cause}");
    if let Err(error) = actor.retain_reconciliation_evidence_strict(
        &accepted.context,
        &[],
        &accepted.compensation_data,
        &reason,
    ) {
        tracing::error!(
            target: "core::saga",
            event = "accepted_step_reconciliation_evidence_failed",
            saga_id = accepted.context.saga_id.get(),
            error = %error
        );
    }
}

pub(crate) fn accept_started_workflow_step<A>(
    actor: &mut A,
    context: SagaContext,
    participant_id: Box<str>,
    execution_id: StepExecutionId,
    policy: AcceptedStepPolicy,
    saga_input: Vec<u8>,
    compensation_data: Vec<u8>,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let saga_id = context.saga_id;
    policy
        .validate()
        .map_err(|source| AcceptedStepError::InvalidPolicy {
            execution_id: execution_id.clone(),
            source,
        })?;
    if actor.saga_support().terminal_sagas.contains(&saga_id) {
        return Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id,
        });
    }
    if actor
        .saga_support()
        .accepted_workflow_steps
        .contains_key(&saga_id)
    {
        return Err(AcceptedStepError::AlreadyAccepted {
            saga_id,
            execution_id,
        });
    }
    // The caller already holds a durable forward intent; only the durable terminal
    // fence (never the bounded cache alone) may refuse the acceptance.
    match actor.terminal_run_outcome_strict(&context) {
        Ok(None) => {}
        Ok(Some(_)) => {
            return Err(AcceptedStepError::AlreadyTerminal {
                saga_id,
                execution_id,
            });
        }
        Err(source) => {
            return Err(AcceptedStepError::Durability {
                saga_id,
                execution_id,
                error: format!("{source:?}").into(),
            });
        }
    }

    let accepted_at_millis = actor.now_millis();
    let mut accepted_context = context;
    accepted_context.event_timestamp_millis = accepted_at_millis;
    let deadline_at_millis =
        accepted_at_millis.saturating_add(policy.idle_timeout.as_millis() as u64);
    let hard_deadline_at_millis =
        accepted_at_millis.saturating_add(policy.hard_timeout.as_millis() as u64);
    let accepted_step = AcceptedWorkflowStep {
        context: accepted_context.clone(),
        participant_id: participant_id.clone(),
        execution_id: execution_id.clone(),
        policy,
        saga_input,
        compensation_data,
        accepted_at_millis,
        deadline_at_millis,
        hard_deadline_at_millis,
    };
    let timeout_outcome = accepted_step.policy.timeout_outcome.clone();
    let compensation_available = !accepted_step.compensation_data.is_empty();
    record_accepted_step_metadata(actor, &accepted_step).map_err(|source| {
        retain_accepted_compensation_evidence(actor, &accepted_step, &source);
        AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        }
    })?;
    actor
        .saga_support_mut()
        .accepted_workflow_steps
        .insert(saga_id, accepted_step);

    Ok(SagaChoreographyEvent::StepAccepted {
        context: accepted_context,
        participant_id,
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis,
        timeouts_enabled: true,
        timeout_outcome,
        compensation_available,
    })
}

pub fn complete_accepted_workflow_step<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    completion: AcceptedStepCompletion,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let accepted = match accepted_step_for_resolution(actor, saga_id, &execution_id) {
        Ok(accepted) => accepted,
        Err(error)
            if matches!(
                error,
                AcceptedStepError::AlreadyTerminal { .. }
                    | AcceptedStepError::NotFound { .. }
                    | AcceptedStepError::ExecutionIdMismatch { .. }
                    | AcceptedStepError::AlreadyResolved { .. }
            ) =>
        {
            return retain_late_accepted_completion(
                actor,
                saga_id,
                execution_id,
                &completion,
                error,
            );
        }
        Err(error) => return Err(error),
    };
    let durability = |source: SagaStateStoreError| AcceptedStepError::Durability {
        saga_id,
        execution_id: execution_id.clone(),
        error: format!("{source:?}").into(),
    };
    let mut context = accepted.context.clone();
    context.event_timestamp_millis = completion.completed_at_millis;
    // One strict proof append carries the full result: the stored context is the
    // exact one the returned completion carries, so recovery can resend it verbatim.
    // An accepted completion declares no dispatch window, so a raw result row first
    // would only manufacture uncertainty. An append failure retains the typed bytes
    // as reconciliation evidence plus a durable quarantine and returns an error.
    let outcome = ParticipantForwardOutcome {
        context,
        output: completion.output.clone(),
        saga_input: accepted.saga_input.clone(),
        compensation_data: completion.compensation_data.clone(),
        effect: None,
        receipt: None,
        recorded_at_millis: actor.now_millis(),
    };
    actor
        .record_forward_outcome_strict(&outcome)
        .map_err(|source| {
            retain_failed_accepted_result(actor, &accepted.context, &completion, &source);
            durability(source)
        })?;
    take_accepted_step(actor, saga_id, execution_id.clone())?;
    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
        let new_state = state.complete(
            completion.output.clone(),
            completion.compensation_data.clone(),
            completion.completed_at_millis,
        );
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Completed(new_state));
    }
    mark_accepted_step_resolved(actor, saga_id, execution_id.clone());

    Ok(outcome.completion_event())
}

/// Failed result persistence retains every available byte and a durable quarantine.
/// Retrying metadata is not permission to repeat the external effect.
fn retain_failed_accepted_result<A: SagaStateExt>(
    actor: &mut A,
    context: &SagaContext,
    completion: &AcceptedStepCompletion,
    source: &SagaStateStoreError,
) {
    let reason = format!("accepted completion result persistence failed: {source}");
    if let Err(error) = actor.retain_reconciliation_evidence_strict(
        context,
        &completion.output,
        &completion.compensation_data,
        &reason,
    ) {
        tracing::error!(target: "core::saga", event = "accepted_result_evidence_failed",
            saga_id = %context.saga_id, error = %error);
    }
    if let Err(error) =
        actor.retain_terminal_saga_strict(context, ParticipantTerminalKind::Quarantined, &reason)
    {
        tracing::error!(target: "core::saga", event = "accepted_result_quarantine_failed",
            saga_id = %context.saga_id, error = %error);
    }
}

/// Late callbacks use retained accepted identity even after restart or successor
/// admission. Typed/context-bearing evidence cannot contaminate the successor's
/// implicit result rows. The returned event is quarantine, never step success.
fn retain_late_accepted_completion<A: SagaStateExt>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    completion: &AcceptedStepCompletion,
    original_error: AcceptedStepError,
) -> Result<SagaChoreographyEvent, AcceptedStepError> {
    let durability = |source: SagaStateStoreError| AcceptedStepError::Durability {
        saga_id,
        execution_id: execution_id.clone(),
        error: format!("{source:?}").into(),
    };
    let entries = actor
        .saga_store()
        .read(saga_id)
        .map_err(|source| durability(SagaStateStoreError::Journal(source)))?;
    let mut identified: Option<SagaContext> = None;
    for entry in entries.iter().rev() {
        if let ParticipantEvent::AcceptedStepRecorded {
            context,
            execution_id: recorded,
            ..
        } = &entry.event
            && *recorded == execution_id
        {
            if let Some(previous) = &identified {
                if previous.saga_type != context.saga_type
                    || previous.saga_started_at_millis != context.saga_started_at_millis
                {
                    return Err(AcceptedStepError::Durability { saga_id, execution_id,
                        error: "callback execution id names multiple runs; explicit run-aware application reconciliation required".into() });
                }
            } else {
                identified = Some(context.clone());
            }
        }
    }
    let Some(context) = identified else {
        return Err(original_error);
    };
    let outcome = actor
        .terminal_run_outcome_strict(&context)
        .map_err(durability)?;
    if outcome == Some(ParticipantTerminalKind::Completed) {
        return Err(original_error);
    }
    let evidence = actor
        .participant_run_evidence_strict(&context)
        .map_err(durability)?;
    if outcome.is_none() && evidence.forward_outcome.is_some() && !evidence.needs_reconciliation {
        return Err(AcceptedStepError::AlreadyResolved {
            saga_id,
            execution_id,
        });
    }
    let reason =
        "accepted completion arrived after terminal/uncertain ownership; reconciliation required";
    actor
        .retain_reconciliation_evidence_strict(
            &context,
            &completion.output,
            &completion.compensation_data,
            reason,
        )
        .map_err(durability)?;
    actor
        .retain_terminal_saga_strict(&context, ParticipantTerminalKind::Quarantined, reason)
        .map_err(durability)?;
    Ok(SagaChoreographyEvent::SagaQuarantined {
        step: context.step_name.clone(),
        participant_id: context.step_name.clone(),
        context,
        reason: reason.into(),
    })
}

pub fn fail_accepted_workflow_step<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    failure: AcceptedStepFailure,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let accepted = accepted_step_for_resolution(actor, saga_id, &execution_id)?;
    actor
        .record_event_strict(
            saga_id,
            ParticipantEvent::StepExecutionFailed {
                error: failure.reason.clone(),
                requires_compensation: failure.requires_compensation,
                failed_at_millis: failure.failed_at_millis,
            },
        )
        .map_err(|source| AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        })?;
    if !failure.requires_compensation {
        take_accepted_step(actor, saga_id, execution_id.clone())?;
        if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
            let new_state = state.fail(failure.reason.clone(), false, failure.failed_at_millis);
            actor
                .saga_states()
                .insert(saga_id, SagaStateEntry::Failed(new_state));
        }
        mark_accepted_step_resolved(actor, saga_id, execution_id);
    } else {
        mark_accepted_step_resolved(actor, saga_id, execution_id);
    }

    let mut context = accepted.context;
    context.event_timestamp_millis = failure.failed_at_millis;

    Ok(SagaChoreographyEvent::StepFailed {
        context,
        participant_id: accepted.participant_id,
        error_code: None,
        error: failure.reason,
        requires_compensation: failure.requires_compensation,
    })
}

pub(crate) fn accept_started_workflow_compensation<A>(
    actor: &mut A,
    context: SagaContext,
    participant_id: Box<str>,
    execution_id: StepExecutionId,
    policy: AcceptedStepPolicy,
    saga_input: Vec<u8>,
    compensation_data: Vec<u8>,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let saga_id = context.saga_id;
    policy
        .validate()
        .map_err(|source| AcceptedStepError::InvalidPolicy {
            execution_id: execution_id.clone(),
            source,
        })?;
    if actor
        .saga_support()
        .accepted_workflow_compensations
        .contains_key(&saga_id)
    {
        return Err(AcceptedStepError::AlreadyAccepted {
            saga_id,
            execution_id,
        });
    }
    let accepted_at_millis = actor.now_millis();
    let mut accepted_context = context;
    accepted_context.event_timestamp_millis = accepted_at_millis;
    let deadline_at_millis =
        accepted_at_millis.saturating_add(policy.idle_timeout.as_millis() as u64);
    let hard_deadline_at_millis =
        accepted_at_millis.saturating_add(policy.hard_timeout.as_millis() as u64);
    let accepted = AcceptedWorkflowCompensation {
        context: accepted_context.clone(),
        participant_id: participant_id.clone(),
        execution_id: execution_id.clone(),
        policy,
        saga_input,
        compensation_data,
        accepted_at_millis,
        deadline_at_millis,
        hard_deadline_at_millis,
    };
    actor
        .record_event_strict(
            saga_id,
            ParticipantEvent::AcceptedCompensationRecorded {
                context: accepted.context.clone(),
                participant_id: accepted.participant_id.clone(),
                execution_id: accepted.execution_id.clone(),
                saga_input: accepted.saga_input.clone(),
                compensation_data: accepted.compensation_data.clone(),
                idle_timeout_millis: accepted.policy.idle_timeout.as_millis() as u64,
                hard_timeout_millis: accepted.policy.hard_timeout.as_millis() as u64,
                accepted_at_millis,
                deadline_at_millis,
                hard_deadline_at_millis,
            },
        )
        .map_err(|source| AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        })?;
    actor
        .saga_support_mut()
        .accepted_workflow_compensations
        .insert(saga_id, accepted);

    Ok(SagaChoreographyEvent::CompensationAccepted {
        context: accepted_context,
        participant_id,
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis,
    })
}

pub fn complete_accepted_workflow_compensation<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    completion: AcceptedCompensationCompletion,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt + HasSagaWorkflowParticipants,
{
    let accepted = accepted_compensation_for_resolution(actor, saga_id, &execution_id)?;
    let workflow = A::saga_workflows().iter().copied().find(|workflow| {
        workflow.step_name() == accepted.context.step_name.as_ref()
            && workflow
                .saga_types()
                .contains(&accepted.context.saga_type.as_ref())
    });
    let Some(workflow) = workflow else {
        return Err(AcceptedStepError::WorkflowNotFound {
            saga_id,
            saga_type: accepted.context.saga_type.clone(),
            step_name: accepted.context.step_name.clone(),
        });
    };
    let event = complete_accepted_compensation_state(actor, saga_id, execution_id, completion)?;
    workflow.on_compensation_completed(actor, event.context());
    Ok(event)
}

pub fn complete_accepted_participant_compensation<P>(
    participant: &mut P,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    completion: AcceptedCompensationCompletion,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    P: SagaStateExt + SagaParticipant,
{
    let event =
        complete_accepted_compensation_state(participant, saga_id, execution_id, completion)?;
    participant.on_compensation_completed(event.context());
    Ok(event)
}

pub fn complete_accepted_async_participant_compensation<P>(
    participant: &mut P,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    completion: AcceptedCompensationCompletion,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    P: SagaStateExt + AsyncSagaParticipant,
{
    let event =
        complete_accepted_compensation_state(participant, saga_id, execution_id, completion)?;
    participant.on_compensation_completed(event.context());
    Ok(event)
}

fn complete_accepted_compensation_state<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    completion: AcceptedCompensationCompletion,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let accepted = accepted_compensation_for_resolution(actor, saga_id, &execution_id)?;
    actor
        .record_event_strict(
            saga_id,
            ParticipantEvent::CompensationCompleted {
                completed_at_millis: completion.completed_at_millis,
            },
        )
        .map_err(|source| AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        })?;
    actor
        .saga_support_mut()
        .accepted_workflow_compensations
        .remove(&saga_id);
    if let Some(SagaStateEntry::Compensating(state)) = actor.saga_states().remove(&saga_id) {
        actor.saga_states().insert(
            saga_id,
            SagaStateEntry::Compensated(
                state.complete_compensation(completion.completed_at_millis),
            ),
        );
    }
    mark_accepted_step_resolved(actor, saga_id, execution_id);
    let mut context = accepted.context;
    context.event_timestamp_millis = completion.completed_at_millis;
    Ok(SagaChoreographyEvent::CompensationCompleted { context })
}

pub fn fail_accepted_workflow_compensation<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    failure: AcceptedCompensationFailure,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let accepted = accepted_compensation_for_resolution(actor, saga_id, &execution_id)?;
    let durable_failure = if failure.is_ambiguous {
        ParticipantEvent::Quarantined {
            reason: failure.reason.clone(),
            quarantined_at_millis: failure.failed_at_millis,
        }
    } else {
        ParticipantEvent::CompensationFailed {
            error: failure.reason.clone(),
            is_ambiguous: false,
            failed_at_millis: failure.failed_at_millis,
        }
    };
    actor
        .record_event_strict(saga_id, durable_failure)
        .map_err(|source| AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        })?;
    actor
        .saga_support_mut()
        .accepted_workflow_compensations
        .remove(&saga_id);
    let state_entry = actor.saga_states().remove(&saga_id);
    match state_entry {
        Some(SagaStateEntry::Compensating(state)) if failure.is_ambiguous => {
            actor.saga_states().insert(
                saga_id,
                SagaStateEntry::Quarantined(
                    state.quarantine(failure.reason.clone(), failure.failed_at_millis),
                ),
            );
        }
        Some(SagaStateEntry::Compensating(state)) => {
            actor.saga_states().insert(
                saga_id,
                SagaStateEntry::Failed(state.fail(
                    failure.reason.clone(),
                    false,
                    failure.failed_at_millis,
                )),
            );
        }
        Some(state) => {
            actor.saga_states().insert(saga_id, state);
        }
        None => {}
    }
    mark_accepted_step_resolved(actor, saga_id, execution_id);
    let mut context = accepted.context;
    context.event_timestamp_millis = failure.failed_at_millis;
    Ok(SagaChoreographyEvent::CompensationFailed {
        context,
        participant_id: accepted.participant_id,
        error: failure.reason,
        is_ambiguous: failure.is_ambiguous,
    })
}

fn accepted_compensation_for_resolution<A>(
    actor: &A,
    saga_id: SagaId,
    execution_id: &StepExecutionId,
) -> Result<AcceptedWorkflowCompensation, AcceptedStepError>
where
    A: HasSagaParticipantSupport,
{
    let Some(accepted) = actor
        .saga_support()
        .accepted_workflow_compensations
        .get(&saga_id)
        .cloned()
    else {
        return Err(AcceptedStepError::NotFound {
            saga_id,
            execution_id: execution_id.clone(),
        });
    };
    if accepted.execution_id != *execution_id {
        return Err(AcceptedStepError::ExecutionIdMismatch {
            saga_id,
            expected: accepted.execution_id,
            actual: execution_id.clone(),
        });
    }
    Ok(accepted)
}

/// Persist the renewed idle deadline before returning a choreography heartbeat.
pub fn record_accepted_workflow_step_progress<A: SagaStateExt>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    now_millis: u64,
) -> Result<SagaChoreographyEvent, AcceptedStepError> {
    let accepted = actor
        .saga_support()
        .accepted_workflow_steps
        .get(&saga_id)
        .cloned()
        .ok_or_else(|| AcceptedStepError::NotFound {
            saga_id,
            execution_id: execution_id.clone(),
        })?;
    if accepted.execution_id != execution_id {
        return Err(AcceptedStepError::ExecutionIdMismatch {
            saga_id,
            expected: accepted.execution_id,
            actual: execution_id,
        });
    }
    let deadline_at_millis = now_millis
        .saturating_add(accepted.policy.idle_timeout.as_millis() as u64)
        .min(accepted.hard_deadline_at_millis)
        .max(accepted.deadline_at_millis);
    persist_deadline_renewal(
        actor,
        &accepted.context,
        &execution_id,
        false,
        deadline_at_millis,
        now_millis,
    )?;
    actor
        .saga_support_mut()
        .accepted_workflow_steps
        .get_mut(&saga_id)
        .unwrap()
        .deadline_at_millis = deadline_at_millis;
    let mut context = accepted.context;
    context.event_timestamp_millis = now_millis;
    Ok(SagaChoreographyEvent::StepAccepted {
        context,
        participant_id: accepted.participant_id,
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis: accepted.hard_deadline_at_millis,
        timeouts_enabled: true,
        timeout_outcome: accepted.policy.timeout_outcome,
        compensation_available: !accepted.compensation_data.is_empty(),
    })
}

/// Persist the renewed idle deadline before returning a choreography heartbeat.
pub fn record_accepted_workflow_compensation_progress<A: SagaStateExt>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    now_millis: u64,
) -> Result<SagaChoreographyEvent, AcceptedStepError> {
    let accepted = actor
        .saga_support()
        .accepted_workflow_compensations
        .get(&saga_id)
        .cloned()
        .ok_or_else(|| AcceptedStepError::NotFound {
            saga_id,
            execution_id: execution_id.clone(),
        })?;
    if accepted.execution_id != execution_id {
        return Err(AcceptedStepError::ExecutionIdMismatch {
            saga_id,
            expected: accepted.execution_id,
            actual: execution_id,
        });
    }
    let deadline_at_millis = now_millis
        .saturating_add(accepted.policy.idle_timeout.as_millis() as u64)
        .min(accepted.hard_deadline_at_millis)
        .max(accepted.deadline_at_millis);
    persist_deadline_renewal(
        actor,
        &accepted.context,
        &execution_id,
        true,
        deadline_at_millis,
        now_millis,
    )?;
    actor
        .saga_support_mut()
        .accepted_workflow_compensations
        .get_mut(&saga_id)
        .unwrap()
        .deadline_at_millis = deadline_at_millis;
    let mut context = accepted.context;
    context.event_timestamp_millis = now_millis;
    Ok(SagaChoreographyEvent::CompensationAccepted {
        context,
        participant_id: accepted.participant_id,
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis: accepted.hard_deadline_at_millis,
    })
}

fn persist_deadline_renewal<A: SagaStateExt>(
    actor: &A,
    context: &SagaContext,
    execution_id: &StepExecutionId,
    is_compensation: bool,
    deadline_at_millis: u64,
    renewed_at_millis: u64,
) -> Result<(), AcceptedStepError> {
    let saga_id = context.saga_id;
    let durability = |source: SagaStateStoreError| AcceptedStepError::Durability {
        saga_id,
        execution_id: execution_id.clone(),
        error: source.to_string().into(),
    };
    if actor
        .terminal_run_outcome_strict(context)
        .map_err(durability)?
        .is_some()
    {
        return Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id: execution_id.clone(),
        });
    }
    if !is_compensation
        && actor
            .saga_support()
            .has_resolved_workflow_step(saga_id, execution_id)
    {
        return Err(AcceptedStepError::AlreadyResolved {
            saga_id,
            execution_id: execution_id.clone(),
        });
    }
    actor
        .record_event_strict(
            saga_id,
            ParticipantEvent::AcceptedDeadlineRenewed {
                context: context.clone(),
                execution_id: execution_id.clone(),
                is_compensation,
                deadline_at_millis,
                renewed_at_millis,
            },
        )
        .map_err(durability)
}

fn record_accepted_step_metadata<A>(
    actor: &A,
    accepted: &AcceptedWorkflowStep,
) -> Result<(), SagaStateStoreError>
where
    A: SagaStateExt,
{
    actor.record_event_strict(
        accepted.context.saga_id,
        ParticipantEvent::AcceptedStepRecorded {
            context: accepted.context.clone(),
            participant_id: accepted.participant_id.clone(),
            execution_id: accepted.execution_id.clone(),
            idle_timeout_millis: accepted.policy.idle_timeout.as_millis() as u64,
            hard_timeout_millis: accepted.policy.hard_timeout.as_millis() as u64,
            timeout_outcome: accepted.policy.timeout_outcome.clone(),
            saga_input: accepted.saga_input.clone(),
            compensation_data: accepted.compensation_data.clone(),
            accepted_at_millis: accepted.accepted_at_millis,
            deadline_at_millis: accepted.deadline_at_millis,
            hard_deadline_at_millis: accepted.hard_deadline_at_millis,
        },
    )
}

pub fn poll_accepted_workflow_step_timeouts<A>(
    actor: &mut A,
    now_millis: u64,
) -> Vec<SagaChoreographyEvent>
where
    A: SagaStateExt,
{
    let expired = actor
        .saga_support()
        .accepted_workflow_steps
        .iter()
        .filter_map(|(saga_id, accepted)| {
            if now_millis > accepted.hard_deadline_at_millis {
                Some((*saga_id, accepted.execution_id.clone(), true))
            } else if now_millis > accepted.deadline_at_millis {
                Some((*saga_id, accepted.execution_id.clone(), false))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    expired
        .into_iter()
        .filter_map(|(saga_id, execution_id, hard_timeout)| {
            resolve_accepted_timeout(actor, saga_id, execution_id, hard_timeout, now_millis).ok()
        })
        .collect()
}

/// Upper bound for terminal fences hydrated into the in-memory cache at startup.
/// The durable journal stays authoritative; this only warms the cache.
const HYDRATED_TERMINAL_FENCE_LIMIT: usize = 4096;

/// The latest participant run recorded in a saga journal.
struct CurrentRun<'a> {
    /// Entries belonging to the latest run (everything from its run record on).
    entries: &'a [JournalEntry],
    /// `(saga_type, saga_started_at_millis)` of the latest run, when identified.
    identity: Option<(&'a str, u64)>,
    /// Durable terminal tombstone of the latest run, if any.
    terminal: Option<ParticipantTerminalKind>,
}

/// Segregates the latest run's history from earlier runs of the same saga id so
/// recovery never folds a resolved older run into the current one.
fn current_run(entries: &[JournalEntry]) -> CurrentRun<'_> {
    let last_run = entries
        .iter()
        .rposition(|entry| matches!(entry.event, ParticipantEvent::ParticipantRunRecorded { .. }));
    let identity = match last_run {
        Some(index) => match &entries[index].event {
            ParticipantEvent::ParticipantRunRecorded {
                saga_type,
                saga_started_at_millis,
                ..
            } => Some((saga_type.as_ref(), *saga_started_at_millis)),
            _ => None,
        },
        // Journals without a run record may still carry an explicit identity: a
        // terminal tombstone or any context-bearing row names its exact run.
        None => entries.iter().rev().find_map(|entry| match &entry.event {
            ParticipantEvent::ParticipantTerminalRecorded {
                saga_type,
                saga_started_at_millis,
                ..
            } => Some((saga_type.as_ref(), *saga_started_at_millis)),
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => Some((
                outcome.context.saga_type.as_ref(),
                outcome.context.saga_started_at_millis,
            )),
            ParticipantEvent::AcceptedDeadlineRenewed { context, .. }
            | ParticipantEvent::CompensationRequestRecorded { context, .. }
            | ParticipantEvent::AcceptedStepRecorded { context, .. }
            | ParticipantEvent::AcceptedCompensationRecorded { context, .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { context, .. } => {
                Some((context.saga_type.as_ref(), context.saga_started_at_millis))
            }
            _ => None,
        }),
    };
    let terminal = identity.and_then(|(run_type, run_started)| {
        entries.iter().rev().find_map(|entry| match &entry.event {
            ParticipantEvent::ParticipantTerminalRecorded {
                saga_type,
                saga_started_at_millis,
                outcome,
                ..
            } if saga_type.as_ref() == run_type && *saga_started_at_millis == run_started => {
                Some(*outcome)
            }
            _ => None,
        })
    });
    CurrentRun {
        entries: last_run.map_or(entries, |index| &entries[index..]),
        identity,
        terminal,
    }
}

fn hydrate_run_fence<S>(
    support: &mut SagaParticipantSupport<S>,
    saga_id: SagaId,
    started_at_millis: u64,
    terminal: bool,
) where
    S: ParticipantStore,
{
    support
        .saga_run_started_at
        .insert(saga_id, started_at_millis);
    if terminal && support.terminal_sagas.insert(saga_id) {
        support.terminal_saga_order.push_back(saga_id);
        while support.terminal_saga_order.len() > HYDRATED_TERMINAL_FENCE_LIMIT {
            let Some(evicted) = support.terminal_saga_order.pop_front() else {
                break;
            };
            support.terminal_sagas.remove(&evicted);
            support.saga_run_started_at.remove(&evicted);
        }
    }
}

pub fn recover_accepted_workflow_steps_for_saga_type<S: ParticipantStore>(
    support: &mut SagaParticipantSupport<S>,
    step_name: &str,
    saga_type: &str,
) -> Result<(), RecoveryCollectionError> {
    let ids = support
        .store
        .unresolved_saga_ids()
        .map_err(RecoveryCollectionError::ListSagas)?;
    let now = SagaContext::now_millis();
    for saga_id in ids {
        let entries = support
            .store
            .read(saga_id)
            .map_err(|source| RecoveryCollectionError::ReadSaga { saga_id, source })?;
        hydrate_recovered_run(support, saga_id, &entries, step_name, saga_type, now)?;
    }
    support.prune_orphan_run_markers();
    Ok(())
}

fn hydrate_recovered_run<S: ParticipantStore>(
    support: &mut SagaParticipantSupport<S>,
    saga_id: SagaId,
    all_entries: &[JournalEntry],
    step_name: &str,
    saga_type: &str,
    now_millis: u64,
) -> Result<(), RecoveryCollectionError> {
    let run = current_run(all_entries);
    if let Some((run_type, run_started)) = run.identity
        && run_type == saga_type
    {
        hydrate_run_fence(support, saga_id, run_started, run.terminal.is_some());
    }
    if run.terminal.is_some() {
        // Resolved (completed, failed or quarantined) runs are never resumed or
        // auto-resolved on restart; their evidence stays in the journal.
        return Ok(());
    }
    if let Some((run_type, run_started)) = run.identity
        && run_type == saga_type
    {
        // Durable AllOf inputs of the current run only; never dependency_fired.
        let seen: std::collections::HashSet<Box<str>> = all_entries
            .iter()
            .filter_map(|entry| match &entry.event {
                ParticipantEvent::ParticipantDependencyCompletedRecorded { context, .. }
                    if context.saga_type.as_ref() == run_type
                        && context.saga_started_at_millis == run_started =>
                {
                    Some(context.step_name.clone())
                }
                _ => None,
            })
            .collect();
        if !seen.is_empty() {
            support.dependency_completions.insert(saga_id, seen);
        }
    }
    let entries = run.entries;
    if entries.iter().any(|entry| {
        matches!(
            entry.event,
            ParticipantEvent::ParticipantReconciliationEvidence { .. }
        )
    }) {
        return Ok(()); // human reconciliation, never automatic re-execution
    }
    if let Some(request) = recover_unstarted_compensation_request_from_entries(entries)
        && request.context().saga_type.as_ref() == saga_type
        && compensation_request_targets_step(&request, step_name)
        && recover_accepted_workflow_step_from_entries(entries).is_none()
    {
        let Some((output, compensation_data, completed_at_millis)) =
            recover_completed_step_effect_for_unstarted_compensation(entries)
        else {
            return Err(RecoveryCollectionError::MissingCompensationState { saga_id });
        };
        if compensation_data.is_empty() {
            return Err(RecoveryCollectionError::MissingCompensationState { saga_id });
        }
        let mut context = request.context().clone();
        context.step_name = step_name.into();
        let state = crate::SagaParticipantState::new(
            saga_id,
            context.saga_type.clone(),
            context.step_name.clone(),
            context.correlation_id,
            context.trace_id,
            context.initiator_peer_id,
            context.saga_started_at_millis,
        )
        .trigger("step_recovered", completed_at_millis)
        .start_execution(completed_at_millis)
        .complete(output, compensation_data, completed_at_millis);
        support
            .saga_states
            .insert(saga_id, SagaStateEntry::Completed(state));
    }
    if let Some(accepted) = recover_accepted_workflow_compensation_from_entries(entries) {
        if accepted.context.saga_type.as_ref() != saga_type
            || accepted.context.step_name.as_ref() != step_name
            || accepted.deadline_at_millis < now_millis
            || accepted.hard_deadline_at_millis < now_millis
        {
            return Ok(());
        }
        let state = crate::SagaParticipantState::new(
            saga_id,
            accepted.context.saga_type.clone(),
            accepted.context.step_name.clone(),
            accepted.context.correlation_id,
            accepted.context.trace_id,
            accepted.context.initiator_peer_id,
            accepted.context.saga_started_at_millis,
        )
        .trigger("compensation_accepted", accepted.accepted_at_millis)
        .start_execution(accepted.accepted_at_millis)
        .start_compensation(accepted.accepted_at_millis);
        support
            .saga_states
            .insert(saga_id, SagaStateEntry::Compensating(state));
        support
            .accepted_workflow_compensations
            .insert(saga_id, accepted);
        return Ok(());
    }
    if let Some(accepted) = recover_accepted_workflow_step_from_entries(entries) {
        let expired = accepted.deadline_at_millis < now_millis
            || accepted.hard_deadline_at_millis < now_millis;
        let retain_expired_for_compensation = expired
            && !accepted.compensation_data.is_empty()
            && matches!(
                &accepted.policy.timeout_outcome,
                AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true
                }
            );
        if accepted.context.saga_type.as_ref() != saga_type
            || accepted.context.step_name.as_ref() != step_name
            || (expired && !retain_expired_for_compensation)
        {
            return Ok(());
        }
        let execution_id = accepted.execution_id.clone();
        let forward_execution_resolved = retain_expired_for_compensation
            || accepted_step_failure_requiring_compensation(entries).is_some();
        let state = crate::SagaParticipantState::new(
            saga_id,
            accepted.context.saga_type.clone(),
            accepted.context.step_name.clone(),
            accepted.context.correlation_id,
            accepted.context.trace_id,
            accepted.context.initiator_peer_id,
            accepted.context.saga_started_at_millis,
        )
        .trigger("step_accepted", accepted.accepted_at_millis)
        .start_execution(accepted.accepted_at_millis);
        support
            .saga_states
            .insert(saga_id, SagaStateEntry::Executing(state));
        support.accepted_workflow_steps.insert(saga_id, accepted);
        if forward_execution_resolved {
            support
                .resolved_workflow_steps
                .entry(saga_id)
                .or_default()
                .insert(execution_id);
        }
        return Ok(());
    }
    // A confirmed forward proof hydrates the completed state and marks the
    // dependency as fired, so replayed upstream events cannot re-execute it.
    if let Some(outcome) = RunProgress::scan(entries).confirmed_clean()
        && outcome.context.saga_type.as_ref() == saga_type
        && outcome.context.step_name.as_ref() == step_name
    {
        let at = outcome.recorded_at_millis;
        let state = crate::SagaParticipantState::new(
            saga_id,
            outcome.context.saga_type.clone(),
            outcome.context.step_name.clone(),
            outcome.context.correlation_id,
            outcome.context.trace_id,
            outcome.context.initiator_peer_id,
            outcome.context.saga_started_at_millis,
        )
        .trigger("step_recovered", at)
        .start_execution(at)
        .complete(
            outcome.output.clone(),
            outcome.compensation_data.clone(),
            at,
        );
        support
            .saga_states
            .insert(saga_id, SagaStateEntry::Completed(state));
        support.dependency_fired.insert(saga_id);
    }
    Ok(())
}

fn same_run_context(left: &SagaContext, right: &SagaContext) -> bool {
    left.saga_id == right.saga_id
        && left.saga_type == right.saga_type
        && left.saga_started_at_millis == right.saga_started_at_millis
        && left.step_name == right.step_name
}

fn recover_accepted_workflow_compensation_from_entries(
    entries: &[JournalEntry],
) -> Option<AcceptedWorkflowCompensation> {
    let mut accepted: Option<AcceptedWorkflowCompensation> = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::AcceptedDeadlineRenewed {
                context,
                execution_id,
                is_compensation,
                deadline_at_millis,
                ..
            } => {
                if *is_compensation
                    && let Some(pending) = accepted.as_mut()
                    && same_run_context(&pending.context, context)
                    && pending.execution_id == *execution_id
                {
                    pending.deadline_at_millis = pending
                        .deadline_at_millis
                        .max((*deadline_at_millis).min(pending.hard_deadline_at_millis));
                }
            }

            ParticipantEvent::AcceptedCompensationRecorded {
                context,
                participant_id,
                execution_id,
                saga_input,
                compensation_data,
                idle_timeout_millis,
                hard_timeout_millis,
                accepted_at_millis,
                deadline_at_millis,
                hard_deadline_at_millis,
            } => {
                accepted = Some(AcceptedWorkflowCompensation {
                    context: context.clone(),
                    participant_id: participant_id.clone(),
                    execution_id: execution_id.clone(),
                    policy: AcceptedStepPolicy {
                        idle_timeout: Duration::from_millis(*idle_timeout_millis),
                        hard_timeout: Duration::from_millis(*hard_timeout_millis),
                        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                    },
                    saga_input: saga_input.clone(),
                    compensation_data: compensation_data.clone(),
                    accepted_at_millis: *accepted_at_millis,
                    deadline_at_millis: *deadline_at_millis,
                    hard_deadline_at_millis: *hard_deadline_at_millis,
                });
            }
            ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::Quarantined { .. } => accepted = None,
            _ => {}
        }
    }
    accepted
}

fn recover_compensation_request_from_entries(
    entries: &[JournalEntry],
) -> Option<SagaChoreographyEvent> {
    let mut request = None;
    for entry in entries {
        if let ParticipantEvent::CompensationRequestRecorded {
            context,
            failed_step,
            reason,
            failure,
            steps_to_compensate,
            requested_at_millis,
        } = &entry.event
        {
            let mut context = context.clone();
            context.event_timestamp_millis = *requested_at_millis;
            request = Some(SagaChoreographyEvent::CompensationRequested {
                context,
                failed_step: failed_step.clone(),
                reason: reason.clone(),
                failure: failure.clone(),
                steps_to_compensate: steps_to_compensate.clone(),
            });
        }
    }
    request
}

fn recover_completed_accepted_compensation_from_entries(
    entries: &[JournalEntry],
) -> Option<(AcceptedWorkflowCompensation, u64)> {
    let mut accepted: Option<AcceptedWorkflowCompensation> = None;
    let mut completed = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::AcceptedDeadlineRenewed {
                context,
                execution_id,
                is_compensation,
                deadline_at_millis,
                ..
            } => {
                if *is_compensation
                    && let Some(pending) = accepted.as_mut()
                    && same_run_context(&pending.context, context)
                    && pending.execution_id == *execution_id
                {
                    pending.deadline_at_millis = pending
                        .deadline_at_millis
                        .max((*deadline_at_millis).min(pending.hard_deadline_at_millis));
                }
            }

            ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
            | ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {}
            ParticipantEvent::CompensationRequestRecorded { .. } => {
                accepted = None;
                completed = None;
            }
            ParticipantEvent::AcceptedCompensationRecorded {
                context,
                participant_id,
                execution_id,
                saga_input,
                compensation_data,
                idle_timeout_millis,
                hard_timeout_millis,
                accepted_at_millis,
                deadline_at_millis,
                hard_deadline_at_millis,
            } => {
                accepted = Some(AcceptedWorkflowCompensation {
                    context: context.clone(),
                    participant_id: participant_id.clone(),
                    execution_id: execution_id.clone(),
                    policy: AcceptedStepPolicy {
                        idle_timeout: Duration::from_millis(*idle_timeout_millis),
                        hard_timeout: Duration::from_millis(*hard_timeout_millis),
                        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                    },
                    saga_input: saga_input.clone(),
                    compensation_data: compensation_data.clone(),
                    accepted_at_millis: *accepted_at_millis,
                    deadline_at_millis: *deadline_at_millis,
                    hard_deadline_at_millis: *hard_deadline_at_millis,
                });
                completed = None;
            }
            ParticipantEvent::CompensationCompleted {
                completed_at_millis,
            } => {
                if let Some(accepted) = accepted.take() {
                    completed = Some((accepted, *completed_at_millis));
                }
            }
            ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. } => {
                accepted = None;
                completed = None;
            }
            ParticipantEvent::ParticipantRunRecorded { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::StepExecutionFailed { .. }
            | ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::AcceptedStepRecorded { .. } => {}
        }
    }
    completed
}

fn recover_failed_accepted_compensation_from_entries(
    entries: &[JournalEntry],
) -> Option<(AcceptedWorkflowCompensation, Box<str>, bool, u64)> {
    let mut accepted: Option<AcceptedWorkflowCompensation> = None;
    let mut failed = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::AcceptedDeadlineRenewed {
                context,
                execution_id,
                is_compensation,
                deadline_at_millis,
                ..
            } => {
                if *is_compensation
                    && let Some(pending) = accepted.as_mut()
                    && same_run_context(&pending.context, context)
                    && pending.execution_id == *execution_id
                {
                    pending.deadline_at_millis = pending
                        .deadline_at_millis
                        .max((*deadline_at_millis).min(pending.hard_deadline_at_millis));
                }
            }

            ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
            | ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {}
            ParticipantEvent::CompensationRequestRecorded { .. } => {
                accepted = None;
                failed = None;
            }
            ParticipantEvent::AcceptedCompensationRecorded {
                context,
                participant_id,
                execution_id,
                saga_input,
                compensation_data,
                idle_timeout_millis,
                hard_timeout_millis,
                accepted_at_millis,
                deadline_at_millis,
                hard_deadline_at_millis,
            } => {
                accepted = Some(AcceptedWorkflowCompensation {
                    context: context.clone(),
                    participant_id: participant_id.clone(),
                    execution_id: execution_id.clone(),
                    policy: AcceptedStepPolicy {
                        idle_timeout: Duration::from_millis(*idle_timeout_millis),
                        hard_timeout: Duration::from_millis(*hard_timeout_millis),
                        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                    },
                    saga_input: saga_input.clone(),
                    compensation_data: compensation_data.clone(),
                    accepted_at_millis: *accepted_at_millis,
                    deadline_at_millis: *deadline_at_millis,
                    hard_deadline_at_millis: *hard_deadline_at_millis,
                });
                failed = None;
            }
            ParticipantEvent::CompensationFailed {
                error,
                is_ambiguous,
                failed_at_millis,
            } => {
                if let Some(accepted) = accepted.take() {
                    failed = Some((accepted, error.clone(), *is_ambiguous, *failed_at_millis));
                }
            }
            ParticipantEvent::Quarantined {
                reason,
                quarantined_at_millis,
            } => {
                if let Some(accepted) = accepted.take() {
                    failed = Some((accepted, reason.clone(), true, *quarantined_at_millis));
                }
            }
            ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. } => {
                accepted = None;
                failed = None;
            }
            ParticipantEvent::ParticipantRunRecorded { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::StepExecutionFailed { .. }
            | ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::AcceptedStepRecorded { .. } => {}
        }
    }
    failed
}

fn recover_unstarted_compensation_request_from_entries(
    entries: &[JournalEntry],
) -> Option<SagaChoreographyEvent> {
    let mut request = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
            | ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {}
            ParticipantEvent::CompensationRequestRecorded {
                context,
                failed_step,
                reason,
                failure,
                steps_to_compensate,
                requested_at_millis,
            } => {
                let mut context = context.clone();
                context.event_timestamp_millis = *requested_at_millis;
                request = Some(SagaChoreographyEvent::CompensationRequested {
                    context,
                    failed_step: failed_step.clone(),
                    reason: reason.clone(),
                    failure: failure.clone(),
                    steps_to_compensate: steps_to_compensate.clone(),
                });
            }
            ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::AcceptedCompensationRecorded { .. }
            | ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. } => request = None,
            ParticipantEvent::AcceptedDeadlineRenewed { .. }
            | ParticipantEvent::ParticipantRunRecorded { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::StepExecutionFailed { .. }
            | ParticipantEvent::AcceptedStepRecorded { .. } => {}
        }
    }
    request
}

fn compensation_request_targets_step(request: &SagaChoreographyEvent, step_name: &str) -> bool {
    match request {
        SagaChoreographyEvent::CompensationRequested {
            steps_to_compensate,
            ..
        } => steps_to_compensate
            .first()
            .is_some_and(|requested_step| requested_step.as_ref() == step_name),
        _ => false,
    }
}

fn recover_completed_step_effect_for_unstarted_compensation(
    entries: &[JournalEntry],
) -> Option<(Vec<u8>, Vec<u8>, u64)> {
    let mut completed = None;
    let mut effect_at_request = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {}
            // A proof-only completion (no raw row) still names the materialised effect.
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
                completed = Some((
                    outcome.output.clone(),
                    outcome.compensation_data.clone(),
                    outcome.recorded_at_millis,
                ));
            }
            ParticipantEvent::StepExecutionStarted { .. } => completed = None,
            ParticipantEvent::StepExecutionCompleted {
                output,
                compensation_data,
                completed_at_millis,
            } => {
                completed = Some((
                    output.clone(),
                    compensation_data.clone(),
                    *completed_at_millis,
                ));
            }
            ParticipantEvent::CompensationRequestRecorded { .. } => {
                effect_at_request = completed.clone();
            }
            ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::AcceptedCompensationRecorded { .. }
            | ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. } => {
                effect_at_request = None
            }
            ParticipantEvent::AcceptedDeadlineRenewed { .. }
            | ParticipantEvent::ParticipantRunRecorded { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionFailed { .. }
            | ParticipantEvent::AcceptedStepRecorded { .. } => {}
        }
    }
    effect_at_request
}

/// True only for durable, exact-run evidence that the forward step was
/// definitively rejected with no effect and nothing since could make an undo or a
/// reconciliation necessary. Missing evidence is never rejection. This asserts safe
/// rejection, not physical remote cancellation.
pub(crate) fn no_effect_rejection_proven(evidence: &crate::ParticipantRunEvidence) -> bool {
    evidence.forward_definitively_rejected
        && !evidence.forward_intent_open
        && !evidence.accepted_forward_pending
        && !evidence.forward_result_recorded
        && evidence.forward_outcome.is_none()
        && !evidence.undo_intent_open
        && !evidence.undo_completed
        && !evidence.needs_reconciliation
        && !evidence.quarantined
}

/// Entry-level mirror of the rejection marker for startup recovery, which has no
/// actor. Deliberately conservative: any real result, compensating failure, undo
/// activity, reconciliation, quarantine or foreign-run row yields `false`.
/// Owned requests and a no-op completion proof do not clear a rejection.
fn rejected_without_effect_in_entries(entries: &[JournalEntry], run: (&str, u64)) -> bool {
    let mut accepted = false;
    let mut rejected = false;
    let mut dirty = false;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::AcceptedStepRecorded { context, .. } => {
                if context.saga_type.as_ref() != run.0 || context.saga_started_at_millis != run.1 {
                    return false;
                }
                rejected = false;
                accepted = true;
            }
            ParticipantEvent::StepExecutionStarted { .. } => rejected = false,
            ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
            | ParticipantEvent::StepExecutionFailed {
                requires_compensation: true,
                ..
            } => {
                rejected = false;
                dirty = true;
            }
            ParticipantEvent::StepExecutionFailed {
                requires_compensation: false,
                ..
            } => {
                if accepted && !dirty {
                    rejected = true;
                }
                accepted = false;
            }
            ParticipantEvent::CompensationCompleted { .. } => dirty = true,
            ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::AcceptedCompensationRecorded { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. } => return false,
            _ => {}
        }
    }
    rejected
}

/// The acknowledgement of an owned undo that is durably complete, if (and only
/// if) the history is unambiguous: the retained request names this step and the
/// run identity, the undo started and completed, and nothing after it reopened
/// forward work, failed the undo, quarantined, or retained reconciliation
/// evidence. The accepted-compensation protocol has its own recovery path.
fn recover_proven_undo_acknowledgement(
    entries: &[JournalEntry],
    identity: Option<(&str, u64)>,
    saga_type: &str,
    step_name: &str,
) -> Option<SagaChoreographyEvent> {
    let (run_type, run_started) = identity?;
    // A no-op completion (definitively rejected forward step) has no undo intent.
    let no_effect = rejected_without_effect_in_entries(entries, (run_type, run_started));
    let mut request: Option<&SagaContext> = None;
    let mut started = false;
    let mut completed = false;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::CompensationRequestRecorded {
                context,
                steps_to_compensate,
                ..
            } => {
                if steps_to_compensate
                    .first()
                    .is_some_and(|step| step.as_ref() == step_name)
                {
                    request = Some(context);
                    started = false;
                    completed = false;
                }
            }
            ParticipantEvent::CompensationStarted { .. } if request.is_some() => {
                started = true;
                completed = false;
            }
            ParticipantEvent::CompensationCompleted { .. } if started || no_effect => {
                completed = true;
            }
            ParticipantEvent::AcceptedCompensationRecorded { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. } => return None,
            ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
                if completed =>
            {
                return None;
            }
            _ => {}
        }
    }
    let request = request?;
    (completed
        && request.saga_type.as_ref() == saga_type
        && request.saga_type.as_ref() == run_type
        && request.saga_started_at_millis == run_started)
        .then(|| SagaChoreographyEvent::CompensationCompleted {
            context: request.next_step(step_name.into()),
        })
}

fn recover_accepted_workflow_step_from_entries(
    entries: &[JournalEntry],
) -> Option<AcceptedWorkflowStep> {
    let mut accepted: Option<AcceptedWorkflowStep> = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::AcceptedDeadlineRenewed {
                context,
                execution_id,
                is_compensation,
                deadline_at_millis,
                ..
            } => {
                if !*is_compensation
                    && let Some(pending) = accepted.as_mut()
                    && same_run_context(&pending.context, context)
                    && pending.execution_id == *execution_id
                {
                    pending.deadline_at_millis = pending
                        .deadline_at_millis
                        .max((*deadline_at_millis).min(pending.hard_deadline_at_millis));
                }
            }

            // The strict proof is the sole record of an authoritative completion.
            ParticipantEvent::ParticipantForwardOutcomeRecorded { .. } => accepted = None,
            ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {}
            ParticipantEvent::AcceptedStepRecorded {
                context,
                participant_id,
                execution_id,
                idle_timeout_millis,
                hard_timeout_millis,
                timeout_outcome,
                saga_input,
                compensation_data,
                accepted_at_millis,
                deadline_at_millis,
                hard_deadline_at_millis,
            } => {
                accepted = Some(AcceptedWorkflowStep {
                    context: context.clone(),
                    participant_id: participant_id.clone(),
                    execution_id: execution_id.clone(),
                    policy: AcceptedStepPolicy {
                        idle_timeout: Duration::from_millis(*idle_timeout_millis),
                        hard_timeout: Duration::from_millis(*hard_timeout_millis),
                        timeout_outcome: timeout_outcome.clone(),
                    },
                    saga_input: saga_input.clone(),
                    compensation_data: compensation_data.clone(),
                    accepted_at_millis: *accepted_at_millis,
                    deadline_at_millis: *deadline_at_millis,
                    hard_deadline_at_millis: *hard_deadline_at_millis,
                });
            }
            ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::StepExecutionFailed {
                requires_compensation: false,
                ..
            }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. } => {
                accepted = None;
            }
            ParticipantEvent::ParticipantRunRecorded { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::StepExecutionFailed {
                requires_compensation: true,
                ..
            }
            | ParticipantEvent::CompensationRequestRecorded { .. }
            | ParticipantEvent::AcceptedCompensationRecorded { .. } => {}
        }
    }
    accepted
}

fn accepted_step_failure_requiring_compensation(
    entries: &[JournalEntry],
) -> Option<(Box<str>, u64)> {
    let mut failure = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::ParticipantForwardOutcomeRecorded { .. } => failure = None,
            ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {}
            ParticipantEvent::AcceptedStepRecorded { .. } => {
                failure = None;
            }
            ParticipantEvent::StepExecutionFailed {
                error,
                requires_compensation: true,
                failed_at_millis,
            } => {
                failure = Some((error.clone(), *failed_at_millis));
            }
            ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::StepExecutionFailed {
                requires_compensation: false,
                ..
            }
            | ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. } => {
                failure = None;
            }
            ParticipantEvent::AcceptedDeadlineRenewed { .. }
            | ParticipantEvent::ParticipantRunRecorded { .. }
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::CompensationRequestRecorded { .. }
            | ParticipantEvent::AcceptedCompensationRecorded { .. } => {}
        }
    }
    failure
}

fn resolve_accepted_timeout<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    hard_timeout: bool,
    timed_out_at_millis: u64,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    let accepted = accepted_step_for_resolution(actor, saga_id, &execution_id)?;
    let timeout_kind = if hard_timeout { "hard" } else { "idle" };
    let reason: Box<str> = format!(
        "accepted step {timeout_kind} timeout saga_id={} step={} execution_id={}",
        saga_id.get(),
        accepted.context.step_name,
        accepted.execution_id
    )
    .into_boxed_str();

    match accepted.policy.timeout_outcome {
        AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation,
        } => {
            actor
                .record_event_strict(
                    saga_id,
                    ParticipantEvent::StepExecutionFailed {
                        error: reason.clone(),
                        requires_compensation,
                        failed_at_millis: timed_out_at_millis,
                    },
                )
                .map_err(|source| AcceptedStepError::Durability {
                    saga_id,
                    execution_id: execution_id.clone(),
                    error: format!("{source:?}").into(),
                })?;
            if !requires_compensation {
                take_accepted_step(actor, saga_id, execution_id.clone())?;
                mark_accepted_step_resolved(actor, saga_id, execution_id.clone());
                if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id)
                {
                    let new_state = state.fail(reason.clone(), false, timed_out_at_millis);
                    actor
                        .saga_states()
                        .insert(saga_id, SagaStateEntry::Failed(new_state));
                }
            } else {
                mark_accepted_step_resolved(actor, saga_id, execution_id.clone());
            }
            let mut context = accepted.context;
            context.event_timestamp_millis = timed_out_at_millis;
            Ok(SagaChoreographyEvent::StepFailed {
                context,
                participant_id: accepted.participant_id,
                error_code: Some(timeout_kind.into()),
                error: reason,
                requires_compensation,
            })
        }
        AcceptedStepTimeoutOutcome::QuarantineSaga => {
            actor
                .record_event_strict(
                    saga_id,
                    ParticipantEvent::Quarantined {
                        reason: reason.clone(),
                        quarantined_at_millis: timed_out_at_millis,
                    },
                )
                .map_err(|source| AcceptedStepError::Durability {
                    saga_id,
                    execution_id: execution_id.clone(),
                    error: format!("{source:?}").into(),
                })?;
            take_accepted_step(actor, saga_id, execution_id.clone())?;
            mark_accepted_step_resolved(actor, saga_id, execution_id);
            if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
                let new_state = state.quarantine(reason.clone(), timed_out_at_millis);
                actor
                    .saga_states()
                    .insert(saga_id, SagaStateEntry::Quarantined(new_state));
            }
            let step = accepted.context.step_name.clone();
            let mut context = accepted.context;
            context.event_timestamp_millis = timed_out_at_millis;
            Ok(SagaChoreographyEvent::SagaQuarantined {
                context,
                reason,
                step,
                participant_id: accepted.participant_id,
            })
        }
    }
}

fn accepted_step_for_resolution<A>(
    actor: &A,
    saga_id: SagaId,
    execution_id: &StepExecutionId,
) -> Result<AcceptedWorkflowStep, AcceptedStepError>
where
    A: SagaStateExt,
{
    if actor.saga_support().terminal_sagas.contains(&saga_id) {
        return Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id: execution_id.clone(),
        });
    }
    if actor
        .saga_support()
        .resolved_workflow_steps
        .get(&saga_id)
        .is_some_and(|steps| steps.contains(execution_id))
    {
        return Err(AcceptedStepError::AlreadyResolved {
            saga_id,
            execution_id: execution_id.clone(),
        });
    }
    let Some(accepted) = actor.saga_support().accepted_workflow_steps.get(&saga_id) else {
        return Err(AcceptedStepError::NotFound {
            saga_id,
            execution_id: execution_id.clone(),
        });
    };
    if accepted.execution_id != *execution_id {
        let expected = accepted.execution_id.clone();
        return Err(AcceptedStepError::ExecutionIdMismatch {
            saga_id,
            expected,
            actual: execution_id.clone(),
        });
    }
    // The bounded latch above is only a fast path; the durable run tombstone decides.
    match actor.terminal_run_outcome_strict(&accepted.context) {
        Ok(None) => Ok(accepted.clone()),
        Ok(Some(_)) => Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id: execution_id.clone(),
        }),
        Err(source) => Err(AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        }),
    }
}

fn take_accepted_step<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
) -> Result<AcceptedWorkflowStep, AcceptedStepError>
where
    A: SagaStateExt,
{
    let accepted = accepted_step_for_resolution(actor, saga_id, &execution_id)?;
    actor
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&saga_id);
    Ok(accepted)
}

pub(crate) fn mark_accepted_step_resolved<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
) where
    A: HasSagaParticipantSupport,
{
    actor
        .saga_support_mut()
        .resolved_workflow_steps
        .entry(saga_id)
        .or_default()
        .insert(execution_id);
}

/// A resolver-origin accepted-step timeout that carries no compensation durably
/// resolves this participant's matching accepted work *before* the volatile
/// metadata is dropped; otherwise a later ordinary failure would find unresolved
/// accepted work and falsely quarantine a definitive rejection. Only the exact
/// run (saga type, id and start time) that owns the metadata and is the journal's
/// current unresolved run may be resolved, so an old context can never resolve a
/// successor. A failed durable write keeps the metadata and the unresolved
/// evidence visible.
pub(crate) fn mark_matching_accepted_step_failed<A>(actor: &mut A, event: &SagaChoreographyEvent)
where
    A: SagaStateExt,
{
    let SagaChoreographyEvent::StepFailed {
        context,
        error_code,
        error,
        requires_compensation,
        ..
    } = event
    else {
        return;
    };
    if !matches!(error_code.as_deref(), Some("idle" | "hard"))
        || !error.starts_with("accepted step ")
        || !error.contains(" timeout")
    {
        return;
    }
    if *requires_compensation {
        return;
    }
    let saga_id = context.saga_id;
    let Some(accepted) = actor.saga_support().accepted_workflow_steps.get(&saga_id) else {
        return;
    };
    if accepted.context.step_name != context.step_name
        || accepted.context.saga_type != context.saga_type
        || accepted.context.saga_started_at_millis != context.saga_started_at_millis
    {
        return;
    }
    let execution_id = accepted.execution_id.clone();
    let accepted_context = accepted.context.clone();
    match actor.saga_store().read(saga_id) {
        Ok(entries) => {
            let run = current_run(&entries);
            if run.terminal.is_none() {
                let owns_run = run.identity
                    == Some((
                        accepted_context.saga_type.as_ref(),
                        accepted_context.saga_started_at_millis,
                    ));
                if !owns_run {
                    return;
                }
                if run_still_pending(actor, &accepted_context)
                    && let Err(source) = actor.record_event_strict(
                        saga_id,
                        ParticipantEvent::StepExecutionFailed {
                            error: error.clone(),
                            requires_compensation: false,
                            failed_at_millis: context.event_timestamp_millis,
                        },
                    )
                {
                    tracing::error!(
                        target: "core::saga",
                        event = "accepted_timeout_resolution_persistence_failed",
                        saga_id = saga_id.get(),
                        error = %source
                    );
                    return;
                }
            }
        }
        Err(source) => {
            tracing::error!(
                target: "core::saga",
                event = "accepted_timeout_resolution_unreadable",
                saga_id = saga_id.get(),
                error = %source
            );
            return;
        }
    }
    actor
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&saga_id);
    mark_accepted_step_resolved(actor, saga_id, execution_id);
}

/// True while the run still has unresolved accepted forward work in the journal.
fn run_still_pending<A: SagaStateExt>(actor: &A, context: &SagaContext) -> bool {
    actor
        .participant_run_evidence_strict(context)
        .map(|evidence| evidence.accepted_forward_pending)
        // Unreadable evidence is not proof of resolution; attempt the write.
        .unwrap_or(true)
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
    let mut emitted = Vec::new();
    let terminal_event = event.clone();
    let retained = crate::helpers::handle_saga_event_core(
        participant,
        event,
        |next_event| emitted.push(next_event),
        |actor, start| {
            publish_workflow_step_started(
                actor,
                start,
                &mut on_invalid_transition,
                &mut on_emitted_transition,
            )
        },
    );
    if retained {
        apply_terminal_side_effects(participant, &terminal_event);
    }

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

        if matches!(next_event, SagaChoreographyEvent::StepStarted { .. }) {
            continue;
        }
        on_emitted_transition(participant, &next_event);

        if let Some(bus) = &saga_bus
            && let Err(err) = bus.publish_strict(next_event)
        {
            tracing::error!(
                target: "core::saga",
                event = "participant_ingress_emit_publish_failed",
                error = ?err
            );
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
            if let Some(bus) = actor.saga_support().bus.as_ref()
                && let Err(publish_err) = bus.publish_strict(framework_failure)
            {
                tracing::error!(
                    target: "core::saga",
                    event = "workflow_resolution_failure_publish_failed",
                    error = ?publish_err
                );
            }
            return;
        }
    };

    let mut emitted = Vec::new();
    // Managed `StepStarted` is published strictly, after durable intent and before
    // business execution, so the resolver can see running work. Validation and the
    // emitted-transition hook run at that moment (before the work, not after it).
    let mut publish_start = |actor: &mut A, start: SagaChoreographyEvent| {
        publish_workflow_step_started(
            actor,
            start,
            &mut on_invalid_transition,
            &mut on_emitted_transition,
        )
    };
    let terminal_event = event.clone();
    let retained = crate::helpers::handle_workflow_saga_event_with_emit(
        actor,
        workflow,
        event,
        |next_event| emitted.push(next_event),
        &mut publish_start,
    );
    if retained {
        apply_terminal_side_effects(actor, &terminal_event);
    }

    let saga_bus = actor.saga_support().bus.clone();
    for next_event in emitted {
        if !is_valid_emitted_transition(
            actor.saga_states_ref().get(&next_event.context().saga_id),
            &next_event,
        ) {
            on_invalid_transition(&next_event);
            continue;
        }

        if matches!(next_event, SagaChoreographyEvent::StepStarted { .. }) {
            continue;
        }
        on_emitted_transition(actor, &next_event);

        if let Some(bus) = &saga_bus
            && let Err(err) = bus.publish_strict(next_event)
        {
            tracing::error!(
                target: "core::saga",
                event = "workflow_participant_ingress_emit_publish_failed",
                error = ?err
            );
        }
    }
}

/// Validates, hooks and strictly publishes a managed step's `StepStarted` before
/// its business work. `Err` means the start is not visible: the work must not run.
fn publish_workflow_step_started<A>(
    actor: &mut A,
    start: SagaChoreographyEvent,
    on_invalid_transition: &mut dyn FnMut(&SagaChoreographyEvent),
    on_emitted_transition: &mut dyn FnMut(&mut A, &SagaChoreographyEvent),
) -> Result<(), Box<str>>
where
    A: HasSagaParticipantSupport,
{
    if !is_valid_emitted_transition(
        actor.saga_states_ref().get(&start.context().saga_id),
        &start,
    ) {
        on_invalid_transition(&start);
        return Err("step start is not a valid transition; effect not run".into());
    }
    on_emitted_transition(actor, &start);
    let Some(bus) = actor.saga_support().bus.clone() else {
        return Ok(());
    };
    bus.publish_strict(start)
        .map(|_| ())
        .map_err(|error| format!("step start publication failed; effect not run: {error:?}").into())
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
    let mut emitted = Vec::new();
    let terminal_event = event.clone();
    let retained = crate::helpers::handle_async_saga_event_core(
        participant,
        event,
        |next_event| emitted.push(next_event),
        |actor, start| {
            publish_workflow_step_started(
                actor,
                start,
                &mut on_invalid_transition,
                &mut on_emitted_transition,
            )
        },
    )
    .await;
    if retained {
        apply_terminal_side_effects(participant, &terminal_event);
    }

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

        if matches!(next_event, SagaChoreographyEvent::StepStarted { .. }) {
            continue;
        }
        on_emitted_transition(participant, &next_event);

        if let Some(bus) = &saga_bus
            && let Err(err) = bus.publish_strict(next_event)
        {
            tracing::error!(
                target: "core::saga",
                event = "async_participant_ingress_emit_publish_failed",
                error = ?err
            );
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

pub fn publish_active_saga_panic_quarantine<S>(
    saga: &mut SagaParticipantSupport<S>,
    context: &SagaContext,
    phase: ActiveSagaExecutionPhase,
    panic_payload: &(dyn std::any::Any + Send),
    step_name: &str,
    participant_id: Box<str>,
) where
    S: ParticipantStore,
{
    let message = panic_message_from_payload(panic_payload);
    let reason = panic_quarantine_reason(phase, message.as_ref());
    let now = SagaContext::now_millis();

    if let Err(err) = saga.store.append(
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
                if stats.delivered > 0
                    && let Err(err) = saga
                        .store
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

pub fn classify_recovery(entries: &[JournalEntry], now_ms: u64) -> RecoveryDecision {
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
            | ParticipantEvent::ParticipantTerminalRecorded { .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { .. }
            | ParticipantEvent::StepExecutionFailed {
                requires_compensation: false,
                ..
            }
    );
    if terminal {
        return RecoveryDecision::TerminalNoAction;
    }
    if matches!(
        &last.event,
        ParticipantEvent::AcceptedStepRecorded {
            hard_deadline_at_millis,
            ..
        }
        | ParticipantEvent::AcceptedCompensationRecorded {
            hard_deadline_at_millis,
            ..
        } if now_ms <= *hard_deadline_at_millis
    ) {
        return RecoveryDecision::Continue;
    }
    // Only genuinely unresolved work is unsafe at startup, and it is unsafe at any age:
    // an idle run record or a confirmed result is healthy in-flight state, while an
    // open intent or unconfirmed result has an unknown outcome that must be reconciled.
    if RunProgress::scan(entries).uncertainty().is_some() {
        RecoveryDecision::QuarantineStale
    } else {
        RecoveryDecision::Continue
    }
}

/// Journal-derived progress of one participant run, used by startup recovery.
#[derive(Default)]
struct RunProgress<'a> {
    intent_open: bool,
    result_unconfirmed: bool,
    undo_open: bool,
    undo_requested: bool,
    confirmed: Option<&'a ParticipantForwardOutcome>,
}

impl<'a> RunProgress<'a> {
    fn scan(entries: &'a [JournalEntry]) -> Self {
        let mut progress = Self::default();
        for entry in entries {
            match &entry.event {
                ParticipantEvent::StepExecutionStarted { .. } => {
                    progress.intent_open = true;
                    progress.result_unconfirmed = false;
                    progress.confirmed = None;
                }
                ParticipantEvent::StepExecutionCompleted { .. } => {
                    progress.intent_open = false;
                    progress.result_unconfirmed = true;
                    progress.confirmed = None;
                }
                ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
                    progress.intent_open = false;
                    progress.result_unconfirmed = false;
                    progress.confirmed = Some(outcome);
                }
                ParticipantEvent::StepExecutionFailed { .. } => progress.intent_open = false,
                ParticipantEvent::CompensationRequestRecorded { .. } => {
                    progress.undo_requested = true;
                }
                ParticipantEvent::CompensationStarted { .. }
                | ParticipantEvent::AcceptedCompensationRecorded { .. }
                | ParticipantEvent::CompensationFailed { .. } => {
                    progress.undo_requested = true;
                    progress.undo_open = true;
                    progress.confirmed = None;
                }
                ParticipantEvent::CompensationCompleted { .. } => {
                    progress.undo_requested = true;
                    progress.undo_open = false;
                    progress.intent_open = false;
                    progress.result_unconfirmed = false;
                    progress.confirmed = None;
                }
                _ => {}
            }
        }
        progress
    }

    /// Why startup must quarantine this run, if it must.
    fn uncertainty(&self) -> Option<&'static str> {
        if self.intent_open {
            Some("startup recovery found a forward execution intent with unknown outcome")
        } else if self.result_unconfirmed {
            Some("startup recovery found an unconfirmed forward result (declared dispatch unknown)")
        } else if self.undo_open {
            Some("startup recovery found an undo intent with unknown outcome")
        } else {
            None
        }
    }

    /// A confirmed forward proof with nothing else pending, safe to resend or hydrate.
    fn confirmed_clean(&self) -> Option<&'a ParticipantForwardOutcome> {
        if self.uncertainty().is_some() || self.undo_requested {
            return None;
        }
        self.confirmed
    }
}

pub fn collect_startup_recovery_events<S: ParticipantStore>(
    journal: &S,
    step_name: &str,
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    collect_startup_recovery_events_for_saga_type(journal, step_name, DEFAULT_RECOVERY_SAGA_TYPE)
}

pub fn collect_startup_recovery_events_for_saga_type<S: ParticipantStore>(
    journal: &S,
    step_name: &str,
    saga_type: &str,
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    collect_startup_recovery_events_for_saga_type_inner(journal, saga_type, step_name, true)
}

fn recorded_saga_type(entries: &[JournalEntry]) -> Option<&str> {
    entries.iter().rev().find_map(|entry| match &entry.event {
        ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
            Some(outcome.context.saga_type.as_ref())
        }
        ParticipantEvent::ParticipantRunRecorded { saga_type, .. }
        | ParticipantEvent::ParticipantTerminalRecorded { saga_type, .. } => {
            Some(saga_type.as_ref())
        }
        ParticipantEvent::AcceptedDeadlineRenewed { context, .. }
        | ParticipantEvent::CompensationRequestRecorded { context, .. }
        | ParticipantEvent::AcceptedStepRecorded { context, .. }
        | ParticipantEvent::AcceptedCompensationRecorded { context, .. }
        | ParticipantEvent::ParticipantReconciliationEvidence { context, .. }
        | ParticipantEvent::ParticipantDependencyCompletedRecorded { context, .. } => {
            Some(context.saga_type.as_ref())
        }
        ParticipantEvent::StepTriggered { .. }
        | ParticipantEvent::StepExecutionStarted { .. }
        | ParticipantEvent::StepExecutionCompleted { .. }
        | ParticipantEvent::StepExecutionFailed { .. }
        | ParticipantEvent::CompensationStarted { .. }
        | ParticipantEvent::CompensationCompleted { .. }
        | ParticipantEvent::CompensationFailed { .. }
        | ParticipantEvent::Quarantined { .. } => None,
    })
}

fn collect_startup_recovery_events_for_saga_type_inner<S: ParticipantStore>(
    store: &S,
    saga_type: &str,
    step_name: &str,
    allow_untyped_recovery: bool,
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    let ids = store
        .unresolved_saga_ids()
        .map_err(RecoveryCollectionError::ListSagas)?;
    let now = SagaContext::now_millis();
    let mut out = Vec::new();
    for saga_id in ids {
        let entries = store
            .read(saga_id)
            .map_err(|source| RecoveryCollectionError::ReadSaga { saga_id, source })?;
        out.extend(collect_recovered_run(
            store,
            saga_id,
            &entries,
            step_name,
            saga_type,
            allow_untyped_recovery,
            now,
        )?);
    }
    Ok(out)
}

/// Recover every registered participant in one unresolved-index pass and one read per saga.
pub fn collect_startup_recovery_events_for_participants<S: ParticipantStore>(
    support: &mut SagaParticipantSupport<S>,
    registrations: &[(&str, &str)],
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    let ids = support
        .store
        .unresolved_saga_ids()
        .map_err(RecoveryCollectionError::ListSagas)?;
    let now = SagaContext::now_millis();
    let mut out = Vec::new();
    for saga_id in ids {
        let entries = support
            .store
            .read(saga_id)
            .map_err(|source| RecoveryCollectionError::ReadSaga { saga_id, source })?;
        let recorded = recorded_saga_type(&entries);
        for &(step_name, saga_type) in registrations {
            if recorded.is_some_and(|recorded| recorded != saga_type) {
                continue;
            }
            out.extend(collect_recovered_run(
                &support.store,
                saga_id,
                &entries,
                step_name,
                saga_type,
                false,
                now,
            )?);
            hydrate_recovered_run(support, saga_id, &entries, step_name, saga_type, now)?;
        }
        if recorded.is_none() && !entries.is_empty() {
            return Err(unidentified_run_error(saga_id, "startup history"));
        }
    }
    support.prune_orphan_run_markers();
    Ok(out)
}

fn collect_recovered_run<S: ParticipantStore>(
    journal: &S,
    saga_id: SagaId,
    all_entries: &[JournalEntry],
    step_name: &str,
    saga_type: &str,
    allow_untyped_recovery: bool,
    now: u64,
) -> Result<Vec<SagaChoreographyEvent>, RecoveryCollectionError> {
    let mut out = Vec::new();
    if all_entries.is_empty() {
        return Ok(out);
    }
    match recorded_saga_type(all_entries) {
        Some(recorded) if recorded != saga_type => return Ok(out),
        None if !allow_untyped_recovery => return Ok(out),
        Some(_) | None => {}
    }
    let run = current_run(all_entries);
    if run.terminal.is_some() {
        // A durably resolved run (completed, failed or quarantined) is never
        // resumed or auto-resolved; quarantined evidence waits for an operator.
        return Ok(out);
    }
    let entries = run.entries;
    if let Some((context, reason)) = entries.iter().rev().find_map(|entry| match &entry.event {
        ParticipantEvent::ParticipantReconciliationEvidence {
            context, reason, ..
        } if context.saga_type.as_ref() == saga_type => Some((context, reason)),
        _ => None,
    }) {
        out.push(SagaChoreographyEvent::SagaQuarantined {
            context: context.next_step(step_name.into()),
            reason: reason.clone(),
            step: step_name.into(),
            participant_id: step_name.into(),
        });
        return Ok(out);
    }
    // Recovery only acts on durable journal evidence. If a persistent
    // dedupe store marked an upstream event but the participant crashed
    // before journaling StepExecutionStarted, there is no local execution
    // intent to resume here; the saga remains externally visible and is
    // resolved by terminal timeout policy instead of silent replay.
    if let Some((accepted, error, is_ambiguous, failed_at_millis)) =
        recover_failed_accepted_compensation_from_entries(entries)
        && accepted.context.saga_type.as_ref() == saga_type
        && accepted.context.step_name.as_ref() == step_name
    {
        let Some(request) = recover_compensation_request_from_entries(entries) else {
            out.push(SagaChoreographyEvent::SagaQuarantined {
                context: accepted.context.clone(),
                reason:
                    "failed accepted compensation recovery missing durable compensation request"
                        .into(),
                step: accepted.context.step_name.clone(),
                participant_id: accepted.participant_id,
            });
            return Ok(out);
        };
        let mut context = accepted.context;
        context.event_timestamp_millis = failed_at_millis;
        out.push(request);
        out.push(SagaChoreographyEvent::CompensationFailed {
            context,
            participant_id: accepted.participant_id,
            error,
            is_ambiguous,
        });
        return Ok(out);
    }
    if let Some((accepted, completed_at_millis)) =
        recover_completed_accepted_compensation_from_entries(entries)
        && accepted.context.saga_type.as_ref() == saga_type
        && accepted.context.step_name.as_ref() == step_name
    {
        let Some(request) = recover_compensation_request_from_entries(entries) else {
            out.push(SagaChoreographyEvent::SagaQuarantined {
                context: accepted.context.clone(),
                reason:
                    "completed accepted compensation recovery missing durable compensation request"
                        .into(),
                step: accepted.context.step_name.clone(),
                participant_id: accepted.participant_id,
            });
            return Ok(out);
        };
        let mut context = accepted.context;
        context.event_timestamp_millis = completed_at_millis;
        out.push(request);
        out.push(SagaChoreographyEvent::CompensationCompleted { context });
        return Ok(out);
    }
    if let Some(accepted) = recover_accepted_workflow_compensation_from_entries(entries)
        && accepted.context.saga_type.as_ref() == saga_type
        && accepted.context.step_name.as_ref() == step_name
    {
        let Some(request) = recover_compensation_request_from_entries(entries) else {
            out.push(SagaChoreographyEvent::SagaQuarantined {
                context: accepted.context.clone(),
                reason: "accepted compensation recovery missing durable compensation request"
                    .into(),
                step: accepted.context.step_name.clone(),
                participant_id: accepted.participant_id,
            });
            return Ok(out);
        };
        out.push(request);
        out.push(startup_accepted_compensation_replay_event(&accepted));
        if let Some(timeout_event) = startup_accepted_compensation_timeout_event(accepted, now) {
            out.push(timeout_event);
        }
        return Ok(out);
    }
    // Undo proven locally (request, intent and completion are all durable) but
    // never acknowledged: resend only the original-run acknowledgement. No new
    // request, no physical undo, no invented identity.
    if let Some(ack) =
        recover_proven_undo_acknowledgement(entries, run.identity, saga_type, step_name)
    {
        out.push(ack);
        return Ok(out);
    }
    if let Some(request) = recover_unstarted_compensation_request_from_entries(entries)
        && request.context().saga_type.as_ref() == saga_type
        && compensation_request_targets_step(&request, step_name)
    {
        let accepted_effect = recover_accepted_workflow_step_from_entries(entries)
            .is_some_and(|accepted| !accepted.compensation_data.is_empty());
        let completed_effect = recover_completed_step_effect_for_unstarted_compensation(entries)
            .is_some_and(|(_, compensation_data, _)| !compensation_data.is_empty());
        let no_effect_rejection = run.identity.is_some_and(|identity| {
            request.context().saga_type.as_ref() == identity.0
                && request.context().saga_started_at_millis == identity.1
                && rejected_without_effect_in_entries(entries, identity)
        });
        if !accepted_effect && !completed_effect && !no_effect_rejection {
            out.push(SagaChoreographyEvent::SagaQuarantined {
                context: request.context().clone(),
                reason: "unstarted compensation recovery missing durable compensation state".into(),
                step: request.context().step_name.clone(),
                participant_id: request.context().step_name.clone(),
            });
            return Ok(out);
        }
        out.push(request);
        return Ok(out);
    }
    if let Some(accepted) = recover_accepted_workflow_step_from_entries(entries)
        && accepted.context.saga_type.as_ref() == saga_type
        && accepted.context.step_name.as_ref() == step_name
    {
        if let Some((error, failed_at_millis)) =
            accepted_step_failure_requiring_compensation(entries)
        {
            out.push(startup_accepted_step_failure_hydration_event(&accepted));
            out.push(startup_accepted_step_failure_event(
                &accepted,
                None,
                error,
                failed_at_millis,
            ));
            return Ok(out);
        }
        if let Some(timeout_event) = startup_accepted_step_timeout_event(accepted.clone(), now) {
            if let SagaChoreographyEvent::StepFailed {
                context,
                error_code,
                error,
                requires_compensation: true,
                ..
            } = timeout_event
            {
                out.push(startup_accepted_step_failure_hydration_event(&accepted));
                out.push(startup_accepted_step_failure_event(
                    &accepted,
                    error_code,
                    error,
                    context.event_timestamp_millis,
                ));
            } else {
                out.push(timeout_event);
            }
            return Ok(out);
        }
        out.push(startup_accepted_step_replay_event(&accepted));
        return Ok(out);
    }
    if let Some(outcome) = RunProgress::scan(entries).confirmed_clean()
        && outcome.context.saga_type.as_ref() == saga_type
        && outcome.context.step_name.as_ref() == step_name
    {
        // Forward success is durably proven (business work and any declared
        // effect), so the original completion is resent verbatim; nothing re-runs.
        out.push(outcome.completion_event());
        return Ok(out);
    }
    match classify_recovery(entries, now) {
        RecoveryDecision::QuarantineStale => {
            let Some((_, started_at)) = &run.identity else {
                return Err(unidentified_run_error(saga_id, "unresolved execution/undo"));
            };
            let mut context = recovery_context_for_saga_type(saga_id, step_name, saga_type);
            context.saga_started_at_millis = *started_at;
            out.push(SagaChoreographyEvent::SagaQuarantined {
                context,
                reason: RunProgress::scan(entries)
                    .uncertainty()
                    .unwrap_or("startup recovery found unresolved execution/undo")
                    .into(),
                step: step_name.into(),
                participant_id: step_name.into(),
            });
        }
        RecoveryDecision::ReplayPanicQuarantine => {
            // The panic row carries no identity; only the journal's recorded run
            // identity may be used. Never invent a run timestamp.
            let Some((_, started_at)) = &run.identity else {
                return Err(unidentified_run_error(saga_id, "panic quarantine"));
            };
            let should_emit = match journal.check_and_mark(saga_id, PANIC_QUARANTINE_PUBLISH_KEY) {
                Ok(value) => value,
                Err(err) => {
                    return Err(RecoveryCollectionError::MarkDedupe {
                        saga_id,
                        source: err,
                    });
                }
            };
            if should_emit {
                let reason = panic_quarantine_reason_from_entries(entries)
                    .unwrap_or_else(|| Box::<str>::from("panic quarantined during execution"));
                let mut context = recovery_context_for_saga_type(saga_id, step_name, saga_type);
                context.saga_started_at_millis = *started_at;
                out.push(SagaChoreographyEvent::SagaQuarantined {
                    context,
                    reason,
                    step: step_name.into(),
                    participant_id: step_name.into(),
                });
            }
        }
        RecoveryDecision::Continue | RecoveryDecision::TerminalNoAction => {}
    }
    Ok(out)
}

fn unidentified_run_error(saga_id: SagaId, what: &str) -> RecoveryCollectionError {
    RecoveryCollectionError::ReadSaga {
        saga_id,
        source: JournalError::Storage(
            format!("legacy {what} has no recorded run identity; operator reconciliation required")
                .into(),
        ),
    }
}

fn startup_accepted_compensation_replay_event(
    accepted: &AcceptedWorkflowCompensation,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationAccepted {
        context: accepted.context.clone(),
        participant_id: accepted.participant_id.clone(),
        execution_id: accepted.execution_id.clone(),
        deadline_at_millis: accepted.deadline_at_millis,
        hard_deadline_at_millis: accepted.hard_deadline_at_millis,
    }
}

fn startup_accepted_step_replay_event(accepted: &AcceptedWorkflowStep) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepAccepted {
        context: accepted.context.clone(),
        participant_id: accepted.participant_id.clone(),
        execution_id: accepted.execution_id.clone(),
        deadline_at_millis: accepted.deadline_at_millis,
        hard_deadline_at_millis: accepted.hard_deadline_at_millis,
        timeouts_enabled: true,
        timeout_outcome: accepted.policy.timeout_outcome.clone(),
        compensation_available: !accepted.compensation_data.is_empty(),
    }
}

fn startup_accepted_step_failure_hydration_event(
    accepted: &AcceptedWorkflowStep,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepAccepted {
        context: accepted.context.clone(),
        participant_id: accepted.participant_id.clone(),
        execution_id: accepted.execution_id.clone(),
        deadline_at_millis: accepted.deadline_at_millis,
        hard_deadline_at_millis: accepted.hard_deadline_at_millis,
        // The immediately following durable StepFailed is authoritative. This
        // recovery-only hydration must not race that failure with a stale timeout.
        timeouts_enabled: false,
        timeout_outcome: accepted.policy.timeout_outcome.clone(),
        compensation_available: !accepted.compensation_data.is_empty(),
    }
}

fn startup_accepted_step_failure_event(
    accepted: &AcceptedWorkflowStep,
    error_code: Option<Box<str>>,
    error: Box<str>,
    failed_at_millis: u64,
) -> SagaChoreographyEvent {
    let mut context = accepted.context.clone();
    context.event_timestamp_millis = failed_at_millis;
    SagaChoreographyEvent::StepFailed {
        context,
        participant_id: accepted.participant_id.clone(),
        error_code,
        error,
        requires_compensation: true,
    }
}

fn startup_accepted_compensation_timeout_event(
    accepted: AcceptedWorkflowCompensation,
    now_millis: u64,
) -> Option<SagaChoreographyEvent> {
    let hard_timeout = now_millis > accepted.hard_deadline_at_millis;
    let idle_timeout = now_millis > accepted.deadline_at_millis;
    if !hard_timeout && !idle_timeout {
        return None;
    }
    let timeout_kind = if hard_timeout { "hard" } else { "idle" };
    let mut context = accepted.context;
    context.event_timestamp_millis = now_millis;
    Some(SagaChoreographyEvent::SagaQuarantined {
        step: context.step_name.clone(),
        context,
        participant_id: accepted.participant_id,
        reason: format!(
            "accepted compensation {timeout_kind} timeout after restart: execution_id={} deadline_at_millis={} hard_deadline_at_millis={}",
            accepted.execution_id,
            accepted.deadline_at_millis,
            accepted.hard_deadline_at_millis
        )
        .into(),
    })
}

fn recovery_context_for_saga_type(
    saga_id: SagaId,
    step_name: &str,
    saga_type: &str,
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

fn startup_accepted_step_timeout_event(
    accepted: AcceptedWorkflowStep,
    now_millis: u64,
) -> Option<SagaChoreographyEvent> {
    let hard_timeout = now_millis > accepted.hard_deadline_at_millis;
    let idle_timeout = now_millis > accepted.deadline_at_millis;
    if !hard_timeout && !idle_timeout {
        return None;
    }
    let timeout_kind = if hard_timeout { "hard" } else { "idle" };
    let mut context = accepted.context;
    context.event_timestamp_millis = now_millis;
    let reason: Box<str> = format!(
        "accepted step {timeout_kind} timeout after restart: step={} execution_id={} deadline_at_millis={} hard_deadline_at_millis={}",
        context.step_name,
        accepted.execution_id,
        accepted.deadline_at_millis,
        accepted.hard_deadline_at_millis
    )
    .into();
    match accepted.policy.timeout_outcome {
        AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation,
        } => Some(SagaChoreographyEvent::StepFailed {
            context,
            participant_id: accepted.participant_id,
            error_code: Some(timeout_kind.into()),
            error: reason,
            requires_compensation,
        }),
        AcceptedStepTimeoutOutcome::QuarantineSaga => {
            Some(SagaChoreographyEvent::SagaQuarantined {
                step: context.step_name.clone(),
                context,
                participant_id: accepted.participant_id,
                reason,
            })
        }
    }
}

pub fn default_runtime_dir(var: &str, fallback: &str) -> PathBuf {
    default_runtime_dir_from_value(std::env::var_os(var), fallback)
}

pub(crate) fn default_runtime_dir_from_value(
    value: Option<std::ffi::OsString>,
    fallback: &str,
) -> PathBuf {
    if let Some(value) = value {
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

#[cfg(test)]
mod tests {
    use super::{
        ActiveSagaExecution, HasActiveSagaExecution,
        apply_sync_workflow_participant_saga_ingress_with_hooks, default_runtime_dir_from_value,
        workflow_for_event,
    };
    use crate::{
        DependencySpec, DeterministicContextBuilder, HasSagaParticipantSupport,
        HasSagaWorkflowParticipants, InMemoryParticipantStore, ParticipantStore,
        SagaParticipantSupport, SagaWorkflowParticipant, StepOutput,
    };
    use std::path::PathBuf;

    #[test]
    fn workflow_nonterminal_does_not_apply_terminal_side_effects() {
        let mut actor = WorkflowTestActor::default();
        let mut calls = 0;
        let mut context = DeterministicContextBuilder::default().build();
        context.saga_type = "alpha_workflow".into();
        apply_sync_workflow_participant_saga_ingress_with_hooks(
            &mut actor,
            crate::SagaChoreographyEvent::SagaStarted {
                context,
                payload: vec![7],
            },
            |_, _| calls += 1,
            |_| {},
            |_, _| {},
        );
        assert_eq!(calls, 0);
    }

    #[test]
    fn local_quarantine_echo_reaches_terminal_callback() {
        use crate::SagaStateExt;
        let mut actor = WorkflowTestActor::default();
        let context = DeterministicContextBuilder::default()
            .with_saga_type("alpha_workflow")
            .build();
        actor
            .retain_terminal_saga_strict(
                &context,
                crate::ParticipantTerminalKind::Quarantined,
                "local failure",
            )
            .unwrap();
        let mut calls = 0;
        super::apply_sync_workflow_participant_saga_ingress(
            &mut actor,
            crate::SagaChoreographyEvent::SagaQuarantined {
                context,
                reason: "local failure".into(),
                step: "alpha_step".into(),
                participant_id: "alpha_step".into(),
            },
            |_, _| calls += 1,
            |_| {},
        );
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn terminal_callbacks_require_persisted_tombstone_in_every_mode() {
        use crate::SagaStateExt;
        for mode in 0..3 {
            for failed in [false, true] {
                let mut actor = WorkflowTestActor::default();
                let context = DeterministicContextBuilder::default()
                    .with_saga_type("alpha_workflow")
                    .build();
                actor.record_saga_run_start(context.saga_id, context.saga_started_at_millis);
                actor
                    .saga
                    .dependency_completions
                    .entry(context.saga_id)
                    .or_default()
                    .insert("upstream".into());
                let event = if failed {
                    crate::SagaChoreographyEvent::SagaFailed {
                        context: context.clone(),
                        reason: "clean failure".into(),
                        failure: None,
                    }
                } else {
                    crate::SagaChoreographyEvent::SagaCompleted {
                        context: context.clone(),
                    }
                };
                actor
                    .saga
                    .store
                    .fail
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                let mut external = 0;
                for retry in 0..3 {
                    match mode {
                        0 => super::apply_sync_participant_saga_ingress(
                            &mut actor,
                            event.clone(),
                            |_, _| external += 1,
                            |_| {},
                        ),
                        1 => {
                            super::apply_async_participant_saga_ingress(
                                &mut actor,
                                event.clone(),
                                |_, _| external += 1,
                                |_| {},
                            )
                            .await
                        }
                        _ => super::apply_sync_workflow_participant_saga_ingress(
                            &mut actor,
                            event.clone(),
                            |_, _| external += 1,
                            |_| {},
                        ),
                    }
                    if retry == 0 {
                        assert_eq!(external, 0);
                        assert_eq!(actor.terminal_hooks, 0);
                        assert!(!actor.saga.terminal_sagas.contains(&context.saga_id));
                        assert!(
                            actor
                                .saga
                                .dependency_completions
                                .contains_key(&context.saga_id)
                        );
                        actor
                            .saga
                            .store
                            .fail
                            .store(false, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        assert_eq!(external, 1);
                        assert_eq!(actor.terminal_hooks, 1);
                        assert!(
                            !actor
                                .saga
                                .dependency_completions
                                .contains_key(&context.saga_id)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn compensation_renewal_is_durable_capped_and_replayed_without_payload_duplication() {
        let (mut actor, context, _) = accepted_actor();
        let execution = crate::StepExecutionId::new("undo");
        super::accept_started_workflow_compensation(
            &mut actor,
            context.clone(),
            "worker".into(),
            execution.clone(),
            crate::AcceptedStepPolicy {
                idle_timeout: std::time::Duration::from_secs(1),
                hard_timeout: std::time::Duration::from_secs(10),
                timeout_outcome: crate::AcceptedStepTimeoutOutcome::QuarantineSaga,
            },
            vec![7; 4096],
            vec![9; 4096],
        )
        .unwrap();
        let hard =
            actor.saga.accepted_workflow_compensations[&context.saga_id].hard_deadline_at_millis;
        let heartbeat = super::record_accepted_workflow_compensation_progress(
            &mut actor,
            context.saga_id,
            execution.clone(),
            hard,
        )
        .unwrap();
        assert!(
            matches!(heartbeat, crate::SagaChoreographyEvent::CompensationAccepted { deadline_at_millis, .. } if deadline_at_millis == hard)
        );
        let entries = actor.saga.store.read(context.saga_id).unwrap();
        assert!(matches!(
            entries.last().unwrap().event,
            crate::ParticipantEvent::AcceptedDeadlineRenewed {
                is_compensation: true,
                ..
            }
        ));
        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&entries.last().unwrap().event).unwrap();
        assert!(archived.len() < 1024);
        assert_eq!(
            super::recover_accepted_workflow_compensation_from_entries(&entries)
                .unwrap()
                .deadline_at_millis,
            hard
        );
        actor
            .saga
            .store
            .fail
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            super::record_accepted_workflow_compensation_progress(
                &mut actor,
                context.saga_id,
                execution,
                hard + 1
            )
            .is_err()
        );
        assert_eq!(
            actor.saga.accepted_workflow_compensations[&context.saga_id].deadline_at_millis,
            hard
        );
    }

    #[test]
    fn renewal_replay_matches_run_execution_and_phase_and_never_resurrects_completion() {
        let (mut actor, context, execution) = accepted_actor();
        let old = actor.saga.accepted_workflow_steps[&context.saga_id].deadline_at_millis;
        let mut foreign = context.clone();
        foreign.saga_started_at_millis += 1;
        for (context, execution_id, is_compensation) in [
            (foreign, execution.clone(), false),
            (context.clone(), crate::StepExecutionId::new("wrong"), false),
            (context.clone(), execution.clone(), true),
        ] {
            actor
                .saga
                .store
                .append(
                    context.saga_id,
                    crate::ParticipantEvent::AcceptedDeadlineRenewed {
                        context,
                        execution_id,
                        is_compensation,
                        deadline_at_millis: old + 500,
                        renewed_at_millis: old,
                    },
                )
                .unwrap();
        }
        let entries = actor.saga.store.read(context.saga_id).unwrap();
        assert_eq!(
            super::recover_accepted_workflow_step_from_entries(&entries)
                .unwrap()
                .deadline_at_millis,
            old
        );
        super::record_accepted_workflow_step_progress(
            &mut actor,
            context.saga_id,
            execution.clone(),
            old,
        )
        .unwrap();
        let entries = actor.saga.store.read(context.saga_id).unwrap();
        assert_eq!(
            super::recover_accepted_workflow_step_from_entries(&entries)
                .unwrap()
                .deadline_at_millis,
            old + 1000
        );
        super::complete_accepted_workflow_step(
            &mut actor,
            context.saga_id,
            execution.clone(),
            crate::AcceptedStepCompletion {
                completed_at_millis: old,
                output: vec![42],
                compensation_data: vec![],
            },
        )
        .unwrap();
        actor
            .saga
            .store
            .append(
                context.saga_id,
                crate::ParticipantEvent::AcceptedDeadlineRenewed {
                    context: context.clone(),
                    execution_id: execution,
                    is_compensation: false,
                    deadline_at_millis: old + 5000,
                    renewed_at_millis: old,
                },
            )
            .unwrap();
        assert!(
            super::recover_accepted_workflow_step_from_entries(
                &actor.saga.store.read(context.saga_id).unwrap()
            )
            .is_none()
        );
    }

    #[cfg(feature = "lmdb")]
    #[test]
    fn renewed_deadlines_survive_lmdb_reopen() {
        let (mut actor, context, execution) = accepted_actor();
        let old = actor.saga.accepted_workflow_steps[&context.saga_id].deadline_at_millis;
        super::record_accepted_workflow_step_progress(&mut actor, context.saga_id, execution, old)
            .unwrap();
        let entries = actor.saga.store.read(context.saga_id).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let store = crate::LmdbParticipantStore::open(dir.path(), "renewals").unwrap();
        store
            .append_batch(
                context.saga_id,
                entries.into_iter().map(|row| row.event).collect(),
            )
            .unwrap();
        drop(store);
        let store = crate::LmdbParticipantStore::open(dir.path(), "renewals").unwrap();
        let mut support = SagaParticipantSupport::new(store);
        let events = super::collect_startup_recovery_events_for_participants(
            &mut support,
            &[(context.step_name.as_ref(), context.saga_type.as_ref())],
        )
        .unwrap();
        assert!(
            matches!(&events[0], crate::SagaChoreographyEvent::StepAccepted { deadline_at_millis, .. } if *deadline_at_millis == old + 1000)
        );
        assert_eq!(
            support.accepted_workflow_steps[&context.saga_id].deadline_at_millis,
            old + 1000
        );
        assert_eq!(
            support.accepted_workflow_steps[&context.saga_id].saga_input,
            vec![7; 4096]
        );
    }

    #[derive(Default)]
    struct FaultStore {
        inner: InMemoryParticipantStore,
        fail: std::sync::atomic::AtomicBool,
        commits: std::sync::atomic::AtomicUsize,
        reads: std::sync::atomic::AtomicUsize,
        scans: std::sync::atomic::AtomicUsize,
    }

    impl ParticipantStore for FaultStore {
        fn append(
            &self,
            id: crate::SagaId,
            event: crate::ParticipantEvent,
        ) -> Result<u64, crate::JournalError> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(crate::JournalError::Storage(
                    "injected append failure".into(),
                ));
            }
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.append(id, event)
        }
        fn read(&self, id: crate::SagaId) -> Result<Vec<crate::JournalEntry>, crate::JournalError> {
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.read(id)
        }
        fn list_sagas(&self) -> Result<Vec<crate::SagaId>, crate::JournalError> {
            panic!("startup must use unresolved index")
        }
        fn prune(&self, id: crate::SagaId) -> Result<(), crate::JournalError> {
            self.inner.prune(id)
        }
        fn append_batch(
            &self,
            id: crate::SagaId,
            events: Vec<crate::ParticipantEvent>,
        ) -> Result<Vec<u64>, crate::JournalError> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(crate::JournalError::Storage(
                    "injected batch failure".into(),
                ));
            }
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.append_batch(id, events)
        }
        fn admit_and_record_intent(
            &self,
            c: &crate::SagaContext,
            key: &str,
            trigger: &str,
        ) -> Result<crate::StoreAdmission, crate::JournalError> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(crate::JournalError::Storage(
                    "injected admission failure".into(),
                ));
            }
            let result = self.inner.admit_and_record_intent(c, key, trigger)?;
            if matches!(result, crate::StoreAdmission::Admitted) {
                self.commits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(result)
        }
        fn unresolved_saga_ids(&self) -> Result<Vec<crate::SagaId>, crate::JournalError> {
            self.scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.unresolved_saga_ids()
        }
        fn compact_terminal_detail(&self, limit: usize) -> Result<usize, crate::JournalError> {
            self.inner.compact_terminal_detail(limit)
        }
        fn prune_resolved_before(
            &self,
            cutoff: u64,
            limit: usize,
        ) -> Result<usize, crate::JournalError> {
            self.inner.prune_resolved_before(cutoff, limit)
        }
        fn check_and_mark(
            &self,
            id: crate::SagaId,
            key: &str,
        ) -> Result<bool, crate::JournalError> {
            self.inner.check_and_mark(id, key)
        }
        fn remove_processed(
            &self,
            id: crate::SagaId,
            key: &str,
        ) -> Result<(), crate::JournalError> {
            self.inner.remove_processed(id, key)
        }
    }

    fn accepted_actor() -> (
        WorkflowTestActor,
        crate::SagaContext,
        crate::StepExecutionId,
    ) {
        let mut actor = WorkflowTestActor::default();
        let context = DeterministicContextBuilder::default().build();
        let execution = crate::StepExecutionId::new("accepted");
        super::accept_workflow_step(
            &mut actor,
            context.clone(),
            "worker".into(),
            execution.clone(),
            crate::AcceptedStepPolicy {
                idle_timeout: std::time::Duration::from_secs(1),
                hard_timeout: std::time::Duration::from_secs(10),
                timeout_outcome: crate::AcceptedStepTimeoutOutcome::QuarantineSaga,
            },
            vec![7; 4096],
            vec![9; 4096],
        )
        .unwrap();
        (actor, context, execution)
    }

    #[test]
    fn startup_reads_each_unresolved_run_once_across_registrations() {
        let (mut actor, mut context, _) = accepted_actor();
        context.saga_id = crate::SagaId::new(2);
        context.saga_type = "another".into();
        super::accept_workflow_step(
            &mut actor,
            context.clone(),
            "worker".into(),
            crate::StepExecutionId::new("another"),
            crate::AcceptedStepPolicy {
                idle_timeout: std::time::Duration::from_secs(1),
                hard_timeout: std::time::Duration::from_secs(10),
                timeout_outcome: crate::AcceptedStepTimeoutOutcome::QuarantineSaga,
            },
            vec![7],
            vec![],
        )
        .unwrap();
        let mut support = SagaParticipantSupport::new(actor.saga.store);
        support
            .store
            .reads
            .store(0, std::sync::atomic::Ordering::Relaxed);
        let events = super::collect_startup_recovery_events_for_participants(
            &mut support,
            &[
                (context.step_name.as_ref(), "order_lifecycle"),
                (context.step_name.as_ref(), "another"),
            ],
        )
        .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(support.accepted_workflow_steps.len(), 2);
        assert_eq!(
            support
                .store
                .scans
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            support
                .store
                .reads
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn renewal_failure_keeps_old_deadline() {
        let (mut actor, context, execution) = accepted_actor();
        let old = actor.saga.accepted_workflow_steps[&context.saga_id].deadline_at_millis;
        actor
            .saga
            .store
            .fail
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            super::record_accepted_workflow_step_progress(
                &mut actor,
                context.saga_id,
                execution,
                old
            )
            .is_err()
        );
        assert_eq!(
            actor.saga.accepted_workflow_steps[&context.saga_id].deadline_at_millis,
            old
        );
    }

    #[test]
    fn renewal_records_compact_payload_free_evidence() {
        let (mut actor, context, execution) = accepted_actor();
        let now = actor.saga.accepted_workflow_steps[&context.saga_id].deadline_at_millis;
        super::record_accepted_workflow_step_progress(&mut actor, context.saga_id, execution, now)
            .unwrap();
        let entries = actor.saga.store.read(context.saga_id).unwrap();
        assert!(matches!(
            entries.last().unwrap().event,
            crate::ParticipantEvent::AcceptedDeadlineRenewed {
                is_compensation: false,
                ..
            }
        ));
    }

    #[test]
    fn low_level_accept_admits_successor_before_old_terminal_cache() {
        use crate::SagaStateExt;
        let mut actor = WorkflowTestActor::default();
        let mut context = DeterministicContextBuilder::default().build();
        actor
            .retain_terminal_saga_strict(
                &context,
                crate::ParticipantTerminalKind::Completed,
                "done",
            )
            .unwrap();
        context.saga_started_at_millis += 1;
        assert!(
            super::accept_workflow_step(
                &mut actor,
                context,
                "worker".into(),
                crate::StepExecutionId::new("new"),
                crate::AcceptedStepPolicy {
                    idle_timeout: std::time::Duration::from_secs(1),
                    hard_timeout: std::time::Duration::from_secs(10),
                    timeout_outcome: crate::AcceptedStepTimeoutOutcome::QuarantineSaga
                },
                vec![7],
                vec![]
            )
            .is_ok()
        );
    }

    struct WorkflowTestActor {
        saga: SagaParticipantSupport<FaultStore>,
        active: Option<ActiveSagaExecution>,
        alpha_calls: usize,
        beta_calls: usize,
        inputs: Vec<Vec<u8>>,
        kind: u8,
        dispatched: usize,
        terminal_hooks: usize,
        dependencies: DependencySpec,
    }

    impl Default for WorkflowTestActor {
        fn default() -> Self {
            Self {
                saga: SagaParticipantSupport::new(FaultStore::default()),
                active: None,
                alpha_calls: 0,
                beta_calls: 0,
                inputs: Vec::new(),
                kind: 0,
                dispatched: 0,
                terminal_hooks: 0,
                dependencies: DependencySpec::OnSagaStart,
            }
        }
    }

    impl HasSagaParticipantSupport for WorkflowTestActor {
        type Store = FaultStore;

        fn saga_support(&self) -> &SagaParticipantSupport<Self::Store> {
            &self.saga
        }

        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Store> {
            &mut self.saga
        }
    }

    impl HasActiveSagaExecution for WorkflowTestActor {
        fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
            &mut self.active
        }
    }

    struct AlphaWorkflow(DependencySpec);
    struct BetaWorkflow;

    static ALPHA_WORKFLOW: AlphaWorkflow = AlphaWorkflow(DependencySpec::OnSagaStart);
    static AFTER_WORKFLOW: AlphaWorkflow = AlphaWorkflow(DependencySpec::After("upstream"));
    static ANY_WORKFLOW: AlphaWorkflow =
        AlphaWorkflow(DependencySpec::AnyOf(&["upstream", "other"]));
    static ALLOF_WORKFLOW: AlphaWorkflow =
        AlphaWorkflow(DependencySpec::AllOf(&["upstream", "other"]));
    static BETA_WORKFLOW: BetaWorkflow = BetaWorkflow;
    static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<WorkflowTestActor>; 5] = [
        &ALPHA_WORKFLOW,
        &BETA_WORKFLOW,
        &AFTER_WORKFLOW,
        &ANY_WORKFLOW,
        &ALLOF_WORKFLOW,
    ];

    impl SagaWorkflowParticipant<WorkflowTestActor> for AlphaWorkflow {
        fn step_name(&self) -> &'static str {
            "alpha_step"
        }

        fn saga_types(&self) -> &[&'static str] {
            match self.0 {
                DependencySpec::OnSagaStart => &["alpha_workflow"],
                DependencySpec::After(_) => &["after_workflow"],
                DependencySpec::AnyOf(_) => &["any_workflow"],
                DependencySpec::AllOf(_) => &["allof_workflow"],
            }
        }

        fn execute_step(
            &self,
            actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _input: &[u8],
        ) -> Result<crate::StepOutput, crate::StepError> {
            actor.alpha_calls += 1;
            actor.inputs.push(_input.to_vec());
            match actor.kind {
                0 => Ok(StepOutput::Completed {
                    output: vec![42],
                    compensation_data: vec![9],
                }),
                1 => Ok(StepOutput::CompletedWithEffect {
                    output: vec![42],
                    compensation_data: vec![9],
                    effect: "dispatch".into(),
                }),
                2 => Ok(StepOutput::Accepted {
                    execution_id: crate::StepExecutionId::new("work"),
                    policy: crate::AcceptedStepPolicy {
                        idle_timeout: std::time::Duration::from_secs(1),
                        hard_timeout: std::time::Duration::from_secs(10),
                        timeout_outcome: crate::AcceptedStepTimeoutOutcome::QuarantineSaga,
                    },
                    compensation_data: vec![9],
                }),
                _ => Err(crate::StepError::Terminal {
                    reason: "failed".into(),
                }),
            }
        }

        fn compensate_step(
            &self,
            _actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _compensation_data: &[u8],
        ) -> Result<crate::CompensationOutput, crate::CompensationError> {
            Ok(crate::CompensationOutput::Completed)
        }
        fn depends_on(&self) -> DependencySpec {
            self.0.clone()
        }

        fn dispatch_effect(
            &self,
            actor: &mut WorkflowTestActor,
            request: &crate::EffectDispatchRequest<'_>,
        ) -> Result<crate::EffectDispatchOutcome, crate::EffectDispatchError> {
            let entries = actor.saga.store.read(request.context.saga_id).unwrap();
            assert!(
                matches!(&entries.last().unwrap().event, crate::ParticipantEvent::StepExecutionCompleted { output, compensation_data, .. } if output == &[42] && compensation_data == &[9])
            );
            actor.dispatched += 1;
            Ok(crate::EffectDispatchOutcome::Durable {
                receipt: "receipt".into(),
            })
        }
        fn on_saga_completed(&self, actor: &mut WorkflowTestActor, _: &crate::SagaContext) {
            actor.terminal_hooks += 1;
        }
        fn on_saga_failed(&self, actor: &mut WorkflowTestActor, _: &crate::SagaContext, _: &str) {
            actor.terminal_hooks += 1;
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
                compensation_data: vec![8],
            })
        }

        fn compensate_step(
            &self,
            _actor: &mut WorkflowTestActor,
            _context: &crate::SagaContext,
            _compensation_data: &[u8],
        ) -> Result<crate::CompensationOutput, crate::CompensationError> {
            Ok(crate::CompensationOutput::Completed)
        }
    }

    impl HasSagaWorkflowParticipants for WorkflowTestActor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &WORKFLOWS
        }
    }

    impl crate::SagaParticipant for WorkflowTestActor {
        fn step_name(&self) -> &str {
            "alpha_step"
        }
        fn saga_types(&self) -> &[&'static str] {
            &[
                "alpha_workflow",
                "after_workflow",
                "any_workflow",
                "allof_workflow",
            ]
        }
        fn depends_on(&self) -> DependencySpec {
            self.dependencies.clone()
        }
        fn execute_step(
            &mut self,
            context: &crate::SagaContext,
            input: &[u8],
        ) -> Result<StepOutput, crate::StepError> {
            ALPHA_WORKFLOW.execute_step(self, context, input)
        }
        fn compensate_step(
            &mut self,
            context: &crate::SagaContext,
            data: &[u8],
        ) -> Result<crate::CompensationOutput, crate::CompensationError> {
            ALPHA_WORKFLOW.compensate_step(self, context, data)
        }
        fn dispatch_effect(
            &mut self,
            request: &crate::EffectDispatchRequest<'_>,
        ) -> Result<crate::EffectDispatchOutcome, crate::EffectDispatchError> {
            ALPHA_WORKFLOW.dispatch_effect(self, request)
        }
        fn on_saga_completed(&mut self, _: &crate::SagaContext) {
            self.terminal_hooks += 1;
        }
        fn on_saga_failed(&mut self, _: &crate::SagaContext, _: &str) {
            self.terminal_hooks += 1;
        }
    }

    impl crate::AsyncSagaParticipant for WorkflowTestActor {
        fn step_name(&self) -> &str {
            "alpha_step"
        }
        fn saga_types(&self) -> &[&'static str] {
            <Self as crate::SagaParticipant>::saga_types(self)
        }
        fn depends_on(&self) -> DependencySpec {
            self.dependencies.clone()
        }
        fn execute_step<'a>(
            &'a mut self,
            context: &'a crate::SagaContext,
            input: &'a [u8],
        ) -> crate::SagaBoxFuture<'a, Result<StepOutput, crate::StepError>> {
            Box::pin(async move { ALPHA_WORKFLOW.execute_step(self, context, input) })
        }
        fn compensate_step<'a>(
            &'a mut self,
            context: &'a crate::SagaContext,
            data: &'a [u8],
        ) -> crate::SagaBoxFuture<'a, Result<crate::CompensationOutput, crate::CompensationError>>
        {
            Box::pin(async move { ALPHA_WORKFLOW.compensate_step(self, context, data) })
        }
        fn dispatch_effect<'a>(
            &'a mut self,
            request: &'a crate::EffectDispatchRequest<'a>,
        ) -> crate::SagaBoxFuture<
            'a,
            Result<crate::EffectDispatchOutcome, crate::EffectDispatchError>,
        > {
            Box::pin(async move { ALPHA_WORKFLOW.dispatch_effect(self, request) })
        }
        fn on_saga_completed(&mut self, _: &crate::SagaContext) {
            self.terminal_hooks += 1;
        }
        fn on_saga_failed(&mut self, _: &crate::SagaContext, _: &str) {
            self.terminal_hooks += 1;
        }
    }

    async fn drive_mode(
        actor: &mut WorkflowTestActor,
        mode: u8,
        event: crate::SagaChoreographyEvent,
    ) -> Vec<crate::SagaChoreographyEvent> {
        let mut emitted = Vec::new();
        match mode {
            0 => super::apply_sync_participant_saga_ingress_with_hooks(
                actor,
                event,
                |_, _| {},
                |_| panic!("invalid transition"),
                |_, event| emitted.push(event.clone()),
            ),
            1 => {
                super::apply_async_participant_saga_ingress_with_hooks(
                    actor,
                    event,
                    |_, _| {},
                    |_| panic!("invalid transition"),
                    |_, event| emitted.push(event.clone()),
                )
                .await
            }
            _ => super::apply_sync_workflow_participant_saga_ingress_with_hooks(
                actor,
                event,
                |_, _| {},
                |_| panic!("invalid transition"),
                |_, event| emitted.push(event.clone()),
            ),
        }
        emitted
    }

    #[tokio::test]
    async fn protocol_modes_preserve_original_input_and_commit_boundaries() {
        for kind in 0..=3 {
            for (dependency, saga_type) in [
                (DependencySpec::OnSagaStart, "alpha_workflow"),
                (DependencySpec::After("upstream"), "after_workflow"),
                (
                    DependencySpec::AnyOf(&["upstream", "other"]),
                    "any_workflow",
                ),
                (
                    DependencySpec::AllOf(&["upstream", "other"]),
                    "allof_workflow",
                ),
            ] {
                for reverse in [false, true] {
                    let mut expected_history = None;
                    let mut expected_emitted = None;
                    for mode in 0..3 {
                        let mut actor = WorkflowTestActor {
                            kind,
                            dependencies: dependency.clone(),
                            ..Default::default()
                        };
                        let context = DeterministicContextBuilder::default()
                            .with_saga_type(saga_type)
                            .build();
                        let mut emitted = Vec::new();
                        if dependency.is_on_saga_start() {
                            emitted.extend(
                                drive_mode(
                                    &mut actor,
                                    mode,
                                    crate::SagaChoreographyEvent::SagaStarted {
                                        context: context.clone(),
                                        payload: vec![7],
                                    },
                                )
                                .await,
                            );
                            assert_eq!(
                                actor
                                    .saga
                                    .store
                                    .commits
                                    .load(std::sync::atomic::Ordering::Relaxed),
                                if kind == 1 { 3 } else { 2 }
                            );
                        } else {
                            for name in if reverse {
                                ["other", "upstream"]
                            } else {
                                ["upstream", "other"]
                            } {
                                let mut predecessor = context.clone();
                                predecessor.step_name = name.into();
                                emitted.extend(
                                    drive_mode(
                                        &mut actor,
                                        mode,
                                        crate::SagaChoreographyEvent::StepCompleted {
                                            context: predecessor,
                                            output: vec![55],
                                            saga_input: vec![7],
                                            compensation_available: false,
                                        },
                                    )
                                    .await,
                                );
                            }
                        }
                        assert_eq!(actor.alpha_calls, 1);
                        assert_eq!(
                            actor.inputs,
                            vec![if dependency.is_on_saga_start()
                                || dependency.prefers_original_saga_input()
                            {
                                vec![7]
                            } else {
                                vec![55]
                            }]
                        );
                        assert_eq!(actor.dispatched, usize::from(kind == 1));
                        if kind == 2 {
                            assert_eq!(
                                actor.saga.accepted_workflow_steps[&context.saga_id].saga_input,
                                vec![7]
                            );
                            // Replaying an accepted trigger must not quarantine or execute twice.
                            let replay = if dependency.is_on_saga_start() {
                                crate::SagaChoreographyEvent::SagaStarted {
                                    context: context.clone(),
                                    payload: vec![7],
                                }
                            } else {
                                let mut c = context.clone();
                                c.step_name = "upstream".into();
                                crate::SagaChoreographyEvent::StepCompleted {
                                    context: c,
                                    output: vec![55],
                                    saga_input: vec![7],
                                    compensation_available: false,
                                }
                            };
                            assert!(drive_mode(&mut actor, mode, replay).await.is_empty());
                            assert_eq!(actor.alpha_calls, 1);
                            let completed = super::complete_accepted_workflow_step(
                                &mut actor,
                                context.saga_id,
                                crate::StepExecutionId::new("work"),
                                crate::AcceptedStepCompletion {
                                    completed_at_millis: crate::SagaContext::now_millis(),
                                    output: vec![42],
                                    compensation_data: vec![9],
                                },
                            )
                            .unwrap();
                            emitted.push(completed);
                        }
                        for event in &emitted {
                            if let crate::SagaChoreographyEvent::StepCompleted {
                                saga_input, ..
                            } = event
                            {
                                assert_eq!(saga_input, &[7]);
                            }
                        }
                        let history: Vec<_> = actor
                            .saga
                            .store
                            .read(context.saga_id)
                            .unwrap()
                            .iter()
                            .map(|row| std::mem::discriminant(&row.event))
                            .collect();
                        if let Some(expected) = &expected_history {
                            assert_eq!(&history, expected);
                        } else {
                            expected_history = Some(history);
                        }
                        let types: Vec<_> =
                            emitted.iter().map(|event| event.event_type()).collect();
                        if let Some(expected) = &expected_emitted {
                            assert_eq!(&types, expected);
                        } else {
                            expected_emitted = Some(types);
                        }
                        if kind <= 2 {
                            let recovered = super::collect_startup_recovery_events_for_saga_type(
                                &actor.saga.store,
                                "alpha_step",
                                saga_type,
                            )
                            .unwrap();
                            assert!(recovered.iter().any(|event| matches!(event, crate::SagaChoreographyEvent::StepCompleted { saga_input, output, .. } if saga_input == &[7] && output == &[42])));
                        }
                    }
                }
            }
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
        ) -> Result<crate::CompensationOutput, crate::CompensationError> {
            Ok(crate::CompensationOutput::Completed)
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
        ) -> Result<crate::CompensationOutput, crate::CompensationError> {
            Ok(crate::CompensationOutput::Completed)
        }
    }

    impl HasSagaWorkflowParticipants for DuplicateWorkflowTestActor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &DUPLICATE_WORKFLOWS
        }
    }

    #[test]
    fn default_runtime_dir_uses_test_tmp_path_when_env_is_missing() {
        let runtime_dir = default_runtime_dir_from_value(None, "fallback-runtime");
        assert!(
            runtime_dir.to_string_lossy().contains("target/test-tmp"),
            "expected test runtime dir under target/test-tmp, got {:?}",
            runtime_dir
        );
    }

    #[test]
    fn default_runtime_dir_uses_provided_env_value() {
        let runtime_dir = default_runtime_dir_from_value(
            Some(std::ffi::OsString::from("/tmp/durability-runtime-dir")),
            "unused-fallback",
        );
        assert_eq!(runtime_dir, PathBuf::from("/tmp/durability-runtime-dir"));
    }

    #[test]
    fn workflow_ingress_routes_to_matching_workflow_spec() {
        use std::cell::RefCell;

        let mut actor = WorkflowTestActor::default();
        let emitted = RefCell::new(Vec::new());
        let event = crate::SagaChoreographyEvent::SagaStarted {
            context: DeterministicContextBuilder::default()
                .with_saga_id(77)
                .with_saga_type("beta_workflow")
                .with_step_name("beta_step")
                .build(),
            payload: Vec::new(),
        };

        apply_sync_workflow_participant_saga_ingress_with_hooks(
            &mut actor,
            event,
            |_actor, _event| {},
            |event| emitted.borrow_mut().push(event.clone()),
            |_actor, event| emitted.borrow_mut().push(event.clone()),
        );

        assert_eq!(actor.alpha_calls, 0);
        assert_eq!(actor.beta_calls, 1);
        assert_eq!(actor.saga.accepted_workflow_step_count(), 0);
        let emitted = emitted.into_inner();
        let started_index = emitted
        .iter()
        .position(|event| matches!(event, crate::SagaChoreographyEvent::StepStarted { context } if context.step_name.as_ref() == "beta_step"))
        .expect("workflow ingress should emit step started");
        assert!(
            !emitted.iter().any(|event| matches!(
                event,
                crate::SagaChoreographyEvent::StepAccepted { context, .. }
                    if context.step_name.as_ref() == "beta_step"
            )),
            "sync workflow ingress must not emit accepted-step events: {emitted:?}"
        );
        let completed_index = emitted
            .iter()
            .position(|event| matches!(event, crate::SagaChoreographyEvent::StepCompleted { context, .. } if context.step_name.as_ref() == "beta_step"))
            .expect("workflow ingress should emit step completed");
        assert!(
            started_index < completed_index,
            "workflow ingress should emit started before completed: {emitted:?}"
        );
        let entries = actor
            .saga
            .store
            .read(crate::SagaId::new(77))
            .expect("journal read should succeed");
        assert!(matches!(
            entries.as_slice(),
            [
                crate::JournalEntry {
                    event: crate::ParticipantEvent::ParticipantRunRecorded { .. },
                    ..
                },
                _,
                _,
                // A plain completion is one strict proof append carrying the full
                // result; no raw result row precedes it.
                crate::JournalEntry {
                    event: crate::ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome },
                    ..
                }
            ] if outcome.compensation_data == [8]
        ));
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

/// Property-based and chaos (loom) tests for the runtime FSM.
///
/// These close the largest testing gap in the crate: the runtime state machine
/// (transition validator, journal-replay recovery, and the dedupe atomicity
/// contract) was previously exercised only by hand-written integration tests.
/// The modules below pin the full (state × event) transition matrix and the
/// replay contract with `proptest`, and model-check the dedupe check-and-mark
/// atomicity with `loom`.
#[cfg(test)]
mod property_tests {
    use super::{
        JournalEntry, ParticipantEvent, SagaChoreographyEvent, SagaStateEntry,
        is_valid_emitted_transition, recover_accepted_workflow_step_from_entries,
    };
    use crate::{AcceptedStepTimeoutOutcome, SagaContext, SagaId, SagaParticipantState};
    use proptest::prelude::*;

    /// Fixed context so generators only vary the FSM-relevant discriminants.
    fn ctx(saga_id: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(saga_id),
            saga_type: "pt".into(),
            step_name: "pt_step".into(),
            correlation_id: 1,
            causation_id: 1,
            trace_id: 1,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0u8; 32],
            saga_started_at_millis: 1_000,
            event_timestamp_millis: 2_000,
        }
    }

    /// Build every `SagaStateEntry` variant (and `None`) by index.
    ///
    /// Index map: 0=None, 1=Idle, 2=Triggered, 3=Executing, 4=Completed,
    /// 5=Failed, 6=Compensating, 7=Compensated, 8=Quarantined.
    fn entry_for(idx: u8) -> Option<SagaStateEntry> {
        use SagaStateEntry as E;
        let idle = SagaParticipantState::new(
            SagaId::new(7),
            "pt".into(),
            "pt_step".into(),
            1,
            1,
            [0u8; 32],
            1_000,
        );
        Some(match idx {
            0 => return None,
            1 => E::Idle(idle),
            2 => E::Triggered(idle.trigger("e", 1_001)),
            3 => E::Executing(idle.trigger("e", 1_001).start_execution(1_002)),
            4 => E::Completed(idle.trigger("e", 1_001).start_execution(1_002).complete(
                vec![],
                vec![],
                1_003,
            )),
            5 => E::Failed(idle.trigger("e", 1_001).start_execution(1_002).fail(
                "err".into(),
                false,
                1_004,
            )),
            6 => E::Compensating(
                idle.trigger("e", 1_001)
                    .start_execution(1_002)
                    .complete(vec![], vec![], 1_003)
                    .start_compensation(1_005),
            ),
            7 => E::Compensated(
                idle.trigger("e", 1_001)
                    .start_execution(1_002)
                    .complete(vec![], vec![], 1_003)
                    .start_compensation(1_005)
                    .complete_compensation(1_006),
            ),
            8 => E::Quarantined(
                idle.trigger("e", 1_001)
                    .start_execution(1_002)
                    .quarantine("r".into(), 1_007),
            ),
            _ => unreachable!("entry index out of range: {idx}"),
        })
    }

    /// Build a representative `SagaChoreographyEvent` per validator branch.
    ///
    /// Index map: 0=StepAccepted, 1=StepCompleted, 2=StepFailed,
    /// 3=CompensationCompleted, 4=CompensationFailed, 5=SagaQuarantined,
    /// 6=StepStarted (stands in for the `_ => true` wildcard class).
    fn event_for(idx: u8) -> SagaChoreographyEvent {
        let c = ctx(7);
        match idx {
            0 => SagaChoreographyEvent::StepAccepted {
                context: c,
                participant_id: "pid".into(),
                execution_id: crate::StepExecutionId::new("exec"),
                deadline_at_millis: 5_000,
                hard_deadline_at_millis: 9_000,
                timeouts_enabled: true,
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                compensation_available: false,
            },
            1 => SagaChoreographyEvent::StepCompleted {
                context: c,
                output: vec![],
                saga_input: vec![],
                compensation_available: false,
            },
            2 => SagaChoreographyEvent::StepFailed {
                context: c,
                participant_id: "pid".into(),
                error_code: None,
                error: "err".into(),
                requires_compensation: false,
            },
            3 => SagaChoreographyEvent::CompensationCompleted { context: c },
            4 => SagaChoreographyEvent::CompensationFailed {
                context: c,
                participant_id: "pid".into(),
                error: "err".into(),
                is_ambiguous: false,
            },
            5 => SagaChoreographyEvent::SagaQuarantined {
                context: c,
                reason: "r".into(),
                step: "step".into(),
                participant_id: "pid".into(),
            },
            6 => SagaChoreographyEvent::StepStarted { context: c },
            _ => unreachable!("event index out of range: {idx}"),
        }
    }

    /// Authoritative reference table for `is_valid_emitted_transition`.
    ///
    /// This closure IS the FSM transition contract. Any change to the
    /// validator must be a deliberate, reflected change here — the property
    /// test below fails loudly on accidental drift.
    fn reference_valid(entry_idx: u8, event_idx: u8) -> bool {
        match event_idx {
            // StepAccepted: only valid once execution has begun/ended.
            0 => matches!(entry_idx, 3 | 4 | 5 | 8),
            // StepCompleted: only from Completed.
            1 => entry_idx == 4,
            // StepFailed: only from Failed.
            2 => entry_idx == 5,
            // CompensationCompleted: only from Compensated (idempotent re-ack).
            3 => entry_idx == 7,
            // CompensationFailed: non-ambiguous errors are Failed; ambiguous errors are Quarantined.
            4 => matches!(entry_idx, 5 | 8),
            // SagaQuarantined: fail-closed signal, always publishable (absent or
            // retained-completed state must not suppress it).
            5 => true,
            // Wildcard class (StepStarted, SagaStarted, ...): always allowed.
            _ => true,
        }
    }

    proptest! {
        /// The validator must agree with the reference table for every
        /// (state, event) pair. With a 9-state × 7-event space this is an
        /// exhaustive matrix lock.
        #[test]
        fn transition_validator_matches_reference_table(
            entry_idx in 0u8..9u8,
            event_idx in 0u8..7u8,
        ) {
            let entry = entry_for(entry_idx);
            let event = event_for(event_idx);
            let actual = is_valid_emitted_transition(entry.as_ref(), &event);
            let expected = reference_valid(entry_idx, event_idx);
            prop_assert_eq!(
                actual, expected,
                "validator drift at entry_idx={} event_idx={}", entry_idx, event_idx
            );
        }

        /// No guarded progress/terminal event is ever valid from a
        /// non-matching or pre-execution state (None/Idle/Triggered/
        /// Compensating). This is the safety invariant that prevents phantom
        /// transitions early in the lifecycle.
        #[test]
        fn guarded_events_rejected_from_pre_terminal_states(
            entry_idx in 0u8..9u8,
            event_idx in 0u8..5u8, // guarded events only (SagaQuarantined is always valid)
        ) {
            let is_pre_terminal = matches!(entry_idx, 0 | 1 | 2 | 6);
            let entry = entry_for(entry_idx);
            let event = event_for(event_idx);
            let actual = is_valid_emitted_transition(entry.as_ref(), &event);
            if is_pre_terminal {
                prop_assert!(!actual, "guarded event {} accepted from pre-terminal state {}", event_idx, entry_idx);
            }
        }
    }

    /// Build a `JournalEntry` wrapping the `ParticipantEvent` variant at `idx`.
    ///
    /// Variant index map (mirrors declaration order in `ParticipantEvent`):
    /// 0=StepTriggered, 1=StepTriggered, 2=StepExecutionStarted,
    /// 3=StepExecutionCompleted, 4=StepExecutionFailed, 5=CompensationStarted,
    /// 6=CompensationCompleted, 7=CompensationFailed, 8=Quarantined,
    /// 9=AcceptedStepRecorded.
    fn participant_event_for(idx: u8) -> ParticipantEvent {
        let now = 1_234u64;
        match idx {
            0 => ParticipantEvent::StepTriggered {
                triggering_event: "registration_removed".into(),
                triggered_at_millis: 0,
            },
            1 => ParticipantEvent::StepTriggered {
                triggering_event: "e".into(),
                triggered_at_millis: now,
            },
            2 => ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: now,
            },
            3 => ParticipantEvent::StepExecutionCompleted {
                output: vec![],
                compensation_data: vec![],
                completed_at_millis: now,
            },
            4 => ParticipantEvent::StepExecutionFailed {
                error: "err".into(),
                requires_compensation: false,
                failed_at_millis: now,
            },
            5 => ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: now,
            },
            6 => ParticipantEvent::CompensationCompleted {
                completed_at_millis: now,
            },
            7 => ParticipantEvent::CompensationFailed {
                error: "err".into(),
                is_ambiguous: false,
                failed_at_millis: now,
            },
            8 => ParticipantEvent::Quarantined {
                reason: "r".into(),
                quarantined_at_millis: now,
            },
            9 => ParticipantEvent::AcceptedStepRecorded {
                context: ctx(7),
                participant_id: "pid".into(),
                execution_id: crate::StepExecutionId::new("exec"),
                idle_timeout_millis: 1_000,
                hard_timeout_millis: 5_000,
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                saga_input: Vec::new(),
                compensation_data: Vec::new(),
                accepted_at_millis: now,
                deadline_at_millis: now + 1_000,
                hard_deadline_at_millis: now + 5_000,
            },
            _ => unreachable!("participant event index out of range: {idx}"),
        }
    }

    fn journal_entry_for(seq: u64, idx: u8) -> JournalEntry {
        JournalEntry {
            sequence: seq,
            recorded_at_millis: 1_234,
            event: participant_event_for(idx),
        }
    }

    /// Reference model of `recover_accepted_workflow_step_from_entries`:
    /// returns whether a recoverable accepted step survives the log. An
    /// `AcceptedStepRecorded` is superseded (cleared) by any later terminal
    /// participant event.
    fn reference_recovered_some(seq: &[u8]) -> bool {
        let mut some = false;
        for &idx in seq {
            match idx {
                9 => some = true,      // AcceptedStepRecorded
                3..=8 => some = false, // terminal/clearing events
                _ => {}                // registration/trigger/start: unchanged
            }
        }
        some
    }

    proptest! {
        /// Recovery must be a pure, deterministic function of the journaled
        /// event log: re-running it on identical entries yields the same
        /// result, and that result matches the reference model for every
        /// generated sequence (including interleaved accept/complete/quarantine).
        #[test]
        fn recovery_is_pure_function_of_log(
            seq in proptest::collection::vec(0u8..10u8, 0..24),
        ) {
            let entries: Vec<JournalEntry> = seq
                .iter()
                .enumerate()
                .map(|(i, &idx)| journal_entry_for(i as u64, idx))
                .collect();

            let first = recover_accepted_workflow_step_from_entries(&entries);
            let again = recover_accepted_workflow_step_from_entries(&entries);

            // Determinism: identical input must produce identical output.
            prop_assert_eq!(
                first.is_some(),
                again.is_some(),
                "recovery is non-deterministic for sequence {:?}", seq
            );
            // Correctness: observable result agrees with the reference model.
            prop_assert_eq!(
                first.is_some(),
                reference_recovered_some(&seq),
                "recovery disagrees with model for sequence {:?}", seq
            );
        }

        /// Recovery convergence: appending any clearing event after an accept
        /// must drop the recovered step (the accepted step is no longer live).
        #[test]
        fn recovery_clears_accepted_step_after_terminal_event(
            clearing in 3u8..9u8, // one of the clearing variants
        ) {
            let entries = vec![
                journal_entry_for(0, 9),          // AcceptedStepRecorded
                journal_entry_for(1, clearing),   // terminal/clearing event
            ];
            let recovered = recover_accepted_workflow_step_from_entries(&entries);
            prop_assert!(
                recovered.is_none(),
                "accepted step should be cleared by clearing event {}", clearing
            );
        }
    }
}

/// Loom (exhaustive concurrency model-checking) scaffold for the dedupe
/// `check_and_mark` atomicity contract.
///
/// Every participant store backend MUST make check-and-mark atomic:
/// across all thread interleavings, exactly one concurrent first-mark for a
/// given (saga_id, key) wins, and every duplicate observes a miss. The in-tree
/// `InMemoryParticipantStore` uses the same canonical mutex-protected check-and-mark
/// operation modeled below. Real backends (LMDB/heed, SQL, KV stores) use
/// shared mutable state and SHOULD be exercised here.
///
/// This module models the canonical shared-state algorithm a backend would
/// write and proves it race-free under loom's exhaustive scheduler. Extend it
/// by swapping `MutexDedupe` for a wrapper around your real backend.
///
/// Run with: `cargo test --features loom`.
#[cfg(all(test, feature = "loom"))]
mod loom_dedupe_model {
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Reference check-and-mark implementation using loom-modeled primitives.
    /// Structurally identical to what a real Mutex/DB-backed dedupe store does.
    struct MutexDedupe {
        seen: loom::sync::Mutex<HashMap<(u64, String), ()>>,
    }

    impl MutexDedupe {
        fn new() -> Self {
            Self {
                seen: loom::sync::Mutex::new(HashMap::new()),
            }
        }

        /// Atomic check-and-mark — the contract under test.
        fn check_and_mark(&self, saga_id: u64, key: &str) -> bool {
            let mut guard = self.seen.lock().unwrap();
            let entry = (saga_id, key.to_string());
            match guard.entry(entry) {
                std::collections::hash_map::Entry::Occupied(_) => false,
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(());
                    true
                }
            }
        }
    }

    #[test]
    fn concurrent_check_and_mark_yields_exactly_one_winner() {
        loom::model(|| {
            let store = Arc::new(MutexDedupe::new());
            let store_b = store.clone();

            let h1 = loom::thread::spawn(move || store.check_and_mark(42, "reserve_inventory"));
            let h2 = loom::thread::spawn(move || store_b.check_and_mark(42, "reserve_inventory"));

            let (a, b) = (h1.join().unwrap(), h2.join().unwrap());

            // Idempotency invariant holds under EVERY interleaving: exactly
            // one concurrent first-mark wins, the duplicate observes a miss.
            // If this assertion ever fires, the dedupe backend is not atomic
            // and double-execution of saga steps is possible.
            assert!(
                a ^ b,
                "dedupe atomicity violated: expected exactly one winner, got ({a}, {b})"
            );
        });
    }

    #[test]
    fn distinct_keys_are_independent() {
        loom::model(|| {
            let store = Arc::new(MutexDedupe::new());
            let store_b = store.clone();

            let h1 = loom::thread::spawn(move || store.check_and_mark(42, "a"));
            let h2 = loom::thread::spawn(move || store_b.check_and_mark(42, "b"));

            let (a, b) = (h1.join().unwrap(), h2.join().unwrap());
            // Distinct keys must both win — never falsely deduplicated.
            assert!(a && b, "distinct keys falsely collided: ({a}, {b})");
        });
    }
}
