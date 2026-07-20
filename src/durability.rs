use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::state_ext::SagaStateStoreError;
use crate::support::{AcceptedWorkflowCompensation, AcceptedWorkflowStep};
use crate::{
    AcceptedCompensationCompletion, AcceptedCompensationFailure, AcceptedStepCompletion,
    AcceptedStepError, AcceptedStepFailure, AcceptedStepPolicy, AcceptedStepTimeoutOutcome,
    AsyncSagaParticipant, CompensationOutput, DedupeError, HasSagaParticipantSupport,
    HasSagaWorkflowParticipants, JournalEntry, JournalError, ParticipantDedupeStore,
    ParticipantEvent, ParticipantJournal, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt, SagaWorkflowParticipant,
    StepExecutionId, StepOutput, handle_async_saga_event_with_emit, handle_saga_event_with_emit,
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
    ResetDedupe {
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
        SagaChoreographyEvent::SagaQuarantined { .. } => {
            matches!(entry, Some(SagaStateEntry::Quarantined(_)))
        }
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

    if actor.saga_support().terminal_sagas.contains(&saga_id) {
        return Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id,
        });
    }
    if actor
        .saga_support()
        .resolved_workflow_steps
        .contains(&(saga_id, execution_id.clone()))
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
        timeout_outcome,
        compensation_available,
    })
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
    let accepted = accepted_step_for_resolution(actor, saga_id, &execution_id)?;
    actor
        .record_event_strict(
            saga_id,
            ParticipantEvent::StepExecutionCompleted {
                output: completion.output.clone(),
                compensation_data: completion.compensation_data.clone(),
                completed_at_millis: completion.completed_at_millis,
            },
        )
        .map_err(|source| AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
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

    let mut context = accepted.context;
    context.event_timestamp_millis = completion.completed_at_millis;

    Ok(SagaChoreographyEvent::StepCompleted {
        context,
        output: completion.output,
        saga_input: accepted.saga_input,
        compensation_available: !completion.compensation_data.is_empty(),
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

pub fn record_accepted_workflow_step_progress<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
    now_millis: u64,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: HasSagaParticipantSupport,
{
    if actor.saga_support().terminal_sagas.contains(&saga_id) {
        return Err(AcceptedStepError::AlreadyTerminal {
            saga_id,
            execution_id,
        });
    }
    if actor
        .saga_support()
        .resolved_workflow_steps
        .contains(&(saga_id, execution_id.clone()))
    {
        return Err(AcceptedStepError::AlreadyResolved {
            saga_id,
            execution_id,
        });
    }
    let Some(accepted) = actor
        .saga_support_mut()
        .accepted_workflow_steps
        .get_mut(&saga_id)
    else {
        return Err(AcceptedStepError::NotFound {
            saga_id,
            execution_id,
        });
    };
    if accepted.execution_id != execution_id {
        return Err(AcceptedStepError::ExecutionIdMismatch {
            saga_id,
            expected: accepted.execution_id.clone(),
            actual: execution_id,
        });
    }
    let next_idle_deadline =
        now_millis.saturating_add(accepted.policy.idle_timeout.as_millis() as u64);
    accepted.deadline_at_millis = next_idle_deadline.min(accepted.hard_deadline_at_millis);
    let mut context = accepted.context.clone();
    context.event_timestamp_millis = now_millis;
    let mut accepted_snapshot = accepted.clone();
    accepted_snapshot.context = context.clone();
    let participant_id = accepted_snapshot.participant_id.clone();
    let deadline_at_millis = accepted.deadline_at_millis;
    let hard_deadline_at_millis = accepted.hard_deadline_at_millis;
    let timeout_outcome = accepted_snapshot.policy.timeout_outcome.clone();
    record_accepted_step_metadata(actor, &accepted_snapshot).map_err(|source| {
        AcceptedStepError::Durability {
            saga_id,
            execution_id: execution_id.clone(),
            error: format!("{source:?}").into(),
        }
    })?;
    Ok(SagaChoreographyEvent::StepAccepted {
        context,
        participant_id,
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis,
        timeout_outcome,
        compensation_available: !accepted_snapshot.compensation_data.is_empty(),
    })
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

pub fn recover_accepted_workflow_steps_for_saga_type<J, D>(
    support: &mut SagaParticipantSupport<J, D>,
    step_name: &'static str,
    saga_type: &'static str,
) -> Result<(), RecoveryCollectionError>
where
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
{
    let saga_ids = support
        .journal
        .list_sagas()
        .map_err(RecoveryCollectionError::ListSagas)?;
    let now_millis = SagaContext::now_millis();
    for saga_id in saga_ids {
        let entries = support
            .journal
            .read(saga_id)
            .map_err(|source| RecoveryCollectionError::ReadSaga { saga_id, source })?;
        if let Some(accepted) = recover_accepted_workflow_compensation_from_entries(&entries) {
            if accepted.context.saga_type.as_ref() != saga_type
                || accepted.context.step_name.as_ref() != step_name
                || accepted.deadline_at_millis < now_millis
                || accepted.hard_deadline_at_millis < now_millis
            {
                continue;
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
            continue;
        }
        if let Some(accepted) = recover_accepted_workflow_step_from_entries(&entries) {
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
                continue;
            }
            let execution_id = accepted.execution_id.clone();
            let forward_execution_resolved = retain_expired_for_compensation
                || accepted_step_failure_requiring_compensation(&entries).is_some();
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
                    .insert((saga_id, execution_id));
            }
        }
    }
    Ok(())
}

fn recover_accepted_workflow_compensation_from_entries(
    entries: &[JournalEntry],
) -> Option<AcceptedWorkflowCompensation> {
    let mut accepted = None;
    for entry in entries {
        match &entry.event {
            ParticipantEvent::AcceptedCompensationRecorded {
                context,
                participant_id,
                execution_id,
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
        match &entry.event {
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
            ParticipantEvent::CompensationCompleted { .. }
            | ParticipantEvent::CompensationFailed { .. }
            | ParticipantEvent::Quarantined { .. } => request = None,
            _ => {}
        }
    }
    request
}

fn recover_unstarted_compensation_request_from_entries(
    entries: &[JournalEntry],
) -> Option<SagaChoreographyEvent> {
    let mut request = None;
    for entry in entries {
        match &entry.event {
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
            | ParticipantEvent::Quarantined { .. } => request = None,
            ParticipantEvent::SagaRegistered { .. }
            | ParticipantEvent::StepTriggered { .. }
            | ParticipantEvent::StepExecutionStarted { .. }
            | ParticipantEvent::StepExecutionCompleted { .. }
            | ParticipantEvent::StepExecutionFailed { .. }
            | ParticipantEvent::AcceptedStepRecorded { .. } => {}
        }
    }
    request
}

fn recover_accepted_workflow_step_from_entries(
    entries: &[JournalEntry],
) -> Option<AcceptedWorkflowStep> {
    let mut accepted = None;
    for entry in entries {
        match &entry.event {
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
            | ParticipantEvent::CompensationFailed { .. } => {
                accepted = None;
            }
            ParticipantEvent::SagaRegistered { .. }
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
            | ParticipantEvent::CompensationFailed { .. } => {
                failure = None;
            }
            ParticipantEvent::SagaRegistered { .. }
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
    A: HasSagaParticipantSupport,
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
        .contains(&(saga_id, execution_id.clone()))
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
    Ok(accepted.clone())
}

fn take_accepted_step<A>(
    actor: &mut A,
    saga_id: SagaId,
    execution_id: StepExecutionId,
) -> Result<AcceptedWorkflowStep, AcceptedStepError>
where
    A: HasSagaParticipantSupport,
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
        .insert((saga_id, execution_id));
}

fn mark_matching_accepted_step_failed<A>(actor: &mut A, event: &SagaChoreographyEvent)
where
    A: HasSagaParticipantSupport,
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
    let Some(accepted) = actor
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&saga_id)
    else {
        return;
    };
    if accepted.context.step_name == context.step_name {
        mark_accepted_step_resolved(actor, saga_id, accepted.execution_id);
    } else {
        actor
            .saga_support_mut()
            .accepted_workflow_steps
            .insert(saga_id, accepted);
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
    mark_matching_accepted_step_failed(participant, &event);

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
    mark_matching_accepted_step_failed(actor, &event);
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
    if is_saga_started
        && actor.is_terminal_saga_start_replay(context.saga_id, context.saga_started_at_millis)
    {
        return;
    }
    if !is_saga_started && actor.is_terminal_saga_latched(context.saga_id) {
        return;
    }

    // Idempotency is marked before workflow execution so replayed upstream
    // events do not duplicate business side effects. With persistent dedupe,
    // a crash between this mark and the StepExecutionStarted journal entry is
    // fail-loud rather than resumed: startup recovery has no durable execution
    // intent and the terminal resolver must eventually fail/quarantine the saga.
    let dedupe_key = workflow_dedupe_key_for_event(&event);
    match actor.check_dedupe_strict(context.saga_id, &dedupe_key) {
        Ok(true) => {}
        Ok(false) => return,
        Err(err) => {
            emit(SagaChoreographyEvent::SagaQuarantined {
                context: context.next_step(workflow.step_name().into()),
                reason: format!("workflow dedupe check failed: {err:?}").into(),
                step: workflow.step_name().into(),
                participant_id: workflow.step_name().into(),
            });
            return;
        }
    }
    mark_matching_accepted_step_failed(actor, &event);

    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. }
            if workflow.depends_on().is_on_saga_start() =>
        {
            actor.record_saga_run_start(context.saga_id, context.saga_started_at_millis);
            actor.unlatch_terminal_saga(context.saga_id);
            actor.clear_in_memory_saga_run_tracking(context.saga_id);
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
            actor.record_saga_run_start(context.saga_id, context.saga_started_at_millis);
            actor.unlatch_terminal_saga(context.saga_id);
            actor.clear_in_memory_saga_run_tracking(context.saga_id);
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
            failed_step,
            reason,
            failure,
            steps_to_compensate,
            ..
        } => {
            if steps_to_compensate.contains(&workflow.step_name().into()) {
                compensate_workflow_with_emit(
                    actor,
                    workflow,
                    &context,
                    failed_step,
                    reason,
                    failure,
                    steps_to_compensate,
                    now,
                    &mut emit,
                );
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

    let accepted_context = context.next_step(workflow.step_name().into());
    emit(SagaChoreographyEvent::StepStarted {
        context: accepted_context.clone(),
    });
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
    if let StepOutput::Accepted {
        execution_id,
        policy,
        compensation_data,
    } = output
    {
        match accept_started_workflow_step(
            actor,
            context.next_step(workflow.step_name().into()),
            workflow.participant_id_owned(),
            execution_id,
            policy,
            saga_input,
            compensation_data,
        ) {
            Ok(event) => emit(event),
            Err(error) => quarantine_workflow_step_persistence_failure(
                actor,
                workflow,
                context,
                format!("accepted workflow step persistence failed: {error:?}").into(),
                now,
                emit,
            ),
        }
        return;
    }
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
        StepOutput::Accepted { .. } => unreachable!("accepted output returned above"),
    };

    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
        let new_state = state.complete(out_data.clone(), comp_data.clone(), now);
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Completed(new_state));
    }

    let emitted_output = out_data.clone();
    actor.record_event(
        saga_id,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data,
            compensation_data: comp_data,
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

fn quarantine_workflow_step_persistence_failure<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    reason: Box<str>,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&saga_id) {
        actor.saga_states().insert(
            saga_id,
            SagaStateEntry::Quarantined(state.quarantine(reason.clone(), now)),
        );
    }
    actor.record_event(
        saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(workflow.step_name().into()),
        reason,
        step: workflow.step_name().into(),
        participant_id: workflow.participant_id_owned(),
    });
}

fn quarantine_workflow_compensation_request_persistence_failure<A, F>(
    actor: &mut A,
    workflow: &'static dyn SagaWorkflowParticipant<A>,
    context: &SagaContext,
    reason: Box<str>,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let state_entry = actor.saga_states().remove(&saga_id);
    let quarantined = match state_entry {
        Some(SagaStateEntry::Executing(state)) => Some(state.quarantine(reason.clone(), now)),
        Some(SagaStateEntry::Completed(state)) => Some(
            state
                .start_compensation(now)
                .quarantine(reason.clone(), now),
        ),
        Some(SagaStateEntry::Compensating(state)) => Some(state.quarantine(reason.clone(), now)),
        Some(other) => {
            actor.saga_states().insert(saga_id, other);
            None
        }
        None => None,
    };
    if let Some(state) = quarantined {
        actor
            .saga_states()
            .insert(saga_id, SagaStateEntry::Quarantined(state));
    }
    actor.record_event(
        saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(workflow.step_name().into()),
        reason,
        step: workflow.step_name().into(),
        participant_id: workflow.participant_id_owned(),
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
    failed_step: Box<str>,
    request_reason: Box<str>,
    failure: crate::SagaFailureDetails,
    steps_to_compensate: Vec<Box<str>>,
    now: u64,
    emit: &mut F,
) where
    A: HasSagaParticipantSupport,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::CompensationRequestRecorded {
            context: context.clone(),
            failed_step,
            reason: request_reason,
            failure,
            steps_to_compensate,
            requested_at_millis: now,
        },
    ) {
        quarantine_workflow_compensation_request_persistence_failure(
            actor,
            workflow,
            context,
            format!("compensation request persistence failed: {error:?}").into(),
            now,
            emit,
        );
        return;
    }
    let accepted_compensation_data = actor
        .saga_support()
        .accepted_workflow_steps
        .get(&saga_id)
        .map(|accepted| accepted.compensation_data.clone());
    let state_entry = actor.saga_states().remove(&saga_id);
    let (comp_data, compensating_state) = match state_entry {
        Some(SagaStateEntry::Completed(state)) => {
            let comp_data = state.state.compensation_data.clone();
            (comp_data, state.start_compensation(now))
        }
        Some(SagaStateEntry::Executing(state)) => {
            let Some(comp_data) = accepted_compensation_data else {
                actor
                    .saga_states()
                    .insert(saga_id, SagaStateEntry::Executing(state));
                return;
            };
            (comp_data, state.start_compensation(now))
        }
        Some(other) => {
            actor.saga_states().insert(saga_id, other);
            return;
        }
        None => return,
    };
    actor
        .saga_states()
        .insert(saga_id, SagaStateEntry::Compensating(compensating_state));
    if let Some(accepted) = actor
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&saga_id)
    {
        mark_accepted_step_resolved(actor, saga_id, accepted.execution_id);
    }

    actor.record_event(
        saga_id,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );

    match workflow.compensate_step(actor, context, &comp_data) {
        Ok(CompensationOutput::Completed) => {
            complete_workflow_compensation(actor, workflow, context, now, emit)
        }
        Ok(CompensationOutput::Accepted {
            execution_id,
            policy,
        }) => match accept_started_workflow_compensation(
            actor,
            context.next_step(workflow.step_name().into()),
            workflow.participant_id_owned(),
            execution_id,
            policy,
        ) {
            Ok(event) => emit(event),
            Err(error) => fail_workflow_compensation(
                actor,
                workflow,
                context,
                crate::CompensationError::Ambiguous {
                    reason: format!("accepted compensation persistence failed: {error:?}").into(),
                },
                now,
                emit,
            ),
        },
        Err(error) => fail_workflow_compensation(actor, workflow, context, error, now, emit),
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
        if is_ambiguous {
            let new_state = state.quarantine(reason.clone(), now);
            actor
                .saga_states()
                .insert(saga_id, SagaStateEntry::Quarantined(new_state));
        } else {
            let new_state = state.fail(reason.clone(), false, now);
            actor
                .saga_states()
                .insert(saga_id, SagaStateEntry::Failed(new_state));
        }
    }

    if is_ambiguous {
        actor.record_event(
            saga_id,
            ParticipantEvent::Quarantined {
                reason: reason.clone(),
                quarantined_at_millis: now,
            },
        );
    } else {
        actor.record_event(
            saga_id,
            ParticipantEvent::CompensationFailed {
                error: reason.clone(),
                is_ambiguous,
                failed_at_millis: now,
            },
        );
    }

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
    mark_matching_accepted_step_failed(participant, &event);

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
                if stats.delivered > 0
                    && let Err(err) = saga
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
        static STALE_AFTER_MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        let stale_after_ms =
            *STALE_AFTER_MS.get_or_init(|| match std::env::var("SAGA_RECOVERY_MAX_AGE_MS") {
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
            });
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
    collect_startup_recovery_events_for_saga_type_inner(journal, saga_type, dedupe, step_name, true)
}

fn recorded_saga_type(entries: &[JournalEntry]) -> Option<&str> {
    entries.iter().rev().find_map(|entry| match &entry.event {
        ParticipantEvent::SagaRegistered { saga_type, .. } => Some(saga_type.as_ref()),
        ParticipantEvent::CompensationRequestRecorded { context, .. }
        | ParticipantEvent::AcceptedStepRecorded { context, .. }
        | ParticipantEvent::AcceptedCompensationRecorded { context, .. } => {
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

fn collect_startup_recovery_events_for_saga_type_inner<
    J: ParticipantJournal,
    D: ParticipantDedupeStore,
>(
    journal: &J,
    saga_type: &'static str,
    dedupe: &D,
    step_name: &'static str,
    allow_untyped_recovery: bool,
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
        match recorded_saga_type(&entries) {
            Some(recorded) if recorded != saga_type => continue,
            None if !allow_untyped_recovery => continue,
            Some(_) | None => {}
        }
        // Recovery only acts on durable journal evidence. If a persistent
        // dedupe store marked an upstream event but the participant crashed
        // before journaling StepExecutionStarted, there is no local execution
        // intent to resume here; the saga remains externally visible and is
        // resolved by terminal timeout policy instead of silent replay.
        if let Some(accepted) = recover_accepted_workflow_compensation_from_entries(&entries)
            && accepted.context.saga_type.as_ref() == saga_type
            && accepted.context.step_name.as_ref() == step_name
        {
            let Some(request) = recover_compensation_request_from_entries(&entries) else {
                out.push(SagaChoreographyEvent::SagaQuarantined {
                    context: accepted.context.clone(),
                    reason: "accepted compensation recovery missing durable compensation request"
                        .into(),
                    step: accepted.context.step_name.clone(),
                    participant_id: accepted.participant_id,
                });
                continue;
            };
            out.push(request);
            out.push(startup_accepted_compensation_replay_event(&accepted));
            if let Some(timeout_event) = startup_accepted_compensation_timeout_event(accepted, now)
            {
                out.push(timeout_event);
            }
            continue;
        }
        if let Some(request) = recover_unstarted_compensation_request_from_entries(&entries)
            && request.context().saga_type.as_ref() == saga_type
            && request.context().step_name.as_ref() == step_name
        {
            let dedupe_key = workflow_dedupe_key_for_event(&request);
            dedupe
                .remove_processed(saga_id, &dedupe_key)
                .map_err(|source| RecoveryCollectionError::ResetDedupe { saga_id, source })?;
            out.push(request);
            continue;
        }
        if let Some(accepted) = recover_accepted_workflow_step_from_entries(&entries)
            && accepted.context.saga_type.as_ref() == saga_type
            && accepted.context.step_name.as_ref() == step_name
        {
            if let Some((error, failed_at_millis)) =
                accepted_step_failure_requiring_compensation(&entries)
            {
                out.push(startup_accepted_step_replay_event(&accepted));
                let mut context = accepted.context;
                context.event_timestamp_millis = failed_at_millis;
                out.push(SagaChoreographyEvent::StepFailed {
                    context,
                    participant_id: accepted.participant_id,
                    error_code: None,
                    error,
                    requires_compensation: true,
                });
                continue;
            }
            if let Some(timeout_event) = startup_accepted_step_timeout_event(accepted.clone(), now)
            {
                if matches!(
                    &timeout_event,
                    SagaChoreographyEvent::StepFailed {
                        requires_compensation: true,
                        ..
                    }
                ) {
                    out.push(startup_accepted_step_replay_event(&accepted));
                }
                out.push(timeout_event);
                continue;
            }
            out.push(startup_accepted_step_replay_event(&accepted));
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
        timeout_outcome: accepted.policy.timeout_outcome.clone(),
        compensation_available: !accepted.compensation_data.is_empty(),
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

#[cfg(feature = "lmdb")]
pub mod lmdb {
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use heed::types::{Bytes, Str};
    use heed::{Database, Env, EnvOpenOptions};

    use super::{
        DEFAULT_RECOVERY_SAGA_TYPE, collect_startup_recovery_events_for_saga_type_inner,
        recover_accepted_workflow_steps_for_saga_type,
    };
    use crate::{
        DedupeError, JournalEntry, JournalError, ParticipantDedupeStore, ParticipantEvent,
        ParticipantJournal, SagaId, SagaParticipantSupport,
    };

    const DEFAULT_LMDB_MAP_SIZE_BYTES: usize = 1024 * 1024 * 1024;
    const SAGA_LMDB_MAP_SIZE_ENV: &str = "SAGA_LMDB_MAP_SIZE_BYTES";
    const JOURNAL_SCHEMA_KEY: &str = "journal_schema_version";
    const JOURNAL_SCHEMA_VERSION: &str = "2";

    fn lmdb_map_size_bytes() -> Result<usize, Box<str>> {
        match std::env::var(SAGA_LMDB_MAP_SIZE_ENV) {
            Ok(raw) => match raw.parse::<usize>() {
                Ok(value) if value > 0 => Ok(value),
                Ok(_) => Err(format!("{SAGA_LMDB_MAP_SIZE_ENV} must be greater than zero").into()),
                Err(err) => Err(format!("{SAGA_LMDB_MAP_SIZE_ENV} parse failed: {err}").into()),
            },
            Err(std::env::VarError::NotPresent) => Ok(DEFAULT_LMDB_MAP_SIZE_BYTES),
            Err(err) => Err(format!("{SAGA_LMDB_MAP_SIZE_ENV} read failed: {err}").into()),
        }
    }

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

    fn key_saga_index(saga_id: SagaId) -> String {
        format!("{:020}", saga_id.get())
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
            let map_size = lmdb_map_size_bytes().map_err(JournalError::Storage)?;
            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(16)
                    .map_size(map_size)
                    .open(path)
            }
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
            let stored_schema = meta
                .get(&wtxn, JOURNAL_SCHEMA_KEY)
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            match stored_schema {
                Some(version) if version == JOURNAL_SCHEMA_VERSION => {}
                Some(version) => {
                    return Err(JournalError::Storage(
                        format!(
                            "incompatible saga journal schema version: stored={version} required={JOURNAL_SCHEMA_VERSION}"
                        )
                        .into(),
                    ));
                }
                None => {
                    let has_unversioned_rows = {
                        let mut iter = rows
                            .iter(&wtxn)
                            .map_err(|err| JournalError::Storage(err.to_string().into()))?;
                        match iter.next() {
                            Some(row) => {
                                row.map_err(|err| JournalError::Storage(err.to_string().into()))?;
                                true
                            }
                            None => false,
                        }
                    };
                    if has_unversioned_rows {
                        return Err(JournalError::Storage(
                            format!(
                                "unversioned saga journal contains rows incompatible with required schema version {JOURNAL_SCHEMA_VERSION}"
                            )
                            .into(),
                        ));
                    }
                    meta.put(&mut wtxn, JOURNAL_SCHEMA_KEY, JOURNAL_SCHEMA_VERSION)
                        .map_err(|err| JournalError::Storage(err.to_string().into()))?;
                }
            }
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
                .put(&mut wtxn, &key_saga_index(saga_id), "1")
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

        fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            let prefix = key_saga_prefix(saga_id);
            let mut iter = self
                .rows
                .prefix_iter_mut(&mut wtxn, &prefix)
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            while iter.next().is_some() {
                unsafe { iter.del_current() }
                    .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            }
            drop(iter);
            self.saga_index
                .delete(&mut wtxn, &key_saga_index(saga_id))
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            wtxn.commit()
                .map_err(|err| JournalError::Storage(err.to_string().into()))?;
            Ok(())
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
            let map_size = lmdb_map_size_bytes().map_err(DedupeError::Storage)?;
            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(8)
                    .map_size(map_size)
                    .open(path)
            }
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

        fn contains(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
            let rtxn = self
                .env
                .read_txn()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            self.entries
                .get(&rtxn, &Self::key(saga_id, key))
                .map(|v| v.is_some())
                .map_err(|err| DedupeError::Storage(err.to_string().into()))
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

        fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
            let mut wtxn = self
                .env
                .write_txn()
                .map_err(|err| DedupeError::Storage(err.to_string().into()))?;
            self.entries
                .delete(&mut wtxn, &Self::key(saga_id, key))
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
        open_lmdb_participant_support_for_saga_types(base, step_name, &[saga_type])
    }

    pub fn open_lmdb_participant_support_for_saga_types(
        base: &Path,
        step_name: &'static str,
        saga_types: &[&'static str],
    ) -> Result<SagaParticipantSupport<LmdbJournal, LmdbDedupe>, String> {
        if saga_types.is_empty() {
            return Err("at least one recovery saga type is required".to_string());
        }
        let journal = LmdbJournal::open(&base.join("journal")).map_err(|err| err.to_string())?;
        let dedupe = LmdbDedupe::open(&base.join("dedupe")).map_err(|err| err.to_string())?;
        let mut startup_recovery_events = Vec::new();
        for saga_type in saga_types {
            let mut events = collect_startup_recovery_events_for_saga_type_inner(
                &journal,
                saga_type,
                &dedupe,
                step_name,
                saga_types.len() == 1,
            )
            .map_err(|err| {
                format!("startup recovery collection failed for saga_type={saga_type}: {err:?}")
            })?;
            startup_recovery_events.append(&mut events);
        }
        let mut support = SagaParticipantSupport::new(journal, dedupe)
            .with_startup_recovery_events(startup_recovery_events);
        for saga_type in saga_types {
            recover_accepted_workflow_steps_for_saga_type(&mut support, step_name, saga_type)
                .map_err(|err| {
                    format!("accepted step recovery failed for saga_type={saga_type}: {err:?}")
                })?;
        }
        Ok(support)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn contains_returns_error_when_reader_slots_are_exhausted() {
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
                dedupe.contains(saga_id, "probe").is_err(),
                "contains should surface read_txn reader slot failure"
            );
            drop(held_reader);

            assert!(
                dedupe
                    .contains(saga_id, "probe")
                    .expect("contains should recover after reader pressure"),
                "contains should recover once reader slot pressure is released"
            );
        }
    }
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
        HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal, ParticipantJournal,
        SagaParticipantSupport, SagaWorkflowParticipant, StepOutput,
    };
    use std::path::PathBuf;

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
        ) -> Result<crate::CompensationOutput, crate::CompensationError> {
            Ok(crate::CompensationOutput::Completed)
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
            .journal
            .read(crate::SagaId::new(77))
            .expect("journal read should succeed");
        assert!(matches!(
            entries.as_slice(),
            [
                _,
                crate::JournalEntry {
                    event: crate::ParticipantEvent::StepExecutionCompleted {
                        compensation_data,
                        ..
                    },
                    ..
                }
            ] if compensation_data == &[8]
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
            // SagaQuarantined: only from Quarantined.
            5 => entry_idx == 8,
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
            event_idx in 0u8..6u8, // guarded events only
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
    /// 0=SagaRegistered, 1=StepTriggered, 2=StepExecutionStarted,
    /// 3=StepExecutionCompleted, 4=StepExecutionFailed, 5=CompensationStarted,
    /// 6=CompensationCompleted, 7=CompensationFailed, 8=Quarantined,
    /// 9=AcceptedStepRecorded.
    fn participant_event_for(idx: u8) -> ParticipantEvent {
        let now = 1_234u64;
        match idx {
            0 => ParticipantEvent::SagaRegistered {
                saga_type: "pt".into(),
                step_name: "pt_step".into(),
                registered_at_millis: now,
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
/// Every `ParticipantDedupeStore` backend MUST make check-and-mark atomic:
/// across all thread interleavings, exactly one concurrent first-mark for a
/// given (saga_id, key) wins, and every duplicate observes a miss. The in-tree
/// `InMemoryDedupe` uses the same canonical mutex-protected check-and-mark
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
            if guard.contains_key(&entry) {
                false
            } else {
                guard.insert(entry, ());
                true
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
