//! Helper functions for saga handling

use crate::state_ext::{EventAdmission, admit_event, finalize_terminal_run};
use crate::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, DependencySpec, ParticipantEvent,
    RunKey, RunTerminalOutcome, SagaChoreographyEvent, SagaContext, SagaParticipant,
    SagaParticipantState, SagaStateEntry, SagaStateExt, SagaStateStoreError, StepError, StepOutput,
    event_identity,
};

/// Saga event handler with an explicit emit sink for produced choreography events.
pub fn handle_saga_event_with_emit<P, F>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut emit: F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let context = event.context().clone();
    let now = participant.now_millis();

    // Check saga type
    if !participant
        .saga_types()
        .iter()
        .any(|t| *t == context.saga_type.as_ref())
    {
        return;
    }

    let run = context.run_key();
    let identity = event_identity(&event);
    match admit_and_dedupe(participant, &event, &run, &identity) {
        Ok(true) => {}
        Ok(false) => return,
        Err(err) => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            quarantine_admission_lookup_failure(
                participant,
                &context,
                step,
                participant_id,
                &err,
                now,
                &mut emit,
            );
            return;
        }
    }

    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. }
            if participant.depends_on().is_on_saga_start() =>
        {
            execute_step_wrapper_with_emit(participant, context.clone(), payload, now, &mut emit);
        }

        // A start for a run that does not execute on saga start only registers the run.
        SagaChoreographyEvent::SagaStarted { .. } => {}

        SagaChoreographyEvent::StepCompleted {
            context: step_ctx,
            output,
            saga_input,
            ..
        } => {
            let dependency_spec = participant.depends_on();
            let should_fire =
                dependency_should_fire(participant, &run, &dependency_spec, &step_ctx.step_name);
            if should_fire {
                let next_context = context.next_step(participant.step_name().into());
                let input = if dependency_spec.prefers_original_saga_input() {
                    saga_input
                } else {
                    output
                };
                execute_step_wrapper_with_emit(participant, next_context, input, now, &mut emit);
            }
        }

        SagaChoreographyEvent::CompensationRequested {
            failed_step,
            reason,
            failure,
            steps_to_compensate,
            ..
        } => {
            if steps_to_compensate.contains(&participant.step_name().into()) {
                compensate_wrapper_with_emit(
                    participant,
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
            participant.latch_terminal_saga(&run);
            participant.on_saga_completed(&context);
            finalize_terminal_run(participant, &run, RunTerminalOutcome::Completed, &identity);
        }

        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_failed(&context, &reason);
            finalize_terminal_run(participant, &run, RunTerminalOutcome::Failed, &identity);
        }

        // Quarantined runs are never finalized: journal rows and dedupe marks stay as evidence.
        SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_quarantined(&context, &reason);
            participant.clear_in_memory_saga_run_tracking(&run);
        }

        _ => {}
    }
}

pub async fn handle_async_saga_event_with_emit<P, F>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut emit: F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let context = event.context().clone();
    let now = participant.now_millis();

    if !participant
        .saga_types()
        .iter()
        .any(|t| *t == context.saga_type.as_ref())
    {
        return;
    }

    let run = context.run_key();
    let identity = event_identity(&event);
    match admit_and_dedupe(participant, &event, &run, &identity) {
        Ok(true) => {}
        Ok(false) => return,
        Err(err) => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            quarantine_admission_lookup_failure(
                participant,
                &context,
                step,
                participant_id,
                &err,
                now,
                &mut emit,
            );
            return;
        }
    }

    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. }
            if participant.depends_on().is_on_saga_start() =>
        {
            execute_step_wrapper_with_emit_async(
                participant,
                context.clone(),
                payload,
                now,
                &mut emit,
            )
            .await;
        }
        SagaChoreographyEvent::SagaStarted { .. } => {}
        SagaChoreographyEvent::StepCompleted {
            context: step_ctx,
            output,
            saga_input,
            ..
        } => {
            let dependency_spec = participant.depends_on();
            let should_fire = dependency_should_fire_async(
                participant,
                &run,
                &dependency_spec,
                &step_ctx.step_name,
            );
            if should_fire {
                let next_context = context.next_step(participant.step_name().into());
                let input = if dependency_spec.prefers_original_saga_input() {
                    saga_input
                } else {
                    output
                };
                execute_step_wrapper_with_emit_async(
                    participant,
                    next_context,
                    input,
                    now,
                    &mut emit,
                )
                .await;
            }
        }
        SagaChoreographyEvent::CompensationRequested {
            failed_step,
            reason,
            failure,
            steps_to_compensate,
            ..
        } => {
            if steps_to_compensate.contains(&participant.step_name().into()) {
                compensate_wrapper_with_emit_async(
                    participant,
                    &context,
                    failed_step,
                    reason,
                    failure,
                    steps_to_compensate,
                    now,
                    &mut emit,
                )
                .await;
            }
        }
        SagaChoreographyEvent::SagaCompleted { .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_completed(&context);
            finalize_terminal_run(participant, &run, RunTerminalOutcome::Completed, &identity);
        }
        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_failed(&context, &reason);
            finalize_terminal_run(participant, &run, RunTerminalOutcome::Failed, &identity);
        }
        SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_quarantined(&context, &reason);
            participant.clear_in_memory_saga_run_tracking(&run);
        }
        _ => {}
    }
}

/// Run admission followed by run-scoped dedupe (ADR-0001 §2.2/§2.3).
///
/// `Ok(false)` means the event must not be processed. Idempotency is marked before
/// execution so replayed upstream events do not duplicate business side effects. With
/// persistent dedupe, a crash between this mark and the `StepExecutionStarted` journal entry is
/// fail-loud rather than resumed: startup recovery has no durable execution intent and the
/// terminal resolver must eventually fail/quarantine the saga. `Err` means a status, tombstone or
/// dedupe lookup failed; the caller quarantines the run (ADR-0001 §2.7).
pub(crate) fn admit_and_dedupe<A>(
    actor: &mut A,
    event: &SagaChoreographyEvent,
    run: &RunKey,
    identity: &str,
) -> Result<bool, SagaStateStoreError>
where
    A: SagaStateExt,
{
    match admit_event(actor, event) {
        EventAdmission::Proceed => {}
        EventAdmission::Skip => return Ok(false),
        EventAdmission::LookupFailed(err) => return Err(err),
    }
    actor
        .check_dedupe_run_strict(run, identity)
        .inspect_err(|err| {
            tracing::error!(
                target: "core::saga",
                event = "saga_dedupe_check_failed",
                run = %run,
                error = ?err
            );
        })
}

/// ADR-0001 §2.7: admission/dedupe lookup failed. No effect ran, so quarantining the run in
/// memory, journaling best-effort, and emitting `SagaQuarantined` is non-contradictory.
pub(crate) fn quarantine_admission_lookup_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    cause: &SagaStateStoreError,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let reason: Box<str> =
        format!("reconciliation_needed: admission_lookup_failed: {cause}").into();
    tracing::error!(
        target: "core::saga",
        event = "saga_admission_quarantined",
        run = %run,
        reason = %reason
    );
    let state = SagaParticipantState::new(
        context.saga_id,
        context.saga_type.clone(),
        step.clone(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("admission_lookup_failed", now)
    .start_execution(now)
    .quarantine(reason.clone(), now);
    actor
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Quarantined(state));
    // Later events of this run are ignored as terminal rather than re-admitted.
    actor.latch_terminal_saga(&run);
    actor.record_event_run(
        &run,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(step.clone()),
        reason,
        step,
        participant_id,
    });
}

fn dependency_should_fire<P>(
    participant: &mut P,
    run: &RunKey,
    dependency_spec: &DependencySpec,
    completed_step: &str,
) -> bool
where
    P: SagaParticipant + SagaStateExt,
{
    dependency_should_fire_inner(participant, run, dependency_spec, completed_step)
}

fn dependency_should_fire_async<P>(
    participant: &mut P,
    run: &RunKey,
    dependency_spec: &DependencySpec,
    completed_step: &str,
) -> bool
where
    P: AsyncSagaParticipant + SagaStateExt,
{
    dependency_should_fire_inner(participant, run, dependency_spec, completed_step)
}

fn dependency_should_fire_inner<P>(
    participant: &mut P,
    run: &RunKey,
    dependency_spec: &DependencySpec,
    completed_step: &str,
) -> bool
where
    P: SagaStateExt,
{
    match dependency_spec {
        DependencySpec::OnSagaStart => false,
        DependencySpec::After(step) => {
            if completed_step != *step {
                return false;
            }
            participant.dependency_fired().insert(run.clone())
        }
        DependencySpec::AnyOf(steps) => {
            if !steps.contains(&completed_step) {
                return false;
            }
            participant.dependency_fired().insert(run.clone())
        }
        DependencySpec::AllOf(steps) => {
            if !steps.contains(&completed_step) {
                return false;
            }
            {
                let seen = participant
                    .dependency_completions()
                    .entry(run.clone())
                    .or_default();
                seen.insert(completed_step.into());
                if !steps.iter().all(|step| seen.contains(*step)) {
                    return false;
                }
            }
            participant.dependency_fired().insert(run.clone())
        }
    }
}

fn execute_step_wrapper_with_emit<P, F>(
    participant: &mut P,
    context: SagaContext,
    input: Vec<u8>,
    now: u64,
    emit: &mut F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();

    // Build state: Idle -> Triggered -> Executing
    let state = SagaParticipantState::new(
        context.saga_id,
        context.saga_type.clone(),
        participant.step_name().into(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("dependency_satisfied", now)
    .start_execution(now);

    // Persist
    participant.record_event_run(
        &run,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );

    // Store state
    participant
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Executing(state));

    emit(SagaChoreographyEvent::StepStarted {
        context: context.next_step(participant.step_name().into()),
    });

    // Execute
    match participant.execute_step(&context, &input) {
        Ok(output) => {
            complete_step(participant, &context, input, output, now, emit);
        }
        Err(error) => {
            fail_step(participant, &context, error, now, emit);
        }
    }
}

async fn execute_step_wrapper_with_emit_async<P, F>(
    participant: &mut P,
    context: SagaContext,
    input: Vec<u8>,
    now: u64,
    emit: &mut F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();

    let state = SagaParticipantState::new(
        context.saga_id,
        context.saga_type.clone(),
        participant.step_name().into(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("dependency_satisfied", now)
    .start_execution(now);

    participant.record_event_run(
        &run,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );

    participant
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Executing(state));

    emit(SagaChoreographyEvent::StepStarted {
        context: context.next_step(participant.step_name().into()),
    });

    match participant.execute_step(&context, &input).await {
        Ok(output) => complete_step_async(participant, &context, input, output, now, emit),
        Err(error) => fail_step_async(participant, &context, error, now, emit),
    }
}

/// Complete a step with state transition
fn complete_step<P, F>(
    participant: &mut P,
    context: &SagaContext,
    saga_input: Vec<u8>,
    output: StepOutput,
    now: u64,
    emit: &mut F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if let StepOutput::Accepted {
        execution_id,
        policy,
        compensation_data,
    } = output
    {
        match crate::durability::accept_started_workflow_step(
            participant,
            context.next_step(participant.step_name().into()),
            participant.participant_id_owned(),
            execution_id,
            policy,
            saga_input,
            compensation_data,
        ) {
            Ok(event) => emit(event),
            Err(error) => {
                let step = participant.step_name().into();
                let participant_id = participant.participant_id_owned();
                quarantine_accepted_step_persistence_failure(
                    participant,
                    context,
                    step,
                    participant_id,
                    format!("accepted step persistence failed: {error:?}").into(),
                    now,
                    emit,
                );
            }
        }
        return;
    }
    let (out_data, comp_data, compensation_available) = match output {
        StepOutput::Completed {
            output,
            compensation_data,
        } => {
            let compensation_available = !compensation_data.is_empty();
            (output, compensation_data, compensation_available)
        }
        StepOutput::CompletedWithEffect {
            output,
            compensation_data,
            ..
        } => {
            let compensation_available = !compensation_data.is_empty();
            (output, compensation_data, compensation_available)
        }
        StepOutput::Accepted { .. } => unreachable!("accepted output returned above"),
    };

    // State: Executing -> Completed
    if let Some(SagaStateEntry::Executing(state)) = participant.saga_states().remove(&run) {
        let new_state = state.complete(out_data.clone(), comp_data.clone(), now);
        participant
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Completed(new_state));
    }

    // Persist
    let emitted_output = out_data.clone();
    participant.record_event_run(
        &run,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data,
            compensation_data: comp_data,
            completed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepCompleted {
        context: context.next_step(participant.step_name().into()),
        output: emitted_output,
        saga_input,
        compensation_available,
    });
}

fn complete_step_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    saga_input: Vec<u8>,
    output: StepOutput,
    now: u64,
    emit: &mut F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if let StepOutput::Accepted {
        execution_id,
        policy,
        compensation_data,
    } = output
    {
        match crate::durability::accept_started_workflow_step(
            participant,
            context.next_step(participant.step_name().into()),
            participant.participant_id_owned(),
            execution_id,
            policy,
            saga_input,
            compensation_data,
        ) {
            Ok(event) => emit(event),
            Err(error) => {
                let step = participant.step_name().into();
                let participant_id = participant.participant_id_owned();
                quarantine_accepted_step_persistence_failure(
                    participant,
                    context,
                    step,
                    participant_id,
                    format!("accepted async step persistence failed: {error:?}").into(),
                    now,
                    emit,
                );
            }
        }
        return;
    }
    let (out_data, comp_data, compensation_available) = match output {
        StepOutput::Completed {
            output,
            compensation_data,
        } => {
            let compensation_available = !compensation_data.is_empty();
            (output, compensation_data, compensation_available)
        }
        StepOutput::CompletedWithEffect {
            output,
            compensation_data,
            ..
        } => {
            let compensation_available = !compensation_data.is_empty();
            (output, compensation_data, compensation_available)
        }
        StepOutput::Accepted { .. } => unreachable!("accepted output returned above"),
    };

    if let Some(SagaStateEntry::Executing(state)) = participant.saga_states().remove(&run) {
        let new_state = state.complete(out_data.clone(), comp_data.clone(), now);
        participant
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Completed(new_state));
    }

    let emitted_output = out_data.clone();
    participant.record_event_run(
        &run,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data,
            compensation_data: comp_data,
            completed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepCompleted {
        context: context.next_step(participant.step_name().into()),
        output: emitted_output,
        saga_input,
        compensation_available,
    });
}

fn quarantine_accepted_step_persistence_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    reason: Box<str>,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&run) {
        actor.saga_states().insert(
            run.clone(),
            SagaStateEntry::Quarantined(state.quarantine(reason.clone(), now)),
        );
    }
    actor.record_event_run(
        &run,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(step.clone()),
        reason,
        step,
        participant_id,
    });
}

fn quarantine_compensation_request_persistence_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    reason: Box<str>,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let state_entry = actor.saga_states().remove(&run);
    let quarantined = match state_entry {
        Some(SagaStateEntry::Executing(state)) => Some(state.quarantine(reason.clone(), now)),
        Some(SagaStateEntry::Completed(state)) => Some(
            state
                .start_compensation(now)
                .quarantine(reason.clone(), now),
        ),
        Some(SagaStateEntry::Compensating(state)) => Some(state.quarantine(reason.clone(), now)),
        Some(other) => {
            actor.saga_states().insert(run.clone(), other);
            None
        }
        None => None,
    };
    if let Some(state) = quarantined {
        actor
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Quarantined(state));
    }
    actor.record_event_run(
        &run,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(step.clone()),
        reason,
        step,
        participant_id,
    });
}

/// Fail a step with state transition
fn fail_step<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: StepError,
    now: u64,
    emit: &mut F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let (reason, requires_comp) = match error {
        StepError::Terminal { reason } => (reason, false),
        StepError::RequireCompensation { reason } => (reason, true),
    };

    // State: Executing -> Failed
    if let Some(SagaStateEntry::Executing(state)) = participant.saga_states().remove(&run) {
        let new_state = state.fail(reason.clone(), requires_comp, now);
        participant
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Failed(new_state));
    }

    // Persist
    participant.record_event_run(
        &run,
        ParticipantEvent::StepExecutionFailed {
            error: reason.clone(),
            requires_compensation: requires_comp,
            failed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepFailed {
        context: context.next_step(participant.step_name().into()),
        participant_id: participant.participant_id_owned(),
        error_code: None,
        error: reason,
        requires_compensation: requires_comp,
    });
}

fn fail_step_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: StepError,
    now: u64,
    emit: &mut F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let (reason, requires_comp) = match error {
        StepError::Terminal { reason } => (reason, false),
        StepError::RequireCompensation { reason } => (reason, true),
    };

    if let Some(SagaStateEntry::Executing(state)) = participant.saga_states().remove(&run) {
        let new_state = state.fail(reason.clone(), requires_comp, now);
        participant
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Failed(new_state));
    }

    participant.record_event_run(
        &run,
        ParticipantEvent::StepExecutionFailed {
            error: reason.clone(),
            requires_compensation: requires_comp,
            failed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepFailed {
        context: context.next_step(participant.step_name().into()),
        participant_id: participant.participant_id_owned(),
        error_code: None,
        error: reason,
        requires_compensation: requires_comp,
    });
}

#[allow(clippy::too_many_arguments)] // signature refactor deferred to W6 R23
fn compensate_wrapper_with_emit<P, F>(
    participant: &mut P,
    context: &SagaContext,
    failed_step: Box<str>,
    request_reason: Box<str>,
    failure: crate::SagaFailureDetails,
    steps_to_compensate: Vec<Box<str>>,
    now: u64,
    emit: &mut F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if let Err(error) = participant.record_event_run_strict(
        &run,
        ParticipantEvent::CompensationRequestRecorded {
            context: context.clone(),
            failed_step,
            reason: request_reason,
            failure,
            steps_to_compensate,
            requested_at_millis: now,
        },
    ) {
        let step = participant.step_name().into();
        let participant_id = participant.participant_id_owned();
        quarantine_compensation_request_persistence_failure(
            participant,
            context,
            step,
            participant_id,
            format!("compensation request persistence failed: {error:?}").into(),
            now,
            emit,
        );
        return;
    }

    let accepted_recovery_data = participant
        .saga_support()
        .accepted_workflow_steps
        .get(&run)
        .map(|accepted| {
            (
                accepted.saga_input.clone(),
                accepted.compensation_data.clone(),
            )
        });
    let state_entry = participant.saga_states().remove(&run);
    let (saga_input, comp_data, new_state) = match state_entry {
        Some(SagaStateEntry::Completed(state)) => {
            let comp_data = state.state.compensation_data.clone();
            (Vec::new(), comp_data, state.start_compensation(now))
        }
        Some(SagaStateEntry::Executing(state)) => {
            let Some((saga_input, comp_data)) = accepted_recovery_data else {
                participant
                    .saga_states()
                    .insert(run.clone(), SagaStateEntry::Executing(state));
                return;
            };
            (saga_input, comp_data, state.start_compensation(now))
        }
        Some(other) => {
            participant.saga_states().insert(run.clone(), other);
            return;
        }
        None => return,
    };
    participant
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Compensating(new_state));
    if let Some(accepted) = participant
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&run)
    {
        crate::durability::mark_accepted_step_resolved(participant, &run, accepted.execution_id);
    }

    participant.record_event_run(
        &run,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );

    match participant.compensate_step(context, &comp_data) {
        Ok(CompensationOutput::Completed) => {
            complete_compensation(participant, context, now, emit);
        }
        Ok(CompensationOutput::Accepted {
            execution_id,
            policy,
        }) => {
            let participant_id = participant.participant_id_owned();
            match crate::durability::accept_started_workflow_compensation(
                participant,
                context.next_step(participant.step_name().into()),
                participant_id,
                execution_id,
                policy,
                saga_input,
                comp_data,
            ) {
                Ok(event) => emit(event),
                Err(error) => fail_compensation(
                    participant,
                    context,
                    CompensationError::Ambiguous {
                        reason: format!("accepted compensation persistence failed: {error:?}")
                            .into(),
                    },
                    now,
                    emit,
                ),
            }
        }
        Err(error) => {
            fail_compensation(participant, context, error, now, emit);
        }
    }
}

#[allow(clippy::too_many_arguments)] // signature refactor deferred to W6 R23
async fn compensate_wrapper_with_emit_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    failed_step: Box<str>,
    request_reason: Box<str>,
    failure: crate::SagaFailureDetails,
    steps_to_compensate: Vec<Box<str>>,
    now: u64,
    emit: &mut F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if let Err(error) = participant.record_event_run_strict(
        &run,
        ParticipantEvent::CompensationRequestRecorded {
            context: context.clone(),
            failed_step,
            reason: request_reason,
            failure,
            steps_to_compensate,
            requested_at_millis: now,
        },
    ) {
        let step = participant.step_name().into();
        let participant_id = participant.participant_id_owned();
        quarantine_compensation_request_persistence_failure(
            participant,
            context,
            step,
            participant_id,
            format!("compensation request persistence failed: {error:?}").into(),
            now,
            emit,
        );
        return;
    }

    let accepted_recovery_data = participant
        .saga_support()
        .accepted_workflow_steps
        .get(&run)
        .map(|accepted| {
            (
                accepted.saga_input.clone(),
                accepted.compensation_data.clone(),
            )
        });
    let state_entry = participant.saga_states().remove(&run);
    let (saga_input, comp_data, new_state) = match state_entry {
        Some(SagaStateEntry::Completed(state)) => {
            let comp_data = state.state.compensation_data.clone();
            (Vec::new(), comp_data, state.start_compensation(now))
        }
        Some(SagaStateEntry::Executing(state)) => {
            let Some((saga_input, comp_data)) = accepted_recovery_data else {
                participant
                    .saga_states()
                    .insert(run.clone(), SagaStateEntry::Executing(state));
                return;
            };
            (saga_input, comp_data, state.start_compensation(now))
        }
        Some(other) => {
            participant.saga_states().insert(run.clone(), other);
            return;
        }
        None => return,
    };
    participant
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Compensating(new_state));
    if let Some(accepted) = participant
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&run)
    {
        crate::durability::mark_accepted_step_resolved(participant, &run, accepted.execution_id);
    }

    participant.record_event_run(
        &run,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );

    match participant.compensate_step(context, &comp_data).await {
        Ok(CompensationOutput::Completed) => {
            complete_compensation_async(participant, context, now, emit)
        }
        Ok(CompensationOutput::Accepted {
            execution_id,
            policy,
        }) => {
            let participant_id = participant.participant_id_owned();
            match crate::durability::accept_started_workflow_compensation(
                participant,
                context.next_step(participant.step_name().into()),
                participant_id,
                execution_id,
                policy,
                saga_input,
                comp_data,
            ) {
                Ok(event) => emit(event),
                Err(error) => fail_compensation_async(
                    participant,
                    context,
                    CompensationError::Ambiguous {
                        reason: format!(
                            "accepted async compensation persistence failed: {error:?}"
                        )
                        .into(),
                    },
                    now,
                    emit,
                ),
            }
        }
        Err(error) => fail_compensation_async(participant, context, error, now, emit),
    }
}

/// Complete compensation
fn complete_compensation<P, F>(participant: &mut P, context: &SagaContext, now: u64, emit: &mut F)
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();

    // State: Compensating -> Compensated
    if let Some(SagaStateEntry::Compensating(state)) = participant.saga_states().remove(&run) {
        let new_state = state.complete_compensation(now);
        participant
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Compensated(new_state));
    }

    // Persist
    participant.record_event_run(
        &run,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::CompensationCompleted {
        context: context.next_step(participant.step_name().into()),
    });

    // Notify
    participant.on_compensation_completed(context);
}

fn complete_compensation_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();

    if let Some(SagaStateEntry::Compensating(state)) = participant.saga_states().remove(&run) {
        let new_state = state.complete_compensation(now);
        participant
            .saga_states()
            .insert(run.clone(), SagaStateEntry::Compensated(new_state));
    }

    participant.record_event_run(
        &run,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::CompensationCompleted {
        context: context.next_step(participant.step_name().into()),
    });

    participant.on_compensation_completed(context);
}

/// Fail compensation (quarantine)
fn fail_compensation<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: CompensationError,
    now: u64,
    emit: &mut F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let (reason, is_ambiguous) = match error {
        CompensationError::SafeToRetry { reason } => (reason, false),
        CompensationError::Ambiguous { reason } => (reason, true),
        CompensationError::Terminal { reason } => (reason, false),
    };

    if let Some(SagaStateEntry::Compensating(state)) = participant.saga_states().remove(&run) {
        if is_ambiguous {
            let new_state = state.quarantine(reason.clone(), now);
            participant
                .saga_states()
                .insert(run.clone(), SagaStateEntry::Quarantined(new_state));
        } else {
            let new_state = state.fail(reason.clone(), false, now);
            participant
                .saga_states()
                .insert(run.clone(), SagaStateEntry::Failed(new_state));
        }
    }

    if is_ambiguous {
        participant.record_event_run(
            &run,
            ParticipantEvent::Quarantined {
                reason: reason.clone(),
                quarantined_at_millis: now,
            },
        );
    } else {
        participant.record_event_run(
            &run,
            ParticipantEvent::CompensationFailed {
                error: reason.clone(),
                is_ambiguous,
                failed_at_millis: now,
            },
        );
    }

    let event_context = context.next_step(participant.step_name().into());
    emit(SagaChoreographyEvent::CompensationFailed {
        context: event_context.clone(),
        participant_id: participant.participant_id_owned(),
        error: reason.clone(),
        is_ambiguous,
    });
    if is_ambiguous {
        emit(SagaChoreographyEvent::SagaQuarantined {
            context: event_context,
            reason: reason.clone(),
            step: participant.step_name().into(),
            participant_id: participant.participant_id_owned(),
        });
    }

    // Notify
    participant.on_quarantined(context, &reason);
}

fn fail_compensation_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: CompensationError,
    now: u64,
    emit: &mut F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let (reason, is_ambiguous) = match error {
        CompensationError::SafeToRetry { reason } => (reason, false),
        CompensationError::Ambiguous { reason } => (reason, true),
        CompensationError::Terminal { reason } => (reason, false),
    };

    if let Some(SagaStateEntry::Compensating(state)) = participant.saga_states().remove(&run) {
        if is_ambiguous {
            let new_state = state.quarantine(reason.clone(), now);
            participant
                .saga_states()
                .insert(run.clone(), SagaStateEntry::Quarantined(new_state));
        } else {
            let new_state = state.fail(reason.clone(), false, now);
            participant
                .saga_states()
                .insert(run.clone(), SagaStateEntry::Failed(new_state));
        }
    }

    if is_ambiguous {
        participant.record_event_run(
            &run,
            ParticipantEvent::Quarantined {
                reason: reason.clone(),
                quarantined_at_millis: now,
            },
        );
    } else {
        participant.record_event_run(
            &run,
            ParticipantEvent::CompensationFailed {
                error: reason.clone(),
                is_ambiguous,
                failed_at_millis: now,
            },
        );
    }

    let event_context = context.next_step(participant.step_name().into());
    emit(SagaChoreographyEvent::CompensationFailed {
        context: event_context.clone(),
        participant_id: participant.participant_id_owned(),
        error: reason.clone(),
        is_ambiguous,
    });
    if is_ambiguous {
        emit(SagaChoreographyEvent::SagaQuarantined {
            context: event_context,
            reason: reason.clone(),
            step: participant.step_name().into(),
            participant_id: participant.participant_id_owned(),
        });
    }

    participant.on_quarantined(context, &reason);
}

#[cfg(test)]
mod tests {
    use crate::{
        DeterministicContextBuilder, HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal,
        ParticipantJournal, SagaContext, SagaId, SagaParticipantSupport,
    };

    use super::*;

    #[derive(Clone, Copy)]
    enum ExecuteMode {
        Completed,
        TerminalFail,
    }

    struct TestParticipant {
        saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
        execute_mode: ExecuteMode,
        compensation_error: Option<CompensationError>,
        executed: usize,
        observed_inputs: Vec<Vec<u8>>,
        dependency_spec: DependencySpec,
    }

    impl Default for TestParticipant {
        fn default() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new())
                    .with_replay_horizon(unbounded_horizon()),
                execute_mode: ExecuteMode::Completed,
                compensation_error: None,
                executed: 0,
                observed_inputs: Vec::new(),
                dependency_spec: DependencySpec::OnSagaStart,
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
            self.dependency_spec.clone()
        }

        fn execute_step(
            &mut self,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            self.executed = self.executed.saturating_add(1);
            self.observed_inputs.push(_input.to_vec());
            match self.execute_mode {
                ExecuteMode::Completed => Ok(StepOutput::Completed {
                    output: vec![1, 2, 3],
                    compensation_data: vec![9],
                }),
                ExecuteMode::TerminalFail => Err(StepError::Terminal {
                    reason: "terminal failure".into(),
                }),
            }
        }

        fn compensate_step(
            &mut self,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<CompensationOutput, CompensationError> {
            if let Some(err) = self.compensation_error.clone() {
                return Err(err);
            }
            Ok(CompensationOutput::Completed)
        }
    }

    /// Fixtures use fixed 2023 timestamps; a horizon this long keeps them inside the replay window.
    fn unbounded_horizon() -> crate::ReplayHorizon {
        crate::ReplayHorizon::new(std::time::Duration::from_secs(100 * 365 * 24 * 3600))
            .expect("horizon above the floor")
    }

    fn started_run() -> RunKey {
        DeterministicContextBuilder::default().build().run_key()
    }

    fn started_event() -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaStarted {
            context: DeterministicContextBuilder::default().build(),
            payload: vec![7],
        }
    }

    #[test]
    fn handle_saga_event_with_emit_emits_step_completed() {
        let mut participant = TestParticipant::default();
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, started_event(), |event| {
            emitted.push(event)
        });

        assert_eq!(participant.executed, 1);
        assert_eq!(emitted.len(), 2);
        assert!(matches!(
            emitted.first(),
            Some(SagaChoreographyEvent::StepStarted { .. })
        ));
        assert!(matches!(
            emitted.get(1),
            Some(SagaChoreographyEvent::StepCompleted {
                compensation_available: true,
                ..
            })
        ));
        let entries = participant
            .saga
            .journal
            .read(SagaId::new(1))
            .expect("journal read should succeed");
        assert!(matches!(
            entries.as_slice(),
            [
                _,
                crate::JournalEntry {
                    event: ParticipantEvent::StepExecutionCompleted {
                        compensation_data,
                        ..
                    },
                    ..
                }
            ] if compensation_data == &[9]
        ));
    }

    #[test]
    fn handle_saga_event_with_emit_emits_step_failed_on_terminal_failure() {
        let mut participant = TestParticipant {
            execute_mode: ExecuteMode::TerminalFail,
            ..TestParticipant::default()
        };
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, started_event(), |event| {
            emitted.push(event)
        });

        assert_eq!(participant.executed, 1);
        assert_eq!(emitted.len(), 2);
        assert!(matches!(
            emitted.first(),
            Some(SagaChoreographyEvent::StepStarted { .. })
        ));
        assert!(matches!(
            emitted.get(1),
            Some(SagaChoreographyEvent::StepFailed {
                requires_compensation: false,
                ..
            })
        ));
    }

    #[test]
    fn handle_saga_event_with_emit_dedupes_replayed_input() {
        let mut participant = TestParticipant::default();
        let input = started_event();
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, input.clone(), |event| emitted.push(event));
        handle_saga_event_with_emit(&mut participant, input, |event| emitted.push(event));

        assert_eq!(participant.executed, 1);
        assert_eq!(emitted.len(), 2);
    }

    #[test]
    fn handle_saga_event_with_emit_accepts_reused_saga_id_for_new_run() {
        let mut participant = TestParticipant::default();
        let mut emitted = Vec::new();
        let first = started_event();
        let mut second_context = first.context().clone();
        second_context.saga_started_at_millis =
            second_context.saga_started_at_millis.saturating_add(1);
        second_context.event_timestamp_millis =
            second_context.event_timestamp_millis.saturating_add(1);
        let second = SagaChoreographyEvent::SagaStarted {
            context: second_context,
            payload: vec![8],
        };

        handle_saga_event_with_emit(&mut participant, first, |event| emitted.push(event));
        handle_saga_event_with_emit(&mut participant, second, |event| emitted.push(event));

        assert_eq!(participant.executed, 2);
        assert_eq!(emitted.len(), 4);
    }

    #[test]
    fn handle_saga_event_with_emit_resets_allof_dependencies_on_new_saga_started() {
        let mut participant = TestParticipant {
            dependency_spec: DependencySpec::AllOf(&["risk_check", "positions_check"]),
            ..TestParticipant::default()
        };
        let first_context = DeterministicContextBuilder::default().build();
        let mut second_context = first_context.clone();
        second_context.saga_started_at_millis =
            second_context.saga_started_at_millis.saturating_add(1);
        second_context.event_timestamp_millis =
            second_context.event_timestamp_millis.saturating_add(1);
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: first_context.next_step("risk_check".into()),
                output: vec![9],
                saga_input: vec![7],
                compensation_available: false,
            },
            |_| {},
        );
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::SagaStarted {
                context: second_context.clone(),
                payload: vec![7],
            },
            |_| {},
        );
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: second_context.next_step("positions_check".into()),
                output: vec![8],
                saga_input: vec![7],
                compensation_available: false,
            },
            |event| emitted.push(event),
        );

        assert_eq!(participant.executed, 0);
        assert!(emitted.is_empty());
    }

    #[test]
    fn handle_saga_event_with_emit_allof_triggers_once_after_full_dependency_set() {
        let mut participant = TestParticipant {
            dependency_spec: DependencySpec::AllOf(&["risk_check", "positions_check"]),
            ..TestParticipant::default()
        };
        let mut emitted = Vec::new();
        let context = DeterministicContextBuilder::default().build();

        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: context.next_step("risk_check".into()),
                output: vec![9],
                saga_input: vec![7],
                compensation_available: false,
            },
            |event| emitted.push(event),
        );
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: context.next_step("positions_check".into()),
                output: vec![8],
                saga_input: vec![7],
                compensation_available: false,
            },
            |event| emitted.push(event),
        );

        assert_eq!(participant.executed, 1);
    }

    #[test]
    fn handle_saga_event_with_emit_allof_uses_original_saga_input() {
        let mut participant = TestParticipant {
            dependency_spec: DependencySpec::AllOf(&["risk_check", "positions_check"]),
            ..TestParticipant::default()
        };
        let context = DeterministicContextBuilder::default().build();

        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: context.next_step("risk_check".into()),
                output: vec![9],
                saga_input: vec![7, 7, 7],
                compensation_available: false,
            },
            |_| {},
        );
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: context.next_step("positions_check".into()),
                output: vec![8],
                saga_input: vec![7, 7, 7],
                compensation_available: false,
            },
            |_| {},
        );

        assert_eq!(participant.observed_inputs, vec![vec![7, 7, 7]]);
    }

    #[test]
    fn handle_saga_event_with_emit_emits_non_ambiguous_compensation_failure_only() {
        let mut participant = TestParticipant {
            compensation_error: Some(CompensationError::Terminal {
                reason: "cannot compensate".into(),
            }),
            ..TestParticipant::default()
        };
        let started = started_event();
        let context = started.context().clone();
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, started, |_| {});
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::CompensationRequested {
                context,
                failed_step: "risk_check".into(),
                reason: "failed downstream".into(),
                failure: crate::SagaFailureDetails {
                    step_name: "risk_check".into(),
                    participant_id: "risk".into(),
                    error_code: None,
                    error_message: "failed downstream".into(),
                    at_millis: 1,
                },
                steps_to_compensate: vec!["risk_check".into()],
            },
            |event| emitted.push(event),
        );

        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            emitted.first(),
            Some(SagaChoreographyEvent::CompensationFailed { .. })
        ));
        assert!(matches!(
            participant.saga_states().get(&started_run()),
            Some(SagaStateEntry::Failed(_))
        ));
        let entries = participant
            .saga
            .journal
            .read(SagaId::new(1))
            .expect("journal read should succeed");
        assert!(matches!(
            entries.last(),
            Some(crate::JournalEntry {
                event: ParticipantEvent::CompensationFailed {
                    error,
                    is_ambiguous: false,
                    ..
                },
                ..
            }) if error.as_ref() == "cannot compensate"
        ));
    }

    #[test]
    fn handle_saga_event_with_emit_emits_quarantine_for_ambiguous_compensation_failure() {
        let mut participant = TestParticipant {
            compensation_error: Some(CompensationError::Ambiguous {
                reason: "cannot confirm rollback".into(),
            }),
            ..TestParticipant::default()
        };
        let started = started_event();
        let context = started.context().clone();
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, started, |_| {});
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::CompensationRequested {
                context,
                failed_step: "risk_check".into(),
                reason: "failed downstream".into(),
                failure: crate::SagaFailureDetails {
                    step_name: "risk_check".into(),
                    participant_id: "risk".into(),
                    error_code: None,
                    error_message: "failed downstream".into(),
                    at_millis: 1,
                },
                steps_to_compensate: vec!["risk_check".into()],
            },
            |event| emitted.push(event),
        );

        assert_eq!(emitted.len(), 2);
        assert!(matches!(
            emitted.first(),
            Some(SagaChoreographyEvent::CompensationFailed {
                is_ambiguous: true,
                ..
            })
        ));
        assert!(matches!(
            participant.saga_states().get(&started_run()),
            Some(SagaStateEntry::Quarantined(_))
        ));
        let entries = participant
            .saga
            .journal
            .read(SagaId::new(1))
            .expect("journal read should succeed");
        assert!(matches!(
            entries.last(),
            Some(crate::JournalEntry {
                event: ParticipantEvent::Quarantined { reason, .. },
                ..
            }) if reason.as_ref() == "cannot confirm rollback"
        ));
        assert!(matches!(
            emitted.get(1),
            Some(SagaChoreographyEvent::SagaQuarantined { .. })
        ));
    }

    #[test]
    fn handle_saga_event_latches_and_prunes_on_quarantine() {
        let mut participant = TestParticipant::default();
        let started = started_event();
        let saga_id = started.context().saga_id;
        let run = started.context().run_key();

        handle_saga_event_with_emit(&mut participant, started, |_| {});
        assert_eq!(participant.executed, 1);
        assert!(participant.saga_states().contains_key(&run));

        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::SagaQuarantined {
                context: DeterministicContextBuilder::default()
                    .with_saga_id(saga_id.get())
                    .build(),
                reason: "panic".into(),
                step: "risk_check".into(),
                participant_id: "risk_check".into(),
            },
            |_| {},
        );

        assert!(participant.is_terminal_saga_latched(&run));
        assert!(!participant.saga_states().contains_key(&run));

        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context: DeterministicContextBuilder::default()
                    .with_saga_id(saga_id.get())
                    .with_step_name("positions_check")
                    .build(),
                output: vec![1],
                saga_input: vec![1],
                compensation_available: false,
            },
            |_| {},
        );

        assert_eq!(
            participant.executed, 1,
            "post-quarantine replay should be ignored once the saga is terminal-latched"
        );
    }

    fn started_event_at(started_at_millis: u64) -> SagaChoreographyEvent {
        let mut context = DeterministicContextBuilder::default().build();
        context.saga_started_at_millis = started_at_millis;
        context.event_timestamp_millis = started_at_millis;
        SagaChoreographyEvent::SagaStarted {
            context,
            payload: vec![7],
        }
    }

    #[test]
    fn new_trace_replay_of_active_start_does_not_reset_state() {
        let mut participant = TestParticipant::default();
        let first = started_event();
        let mut replay_context = first.context().clone();
        replay_context.trace_id = replay_context.trace_id.saturating_add(100);
        let replay = SagaChoreographyEvent::SagaStarted {
            context: replay_context,
            payload: vec![7],
        };

        handle_saga_event_with_emit(&mut participant, first, |_| {});
        handle_saga_event_with_emit(&mut participant, replay, |_| {});

        assert_eq!(
            participant.executed, 1,
            "a replayed start with a re-minted trace_id must not re-run the run"
        );
        let started_rows = participant
            .saga
            .journal
            .read(SagaId::new(1))
            .expect("journal read")
            .iter()
            .filter(|row| matches!(row.event, ParticipantEvent::StepExecutionStarted { .. }))
            .count();
        assert_eq!(started_rows, 1);
        assert!(
            participant.is_saga_active(SagaId::new(1)) || participant.saga_states_ref().len() == 1
        );
    }

    #[test]
    fn older_start_than_known_newer_run_is_rejected() {
        let mut participant = TestParticipant::default();
        let base = started_event().context().saga_started_at_millis;

        handle_saga_event_with_emit(&mut participant, started_event_at(base + 1), |_| {});
        handle_saga_event_with_emit(&mut participant, started_event_at(base), |_| {});

        assert_eq!(
            participant.executed, 1,
            "older incarnation must not execute"
        );
        assert_eq!(participant.saga.stats.snapshot().runs_rejected_stale, 1);
    }

    #[test]
    fn expired_incarnation_is_rejected_not_admitted() {
        // default participant horizon (24 h): a start two days old, no state, no tombstone
        let mut participant = TestParticipant {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            ..TestParticipant::default()
        };
        let two_days_ago = SagaContext::now_millis().saturating_sub(2 * 24 * 3600 * 1000);
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, started_event_at(two_days_ago), |event| {
            emitted.push(event)
        });

        assert_eq!(participant.executed, 0, "callback must never run");
        assert!(emitted.is_empty());
        assert_eq!(participant.saga.stats.snapshot().runs_rejected_expired, 1);
    }

    #[test]
    fn concurrent_newer_run_is_admitted_and_counted() {
        let mut participant = TestParticipant {
            dependency_spec: DependencySpec::AllOf(&["risk_check", "positions_check"]),
            ..TestParticipant::default()
        };
        let old = DeterministicContextBuilder::default().build();
        let mut new = old.clone();
        new.saga_started_at_millis += 1;
        new.event_timestamp_millis += 1;
        let completed = |ctx: &SagaContext, step: &str| SagaChoreographyEvent::StepCompleted {
            context: ctx.next_step(step.into()),
            output: vec![1],
            saga_input: vec![7],
            compensation_available: false,
        };

        handle_saga_event_with_emit(&mut participant, completed(&old, "risk_check"), |_| {});
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::SagaStarted {
                context: new.clone(),
                payload: vec![7],
            },
            |_| {},
        );
        assert_eq!(
            participant.saga.stats.snapshot().concurrent_runs_admitted,
            1
        );
        handle_saga_event_with_emit(&mut participant, completed(&new, "positions_check"), |_| {});
        assert_eq!(
            participant.executed, 0,
            "new run has only one of two dependencies"
        );

        // The older run kept its own dependency state and still completes independently.
        handle_saga_event_with_emit(&mut participant, completed(&old, "positions_check"), |_| {});
        assert_eq!(participant.executed, 1);
    }

    struct FaultJournal {
        inner: InMemoryJournal,
        fail_tombstones: std::sync::atomic::AtomicBool,
        fail_finalize: std::sync::atomic::AtomicBool,
    }

    impl FaultJournal {
        fn new() -> Self {
            Self {
                inner: InMemoryJournal::new(),
                fail_tombstones: false.into(),
                fail_finalize: false.into(),
            }
        }
    }

    impl ParticipantJournal for FaultJournal {
        fn append(
            &self,
            saga_id: SagaId,
            event: ParticipantEvent,
        ) -> Result<u64, crate::JournalError> {
            self.inner.append(saga_id, event)
        }
        fn read(&self, saga_id: SagaId) -> Result<Vec<crate::JournalEntry>, crate::JournalError> {
            self.inner.read(saga_id)
        }
        fn list_sagas(&self) -> Result<Vec<SagaId>, crate::JournalError> {
            self.inner.list_sagas()
        }
        fn prune(&self, saga_id: SagaId) -> Result<(), crate::JournalError> {
            self.inner.prune(saga_id)
        }
        fn append_run(
            &self,
            run: &RunKey,
            event: ParticipantEvent,
        ) -> Result<u64, crate::JournalError> {
            self.inner.append_run(run, event)
        }
        fn read_run(&self, run: &RunKey) -> Result<Vec<crate::JournalEntry>, crate::JournalError> {
            self.inner.read_run(run)
        }
        fn list_runs(&self) -> Result<Vec<RunKey>, crate::JournalError> {
            self.inner.list_runs()
        }
        fn finalize_run(
            &self,
            tombstone: &crate::RunTombstone,
            cutoff: crate::RunIncarnation,
        ) -> Result<(), crate::JournalError> {
            if self.fail_finalize.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::JournalError::Storage(
                    "injected finalize failure".into(),
                ));
            }
            self.inner.finalize_run(tombstone, cutoff)
        }
        fn run_tombstones(
            &self,
            saga_type: &str,
            saga_id: SagaId,
        ) -> Result<Vec<crate::RunTombstone>, crate::JournalError> {
            if self
                .fail_tombstones
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::JournalError::Storage(
                    "injected tombstone failure".into(),
                ));
            }
            self.inner.run_tombstones(saga_type, saga_id)
        }
        fn prune_expired_tombstones(
            &self,
            cutoff: crate::RunIncarnation,
        ) -> Result<u64, crate::JournalError> {
            self.inner.prune_expired_tombstones(cutoff)
        }
    }

    struct FaultParticipant {
        saga: SagaParticipantSupport<FaultJournal, InMemoryDedupe>,
        executed: usize,
    }

    impl FaultParticipant {
        fn new() -> Self {
            Self {
                saga: SagaParticipantSupport::new(FaultJournal::new(), InMemoryDedupe::new())
                    .with_replay_horizon(unbounded_horizon()),
                executed: 0,
            }
        }
    }

    impl HasSagaParticipantSupport for FaultParticipant {
        type Journal = FaultJournal;
        type Dedupe = InMemoryDedupe;

        fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &self.saga
        }

        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &mut self.saga
        }
    }

    impl SagaParticipant for FaultParticipant {
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
            self.executed += 1;
            Ok(StepOutput::Completed {
                output: vec![1],
                compensation_data: vec![],
            })
        }

        fn compensate_step(
            &mut self,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<CompensationOutput, CompensationError> {
            Ok(CompensationOutput::Completed)
        }
    }

    fn completed_event() -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaCompleted {
            context: DeterministicContextBuilder::default().build(),
        }
    }

    #[test]
    fn admission_lookup_failure_quarantines_run_without_executing() {
        use std::sync::atomic::Ordering;
        let mut participant = FaultParticipant::new();
        participant
            .saga
            .journal
            .fail_tombstones
            .store(true, Ordering::SeqCst);
        let mut emitted = Vec::new();

        handle_saga_event_with_emit(&mut participant, started_event(), |event| {
            emitted.push(event)
        });

        assert_eq!(
            participant.executed, 0,
            "no effect may run on a failed lookup"
        );
        assert!(matches!(
            emitted.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { reason, .. }]
                if reason.contains("admission_lookup_failed")
        ));
        assert!(matches!(
            participant.saga_states_ref().get(&started_run()),
            Some(SagaStateEntry::Quarantined(_))
        ));
    }

    #[test]
    fn terminal_event_writes_tombstone_and_replay_is_rejected_after_latch_eviction() {
        let mut participant = FaultParticipant::new();
        handle_saga_event_with_emit(&mut participant, started_event(), |_| {});
        assert_eq!(participant.executed, 1);

        handle_saga_event_with_emit(&mut participant, completed_event(), |_| {});

        let run = started_run();
        let tombstones = participant
            .saga
            .journal
            .run_tombstones(run.saga_type(), run.saga_id())
            .expect("tombstones");
        assert_eq!(tombstones.len(), 1);
        assert!(
            participant
                .saga
                .journal
                .read_run(&run)
                .expect("read run")
                .is_empty(),
            "finalize deletes the run's rows"
        );
        assert!(participant.saga_states_ref().is_empty());

        // Latch eviction (or restart) must not let the start be admitted again.
        participant.saga.terminal_sagas.clear();
        participant.saga.terminal_saga_order.clear();
        let mut other_dedupe = InMemoryDedupe::new();
        std::mem::swap(&mut participant.saga.dedupe, &mut other_dedupe);
        handle_saga_event_with_emit(&mut participant, started_event(), |_| {});
        assert_eq!(participant.executed, 1, "the tombstone answers for the run");
    }

    #[test]
    fn finalize_failure_keeps_memory_and_terminal_redelivery_retries() {
        use std::sync::atomic::Ordering;
        let mut participant = FaultParticipant::new();
        handle_saga_event_with_emit(&mut participant, started_event(), |_| {});
        participant
            .saga
            .journal
            .fail_finalize
            .store(true, Ordering::SeqCst);

        handle_saga_event_with_emit(&mut participant, completed_event(), |_| {});

        assert_eq!(
            participant.saga_states_ref().len(),
            1,
            "memory must not be cleared before the journal confirms finalize"
        );
        assert_eq!(participant.saga.stats.snapshot().gc_failures, 1);

        participant
            .saga
            .journal
            .fail_finalize
            .store(false, Ordering::SeqCst);
        handle_saga_event_with_emit(&mut participant, completed_event(), |_| {});

        assert!(participant.saga_states_ref().is_empty());
        let run = started_run();
        assert_eq!(
            participant
                .saga
                .journal
                .run_tombstones(run.saga_type(), run.saga_id())
                .expect("tombstones")
                .len(),
            1
        );
    }
}
