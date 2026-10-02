//! Helper functions for saga handling

use crate::state_ext::{
    EventAdmission, admit_event, finalize_terminal_run, keep_quarantine_compensation_data,
    quarantine_run_with_evidence,
};
use crate::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, DependencySpec, ParticipantEvent,
    RunKey, RunTerminalOutcome, SagaChoreographyEvent, SagaContext, SagaParticipant,
    SagaParticipantState, SagaStateEntry, SagaStateExt, SagaStateStoreError, StepError, StepOutput,
    event_identity,
};
use crate::{CommitStage, IngressFailure, IngressOutcome, IngressRejection, ReconciliationCause};
use crate::{JournalError, ReconciliationNeeded};

/// Saga event handler with an explicit emit sink for produced choreography events.
pub fn handle_saga_event_with_emit<P, F>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut emit: F,
) -> IngressOutcome
where
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
        return IngressOutcome::Rejected(IngressRejection::NotParticipant);
    }

    let run = context.run_key();
    let identity = event_identity(&event);
    match admit_for_ingress(participant, &event, &run, &identity) {
        Ok(None) => {}
        Ok(Some(outcome)) => return outcome,
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
            return IngressOutcome::Failed(IngressFailure {
                run,
                stage: admission_stage(&err),
                source: err,
            });
        }
    }

    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. }
            if participant.depends_on().is_on_saga_start() =>
        {
            execute_step_wrapper_with_emit(participant, context.clone(), payload, now, &mut emit)
        }

        // A start for a run that does not execute on saga start only registers the run.
        SagaChoreographyEvent::SagaStarted { .. } => IngressOutcome::Applied,

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
                execute_step_wrapper_with_emit(participant, next_context, input, now, &mut emit)
            } else {
                IngressOutcome::Applied
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
                )
            } else {
                IngressOutcome::Applied
            }
        }

        SagaChoreographyEvent::SagaCompleted { .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_completed(&context);
            match finalize_terminal_run(participant, &run, RunTerminalOutcome::Completed, &identity)
            {
                Ok(()) => IngressOutcome::Applied,
                Err(failure) => IngressOutcome::Failed(failure),
            }
        }

        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_failed(&context, &reason);
            match finalize_terminal_run(participant, &run, RunTerminalOutcome::Failed, &identity) {
                Ok(()) => IngressOutcome::Applied,
                Err(failure) => IngressOutcome::Failed(failure),
            }
        }

        // Quarantined runs are never finalized or pruned: journal rows, dedupe marks and the
        // in-memory quarantine entry stay as evidence for reconciliation.
        SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_quarantined(&context, &reason);
            IngressOutcome::Applied
        }

        _ => IngressOutcome::Applied,
    }
}

pub async fn handle_async_saga_event_with_emit<P, F>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut emit: F,
) -> IngressOutcome
where
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
        return IngressOutcome::Rejected(IngressRejection::NotParticipant);
    }

    let run = context.run_key();
    let identity = event_identity(&event);
    match admit_for_ingress(participant, &event, &run, &identity) {
        Ok(None) => {}
        Ok(Some(outcome)) => return outcome,
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
            return IngressOutcome::Failed(IngressFailure {
                run,
                stage: admission_stage(&err),
                source: err,
            });
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
            .await
        }
        SagaChoreographyEvent::SagaStarted { .. } => IngressOutcome::Applied,
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
                .await
            } else {
                IngressOutcome::Applied
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
                .await
            } else {
                IngressOutcome::Applied
            }
        }
        SagaChoreographyEvent::SagaCompleted { .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_completed(&context);
            match finalize_terminal_run(participant, &run, RunTerminalOutcome::Completed, &identity)
            {
                Ok(()) => IngressOutcome::Applied,
                Err(failure) => IngressOutcome::Failed(failure),
            }
        }
        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_saga_failed(&context, &reason);
            match finalize_terminal_run(participant, &run, RunTerminalOutcome::Failed, &identity) {
                Ok(()) => IngressOutcome::Applied,
                Err(failure) => IngressOutcome::Failed(failure),
            }
        }
        SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
            participant.latch_terminal_saga(&run);
            participant.on_quarantined(&context, &reason);
            IngressOutcome::Applied
        }
        _ => IngressOutcome::Applied,
    }
}

/// Run admission followed by run-scoped dedupe (ADR-0001 §2.2/§2.3).
///
/// Idempotency is marked before
/// execution so replayed upstream events do not duplicate business side effects. With
/// persistent dedupe, a crash between this mark and the `StepExecutionStarted` journal entry is
/// fail-loud rather than resumed: startup recovery has no durable execution intent and the
/// terminal resolver must eventually fail/quarantine the saga. `Err` means a status, tombstone or
/// dedupe lookup failed; the caller quarantines the run (ADR-0001 §2.7).
/// A skipped event carries its typed [`IngressOutcome`]:
/// `Ok(None)` = process, `Ok(Some(outcome))` = do not process, `Err` = lookup failed.
pub(crate) fn admit_for_ingress<A>(
    actor: &mut A,
    event: &SagaChoreographyEvent,
    run: &RunKey,
    identity: &str,
) -> Result<Option<IngressOutcome>, SagaStateStoreError>
where
    A: SagaStateExt,
{
    match admit_event(actor, event) {
        EventAdmission::Proceed => {}
        EventAdmission::Skip(outcome) => return Ok(Some(outcome)),
        EventAdmission::LookupFailed(err) => return Err(err),
    }
    let is_new = actor
        .check_dedupe_run_strict(run, identity)
        .inspect_err(|err| {
            tracing::error!(
                target: "core::saga",
                event = "saga_dedupe_check_failed",
                run = %run,
                error = ?err
            );
        })?;
    Ok((!is_new).then_some(IngressOutcome::Duplicate))
}

fn admission_stage(err: &SagaStateStoreError) -> CommitStage {
    match err {
        SagaStateStoreError::Dedupe(_) => CommitStage::Dedupe,
        SagaStateStoreError::Journal(_) => CommitStage::Admission,
    }
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
    let label = match cause {
        SagaStateStoreError::Dedupe(_) => "dedupe_check_failed",
        SagaStateStoreError::Journal(_) => "admission_lookup_failed",
    };
    let reason: Box<str> = format!("reconciliation_needed: {label}: {cause}").into();
    tracing::error!(
        target: "core::saga",
        event = "saga_admission_quarantined",
        run = %run,
        reason = %reason
    );
    quarantine_run_with_evidence(
        actor,
        context,
        (step, participant_id),
        (label, reason),
        now,
        emit,
    );
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
) -> IngressOutcome
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();

    // Commit the execution intent before the callback may run (ADR-0002, Q5).
    if let Err(err) = participant.commit_transition(
        &run,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: now,
        },
    ) {
        let step = participant.step_name().into();
        let participant_id = participant.participant_id_owned();
        return fail_intent_commit(participant, &context, step, participant_id, err, now, emit);
    }

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

    // Store state
    participant
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Executing(state));

    emit(SagaChoreographyEvent::StepStarted {
        context: context.next_step(participant.step_name().into()),
    });

    // Execute
    match participant.execute_step(&context, &input) {
        Ok(output) => complete_step(participant, &context, input, output, now, emit),
        Err(error) => fail_step(participant, &context, error, now, emit),
    }
}

async fn execute_step_wrapper_with_emit_async<P, F>(
    participant: &mut P,
    context: SagaContext,
    input: Vec<u8>,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();

    if let Err(err) = participant.commit_transition(
        &run,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: now,
        },
    ) {
        let step = participant.step_name().into();
        let participant_id = participant.participant_id_owned();
        return fail_intent_commit(participant, &context, step, participant_id, err, now, emit);
    }

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

/// ADR-0002 §2.2 `Intent` (Q5): the execution intent could not be committed, so the callback was
/// not invoked. Nothing ran, so a non-ambiguous `StepFailed { requires_compensation: true }` is
/// safe and lets the resolver roll back other completed steps. The dedupe mark is kept, so an
/// in-process redelivery is `Duplicate`.
fn fail_intent_commit<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    err: SagaStateStoreError,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    tracing::error!(
        target: "core::saga",
        event = "saga_intent_commit_failed",
        run = %run,
        step = %step,
        error = ?err
    );
    let reason: Box<str> = format!("intent commit failed: {err}").into();
    let failed = SagaParticipantState::new(
        context.saga_id,
        context.saga_type.clone(),
        step.clone(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("dependency_satisfied", now)
    .start_execution(now)
    .fail(reason.clone(), true, now);
    actor
        .saga_states()
        .insert(run.clone(), SagaStateEntry::Failed(failed));
    emit(SagaChoreographyEvent::StepFailed {
        context: context.next_step(step),
        participant_id,
        error_code: Some("intent_commit_failed".into()),
        error: reason,
        requires_compensation: true,
    });
    IngressOutcome::Failed(IngressFailure {
        run,
        stage: CommitStage::Intent,
        source: err,
    })
}

/// ADR-0002 §2.2 `Result` (Q6): the step callback ran (or may have) but its outcome could not be
/// committed. The success/failure is never acknowledged: the step is quarantined, `SagaQuarantined`
/// is emitted, and the outcome carries the compensation data as evidence.
#[allow(clippy::too_many_arguments)] // shared by sync/async engines; signature refactor deferred to W6 R23
fn quarantine_result_commit_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    err: SagaStateStoreError,
    compensation_data: Vec<u8>,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let reason: Box<str> = format!("reconciliation_needed: result_commit_failed: {err}").into();
    tracing::error!(
        target: "core::saga",
        event = "saga_result_commit_failed",
        run = %run,
        step = %step,
        error = ?err
    );
    quarantine_after_commit_failure(
        actor,
        context,
        step.clone(),
        participant_id,
        &reason,
        now,
        emit,
    );
    IngressOutcome::ReconciliationNeeded(ReconciliationNeeded {
        run,
        step,
        cause: ReconciliationCause::ResultCommitFailed(err),
        compensation_data,
    })
}

/// Post-effect commit failure: routes through the single shared quarantine path
/// ([`quarantine_run_with_evidence`]) so the run is un-admitted, latched and keeps its evidence.
fn quarantine_after_commit_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    reason: &str,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    quarantine_run_with_evidence(
        actor,
        context,
        (step, participant_id),
        ("commit_failed", Box::<str>::from(reason)),
        now,
        emit,
    );
}

/// Complete a step with state transition
fn complete_step<P, F>(
    participant: &mut P,
    context: &SagaContext,
    saga_input: Vec<u8>,
    output: StepOutput,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
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
            compensation_data.clone(),
        ) {
            Ok(event) => emit(event),
            Err(error) => {
                let step: Box<str> = participant.step_name().into();
                let participant_id = participant.participant_id_owned();
                let needs_reconciliation =
                    accepted_step_reconciliation(context, &step, &error, compensation_data.clone());
                quarantine_accepted_step_persistence_failure(
                    participant,
                    context,
                    (step, participant_id),
                    format!("accepted step persistence failed: {error:?}").into(),
                    &compensation_data,
                    now,
                    emit,
                );
                return needs_reconciliation;
            }
        }
        return IngressOutcome::Applied;
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
            effect,
        } => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return quarantine_unsupported_effect(
                participant,
                context,
                step,
                participant_id,
                (output, compensation_data),
                effect,
                now,
                emit,
            );
        }
        StepOutput::Accepted { .. } => unreachable!("accepted output returned above"),
    };

    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    commit_step_completed(
        participant,
        context,
        step,
        participant_id,
        saga_input,
        (out_data, comp_data),
        compensation_available,
        now,
        emit,
    )
}

fn complete_step_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    saga_input: Vec<u8>,
    output: StepOutput,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
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
            compensation_data.clone(),
        ) {
            Ok(event) => emit(event),
            Err(error) => {
                let step: Box<str> = participant.step_name().into();
                let participant_id = participant.participant_id_owned();
                let needs_reconciliation =
                    accepted_step_reconciliation(context, &step, &error, compensation_data.clone());
                quarantine_accepted_step_persistence_failure(
                    participant,
                    context,
                    (step, participant_id),
                    format!("accepted async step persistence failed: {error:?}").into(),
                    &compensation_data,
                    now,
                    emit,
                );
                return needs_reconciliation;
            }
        }
        return IngressOutcome::Applied;
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
            effect,
        } => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return quarantine_unsupported_effect(
                participant,
                context,
                step,
                participant_id,
                (output, compensation_data),
                effect,
                now,
                emit,
            );
        }
        StepOutput::Accepted { .. } => unreachable!("accepted output returned above"),
    };

    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    commit_step_completed(
        participant,
        context,
        step,
        participant_id,
        saga_input,
        (out_data, comp_data),
        compensation_available,
        now,
        emit,
    )
}

/// R21 / ADR-0002 §2.2: `CompletedWithEffect` has no dispatcher yet, so the promised effect cannot
/// be delivered. The step ran, so the completion is committed as evidence (undo data survives a
/// restart) but never acknowledged: no `StepCompleted` is emitted or put in an outbox. The step is
/// quarantined and the outcome is `ReconciliationNeeded { UnsupportedEffect }`.
#[allow(clippy::too_many_arguments)] // shared by sync/async engines; signature refactor deferred to W6 R23
fn quarantine_unsupported_effect<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    (out_data, comp_data): (Vec<u8>, Vec<u8>),
    effect: Box<str>,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if let Err(err) = actor.commit_transition(
        &run,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data,
            compensation_data: comp_data.clone(),
            completed_at_millis: now,
        },
    ) {
        return quarantine_result_commit_failure(
            actor,
            context,
            step,
            participant_id,
            err,
            comp_data,
            now,
            emit,
        );
    }
    let reason: Box<str> = format!("reconciliation_needed: unsupported_effect: {effect}").into();
    tracing::error!(
        target: "core::saga",
        event = "saga_unsupported_effect",
        run = %run,
        step = %step,
        effect = %effect
    );
    quarantine_after_commit_failure(
        actor,
        context,
        step.clone(),
        participant_id,
        &reason,
        now,
        emit,
    );
    IngressOutcome::ReconciliationNeeded(ReconciliationNeeded {
        run,
        step,
        cause: ReconciliationCause::UnsupportedEffect { effect },
        compensation_data: comp_data,
    })
}

/// Commits `StepExecutionCompleted` together with its `StepCompleted` obligation (ADR-0003), and
/// only then moves `Executing -> Completed` and emits. A failed commit quarantines (Q6).
#[allow(clippy::too_many_arguments)] // shared by sync/async engines; signature refactor deferred to W6 R23
fn commit_step_completed<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    saga_input: Vec<u8>,
    (out_data, comp_data): (Vec<u8>, Vec<u8>),
    compensation_available: bool,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let completed = SagaChoreographyEvent::StepCompleted {
        context: context.next_step(step.clone()),
        output: out_data.clone(),
        saga_input,
        compensation_available,
    };
    if let Err(err) = actor.commit_transition_with_outbox(
        &run,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data.clone(),
            compensation_data: comp_data.clone(),
            completed_at_millis: now,
        },
        vec![completed.clone()],
    ) {
        return quarantine_result_commit_failure(
            actor,
            context,
            step,
            participant_id,
            err,
            comp_data,
            now,
            emit,
        );
    }
    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&run) {
        actor.saga_states().insert(
            run,
            SagaStateEntry::Completed(state.complete(out_data, comp_data, now)),
        );
    }
    emit(completed);
    IngressOutcome::Applied
}

/// Commits `StepExecutionFailed` together with its `StepFailed` obligation, and only then moves
/// `Executing -> Failed` and emits. A failed commit quarantines (Q6): the callback ran.
#[allow(clippy::too_many_arguments)] // shared by sync/async engines; signature refactor deferred to W6 R23
fn commit_step_failed<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    reason: Box<str>,
    requires_comp: bool,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let failed = SagaChoreographyEvent::StepFailed {
        context: context.next_step(step.clone()),
        participant_id: participant_id.clone(),
        error_code: None,
        error: reason.clone(),
        requires_compensation: requires_comp,
    };
    if let Err(err) = actor.commit_transition_with_outbox(
        &run,
        ParticipantEvent::StepExecutionFailed {
            error: reason.clone(),
            requires_compensation: requires_comp,
            failed_at_millis: now,
        },
        vec![failed.clone()],
    ) {
        return quarantine_result_commit_failure(
            actor,
            context,
            step,
            participant_id,
            err,
            Vec::new(),
            now,
            emit,
        );
    }
    if let Some(SagaStateEntry::Executing(state)) = actor.saga_states().remove(&run) {
        actor.saga_states().insert(
            run,
            SagaStateEntry::Failed(state.fail(reason, requires_comp, now)),
        );
    }
    emit(failed);
    IngressOutcome::Applied
}

/// An accepted step whose metadata could not be persisted may have started an external effect
/// the journal does not know about: report it like the workflow adapter does (W3 review MEDIUM 6).
fn accepted_step_reconciliation(
    context: &SagaContext,
    step: &str,
    error: &crate::AcceptedStepError,
    compensation_data: Vec<u8>,
) -> IngressOutcome {
    let run = context.run_key();
    tracing::error!(
        target: "core::saga",
        event = "accepted_step_persistence_failed",
        run = %run,
        error = ?error
    );
    IngressOutcome::ReconciliationNeeded(ReconciliationNeeded {
        run,
        step: step.into(),
        cause: ReconciliationCause::ResultCommitFailed(SagaStateStoreError::Journal(
            JournalError::Storage(format!("accepted step persistence failed: {error:?}").into()),
        )),
        compensation_data,
    })
}

fn quarantine_accepted_step_persistence_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    (step, participant_id): (Box<str>, Box<str>),
    reason: Box<str>,
    compensation_data: &[u8],
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    quarantine_run_with_evidence(
        actor,
        context,
        (step, participant_id),
        ("accepted_step_persistence_failed", reason),
        now,
        emit,
    );
    keep_quarantine_compensation_data(actor, &run, compensation_data);
}

/// A failed compensation-request commit keeps the run's undo data in memory (`Completed` goes
/// through `Completed::quarantine`, W3 review LOW 2) and quarantines through the shared path.
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
    quarantine_run_with_evidence(
        actor,
        context,
        (step, participant_id),
        ("compensation_request_persistence_failed", reason),
        now,
        emit,
    );
}

/// Fail a step with state transition
fn fail_step<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: StepError,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let (reason, requires_comp) = match error {
        StepError::Terminal { reason } => (reason, false),
        StepError::RequireCompensation { reason } => (reason, true),
    };

    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    commit_step_failed(
        participant,
        context,
        step,
        participant_id,
        reason,
        requires_comp,
        now,
        emit,
    )
}

fn fail_step_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: StepError,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let (reason, requires_comp) = match error {
        StepError::Terminal { reason } => (reason, false),
        StepError::RequireCompensation { reason } => (reason, true),
    };

    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    commit_step_failed(
        participant,
        context,
        step,
        participant_id,
        reason,
        requires_comp,
        now,
        emit,
    )
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
) -> IngressOutcome
where
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
        return IngressOutcome::Failed(IngressFailure {
            run,
            stage: CommitStage::CompensationRequest,
            source: error,
        });
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
    let will_compensate = match participant.saga_states_ref().get(&run) {
        Some(SagaStateEntry::Completed(_)) => true,
        Some(SagaStateEntry::Executing(_)) => accepted_recovery_data.is_some(),
        // R2: a retry after `SafeToRetry` is a new undo invocation and needs its own durable start.
        Some(SagaStateEntry::Compensating(state)) => {
            state.state.compensation_data.is_some()
                && !participant
                    .saga_support()
                    .accepted_workflow_compensations
                    .contains_key(&run)
        }
        _ => false,
    };
    if will_compensate
        && let Err(err) = participant.commit_transition(
            &run,
            ParticipantEvent::CompensationStarted {
                attempt: context.attempt,
                started_at_millis: now,
            },
        )
    {
        let step = participant.step_name().into();
        let participant_id = participant.participant_id_owned();
        return fail_compensation_start_commit(context, step, participant_id, err, emit);
    }
    let compensation_in_flight = participant
        .saga_support()
        .accepted_workflow_compensations
        .contains_key(&run);
    let state_entry = participant.saga_states().remove(&run);
    let (saga_input, comp_data, new_state) = match state_entry {
        Some(SagaStateEntry::Completed(state)) => {
            let comp_data = state.state.compensation_data.clone();
            (Vec::new(), comp_data, state.start_compensation(now))
        }
        Some(SagaStateEntry::Executing(state)) => {
            let Some((saga_input, comp_data)) = accepted_recovery_data else {
                let step = participant.step_name().into();
                let participant_id = participant.participant_id_owned();
                return crate::state_ext::unexpected_compensation_state_outcome(
                    participant,
                    context,
                    SagaStateEntry::Executing(state),
                    (step, participant_id, compensation_in_flight),
                    now,
                    emit,
                );
            };
            (saga_input, comp_data, state.start_compensation(now))
        }
        // ADR-0004 §2.5: a re-request after `CompensationFailedRetryable` re-invokes the undo
        // with the kept data, unless an accepted async undo is still in flight.
        Some(SagaStateEntry::Compensating(mut state))
            if state.state.compensation_data.is_some() && !compensation_in_flight =>
        {
            state.state.attempt = context.attempt;
            let comp_data = state.state.compensation_data.clone().unwrap_or_default();
            (Vec::new(), comp_data, state)
        }
        Some(other) => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return crate::state_ext::unexpected_compensation_state_outcome(
                participant,
                context,
                other,
                (step, participant_id, compensation_in_flight),
                now,
                emit,
            );
        }
        None => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return crate::state_ext::missing_completed_state_outcome(
                participant,
                context,
                (step, participant_id),
                now,
                emit,
            );
        }
    };
    let mut new_state = new_state;
    new_state.state.compensation_data = Some(comp_data.clone());
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

    match participant.compensate_step(context, &comp_data) {
        Ok(CompensationOutput::Completed) => complete_compensation(participant, context, now, emit),
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
                Ok(event) => {
                    emit(event);
                    IngressOutcome::Applied
                }
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
        Err(error) => fail_compensation(participant, context, error, now, emit),
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
) -> IngressOutcome
where
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
        return IngressOutcome::Failed(IngressFailure {
            run,
            stage: CommitStage::CompensationRequest,
            source: error,
        });
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
    let will_compensate = match participant.saga_states_ref().get(&run) {
        Some(SagaStateEntry::Completed(_)) => true,
        Some(SagaStateEntry::Executing(_)) => accepted_recovery_data.is_some(),
        // R2: a retry after `SafeToRetry` is a new undo invocation and needs its own durable start.
        Some(SagaStateEntry::Compensating(state)) => {
            state.state.compensation_data.is_some()
                && !participant
                    .saga_support()
                    .accepted_workflow_compensations
                    .contains_key(&run)
        }
        _ => false,
    };
    if will_compensate
        && let Err(err) = participant.commit_transition(
            &run,
            ParticipantEvent::CompensationStarted {
                attempt: context.attempt,
                started_at_millis: now,
            },
        )
    {
        let step = participant.step_name().into();
        let participant_id = participant.participant_id_owned();
        return fail_compensation_start_commit(context, step, participant_id, err, emit);
    }
    let compensation_in_flight = participant
        .saga_support()
        .accepted_workflow_compensations
        .contains_key(&run);
    let state_entry = participant.saga_states().remove(&run);
    let (saga_input, comp_data, new_state) = match state_entry {
        Some(SagaStateEntry::Completed(state)) => {
            let comp_data = state.state.compensation_data.clone();
            (Vec::new(), comp_data, state.start_compensation(now))
        }
        Some(SagaStateEntry::Executing(state)) => {
            let Some((saga_input, comp_data)) = accepted_recovery_data else {
                let step = participant.step_name().into();
                let participant_id = participant.participant_id_owned();
                return crate::state_ext::unexpected_compensation_state_outcome(
                    participant,
                    context,
                    SagaStateEntry::Executing(state),
                    (step, participant_id, compensation_in_flight),
                    now,
                    emit,
                );
            };
            (saga_input, comp_data, state.start_compensation(now))
        }
        // ADR-0004 §2.5: a re-request after `CompensationFailedRetryable` re-invokes the undo
        // with the kept data, unless an accepted async undo is still in flight.
        Some(SagaStateEntry::Compensating(mut state))
            if state.state.compensation_data.is_some() && !compensation_in_flight =>
        {
            state.state.attempt = context.attempt;
            let comp_data = state.state.compensation_data.clone().unwrap_or_default();
            (Vec::new(), comp_data, state)
        }
        Some(other) => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return crate::state_ext::unexpected_compensation_state_outcome(
                participant,
                context,
                other,
                (step, participant_id, compensation_in_flight),
                now,
                emit,
            );
        }
        None => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return crate::state_ext::missing_completed_state_outcome(
                participant,
                context,
                (step, participant_id),
                now,
                emit,
            );
        }
    };
    let mut new_state = new_state;
    new_state.state.compensation_data = Some(comp_data.clone());
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
                Ok(event) => {
                    emit(event);
                    IngressOutcome::Applied
                }
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

/// Outcome context stamped with the request's `attempt`, so the retryable event's identity is
/// distinct per attempt (`next_step` resets it to 0).
fn retryable_context(context: &SagaContext, step: Box<str>) -> SagaContext {
    let mut outcome = context.next_step(step);
    outcome.attempt = context.attempt;
    outcome
}

/// ADR-0004 §2.5: the undo reported `SafeToRetry`. State stays `Compensating` with its undo
/// data; a durable `CompensationRetryable` marker closes the `CompensationStarted` row (R2), and
/// `CompensationFailedRetryable` is emitted so the resolver can re-request. Not a quarantine:
/// `on_quarantined` is not called.
///
/// If the marker cannot be committed the run is still retryable live (the retry commits its own
/// `CompensationStarted`), but a crash before then reopens `Quarantined`; the commit failure is
/// logged and returned as a typed `CompensationResult` failure after the event is emitted.
pub(crate) fn emit_compensation_retryable<A, F>(
    actor: &A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    (reason, now): (&str, u64),
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt + ?Sized,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    tracing::warn!(
        target: "core::saga",
        event = "saga_compensation_retryable",
        run = %run,
        attempt = context.attempt,
        reason = %reason
    );
    let marker = actor.commit_transition(
        &run,
        ParticipantEvent::CompensationRetryable {
            attempt: context.attempt,
            reason: reason.into(),
            retryable_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::CompensationFailedRetryable {
        context: retryable_context(context, step),
        participant_id,
        error: reason.into(),
    });
    match marker {
        Ok(()) => IngressOutcome::Applied,
        Err(source) => {
            tracing::error!(
                target: "core::saga",
                event = "saga_compensation_retryable_marker_failed",
                run = %run,
                error = ?source,
                "retryable marker not durable: a crash before the retry reopens Quarantined"
            );
            IngressOutcome::Failed(IngressFailure {
                run,
                stage: CommitStage::CompensationResult,
                source,
            })
        }
    }
}

/// ADR-0002 §2.2 `CompensationStart`: `CompensationStarted` could not be committed, so the undo
/// was not invoked and nothing changed. The `Completed` state (with its undo data) is kept and a
/// `CompensationFailedRetryable` is emitted so the resolver can re-request.
pub(crate) fn fail_compensation_start_commit<F>(
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    err: SagaStateStoreError,
    emit: &mut F,
) -> IngressOutcome
where
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    tracing::error!(
        target: "core::saga",
        event = "saga_compensation_start_commit_failed",
        run = %run,
        error = ?err
    );
    emit(SagaChoreographyEvent::CompensationFailedRetryable {
        context: retryable_context(context, step),
        participant_id,
        error: format!("compensation start commit failed: {err}").into(),
    });
    IngressOutcome::Failed(IngressFailure {
        run,
        stage: CommitStage::CompensationStart,
        source: err,
    })
}

/// ADR-0002 §2.2 `CompensationResult` (Q6): the undo ran (or may have) but its outcome could not
/// be committed. Never acknowledged: `Compensating -> Quarantined`, `SagaQuarantined` emitted.
fn quarantine_compensation_result_commit_failure<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    err: SagaStateStoreError,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let reason: Box<str> =
        format!("reconciliation_needed: compensation_result_commit_failed: {err}").into();
    tracing::error!(
        target: "core::saga",
        event = "saga_compensation_result_commit_failed",
        run = %run,
        step = %step,
        error = ?err
    );
    quarantine_after_commit_failure(
        actor,
        context,
        step.clone(),
        participant_id,
        &reason,
        now,
        emit,
    );
    IngressOutcome::ReconciliationNeeded(ReconciliationNeeded {
        run,
        step,
        cause: ReconciliationCause::CompensationResultCommitFailed(err),
        compensation_data: Vec::new(),
    })
}

/// Commits `CompensationCompleted` with its obligation, then `Compensating -> Compensated`, then
/// emits. A failed commit quarantines; the caller runs `on_compensation_completed` only on
/// `Applied`.
pub(crate) fn commit_compensation_completed<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let completed = SagaChoreographyEvent::CompensationCompleted {
        context: context.next_step(step.clone()),
    };
    if let Err(err) = actor.commit_transition_with_outbox(
        &run,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: now,
        },
        vec![completed.clone()],
    ) {
        return quarantine_compensation_result_commit_failure(
            actor,
            context,
            step,
            participant_id,
            err,
            now,
            emit,
        );
    }
    if let Some(SagaStateEntry::Compensating(state)) = actor.saga_states().remove(&run) {
        actor.saga_states().insert(
            run,
            SagaStateEntry::Compensated(state.complete_compensation(now)),
        );
    }
    emit(completed);
    IngressOutcome::Applied
}

/// Commits the compensation failure (or quarantine) row with the events it obliges, then moves
/// the state and emits. A failed commit quarantines with `CompensationResultCommitFailed`.
pub(crate) fn commit_compensation_failed<A, F>(
    actor: &mut A,
    context: &SagaContext,
    step: Box<str>,
    participant_id: Box<str>,
    (reason, is_ambiguous): (Box<str>, bool),
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let event_context = context.next_step(step.clone());
    let mut outbox = vec![SagaChoreographyEvent::CompensationFailed {
        context: event_context.clone(),
        participant_id: participant_id.clone(),
        error: reason.clone(),
        is_ambiguous,
    }];
    if is_ambiguous {
        outbox.push(SagaChoreographyEvent::SagaQuarantined {
            context: event_context,
            reason: reason.clone(),
            step: step.clone(),
            participant_id: participant_id.clone(),
        });
    }
    let row = if is_ambiguous {
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        }
    } else {
        ParticipantEvent::CompensationFailed {
            error: reason.clone(),
            is_ambiguous,
            failed_at_millis: now,
        }
    };
    if let Err(err) = actor.commit_transition_with_outbox(&run, row, outbox.clone()) {
        return quarantine_compensation_result_commit_failure(
            actor,
            context,
            step,
            participant_id,
            err,
            now,
            emit,
        );
    }
    if let Some(SagaStateEntry::Compensating(state)) = actor.saga_states().remove(&run) {
        let entry = if is_ambiguous {
            SagaStateEntry::Quarantined(state.quarantine(reason, now))
        } else {
            SagaStateEntry::Failed(state.fail(reason, false, now))
        };
        let quarantined = matches!(entry, SagaStateEntry::Quarantined(_));
        actor.saga_states().insert(run.clone(), entry);
        if quarantined {
            crate::state_ext::un_admit_and_latch_quarantined(actor, &run);
        }
    }
    for event in outbox {
        emit(event);
    }
    IngressOutcome::Applied
}

/// Complete compensation
fn complete_compensation<P, F>(
    participant: &mut P,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    let outcome =
        commit_compensation_completed(participant, context, step, participant_id, now, emit);
    if matches!(outcome, IngressOutcome::Applied) {
        participant.on_compensation_completed(context);
    }
    outcome
}

fn complete_compensation_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    let outcome =
        commit_compensation_completed(participant, context, step, participant_id, now, emit);
    if matches!(outcome, IngressOutcome::Applied) {
        participant.on_compensation_completed(context);
    }
    outcome
}

/// Fail compensation (quarantine)
fn fail_compensation<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: CompensationError,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let (reason, is_ambiguous) = match error {
        CompensationError::SafeToRetry { reason } => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return emit_compensation_retryable(
                participant,
                context,
                step,
                participant_id,
                (&reason, now),
                emit,
            );
        }
        CompensationError::Ambiguous { reason } => (reason, true),
        CompensationError::Terminal { reason } => (reason, false),
    };
    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    let outcome = commit_compensation_failed(
        participant,
        context,
        step,
        participant_id,
        (reason.clone(), is_ambiguous),
        now,
        emit,
    );
    if is_ambiguous && matches!(outcome, IngressOutcome::Applied) {
        participant.on_quarantined(context, &reason);
    }
    outcome
}

fn fail_compensation_async<P, F>(
    participant: &mut P,
    context: &SagaContext,
    error: CompensationError,
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let (reason, is_ambiguous) = match error {
        CompensationError::SafeToRetry { reason } => {
            let step = participant.step_name().into();
            let participant_id = participant.participant_id_owned();
            return emit_compensation_retryable(
                participant,
                context,
                step,
                participant_id,
                (&reason, now),
                emit,
            );
        }
        CompensationError::Ambiguous { reason } => (reason, true),
        CompensationError::Terminal { reason } => (reason, false),
    };
    let step = participant.step_name().into();
    let participant_id = participant.participant_id_owned();
    let outcome = commit_compensation_failed(
        participant,
        context,
        step,
        participant_id,
        (reason.clone(), is_ambiguous),
        now,
        emit,
    );
    if is_ambiguous && matches!(outcome, IngressOutcome::Applied) {
        participant.on_quarantined(context, &reason);
    }
    outcome
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
        let rows: Vec<&ParticipantEvent> = entries
            .iter()
            .map(|entry| entry.event.transition())
            .collect();
        assert!(matches!(
            rows.as_slice(),
            [
                _,
                ParticipantEvent::StepExecutionCompleted {
                    compensation_data,
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
        let rows: Vec<&ParticipantEvent> = entries
            .iter()
            .map(|entry| entry.event.transition())
            .collect();
        assert!(matches!(
            rows.last(),
            Some(ParticipantEvent::CompensationFailed {
                error,
                is_ambiguous: false,
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
        let rows: Vec<&ParticipantEvent> = entries
            .iter()
            .map(|entry| entry.event.transition())
            .collect();
        assert!(matches!(
            rows.last(),
            Some(ParticipantEvent::Quarantined { reason, .. })
                if reason.as_ref() == "cannot confirm rollback"
        ));
        assert!(matches!(
            emitted.get(1),
            Some(SagaChoreographyEvent::SagaQuarantined { .. })
        ));
    }

    #[test]
    fn handle_saga_event_latches_and_retains_on_quarantine() {
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
        assert!(
            participant.saga_states().contains_key(&run),
            "quarantine evidence (prior_state/compensation_data) must be retained"
        );

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

    #[test]
    fn quarantined_run_stays_fenced_after_terminal_latch_eviction() {
        let mut participant = TestParticipant::default();
        let started = started_event();
        let run = started.context().run_key();
        handle_saga_event_with_emit(&mut participant, started.clone(), |_| {});
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::SagaQuarantined {
                context: started.context().clone(),
                reason: "uncertain".into(),
                step: "risk_check".into(),
                participant_id: "risk_check".into(),
            },
            |_| {},
        );
        // Simulate bounded-latch eviction.
        participant.unlatch_terminal_saga(&run);
        handle_saga_event_with_emit(&mut participant, started, |_| {});
        assert_eq!(
            participant.executed, 1,
            "quarantined run must not re-execute"
        );
        assert!(participant.saga_states().contains_key(&run));
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
