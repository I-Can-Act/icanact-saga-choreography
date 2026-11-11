//! Helper functions for saga handling
//!
//! Both the sync and async entry points share one fail-closed protocol:
//!
//! * the durable journal (never a bounded memory cache) decides whether an event's
//!   run may be processed (`admit_participant_event_strict`);
//! * storage failures visibly quarantine rather than masquerading as duplicates;
//! * execution and undo intent are persisted before the business effect runs;
//! * success (`StepCompleted`, `CompensationCompleted`) is only published after its
//!   result was durably recorded, otherwise the run is quarantined with evidence;
//! * terminal outcomes leave a durable tombstone; journals and dedupe are never
//!   pruned automatically.

use crate::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, DependencySpec,
    EffectDispatchOutcome, EffectDispatchRequest, ParticipantAdmission, ParticipantEvent,
    ParticipantForwardOutcome, ParticipantStore, ParticipantTerminalKind, SagaChoreographyEvent,
    SagaContext, SagaFailureDetails, SagaParticipant, SagaParticipantState, SagaStateEntry,
    SagaStateExt, StepError, StepOutput, StoreAdmission,
};

/// Owned work prepared by the synchronous protocol; no actor borrow crosses business IO.
enum Prepared {
    Step {
        context: SagaContext,
        input: Vec<u8>,
        saga_input: Vec<u8>,
    },
    Compensation {
        context: SagaContext,
        saga_input: Vec<u8>,
        data: Vec<u8>,
    },
    Notify {
        context: SagaContext,
        notification: Notification,
        terminal: bool,
    },
    Ignore,
}

enum Notification {
    Completed,
    Failed(Box<str>),
    Quarantined(Box<str>),
    Compensated,
    None,
}

fn prepare_event<A, F, S>(
    actor: &mut A,
    event: SagaChoreographyEvent,
    who: &Who,
    dependencies: &DependencySpec,
    matches_type: bool,
    emit: &mut F,
    start: &mut S,
) -> Prepared
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
    S: FnMut(&mut A, SagaChoreographyEvent) -> Result<(), Box<str>>,
{
    if !matches_type {
        return Prepared::Ignore;
    }
    if let SagaChoreographyEvent::CompensationRequested {
        steps_to_compensate,
        ..
    } = &event
        && !owns_compensation(&who.step, steps_to_compensate)
    {
        return Prepared::Ignore;
    }
    if matches!(event, SagaChoreographyEvent::StepFailed { .. })
        && actor
            .inspect_participant_event_strict(event.context())
            .is_ok_and(|admission| admission.is_admitted())
    {
        crate::durability::mark_matching_accepted_step_failed(actor, &event);
    }
    // Irrelevant events must not allocate run metadata or consume admission markers.
    let relevant = match &event {
        SagaChoreographyEvent::SagaStarted { .. } => dependencies.is_on_saga_start(),
        SagaChoreographyEvent::StepCompleted { context, .. } => match dependencies {
            DependencySpec::OnSagaStart => false,
            DependencySpec::After(step) => context.step_name.as_ref() == *step,
            DependencySpec::AnyOf(steps) | DependencySpec::AllOf(steps) => {
                steps.contains(&context.step_name.as_ref())
            }
        },
        SagaChoreographyEvent::CompensationRequested { .. } => true,
        _ => is_terminal_event(&event),
    };
    if !relevant || !admit_event(actor, &event, who, dependencies, emit) {
        return Prepared::Ignore;
    }
    let context = event.context().clone();
    let now = actor.now_millis();
    if is_terminal_event(&event) {
        let (notification, terminal) = commit_terminal(actor, &event, who, now, emit);
        return Prepared::Notify {
            context,
            notification,
            terminal,
        };
    }
    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. } => {
            if begin_step(actor, who, &context, "saga_started", now, emit, start) {
                Prepared::Step {
                    context,
                    input: payload.clone(),
                    saga_input: payload,
                }
            } else {
                Prepared::Ignore
            }
        }
        SagaChoreographyEvent::StepCompleted {
            context,
            output,
            saga_input,
            ..
        } => {
            if !dependency_ready(actor, who, &context, dependencies, now, emit)
                || forward_already_confirmed(actor, &context)
            {
                return Prepared::Ignore;
            }
            let input = if dependencies.prefers_original_saga_input() {
                saga_input.clone()
            } else {
                output
            };
            let context = context.next_step(who.step.clone());
            if begin_step(actor, who, &context, "step_completed", now, emit, start) {
                Prepared::Step {
                    context,
                    input,
                    saga_input,
                }
            } else {
                Prepared::Ignore
            }
        }
        SagaChoreographyEvent::CompensationRequested {
            failed_step,
            reason,
            failure,
            steps_to_compensate,
            ..
        } => {
            let notification = match start_compensation(
                actor,
                who,
                &context,
                CompensationRequest {
                    failed_step,
                    reason,
                    failure,
                    steps_to_compensate,
                },
                now,
                emit,
            ) {
                CompensationStart::Run {
                    saga_input,
                    compensation_data,
                } => {
                    return Prepared::Compensation {
                        context,
                        saga_input,
                        data: compensation_data,
                    };
                }
                CompensationStart::Skip => return Prepared::Ignore,
                CompensationStart::Acknowledge => {
                    resend_compensation_ack(who, &context, emit);
                    Notification::None
                }
                CompensationStart::NoEffect => {
                    match complete_no_effect_compensation(actor, who, &context, now, emit) {
                        Ok(()) => Notification::Compensated,
                        Err(reason) => Notification::Quarantined(reason),
                    }
                }
                CompensationStart::Quarantined(reason) => Notification::Quarantined(reason),
            };
            Prepared::Notify {
                context,
                notification,
                terminal: false,
            }
        }
        _ => Prepared::Ignore,
    }
}

fn commit_terminal<A: SagaStateExt, F: FnMut(SagaChoreographyEvent)>(
    actor: &mut A,
    event: &SagaChoreographyEvent,
    who: &Who,
    now: u64,
    emit: &mut F,
) -> (Notification, bool) {
    match event {
        SagaChoreographyEvent::SagaCompleted { .. } if retain_terminal(actor, event) => {
            (Notification::Completed, true)
        }
        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            match resolve_failed(actor, event, who, now, emit) {
                FailedResolution::Ordinary => (Notification::Failed(reason.clone()), true),
                FailedResolution::Escalated(reason) => (Notification::Quarantined(reason), false),
                FailedResolution::NotRetained => (Notification::None, false),
            }
        }
        SagaChoreographyEvent::SagaQuarantined { reason, .. } if retain_terminal(actor, event) => {
            (Notification::Quarantined(reason.clone()), true)
        }
        _ => (Notification::None, false),
    }
}

/// Validate and strictly publish the start before invoking business code.
pub(crate) fn publish_start<A: SagaStateExt>(
    actor: &mut A,
    event: SagaChoreographyEvent,
) -> Result<(), Box<str>> {
    if !crate::is_valid_emitted_transition(
        actor.saga_states_ref().get(&event.context().saga_id),
        &event,
    ) {
        return Err("invalid step start; effect not run".into());
    }
    if let Some(bus) = &actor.saga_support().bus {
        bus.publish_strict(event).map_err(|error| -> Box<str> {
            format!("step start publication failed; effect not run: {error:?}").into()
        })?;
    }
    Ok(())
}

pub(crate) fn handle_saga_event_core<P, F, S>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut emit: F,
    mut start: S,
) -> bool
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
    S: FnMut(&mut P, SagaChoreographyEvent) -> Result<(), Box<str>>,
{
    let who = Who::sync(participant);
    let dependencies = participant.depends_on();
    let matches_type = participant
        .saga_types()
        .contains(&event.context().saga_type.as_ref());
    let prepared = prepare_event(
        participant,
        event,
        &who,
        &dependencies,
        matches_type,
        &mut emit,
        &mut start,
    );
    let (context, notification, terminal) = match prepared {
        Prepared::Step {
            context,
            input,
            saga_input,
        } => {
            let now = participant.now_millis();
            let output = participant.execute_step(&context, &input);
            let (dispatch, mut notification) = commit_step_result(
                participant,
                &who,
                &context,
                saga_input.clone(),
                output,
                now,
                &mut emit,
            );
            if let Some(result) = dispatch {
                let request = EffectDispatchRequest {
                    context: &context,
                    effect: result.effect.as_deref().unwrap(),
                    output: &result.output,
                    compensation_data: &result.compensation_data,
                };
                let dispatched = participant.dispatch_effect(&request);
                notification = commit_dispatch_result(
                    participant,
                    &who,
                    &context,
                    saga_input,
                    result,
                    dispatched,
                    now,
                    &mut emit,
                );
            }
            (context, notification, false)
        }
        Prepared::Compensation {
            context,
            saga_input,
            data,
        } => {
            let now = participant.now_millis();
            let result = participant.compensate_step(&context, &data);
            let notification = commit_compensation_result(
                participant,
                &who,
                &context,
                saga_input,
                data,
                result,
                now,
                &mut emit,
            );
            (context, notification, false)
        }
        Prepared::Notify {
            context,
            notification,
            terminal,
        } => (context, notification, terminal),
        Prepared::Ignore => return false,
    };
    match notification {
        Notification::Completed => participant.on_saga_completed(&context),
        Notification::Failed(reason) => participant.on_saga_failed(&context, &reason),
        Notification::Quarantined(reason) => participant.on_quarantined(&context, &reason),
        Notification::Compensated => participant.on_compensation_completed(&context),
        Notification::None => {}
    }
    terminal
}

pub fn handle_saga_event_with_emit<P, F>(participant: &mut P, event: SagaChoreographyEvent, emit: F)
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    handle_saga_event_core(participant, event, emit, publish_start);
}

pub(crate) async fn handle_async_saga_event_core<P, F, S>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    mut emit: F,
    mut start: S,
) -> bool
where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
    S: FnMut(&mut P, SagaChoreographyEvent) -> Result<(), Box<str>>,
{
    let who = Who::asynchronous(participant);
    let dependencies = participant.depends_on();
    let matches_type = participant
        .saga_types()
        .contains(&event.context().saga_type.as_ref());
    let prepared = prepare_event(
        participant,
        event,
        &who,
        &dependencies,
        matches_type,
        &mut emit,
        &mut start,
    );
    let (context, notification, terminal) = match prepared {
        Prepared::Step {
            context,
            input,
            saga_input,
        } => {
            let now = participant.now_millis();
            let output = participant.execute_step(&context, &input).await;
            let (dispatch, mut notification) = commit_step_result(
                participant,
                &who,
                &context,
                saga_input.clone(),
                output,
                now,
                &mut emit,
            );
            if let Some(result) = dispatch {
                let request = EffectDispatchRequest {
                    context: &context,
                    effect: result.effect.as_deref().unwrap(),
                    output: &result.output,
                    compensation_data: &result.compensation_data,
                };
                let dispatched = participant.dispatch_effect(&request).await;
                notification = commit_dispatch_result(
                    participant,
                    &who,
                    &context,
                    saga_input,
                    result,
                    dispatched,
                    now,
                    &mut emit,
                );
            }
            (context, notification, false)
        }
        Prepared::Compensation {
            context,
            saga_input,
            data,
        } => {
            let now = participant.now_millis();
            let result = participant.compensate_step(&context, &data).await;
            let notification = commit_compensation_result(
                participant,
                &who,
                &context,
                saga_input,
                data,
                result,
                now,
                &mut emit,
            );
            (context, notification, false)
        }
        Prepared::Notify {
            context,
            notification,
            terminal,
        } => (context, notification, terminal),
        Prepared::Ignore => return false,
    };
    match notification {
        Notification::Completed => participant.on_saga_completed(&context),
        Notification::Failed(reason) => participant.on_saga_failed(&context, &reason),
        Notification::Quarantined(reason) => participant.on_quarantined(&context, &reason),
        Notification::Compensated => participant.on_compensation_completed(&context),
        Notification::None => {}
    }
    terminal
}

pub async fn handle_async_saga_event_with_emit<P, F>(
    participant: &mut P,
    event: SagaChoreographyEvent,
    emit: F,
) where
    P: AsyncSagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    handle_async_saga_event_core(participant, event, emit, publish_start).await;
}

pub(crate) fn handle_workflow_saga_event_with_emit<A, F, S>(
    actor: &mut A,
    workflow: &'static dyn crate::SagaWorkflowParticipant<A>,
    event: SagaChoreographyEvent,
    mut emit: F,
    mut start: S,
) -> bool
where
    A: SagaStateExt + crate::HasSagaWorkflowParticipants,
    F: FnMut(SagaChoreographyEvent),
    S: FnMut(&mut A, SagaChoreographyEvent) -> Result<(), Box<str>>,
{
    let who = Who {
        step: workflow.step_name().into(),
        participant_id: workflow.participant_id_owned(),
    };
    let dependencies = workflow.depends_on();
    let matches_type = workflow
        .saga_types()
        .contains(&event.context().saga_type.as_ref());
    let prepared = prepare_event(
        actor,
        event,
        &who,
        &dependencies,
        matches_type,
        &mut emit,
        &mut start,
    );
    let (context, notification, terminal) = match prepared {
        Prepared::Step {
            context,
            input,
            saga_input,
        } => {
            let now = actor.now_millis();
            let output = workflow.execute_step(actor, &context, &input);
            let (dispatch, mut notification) = commit_step_result(
                actor,
                &who,
                &context,
                saga_input.clone(),
                output,
                now,
                &mut emit,
            );
            if let Some(result) = dispatch {
                let request = EffectDispatchRequest {
                    context: &context,
                    effect: result.effect.as_deref().unwrap(),
                    output: &result.output,
                    compensation_data: &result.compensation_data,
                };
                let dispatched = workflow.dispatch_effect(actor, &request);
                notification = commit_dispatch_result(
                    actor, &who, &context, saga_input, result, dispatched, now, &mut emit,
                );
            }
            (context, notification, false)
        }
        Prepared::Compensation {
            context,
            saga_input,
            data,
        } => {
            let now = actor.now_millis();
            let result = workflow.compensate_step(actor, &context, &data);
            let notification = commit_compensation_result(
                actor, &who, &context, saga_input, data, result, now, &mut emit,
            );
            (context, notification, false)
        }
        Prepared::Notify {
            context,
            notification,
            terminal,
        } => (context, notification, terminal),
        Prepared::Ignore => return false,
    };
    match notification {
        Notification::Completed => workflow.on_saga_completed(actor, &context),
        Notification::Failed(reason) => workflow.on_saga_failed(actor, &context, &reason),
        Notification::Quarantined(reason) => workflow.on_quarantined(actor, &context, &reason),
        Notification::Compensated => workflow.on_compensation_completed(actor, &context),
        Notification::None => {}
    }
    terminal
}

/// Step identity captured once per handled event.
struct Who {
    step: Box<str>,
    participant_id: Box<str>,
}

impl Who {
    fn sync<P: SagaParticipant>(participant: &P) -> Self {
        Self {
            step: participant.step_name().into(),
            participant_id: participant.participant_id_owned(),
        }
    }

    fn asynchronous<P: AsyncSagaParticipant>(participant: &P) -> Self {
        Self {
            step: participant.step_name().into(),
            participant_id: participant.participant_id_owned(),
        }
    }
}

struct CompensationRequest {
    failed_step: Box<str>,
    reason: Box<str>,
    failure: SagaFailureDetails,
    steps_to_compensate: Vec<Box<str>>,
}

/// Singleton compensation ownership: the resolver emits one step per request, so a
/// request belongs to its only (head) step. A legacy multi-step list is honored only
/// by its head (the next step in reverse completion order); the other listed steps
/// wait for their own singleton request instead of rolling back out of order.
fn owns_compensation(step: &str, steps_to_compensate: &[Box<str>]) -> bool {
    steps_to_compensate
        .first()
        .is_some_and(|head| &**head == step)
}

fn is_terminal_event(event: &SagaChoreographyEvent) -> bool {
    matches!(
        event,
        SagaChoreographyEvent::SagaCompleted { .. }
            | SagaChoreographyEvent::SagaFailed { .. }
            | SagaChoreographyEvent::SagaQuarantined { .. }
    )
}

/// Durable admission plus run-scoped dedupe. Returns whether the event may be
/// processed. Replay/stale events are ignored; ambiguous ownership and storage
/// failures quarantine visibly before any effect.
fn admit_event<A, F>(
    actor: &mut A,
    event: &SagaChoreographyEvent,
    who: &Who,
    dependencies: &DependencySpec,
    emit: &mut F,
) -> bool
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let context = event.context();
    match actor.inspect_participant_event_strict(context) {
        Ok(ParticipantAdmission::Admitted) => {}
        Ok(ParticipantAdmission::TerminalReplay { .. })
            if matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }) =>
        {
            return true;
        }
        Ok(ParticipantAdmission::TerminalReplay { .. } | ParticipantAdmission::StaleRun { .. }) => {
            return false;
        }
        Ok(other) => {
            tracing::warn!(
                target: "core::saga",
                event = "saga_participant_event_refused",
                saga_id = context.saga_id.get(),
                event_type = event.event_type(),
                admission = ?other
            );
            // A rejected foreign run must not quarantine/reset the legitimate
            // active owner's state. Ambiguous legacy history needs reconciliation.
            if matches!(other, ParticipantAdmission::LegacyHistory) && !is_terminal_event(event) {
                quarantine_run(
                    actor,
                    who,
                    context,
                    format!("participant admission refused: {other:?}").into(),
                    actor.now_millis(),
                    emit,
                );
            }
            return false;
        }
        Err(error) => {
            tracing::error!(
                target: "core::saga",
                event = "saga_participant_admission_failed",
                saga_id = context.saga_id.get(),
                error = %error
            );
            // An unreadable journal cannot prove a failure is ordinary, so a failure
            // escalates visibly as well; success/quarantine terminals stay idempotent.
            if !is_terminal_event(event)
                || matches!(event, SagaChoreographyEvent::SagaFailed { .. })
            {
                quarantine_run(
                    actor,
                    who,
                    context,
                    format!("participant admission failed: {error}").into(),
                    actor.now_millis(),
                    emit,
                );
            }
            return false;
        }
    }
    // Terminal events are idempotent: the durable tombstone is their replay fence.
    if is_terminal_event(event) {
        return true;
    }
    // Persist a relevant AllOf input before its input marker can consume it.
    // A crash after marking but before recording must not silently lose a branch.
    // Duplicate observations are harmless, but avoid growing the journal on replay.
    if matches!(event, SagaChoreographyEvent::StepCompleted { .. })
        && matches!(dependencies, DependencySpec::AllOf(steps)
            if steps.contains(&context.step_name.as_ref()))
    {
        let observed = actor
            .admit_participant_event_strict(context)
            .and_then(|_| actor.completed_dependency_steps_strict(context))
            .and_then(|seen| {
                if seen.contains(&context.step_name) {
                    Ok(())
                } else {
                    actor.record_dependency_completion_strict(context)
                }
            });
        if let Err(error) = observed {
            quarantine_run(
                actor,
                who,
                context,
                format!("dependency observation unavailable; step not run: {error}").into(),
                actor.now_millis(),
                emit,
            );
            return false;
        }
    }
    reset_in_memory_run_if_changed(actor, context);
    true
}

/// Per-run in-memory dependency/state tracking must never leak across runs that
/// reuse a saga id.
pub(crate) fn reset_in_memory_run_if_changed<A>(actor: &mut A, context: &SagaContext)
where
    A: SagaStateExt,
{
    let saga_id = context.saga_id;
    if actor.saga_support().saga_run_started_at.get(&saga_id)
        != Some(&context.saga_started_at_millis)
    {
        actor.unlatch_terminal_saga(saga_id);
        actor.clear_in_memory_saga_run_tracking(saga_id);
        actor.record_saga_run_start(saga_id, context.saga_started_at_millis);
    }
}

/// Durably retains the terminal tombstone (no journal/dedupe pruning) and drops
/// in-memory run tracking. Returns whether the tombstone is durable.
fn retain_terminal<A>(actor: &mut A, event: &SagaChoreographyEvent) -> bool
where
    A: SagaStateExt,
{
    match actor.retain_terminal_event_strict(event) {
        Ok(retained) => {
            if retained && !matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                actor.clear_in_memory_saga_run_tracking(event.context().saga_id);
            }
            retained
        }
        Err(error) => {
            tracing::error!(
                target: "core::saga",
                event = "saga_participant_terminal_retention_failed",
                saga_id = event.context().saga_id.get(),
                error = %error
            );
            false
        }
    }
}

/// Outcome of handling an ordinary `SagaFailed` for this participant.
enum FailedResolution {
    /// Nothing unresolved: ordinary failure cleanup may run.
    Ordinary,
    /// Unresolved intent/effect/undo (or unreadable evidence): quarantined and
    /// published as `SagaQuarantined`; ordinary failure cleanup must not run.
    Escalated(Box<str>),
    /// The tombstone could not be made durable.
    NotRetained,
}

/// A failure is ordinary only when the journal shows no open intent, accepted work,
/// unreversed compensation/effect or undo uncertainty. Otherwise the failure is
/// escalated to a retained quarantine (evidence, accepted metadata and dedupe stay).
fn resolve_failed<A, F>(
    actor: &mut A,
    event: &SagaChoreographyEvent,
    who: &Who,
    now: u64,
    emit: &mut F,
) -> FailedResolution
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let context = event.context();
    let reason: Box<str> = match actor.participant_run_evidence_strict(context) {
        Ok(evidence) if !evidence.failure_requires_quarantine() => {
            return if retain_terminal(actor, event) {
                FailedResolution::Ordinary
            } else {
                FailedResolution::NotRetained
            };
        }
        Ok(_) => {
            let SagaChoreographyEvent::SagaFailed { reason, .. } = event else {
                return FailedResolution::NotRetained;
            };
            format!("saga failed ({reason}) with unresolved participant intent/effect/undo").into()
        }
        Err(error) => format!("saga failure evidence unavailable: {error}").into(),
    };
    quarantine_run(actor, who, context, reason.clone(), now, emit);
    FailedResolution::Escalated(reason)
}

/// True when this run already has durably confirmed forward work and no undo
/// uncertainty, so a repeated dependency firing (e.g. the second AnyOf branch after
/// a restart cleared volatile tracking) is ignored rather than re-executed or
/// quarantined. Unconfirmed or unreadable evidence returns false so `begin_step`
/// fails closed.
fn forward_already_confirmed<A>(actor: &A, context: &SagaContext) -> bool
where
    A: SagaStateExt,
{
    actor
        .participant_run_evidence_strict(context)
        .is_ok_and(|evidence| {
            evidence.forward_outcome.is_some()
                && !evidence.undo_intent_open
                && !evidence.needs_reconciliation
                && !evidence.quarantined
        })
}

/// Decides whether this completed dependency completes the participant's trigger.
///
/// `AllOf` observations are durable: each relevant completion is strictly appended
/// (exact run context) *before* it counts, and the seen set is rebuilt from the
/// journal for this exact run, so a restart between branches cannot stall the step.
/// A storage failure quarantines visibly and never authorizes execution.
fn dependency_ready<A, F>(
    actor: &mut A,
    who: &Who,
    completed: &SagaContext,
    dependency_spec: &DependencySpec,
    now: u64,
    emit: &mut F,
) -> bool
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = completed.saga_id;
    let completed_step = completed.step_name.as_ref();
    match dependency_spec {
        DependencySpec::OnSagaStart => false,
        DependencySpec::After(step) => {
            completed_step == *step && actor.dependency_fired().insert(saga_id)
        }
        DependencySpec::AnyOf(steps) => {
            steps.contains(&completed_step) && actor.dependency_fired().insert(saga_id)
        }
        DependencySpec::AllOf(steps) => {
            if !steps.contains(&completed_step) {
                return false;
            }
            // Admission already persisted this observation before input dedupe.
            let observed = actor.completed_dependency_steps_strict(completed);
            let observed = match observed {
                Ok(observed) => observed,
                Err(error) => {
                    tracing::error!(
                        target: "core::saga",
                        event = "saga_dependency_observation_failed",
                        saga_id = saga_id.get(),
                        step = completed_step,
                        error = %error
                    );
                    quarantine_run(
                        actor,
                        who,
                        completed,
                        format!("dependency observation unavailable; step not run: {error}").into(),
                        now,
                        emit,
                    );
                    return false;
                }
            };
            let seen = actor.dependency_completions().entry(saga_id).or_default();
            seen.extend(observed);
            seen.insert(completed_step.into());
            if !steps.iter().all(|step| seen.contains(*step)) {
                return false;
            }
            actor.dependency_fired().insert(saga_id)
        }
    }
}

/// Output of a step that completed (not accepted) and its optional declared effect.
struct StepResult {
    output: Vec<u8>,
    compensation_data: Vec<u8>,
    effect: Option<Box<str>>,
    /// Durable dispatch receipt, set once a declared effect was dispatched.
    receipt: Option<Box<str>>,
}

/// Persists forward execution intent, then publishes `StepStarted`. When intent
/// cannot be made durable the effect is not run and the step fails (nothing to undo).
fn begin_step<A, F, S>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    trigger: &str,
    now: u64,
    emit: &mut F,
    start: &mut S,
) -> bool
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
    S: FnMut(&mut A, SagaChoreographyEvent) -> Result<(), Box<str>>,
{
    let saga_id = context.saga_id;
    if forward_already_confirmed(actor, context)
        || actor
            .participant_run_evidence_strict(context)
            .is_ok_and(|e| e.accepted_forward_pending)
    {
        return false;
    }
    let refusal = match actor.forward_execution_recorded_strict(context) {
        Ok(false) => None,
        Ok(true) => Some(
            "forward execution already has durable intent/evidence; reconciliation required".into(),
        ),
        Err(error) => Some(format!("forward execution history unavailable: {error}").into()),
    };
    if let Some(reason) = refusal {
        quarantine_run(actor, who, context, reason, now, emit);
        return false;
    }
    match actor
        .saga_store()
        .admit_and_record_intent(context, "forward", trigger)
    {
        Ok(StoreAdmission::Admitted) => {}
        Ok(StoreAdmission::Duplicate | StoreAdmission::Rejected(_)) => return false,
        Err(error) => {
            quarantine_run(
                actor,
                who,
                context,
                format!("step intent persistence failed; effect not run: {error}").into(),
                now,
                emit,
            );
            return false;
        }
    }

    let state = SagaParticipantState::new(
        saga_id,
        context.saga_type.clone(),
        who.step.clone(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("dependency_satisfied", now)
    .start_execution(now);
    actor
        .saga_states()
        .insert(saga_id, SagaStateEntry::Executing(state));
    let started = SagaChoreographyEvent::StepStarted {
        context: context.next_step(who.step.clone()),
    };
    if let Err(reason) = start(actor, started.clone()) {
        quarantine_run(actor, who, context, reason, now, emit);
        return false;
    }
    emit(started);
    true
}

fn split_output(output: StepOutput) -> Result<StepResult, AcceptedOutput> {
    match output {
        StepOutput::Completed {
            output,
            compensation_data,
        } => Ok(StepResult {
            output,
            compensation_data,
            effect: None,
            receipt: None,
        }),
        StepOutput::CompletedWithEffect {
            output,
            compensation_data,
            effect,
        } => Ok(StepResult {
            output,
            compensation_data,
            effect: Some(effect),
            receipt: None,
        }),
        StepOutput::Accepted {
            execution_id,
            policy,
            compensation_data,
        } => Err(AcceptedOutput {
            execution_id,
            policy,
            compensation_data,
        }),
    }
}

struct AcceptedOutput {
    execution_id: crate::StepExecutionId,
    policy: crate::AcceptedStepPolicy,
    compensation_data: Vec<u8>,
}

fn accept_step<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    saga_input: Vec<u8>,
    accepted: AcceptedOutput,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    match crate::durability::accept_started_workflow_step(
        actor,
        context.next_step(who.step.clone()),
        who.participant_id.clone(),
        accepted.execution_id,
        accepted.policy,
        saga_input,
        accepted.compensation_data,
    ) {
        Ok(event) => emit(event),
        Err(error) => quarantine_run(
            actor,
            who,
            context,
            format!("accepted step persistence failed: {error:?}").into(),
            now,
            emit,
        ),
    }
}

/// Durably records the step result before success may be claimed. A failed write
/// quarantines the run and keeps the compensation payload in durable-or-logged
/// evidence; it returns the quarantine reason.
fn persist_result<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    result: &StepResult,
    now: u64,
    emit: &mut F,
) -> Result<(), Box<str>>
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    match actor.record_event_strict(
        saga_id,
        ParticipantEvent::StepExecutionCompleted {
            output: result.output.clone(),
            compensation_data: result.compensation_data.clone(),
            completed_at_millis: now,
        },
    ) {
        Ok(()) => {
            match actor.saga_states().remove(&saga_id) {
                Some(SagaStateEntry::Executing(state)) => {
                    let done = state.complete(
                        result.output.clone(),
                        result.compensation_data.clone(),
                        now,
                    );
                    actor
                        .saga_states()
                        .insert(saga_id, SagaStateEntry::Completed(done));
                }
                Some(other) => {
                    actor.saga_states().insert(saga_id, other);
                }
                None => {}
            }
            Ok(())
        }
        Err(error) => {
            let reason: Box<str> = format!("step result persistence failed: {error}").into();
            if let Err(evidence_error) = actor.retain_reconciliation_evidence_strict(
                context,
                &result.output,
                &result.compensation_data,
                &reason,
            ) {
                tracing::error!(target: "core::saga", event = "saga_reconciliation_evidence_failed",
                    saga_id = saga_id.get(), error = %evidence_error);
            }
            quarantine_run(actor, who, context, reason.clone(), now, emit);
            Err(reason)
        }
    }
}

fn dispatch_failure_reason(error: &crate::EffectDispatchError) -> Box<str> {
    format!("effect dispatch failed: {error}").into()
}

fn log_dispatch_receipt(
    context: &SagaContext,
    effect: &str,
    outcome: EffectDispatchOutcome,
) -> Box<str> {
    let EffectDispatchOutcome::Durable { receipt } = outcome;
    tracing::debug!(
        target: "core::saga",
        event = "saga_effect_dispatched",
        saga_id = context.saga_id.get(),
        effect,
        receipt = %receipt
    );
    receipt
}

/// Persists the confirmed forward proof (after business and declared-effect success)
/// and only then publishes the original `StepCompleted`. A failed append quarantines
/// and publishes no success.
///
/// A plain result (no declared effect) has no earlier raw-result row: the proof
/// append is its only durable record, so a failure retains the business bytes as
/// typed reconciliation evidence, and success completes the in-memory state.
fn finish_step<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    saga_input: Vec<u8>,
    result: StepResult,
    now: u64,
    emit: &mut F,
) -> Result<(), Box<str>>
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let plain = result.effect.is_none();
    let outcome = ParticipantForwardOutcome {
        context: context.next_step(who.step.clone()),
        output: result.output,
        saga_input,
        compensation_data: result.compensation_data,
        effect: result.effect,
        receipt: result.receipt,
        recorded_at_millis: now,
    };
    if let Err(error) = actor.record_forward_outcome_strict(&outcome) {
        let reason: Box<str> = format!("forward outcome persistence failed: {error}").into();
        if plain
            && let Err(evidence_error) = actor.retain_reconciliation_evidence_strict(
                context,
                &outcome.output,
                &outcome.compensation_data,
                &reason,
            )
        {
            tracing::error!(target: "core::saga", event = "saga_reconciliation_evidence_failed",
                saga_id = context.saga_id.get(), error = %evidence_error);
        }
        quarantine_run(actor, who, context, reason.clone(), now, emit);
        return Err(reason);
    }
    if plain {
        let saga_id = context.saga_id;
        match actor.saga_states().remove(&saga_id) {
            Some(SagaStateEntry::Executing(state)) => {
                let done = state.complete(
                    outcome.output.clone(),
                    outcome.compensation_data.clone(),
                    now,
                );
                actor
                    .saga_states()
                    .insert(saga_id, SagaStateEntry::Completed(done));
            }
            Some(other) => {
                actor.saga_states().insert(saga_id, other);
            }
            None => {}
        }
    }
    emit(outcome.completion_event());
    Ok(())
}

/// Evidence-preserving quarantine: state, durable `Quarantined` row, durable terminal
/// tombstone (no pruning), then the `SagaQuarantined` publication.
fn quarantine_run<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    reason: Box<str>,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let quarantined = match actor.saga_states().remove(&saga_id) {
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
    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    ) {
        tracing::error!(
            target: "core::saga",
            event = "saga_quarantine_persistence_failed",
            saga_id = saga_id.get(),
            reason = %reason,
            error = %error
        );
    }
    if let Err(error) =
        actor.retain_terminal_saga_strict(context, ParticipantTerminalKind::Quarantined, &reason)
    {
        tracing::error!(
            target: "core::saga",
            event = "saga_quarantine_tombstone_failed",
            saga_id = saga_id.get(),
            error = %error
        );
    }
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(who.step.clone()),
        reason,
        step: who.step.clone(),
        participant_id: who.participant_id.clone(),
    });
}

fn commit_step_result<A: SagaStateExt, F: FnMut(SagaChoreographyEvent)>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    saga_input: Vec<u8>,
    output: Result<StepOutput, StepError>,
    now: u64,
    emit: &mut F,
) -> (Option<StepResult>, Notification) {
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            fail_step(actor, who, context, error, now, emit);
            return (None, Notification::None);
        }
    };
    let result = match split_output(output) {
        Ok(result) => result,
        Err(accepted) => {
            accept_step(actor, who, context, saga_input, accepted, now, emit);
            return (None, Notification::None);
        }
    };
    if result.effect.is_none() {
        let notification = match finish_step(actor, who, context, saga_input, result, now, emit) {
            Ok(()) => Notification::None,
            Err(reason) => Notification::Quarantined(reason),
        };
        return (None, notification);
    }
    match persist_result(actor, who, context, &result, now, emit) {
        Ok(()) => (Some(result), Notification::None),
        Err(reason) => (None, Notification::Quarantined(reason)),
    }
}

fn commit_dispatch_result<A: SagaStateExt, F: FnMut(SagaChoreographyEvent)>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    saga_input: Vec<u8>,
    mut result: StepResult,
    dispatched: Result<EffectDispatchOutcome, crate::EffectDispatchError>,
    now: u64,
    emit: &mut F,
) -> Notification {
    match dispatched {
        Ok(outcome) => {
            result.receipt = Some(log_dispatch_receipt(
                context,
                result.effect.as_deref().unwrap(),
                outcome,
            ))
        }
        Err(error) => {
            let reason = dispatch_failure_reason(&error);
            quarantine_run(actor, who, context, reason.clone(), now, emit);
            return Notification::Quarantined(reason);
        }
    }
    match finish_step(actor, who, context, saga_input, result, now, emit) {
        Ok(()) => Notification::None,
        Err(reason) => Notification::Quarantined(reason),
    }
}

fn commit_compensation_result<A: SagaStateExt, F: FnMut(SagaChoreographyEvent)>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    saga_input: Vec<u8>,
    data: Vec<u8>,
    result: Result<CompensationOutput, CompensationError>,
    now: u64,
    emit: &mut F,
) -> Notification {
    match result {
        Ok(CompensationOutput::Completed) => {
            match complete_compensation(actor, who, context, now, emit) {
                Ok(()) => Notification::Compensated,
                Err(reason) => Notification::Quarantined(reason),
            }
        }
        Ok(CompensationOutput::Accepted {
            execution_id,
            policy,
        }) => {
            match crate::accept_started_workflow_compensation(
                actor,
                context.next_step(who.step.clone()),
                who.participant_id.clone(),
                execution_id,
                policy,
                saga_input,
                data,
            ) {
                Ok(event) => {
                    emit(event);
                    Notification::None
                }
                Err(error) => Notification::Quarantined(fail_compensation(
                    actor,
                    who,
                    context,
                    CompensationError::Ambiguous {
                        reason: format!("accepted compensation persistence failed: {error:?}")
                            .into(),
                    },
                    now,
                    emit,
                )),
            }
        }
        Err(error) => {
            Notification::Quarantined(fail_compensation(actor, who, context, error, now, emit))
        }
    }
}

/// Fail a step with state transition. A failure is conservative, so a lost failure
/// row is logged but the failure is still published.
fn fail_step<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    error: StepError,
    now: u64,
    emit: &mut F,
) where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let (reason, requires_comp) = match error {
        StepError::Terminal { reason } => (reason, false),
        StepError::RequireCompensation { reason } => (reason, true),
    };

    match actor.saga_states().remove(&saga_id) {
        Some(SagaStateEntry::Executing(state)) => {
            let failed = state.fail(reason.clone(), requires_comp, now);
            actor
                .saga_states()
                .insert(saga_id, SagaStateEntry::Failed(failed));
        }
        Some(other) => {
            actor.saga_states().insert(saga_id, other);
        }
        None => {}
    }

    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::StepExecutionFailed {
            error: reason.clone(),
            requires_compensation: requires_comp,
            failed_at_millis: now,
        },
    ) {
        tracing::error!(
            target: "core::saga",
            event = "saga_step_failure_persistence_failed",
            saga_id = saga_id.get(),
            error = %error
        );
    }

    emit(SagaChoreographyEvent::StepFailed {
        context: context.next_step(who.step.clone()),
        participant_id: who.participant_id.clone(),
        error_code: None,
        error: reason,
        requires_compensation: requires_comp,
    });
}

enum CompensationStart {
    Run {
        saga_input: Vec<u8>,
        compensation_data: Vec<u8>,
    },
    Skip,
    /// The undo already completed durably; only its acknowledgement is resent.
    Acknowledge,
    /// Forward work was durably and definitively rejected: persist a no-op
    /// completion proof (no business undo), then acknowledge.
    NoEffect,
    Quarantined(Box<str>),
}

/// Journal-derived answer for an undo request when no usable in-memory state exists.
enum UndoRecovery {
    /// Confirmed forward work: completed state rebuilt; carries its undo data.
    Rebuilt(Vec<u8>),
    Acknowledge,
    /// Durably rejected forward step with no effect evidence.
    NoEffect,
    /// No forward work or undo obligation exists for this participant.
    Nothing,
    Unsafe(Box<str>),
}

/// Rebuilds completed state on demand from durable, confirmed forward proof. Never
/// re-executes forward work and never guesses: open/unconfirmed intent, accepted
/// work, undo uncertainty or late forward evidence after an undo are `Unsafe`.
fn recover_for_undo<A>(actor: &mut A, who: &Who, context: &SagaContext, now: u64) -> UndoRecovery
where
    A: SagaStateExt,
{
    let evidence = match actor.participant_run_evidence_strict(context) {
        Ok(evidence) => evidence,
        Err(error) => {
            return UndoRecovery::Unsafe(
                format!("undo recovery evidence unavailable: {error}").into(),
            );
        }
    };
    if evidence.quarantined {
        return UndoRecovery::Nothing;
    }
    if evidence.undo_completed && !evidence.needs_reconciliation && !evidence.undo_intent_open {
        let state = SagaParticipantState::new(
            context.saga_id,
            context.saga_type.clone(),
            who.step.clone(),
            context.correlation_id,
            context.trace_id,
            context.initiator_peer_id,
            context.saga_started_at_millis,
        )
        .trigger("undo_recovered", now)
        .start_execution(now)
        .complete(evidence.output, evidence.compensation_data, now)
        .start_compensation(now)
        .complete_compensation(now);
        actor
            .saga_states()
            .insert(context.saga_id, SagaStateEntry::Compensated(state));
        return UndoRecovery::Acknowledge;
    }
    if crate::durability::no_effect_rejection_proven(&evidence) {
        return UndoRecovery::NoEffect;
    }
    if evidence.needs_reconciliation {
        return UndoRecovery::Unsafe(
            "undo requested but the run has unreconciled or late forward evidence".into(),
        );
    }
    if evidence.undo_intent_open {
        return UndoRecovery::Unsafe(
            "undo intent has no durable completion; its effect is uncertain".into(),
        );
    }
    if evidence.forward_intent_open
        || evidence.accepted_forward_pending
        || (evidence.forward_result_recorded && evidence.forward_outcome.is_none())
    {
        return UndoRecovery::Unsafe(
            "undo requested but forward work is unconfirmed; reconciliation required".into(),
        );
    }
    let Some(outcome) = evidence.forward_outcome else {
        return UndoRecovery::Nothing;
    };
    let completed = SagaParticipantState::new(
        context.saga_id,
        context.saga_type.clone(),
        who.step.clone(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("recovered_forward_outcome", now)
    .start_execution(outcome.recorded_at_millis)
    .complete(
        outcome.output,
        outcome.compensation_data.clone(),
        outcome.recorded_at_millis,
    );
    actor
        .saga_states()
        .insert(context.saga_id, SagaStateEntry::Completed(completed));
    UndoRecovery::Rebuilt(outcome.compensation_data)
}

fn resend_compensation_ack<F>(who: &Who, context: &SagaContext, emit: &mut F)
where
    F: FnMut(SagaChoreographyEvent),
{
    emit(SagaChoreographyEvent::CompensationCompleted {
        context: context.next_step(who.step.clone()),
    });
}

/// Persists the rollback request and the undo intent *before* the undo effect, then
/// moves the state to `Compensating`. A failed write quarantines (the obligation is
/// never silently dropped) and leaves the compensation payload in the retained state.
fn start_compensation<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    request: CompensationRequest,
    now: u64,
    emit: &mut F,
) -> CompensationStart
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::CompensationRequestRecorded {
            context: context.clone(),
            failed_step: request.failed_step,
            reason: request.reason,
            failure: request.failure,
            steps_to_compensate: request.steps_to_compensate,
            requested_at_millis: now,
        },
    ) {
        let reason: Box<str> = format!("compensation request persistence failed: {error:?}").into();
        quarantine_run(actor, who, context, reason.clone(), now, emit);
        return CompensationStart::Quarantined(reason);
    }

    // Volatile Completed/Executing(accepted) cache is never authority: a durably
    // rejected forward step can still look live when the post-append cleanup read
    // failed. Strict exact-run evidence is consulted first; an unreadable journal
    // quarantines instead of falling back to the cache.
    if matches!(
        actor.saga_states_ref().get(&saga_id),
        Some(SagaStateEntry::Completed(_) | SagaStateEntry::Executing(_))
    ) {
        match actor.participant_run_evidence_strict(context) {
            Ok(evidence) if crate::durability::no_effect_rejection_proven(&evidence) => {
                if let Some(accepted) = actor
                    .saga_support_mut()
                    .accepted_workflow_steps
                    .remove(&saga_id)
                {
                    crate::durability::mark_accepted_step_resolved(
                        actor,
                        saga_id,
                        accepted.execution_id,
                    );
                }
                return CompensationStart::NoEffect;
            }
            Ok(_) => {}
            Err(error) => {
                let reason: Box<str> =
                    format!("undo evidence unavailable before local cache use: {error}").into();
                quarantine_run(actor, who, context, reason.clone(), now, emit);
                return CompensationStart::Quarantined(reason);
            }
        }
    }

    let local = match actor.saga_states_ref().get(&saga_id) {
        Some(SagaStateEntry::Completed(state)) => {
            Some((Vec::new(), state.state.compensation_data.clone()))
        }
        Some(SagaStateEntry::Executing(_)) => actor
            .saga_support()
            .accepted_workflow_steps
            .get(&saga_id)
            .map(|accepted| {
                (
                    accepted.saga_input.clone(),
                    accepted.compensation_data.clone(),
                )
            }),
        // An undo is already in flight (or the run is quarantined): this is a repeat.
        Some(SagaStateEntry::Compensating(_) | SagaStateEntry::Quarantined(_)) => {
            return CompensationStart::Skip;
        }
        _ => None,
    };
    let (saga_input, compensation_data) = match local {
        Some(local) => local,
        None => match recover_for_undo(actor, who, context, now) {
            UndoRecovery::Rebuilt(data) => (Vec::new(), data),
            UndoRecovery::Acknowledge => return CompensationStart::Acknowledge,
            UndoRecovery::NoEffect => return CompensationStart::NoEffect,
            UndoRecovery::Nothing => return CompensationStart::Skip,
            UndoRecovery::Unsafe(reason) => {
                quarantine_run(actor, who, context, reason.clone(), now, emit);
                return CompensationStart::Quarantined(reason);
            }
        },
    };

    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: now,
        },
    ) {
        let reason: Box<str> = format!("compensation intent persistence failed: {error:?}").into();
        quarantine_run(actor, who, context, reason.clone(), now, emit);
        return CompensationStart::Quarantined(reason);
    }

    let compensating = match actor.saga_states().remove(&saga_id) {
        Some(SagaStateEntry::Completed(state)) => state.start_compensation(now),
        Some(SagaStateEntry::Executing(state)) => state.start_compensation(now),
        Some(other) => {
            actor.saga_states().insert(saga_id, other);
            return CompensationStart::Skip;
        }
        None => return CompensationStart::Skip,
    };
    actor
        .saga_states()
        .insert(saga_id, SagaStateEntry::Compensating(compensating));
    if let Some(accepted) = actor
        .saga_support_mut()
        .accepted_workflow_steps
        .remove(&saga_id)
    {
        crate::durability::mark_accepted_step_resolved(actor, saga_id, accepted.execution_id);
    }
    let saga_input = actor
        .participant_run_evidence_strict(context)
        .ok()
        .and_then(|e| e.forward_outcome.map(|outcome| outcome.saga_input))
        .unwrap_or(saga_input);
    CompensationStart::Run {
        saga_input,
        compensation_data,
    }
}

/// Persists the completion proof of a durably rejected (no-effect) forward step
/// before acknowledging it. No business undo ran; a failed append quarantines and
/// publishes nothing, leaving the owned request unanswered.
fn complete_no_effect_compensation<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) -> Result<(), Box<str>>
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: now,
        },
    ) {
        let reason: Box<str> =
            format!("no-effect compensation proof persistence failed: {error:?}").into();
        quarantine_run(actor, who, context, reason.clone(), now, emit);
        return Err(reason);
    }
    let state = SagaParticipantState::new(
        saga_id,
        context.saga_type.clone(),
        who.step.clone(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("no_effect_rejection", now)
    .start_execution(now)
    .complete(Vec::new(), Vec::new(), now)
    .start_compensation(now)
    .complete_compensation(now);
    actor
        .saga_states()
        .insert(saga_id, SagaStateEntry::Compensated(state));
    resend_compensation_ack(who, context, emit);
    Ok(())
}

/// Records and publishes compensation completion only after the result is durable;
/// otherwise the (possibly applied) undo is quarantined as ambiguous.
fn complete_compensation<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) -> Result<(), Box<str>>
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    if let Err(error) = actor.record_event_strict(
        saga_id,
        ParticipantEvent::CompensationCompleted {
            completed_at_millis: now,
        },
    ) {
        let reason: Box<str> = format!("compensation result persistence failed: {error:?}").into();
        quarantine_run(actor, who, context, reason.clone(), now, emit);
        return Err(reason);
    }
    match actor.saga_states().remove(&saga_id) {
        Some(SagaStateEntry::Compensating(state)) => {
            actor.saga_states().insert(
                saga_id,
                SagaStateEntry::Compensated(state.complete_compensation(now)),
            );
        }
        Some(other) => {
            actor.saga_states().insert(saga_id, other);
        }
        None => {}
    }
    emit(SagaChoreographyEvent::CompensationCompleted {
        context: context.next_step(who.step.clone()),
    });
    Ok(())
}

/// Fail compensation. Ambiguous failures quarantine with retained evidence and a
/// durable tombstone. Returns the reason for the `on_quarantined` hook.
fn fail_compensation<A, F>(
    actor: &mut A,
    who: &Who,
    context: &SagaContext,
    error: CompensationError,
    now: u64,
    emit: &mut F,
) -> Box<str>
where
    A: SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;
    let (reason, is_ambiguous) = match error {
        CompensationError::Ambiguous { reason } => (reason, true),
        CompensationError::Terminal { reason } => (reason, false),
    };

    match actor.saga_states().remove(&saga_id) {
        Some(SagaStateEntry::Compensating(state)) => {
            if is_ambiguous {
                actor.saga_states().insert(
                    saga_id,
                    SagaStateEntry::Quarantined(state.quarantine(reason.clone(), now)),
                );
            } else {
                actor.saga_states().insert(
                    saga_id,
                    SagaStateEntry::Failed(state.fail(reason.clone(), false, now)),
                );
            }
        }
        Some(other) => {
            actor.saga_states().insert(saga_id, other);
        }
        None => {}
    }

    let event = if is_ambiguous {
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
    if let Err(error) = actor.record_event_strict(saga_id, event) {
        tracing::error!(
            target: "core::saga",
            event = "saga_compensation_failure_persistence_failed",
            saga_id = saga_id.get(),
            error = %error
        );
    }

    let event_context = context.next_step(who.step.clone());
    emit(SagaChoreographyEvent::CompensationFailed {
        context: event_context.clone(),
        participant_id: who.participant_id.clone(),
        error: reason.clone(),
        is_ambiguous,
    });
    if is_ambiguous {
        if let Err(error) = actor.retain_terminal_saga_strict(
            context,
            ParticipantTerminalKind::Quarantined,
            &reason,
        ) {
            tracing::error!(
                target: "core::saga",
                event = "saga_quarantine_tombstone_failed",
                saga_id = saga_id.get(),
                error = %error
            );
        }
        emit(SagaChoreographyEvent::SagaQuarantined {
            context: event_context,
            reason: reason.clone(),
            step: who.step.clone(),
            participant_id: who.participant_id.clone(),
        });
    }
    reason
}

#[cfg(test)]
mod tests {
    use crate::{
        DeterministicContextBuilder, HasSagaParticipantSupport, InMemoryParticipantStore,
        ParticipantStore, SagaContext, SagaId, SagaParticipantSupport,
    };

    use super::*;

    #[derive(Clone, Copy)]
    enum ExecuteMode {
        Completed,
        TerminalFail,
    }

    struct TestParticipant {
        saga: SagaParticipantSupport<InMemoryParticipantStore>,
        execute_mode: ExecuteMode,
        compensation_error: Option<CompensationError>,
        executed: usize,
        observed_inputs: Vec<Vec<u8>>,
        dependency_spec: DependencySpec,
    }

    impl Default for TestParticipant {
        fn default() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryParticipantStore::new()),
                execute_mode: ExecuteMode::Completed,
                compensation_error: None,
                executed: 0,
                observed_inputs: Vec::new(),
                dependency_spec: DependencySpec::OnSagaStart,
            }
        }
    }

    impl HasSagaParticipantSupport for TestParticipant {
        type Store = InMemoryParticipantStore;

        fn saga_support(&self) -> &SagaParticipantSupport<Self::Store> {
            &self.saga
        }

        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Store> {
            &mut self.saga
        }
    }

    impl SagaParticipant for TestParticipant {
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

    fn started_event() -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaStarted {
            context: DeterministicContextBuilder::default().build(),
            payload: vec![7],
        }
    }

    #[test]
    fn after_receives_output_but_preserves_original_saga_input() {
        let mut participant = TestParticipant {
            dependency_spec: DependencySpec::After("upstream"),
            ..TestParticipant::default()
        };
        let mut context = started_event().context().clone();
        context.step_name = "upstream".into();
        let mut emitted = Vec::new();
        handle_saga_event_with_emit(
            &mut participant,
            SagaChoreographyEvent::StepCompleted {
                context,
                output: vec![42],
                saga_input: vec![7],
                compensation_available: false,
            },
            |event| emitted.push(event),
        );
        assert_eq!(participant.observed_inputs, vec![vec![42]]);
        assert!(emitted.iter().any(|event| matches!(event,
            SagaChoreographyEvent::StepCompleted { saga_input, .. } if saga_input == &[7])));
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
            .store
            .read(SagaId::new(1))
            .expect("journal read should succeed");
        assert!(matches!(
            entries.as_slice(),
            [
                _,
                _,
                _,
                crate::JournalEntry {
                    event: ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome },
                    ..
                }
            ] if outcome.compensation_data == [9] && outcome.effect.is_none()
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

        let first_completed = SagaChoreographyEvent::SagaCompleted {
            context: first.context().clone(),
        };

        handle_saga_event_with_emit(&mut participant, first, |event| emitted.push(event));
        // An unresolved run owns the saga id; the reuse is admitted only after it
        // reached a terminal outcome.
        handle_saga_event_with_emit(&mut participant, first_completed, |event| {
            emitted.push(event)
        });
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
            SagaChoreographyEvent::SagaCompleted {
                context: first_context.clone(),
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
            participant.saga_states().get(&SagaId::new(1)),
            Some(SagaStateEntry::Failed(_))
        ));
        let entries = participant
            .saga
            .store
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
            participant.saga_states().get(&SagaId::new(1)),
            Some(SagaStateEntry::Quarantined(_))
        ));
        let entries = participant
            .saga
            .store
            .read(SagaId::new(1))
            .expect("journal read should succeed");
        assert!(entries.iter().any(|entry| matches!(
            &entry.event,
            ParticipantEvent::Quarantined { reason, .. }
                if reason.as_ref() == "cannot confirm rollback"
        )));
        assert!(matches!(
            entries.last(),
            Some(crate::JournalEntry {
                event: ParticipantEvent::ParticipantTerminalRecorded {
                    outcome: crate::ParticipantTerminalKind::Quarantined,
                    ..
                },
                ..
            })
        ));
        assert!(matches!(
            emitted.get(1),
            Some(SagaChoreographyEvent::SagaQuarantined { .. })
        ));
    }

    #[test]
    fn handle_saga_event_latches_and_retains_evidence_on_quarantine() {
        let mut participant = TestParticipant::default();
        let started = started_event();
        let saga_id = started.context().saga_id;

        handle_saga_event_with_emit(&mut participant, started, |_| {});
        assert_eq!(participant.executed, 1);
        assert!(participant.saga_states().contains_key(&saga_id));

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

        assert!(participant.is_terminal_saga_latched(saga_id));
        assert!(
            participant.saga_states().contains_key(&saga_id),
            "quarantine retains evidence"
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
}
