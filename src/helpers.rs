//! Helper functions for saga handling

use crate::{
    Compensating, CompensationError, Completed, DependencySpec, Executing, ParticipantEvent,
    ParticipantJournal, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant,
    SagaParticipantState, SagaStateEntry, SagaStateExt, StepError, StepOutput,
};
use std::sync::atomic::Ordering;

/// Default saga event handler.
///
/// Call this from your actor's `handle` method when a `SagaEvent` message arrives.
///
/// ```rust,ignore
/// impl Actor for MyActor {
///     type Msg = MyCommand;
///     fn handle(&mut self, msg: Self::Msg) {
///         match msg {
///             MyCommand::SagaEvent { event } => {
///                 handle_saga_event(self, event);
///             }
///             // ...
///         }
///     }
/// }
/// ```
pub fn handle_saga_event<P>(participant: &mut P, event: SagaChoreographyEvent)
where
    P: SagaParticipant + SagaStateExt,
{
    handle_saga_event_with_emit(participant, event, |_| {});
}

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

    // Idempotency check
    let dedupe_key = dedupe_key_for_event(&event);
    if !participant.check_dedupe(context.saga_id, &dedupe_key) {
        return; // Already processed
    }

    match event {
        SagaChoreographyEvent::SagaStarted { payload, .. }
            if participant.depends_on().is_on_saga_start() =>
        {
            participant.saga_states().remove(&context.saga_id);
            participant.dependency_completions().remove(&context.saga_id);
            participant.dependency_fired().remove(&context.saga_id);
            execute_step_wrapper_with_emit(participant, context.clone(), payload, now, &mut emit);
        }

        SagaChoreographyEvent::SagaStarted { .. } => {
            participant.saga_states().remove(&context.saga_id);
            participant.dependency_completions().remove(&context.saga_id);
            participant.dependency_fired().remove(&context.saga_id);
        }

        SagaChoreographyEvent::StepCompleted {
            context: step_ctx,
            output,
            saga_input,
            ..
        } => {
            let dependency_spec = participant.depends_on();
            if dependency_spec.is_satisfied_by(&step_ctx.step_name) {
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
            steps_to_compensate,
            ..
        } => {
            if steps_to_compensate.contains(&participant.step_name().into()) {
                compensate_wrapper_with_emit(participant, &context, now, &mut emit);
            }
        }

        SagaChoreographyEvent::SagaCompleted { .. } => {
            participant.on_saga_completed(&context);
            participant.prune_saga(context.saga_id);
        }

        SagaChoreographyEvent::SagaFailed { reason, .. } => {
            participant.on_saga_failed(&context, &reason);
            participant.prune_saga(context.saga_id);
        }

        _ => {}
    }
}

fn dedupe_key_for_event(event: &SagaChoreographyEvent) -> String {
    let context = event.context();
    match event {
        SagaChoreographyEvent::SagaStarted { .. } => {
            format!(
                "{}:{}:{}:{}",
                context.trace_id,
                context.saga_started_at_millis,
                event.event_type(),
                context.step_name
            )
        }
        SagaChoreographyEvent::StepCompleted { .. }
        | SagaChoreographyEvent::StepFailed { .. }
        | SagaChoreographyEvent::CompensationStarted { .. }
        | SagaChoreographyEvent::CompensationCompleted { .. }
        | SagaChoreographyEvent::CompensationFailed { .. }
        | SagaChoreographyEvent::SagaCompleted { .. }
        | SagaChoreographyEvent::SagaFailed { .. }
        | SagaChoreographyEvent::SagaQuarantined { .. }
        | SagaChoreographyEvent::StepStarted { .. }
        | SagaChoreographyEvent::StepAck { .. } => {
            format!(
                "{}:{}:{}:{}",
                context.trace_id,
                context.saga_started_at_millis,
                event.event_type(),
                context.step_name
            )
        }
        SagaChoreographyEvent::CompensationRequested { failed_step, .. } => format!(
            "{}:{}:{}:{}:{}",
            context.trace_id,
            context.saga_started_at_millis,
            event.event_type(),
            context.step_name,
            failed_step
        ),
    }
}

/// Execute a step with full state management
pub fn execute_step_wrapper<P>(participant: &mut P, context: SagaContext, input: Vec<u8>, now: u64)
where
    P: SagaParticipant + SagaStateExt,
{
    let mut ignore_emit = |_| {};
    execute_step_wrapper_with_emit(participant, context, input, now, &mut ignore_emit);
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
    let saga_id = context.saga_id;

    // Build state: Idle -> Triggered -> Executing
    let state = SagaParticipantState::new(
        saga_id,
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
    participant.record_event(
        saga_id,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: now,
        },
    );

    // Store state
    participant
        .saga_states()
        .insert(saga_id, SagaStateEntry::Executing(state));

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
    let saga_id = context.saga_id;
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
    };

    // State: Executing -> Completed
    if let Some(SagaStateEntry::Executing(state)) = participant.saga_states().remove(&saga_id) {
        let new_state = state.complete(out_data.clone(), comp_data, now);
        participant
            .saga_states()
            .insert(saga_id, SagaStateEntry::Completed(new_state));
    }

    // Persist
    let emitted_output = out_data.clone();
    participant.record_event(
        saga_id,
        ParticipantEvent::StepExecutionCompleted {
            output: out_data,
            compensation_data: vec![],
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
    let saga_id = context.saga_id;
    let (reason, requires_comp) = match error {
        StepError::Retriable { reason } => {
            // TODO: Handle retry with backoff
            return;
        }
        StepError::Terminal { reason } => (reason, false),
        StepError::RequireCompensation { reason } => (reason, true),
    };

    // State: Executing -> Failed
    if let Some(SagaStateEntry::Executing(state)) = participant.saga_states().remove(&saga_id) {
        use crate::state::Failed;
        let new_state = state.fail(reason.clone(), requires_comp, now);
        participant
            .saga_states()
            .insert(saga_id, SagaStateEntry::Failed(new_state));
    }

    // Persist
    participant.record_event(
        saga_id,
        ParticipantEvent::StepExecutionFailed {
            error: reason.clone(),
            requires_compensation: requires_comp,
            failed_at_millis: now,
        },
    );

    emit(SagaChoreographyEvent::StepFailed {
        context: context.next_step(participant.step_name().into()),
        error: reason,
        will_retry: false,
        requires_compensation: requires_comp,
    });
}

/// Execute compensation with state management
pub fn compensate_wrapper<P>(participant: &mut P, context: &SagaContext, now: u64)
where
    P: SagaParticipant + SagaStateExt,
{
    let mut ignore_emit = |_| {};
    compensate_wrapper_with_emit(participant, context, now, &mut ignore_emit);
}

fn compensate_wrapper_with_emit<P, F>(
    participant: &mut P,
    context: &SagaContext,
    now: u64,
    emit: &mut F,
) where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;

    // Get compensation data from Completed state
    if let Some(SagaStateEntry::Completed(state)) = participant.saga_states().remove(&saga_id) {
        let comp_data = state.state.compensation_data.clone();

        // State: Completed -> Compensating
        let new_state = state.start_compensation(now);
        participant
            .saga_states()
            .insert(saga_id, SagaStateEntry::Compensating(new_state));

        // Persist
        participant.record_event(
            saga_id,
            ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: now,
            },
        );

        // Execute compensation
        match participant.compensate_step(context, &comp_data) {
            Ok(()) => {
                complete_compensation(participant, context, now, emit);
            }
            Err(error) => {
                fail_compensation(participant, context, error, now, emit);
            }
        }
    }
}

/// Complete compensation
fn complete_compensation<P, F>(participant: &mut P, context: &SagaContext, now: u64, emit: &mut F)
where
    P: SagaParticipant + SagaStateExt,
    F: FnMut(SagaChoreographyEvent),
{
    let saga_id = context.saga_id;

    // State: Compensating -> Compensated
    if let Some(SagaStateEntry::Compensating(state)) = participant.saga_states().remove(&saga_id) {
        use crate::state::Compensated;
        let new_state = state.complete_compensation(now);
        participant
            .saga_states()
            .insert(saga_id, SagaStateEntry::Compensated(new_state));
    }

    // Persist
    participant.record_event(
        saga_id,
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
    let saga_id = context.saga_id;
    let (reason, is_ambiguous) = match error {
        CompensationError::SafeToRetry { reason } => (reason, false),
        CompensationError::Ambiguous { reason } => (reason, true),
        CompensationError::Terminal { reason } => (reason, false),
    };

    // State: Compensating -> Quarantined
    if let Some(SagaStateEntry::Compensating(state)) = participant.saga_states().remove(&saga_id) {
        use crate::state::Quarantined;
        let new_state = state.quarantine(reason.clone(), now);
        participant
            .saga_states()
            .insert(saga_id, SagaStateEntry::Quarantined(new_state));
    }

    // Persist
    participant.record_event(
        saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );

    let event_context = context.next_step(participant.step_name().into());
    emit(SagaChoreographyEvent::CompensationFailed {
        context: event_context.clone(),
        error: reason.clone(),
        is_ambiguous,
    });
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: event_context,
        reason: reason.clone(),
        step: participant.step_name().into(),
    });

    // Notify
    participant.on_quarantined(context, &reason);
}

/// Recovery bootstrap - find and resume pending sagas
pub fn recover_sagas<P>(participant: &mut P) -> Vec<SagaId>
where
    P: SagaParticipant + SagaStateExt,
{
    let mut recovered = Vec::new();

    if let Ok(saga_ids) = participant.saga_journal().list_sagas() {
        for saga_id in saga_ids {
            if let Ok(events) = participant.saga_journal().read(saga_id) {
                let state = rebuild_state(&events);

                if !state.is_terminal() {
                    recovered.push(saga_id);
                    // TODO: Resume based on state
                }
            }
        }
    }

    recovered
}

/// Rebuild state from event history
fn rebuild_state(entries: &[crate::JournalEntry]) -> RebuiltState {
    let mut state = RebuiltState::Unknown;

    for entry in entries {
        state = match (state, &entry.event) {
            (_, ParticipantEvent::StepExecutionStarted { .. }) => RebuiltState::Executing,
            (_, ParticipantEvent::StepExecutionCompleted { .. }) => RebuiltState::Completed,
            (
                _,
                ParticipantEvent::StepExecutionFailed {
                    requires_compensation: true,
                    ..
                },
            ) => RebuiltState::FailedNeedsCompensation,
            (
                _,
                ParticipantEvent::StepExecutionFailed {
                    requires_compensation: false,
                    ..
                },
            ) => RebuiltState::FailedTerminal,
            (_, ParticipantEvent::CompensationStarted { .. }) => RebuiltState::Compensating,
            (_, ParticipantEvent::CompensationCompleted { .. }) => RebuiltState::Compensated,
            (_, ParticipantEvent::Quarantined { .. }) => RebuiltState::Quarantined,
            _ => state,
        };
    }

    state
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RebuiltState {
    Unknown,
    Executing,
    Completed,
    FailedNeedsCompensation,
    FailedTerminal,
    Compensating,
    Compensated,
    Quarantined,
}

impl RebuiltState {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            RebuiltState::Completed
                | RebuiltState::FailedTerminal
                | RebuiltState::Compensated
                | RebuiltState::Quarantined
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DeterministicContextBuilder, HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal,
        SagaContext, SagaId, SagaParticipantSupport,
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
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
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
        ) -> Result<(), CompensationError> {
            if let Some(err) = self.compensation_error.clone() {
                return Err(err);
            }
            Ok(())
        }
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
        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            emitted.first(),
            Some(SagaChoreographyEvent::StepCompleted {
                compensation_available: true,
                ..
            })
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
        assert_eq!(emitted.len(), 1);
        assert!(matches!(
            emitted.first(),
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
        assert_eq!(emitted.len(), 1);
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
        assert_eq!(emitted.len(), 2);
    }

    #[test]
    fn handle_saga_event_compat_wrapper_still_executes() {
        let mut participant = TestParticipant::default();
        handle_saga_event(&mut participant, started_event());
        assert_eq!(participant.executed, 1);
    }

    #[test]
    fn handle_saga_event_with_emit_processes_distinct_step_completed_events() {
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

        assert_eq!(participant.executed, 2);
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

        assert_eq!(participant.observed_inputs, vec![vec![7, 7, 7]]);
    }

    #[test]
    fn handle_saga_event_with_emit_emits_compensation_terminal_signals() {
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
                steps_to_compensate: vec!["risk_check".into()],
            },
            |event| emitted.push(event),
        );

        assert_eq!(emitted.len(), 2);
        assert!(matches!(
            emitted.first(),
            Some(SagaChoreographyEvent::CompensationFailed { .. })
        ));
        assert!(matches!(
            emitted.get(1),
            Some(SagaChoreographyEvent::SagaQuarantined { .. })
        ));
    }
}
