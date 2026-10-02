//! U5 safety regressions for the sync and async participant event helpers: durable
//! admission, intent-before-effect, no success without durable evidence, terminal
//! fences, declared effect dispatch and singleton compensation ownership.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Waker};

use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, EffectDispatchError,
    EffectDispatchOutcome, EffectDispatchRequest, HasSagaParticipantSupport, InMemoryDedupe,
    InMemoryJournal, ParticipantEvent, ParticipantJournal, PeerId, SagaBoxFuture,
    SagaChoreographyEvent, SagaContext, SagaFailureDetails, SagaId, SagaParticipant,
    SagaParticipantSupport, StepError, StepOutput, handle_async_saga_event_with_emit,
    handle_saga_event_with_emit,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Sync,
    Async,
}
const MODES: [Mode; 2] = [Mode::Sync, Mode::Async];

#[derive(Clone)]
enum Dispatch {
    /// Trait default: unsupported.
    Default,
    Durable,
    Failed {
        ambiguous: bool,
    },
}

#[derive(Default)]
struct Counters {
    effects: AtomicUsize,
    undos: AtomicUsize,
    dispatched: Mutex<Vec<String>>,
}

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    shared: Arc<Counters>,
    step: &'static str,
    with_effect: bool,
    dispatch: Dispatch,
}

impl Actor {
    fn new(journal: &Journal, dedupe: &Dedupe, shared: &Arc<Counters>) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
            shared: shared.clone(),
            step: "a",
            with_effect: false,
            dispatch: Dispatch::Default,
        }
    }

    fn effect(&self) -> Result<StepOutput, StepError> {
        self.shared.effects.fetch_add(1, Ordering::SeqCst);
        Ok(if self.with_effect {
            StepOutput::CompletedWithEffect {
                output: vec![1],
                compensation_data: vec![42],
                effect: "notify".into(),
            }
        } else {
            StepOutput::Completed {
                output: vec![1],
                compensation_data: vec![42],
            }
        })
    }

    fn undo(&self) -> Result<CompensationOutput, CompensationError> {
        self.shared.undos.fetch_add(1, Ordering::SeqCst);
        Ok(CompensationOutput::Completed)
    }

    fn do_dispatch(
        &self,
        request: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        match &self.dispatch {
            Dispatch::Default => Err(EffectDispatchError::Unsupported {
                effect: request.effect.into(),
            }),
            Dispatch::Durable => {
                self.shared
                    .dispatched
                    .lock()
                    .unwrap()
                    .push(request.effect.to_owned());
                Ok(EffectDispatchOutcome::Durable {
                    receipt: "r-1".into(),
                })
            }
            Dispatch::Failed { ambiguous } => Err(EffectDispatchError::Failed {
                reason: "outbox down".into(),
                ambiguous: *ambiguous,
            }),
        }
    }
}

impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.saga
    }
}

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        self.step
    }
    fn saga_types(&self) -> &[&'static str] {
        &["order"]
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.effect()
    }
    fn compensate_step(
        &mut self,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        self.undo()
    }
    fn dispatch_effect(
        &mut self,
        request: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        self.do_dispatch(request)
    }
}

impl AsyncSagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        self.step
    }
    fn saga_types(&self) -> &[&'static str] {
        &["order"]
    }
    fn execute_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        Box::pin(async move { self.effect() })
    }
    fn compensate_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        Box::pin(async move { self.undo() })
    }
    fn dispatch_effect<'a>(
        &'a mut self,
        request: &'a EffectDispatchRequest<'a>,
    ) -> SagaBoxFuture<'a, Result<EffectDispatchOutcome, EffectDispatchError>> {
        Box::pin(async move { self.do_dispatch(request) })
    }
}

struct World {
    journal: Journal,
    dedupe: Dedupe,
    shared: Arc<Counters>,
}

impl World {
    fn new() -> Self {
        Self {
            journal: FaultJournal::new(InMemoryJournal::new()),
            dedupe: FaultDedupe::new(InMemoryDedupe::new()),
            shared: Arc::default(),
        }
    }
    /// A fresh actor over the same durable stores (simulated restart).
    fn actor(&self) -> Actor {
        Actor::new(&self.journal, &self.dedupe, &self.shared)
    }
    fn effects(&self) -> usize {
        self.shared.effects.load(Ordering::SeqCst)
    }
    fn undos(&self) -> usize {
        self.shared.undos.load(Ordering::SeqCst)
    }
    fn rows(&self) -> Vec<ParticipantEvent> {
        self.journal
            .read(SagaId::new(7))
            .unwrap()
            .into_iter()
            .map(|e| e.event)
            .collect()
    }
}

fn ctx(started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(7),
        saga_type: "order".into(),
        step_name: "start".into(),
        correlation_id: 7,
        causation_id: 7,
        trace_id: 7,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn start(started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx(started_at),
        payload: vec![9],
    }
}

fn completed(started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaCompleted {
        context: ctx(started_at),
    }
}

fn quarantined(started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaQuarantined {
        context: ctx(started_at),
        reason: "operator".into(),
        step: "a".into(),
        participant_id: "a".into(),
    }
}

fn comp_request(started_at: u64, steps: &[&str]) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context: ctx(started_at),
        failed_step: "b".into(),
        reason: "downstream failed".into(),
        failure: SagaFailureDetails {
            step_name: "b".into(),
            participant_id: "b".into(),
            error_code: None,
            error_message: "downstream failed".into(),
            at_millis: started_at,
        },
        steps_to_compensate: steps.iter().map(|s| (*s).into()).collect(),
    }
}

fn drive(
    mode: Mode,
    actor: &mut Actor,
    event: SagaChoreographyEvent,
) -> Vec<SagaChoreographyEvent> {
    let mut out = Vec::new();
    match mode {
        Mode::Sync => handle_saga_event_with_emit(actor, event, |e| out.push(e)),
        Mode::Async => {
            let mut fut = std::pin::pin!(handle_async_saga_event_with_emit(actor, event, |e| {
                out.push(e)
            }));
            let mut cx = Context::from_waker(Waker::noop());
            loop {
                if let Poll::Ready(()) = fut.as_mut().poll(&mut cx) {
                    break;
                }
            }
        }
    }
    out
}

fn has_step_completed(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
}

fn has_quarantine(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. }))
}

#[test]
fn failed_intent_write_runs_no_effect_and_publishes_no_success() {
    for mode in MODES {
        let world = World::new();
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("StepExecutionStarted"),
        );
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, start(100));
        assert_eq!(world.effects(), 0, "{mode:?}: effect ran without intent");
        assert!(!has_step_completed(&out), "{mode:?}");
    }
}

#[test]
fn failed_result_write_publishes_no_success_and_keeps_compensation_evidence() {
    for mode in MODES {
        let world = World::new();
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("StepExecutionCompleted"),
        );
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, start(100));
        assert_eq!(world.effects(), 1, "{mode:?}");
        assert!(
            !has_step_completed(&out),
            "{mode:?}: success without evidence"
        );
        assert!(has_quarantine(&out), "{mode:?}: must surface quarantine");
        let rows = world.rows();
        assert!(
            rows.iter().any(|e| matches!(e,
                ParticipantEvent::ParticipantReconciliationEvidence { compensation_data, .. }
                    if compensation_data.as_slice() == [42])),
            "{mode:?}: compensation data must survive in durable evidence: {rows:?}"
        );
        // Restart must not repeat the effect.
        let mut restarted = world.actor();
        drive(mode, &mut restarted, start(100));
        assert_eq!(
            world.effects(),
            1,
            "{mode:?}: effect repeated after restart"
        );
    }
}

#[test]
fn dedupe_outage_is_a_visible_quarantine_not_a_silent_duplicate() {
    for mode in MODES {
        let world = World::new();
        world.dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, start(100));
        assert_eq!(world.effects(), 0, "{mode:?}: fail closed on outage");
        assert!(!has_step_completed(&out), "{mode:?}");
        assert!(
            has_quarantine(&out),
            "{mode:?}: callers must observe storage failure"
        );
        world.dedupe.disarm();
        drive(mode, &mut actor, start(100));
        assert_eq!(
            world.effects(),
            0,
            "{mode:?}: quarantine requires reconciliation, not automatic retry"
        );
    }
}

#[test]
fn changed_delivery_trace_cannot_reopen_a_durably_started_step() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        let mut retry = start(100);
        if let SagaChoreographyEvent::SagaStarted { context, .. } = &mut retry {
            context.trace_id = 999;
            context.attempt = 1;
        }
        let out = drive(mode, &mut actor, retry);
        assert_eq!(
            world.effects(),
            1,
            "{mode:?}: a new trace reopened durable execution"
        );
        assert!(
            has_quarantine(&out),
            "uncertain replay requires reconciliation"
        );
    }
}

#[test]
fn terminal_run_replay_after_restart_does_not_repeat_effect() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        drive(mode, &mut actor, completed(100));
        assert_eq!(world.effects(), 1);

        let mut restarted = world.actor();
        drive(mode, &mut restarted, start(100));
        assert_eq!(world.effects(), 1, "{mode:?}: completed run re-executed");
        assert!(
            world
                .rows()
                .iter()
                .any(|e| matches!(e, ParticipantEvent::ParticipantTerminalRecorded { .. })),
            "{mode:?}: terminal fence must be durable"
        );
    }
}

#[test]
fn failed_run_fence_survives_cache_eviction() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        drive(
            mode,
            &mut actor,
            SagaChoreographyEvent::SagaFailed {
                context: ctx(100),
                reason: "boom".into(),
                failure: None,
            },
        );
        let mut evicted = world.actor();
        drive(mode, &mut evicted, start(100));
        assert_eq!(world.effects(), 1, "{mode:?}");
    }
}

#[test]
fn quarantine_retains_journal_and_dedupe_and_blocks_restart_reexecution() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        let before = world.rows().len();
        drive(mode, &mut actor, quarantined(100));
        assert!(world.rows().len() > before, "{mode:?}: evidence kept");
        assert_eq!(world.journal.call_count(JournalOp::Prune), 0, "{mode:?}");
        assert_eq!(world.dedupe.prune_calls(), 0, "{mode:?}");
        let mut restarted = world.actor();
        drive(mode, &mut restarted, start(100));
        assert_eq!(world.effects(), 1, "{mode:?}");
        // A newer run may not silently reuse a quarantined saga id either.
        drive(mode, &mut restarted, start(200));
        assert_eq!(world.effects(), 1, "{mode:?}: quarantine reuse");
    }
}

#[test]
fn later_valid_run_executes_after_resolution_and_stale_events_are_ignored() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        drive(mode, &mut actor, completed(100));
        let mut restarted = world.actor();
        let out = drive(mode, &mut restarted, start(200));
        assert_eq!(world.effects(), 2, "{mode:?}: later run must execute");
        assert!(has_step_completed(&out), "{mode:?}");
        // Late events of the old run are fenced.
        drive(mode, &mut restarted, start(100));
        drive(mode, &mut restarted, comp_request(100, &["a"]));
        assert_eq!(world.effects(), 2, "{mode:?}");
        assert_eq!(world.undos(), 0, "{mode:?}: stale rollback ran");
    }
}

#[test]
fn unresolved_run_ownership_is_never_reset_by_a_newer_start() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        let runs_before = world.rows().len();
        drive(mode, &mut actor, start(200));
        assert_eq!(world.effects(), 1, "{mode:?}: active run overwritten");
        assert_eq!(world.rows().len(), runs_before, "{mode:?}");
        // The original run can still be rolled back.
        drive(mode, &mut actor, comp_request(100, &["a"]));
        assert_eq!(world.undos(), 1, "{mode:?}");
    }
}

#[test]
fn legacy_history_without_run_identity_requires_reconciliation() {
    for mode in MODES {
        let world = World::new();
        world
            .journal
            .append(
                SagaId::new(7),
                ParticipantEvent::StepExecutionStarted {
                    attempt: 1,
                    started_at_millis: 50,
                },
            )
            .unwrap();
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, start(100));
        assert_eq!(world.effects(), 0, "{mode:?}: legacy history re-executed");
        assert!(!has_step_completed(&out), "{mode:?}");
    }
}

#[test]
fn registration_only_history_is_admitted() {
    for mode in MODES {
        let world = World::new();
        world
            .journal
            .append(
                SagaId::new(7),
                ParticipantEvent::SagaRegistered {
                    saga_type: "order".into(),
                    step_name: "a".into(),
                    registered_at_millis: 1,
                },
            )
            .unwrap();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        assert_eq!(world.effects(), 1, "{mode:?}");
    }
}

#[test]
fn declared_effect_is_dispatched_once_before_success() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        actor.with_effect = true;
        actor.dispatch = Dispatch::Durable;
        let out = drive(mode, &mut actor, start(100));
        assert!(has_step_completed(&out), "{mode:?}");
        assert_eq!(
            *world.shared.dispatched.lock().unwrap(),
            vec!["notify".to_owned()],
            "{mode:?}"
        );
        drive(mode, &mut actor, start(100));
        assert_eq!(world.shared.dispatched.lock().unwrap().len(), 1, "{mode:?}");
    }
}

#[test]
fn unsupported_or_failed_effect_dispatch_quarantines_without_success() {
    for mode in MODES {
        for dispatch in [
            Dispatch::Default,
            Dispatch::Failed { ambiguous: false },
            Dispatch::Failed { ambiguous: true },
        ] {
            let world = World::new();
            let mut actor = world.actor();
            actor.with_effect = true;
            actor.dispatch = dispatch;
            let out = drive(mode, &mut actor, start(100));
            assert!(
                !has_step_completed(&out),
                "{mode:?}: success without dispatch"
            );
            assert!(has_quarantine(&out), "{mode:?}");
            assert!(
                world
                    .rows()
                    .iter()
                    .any(|e| matches!(e, ParticipantEvent::Quarantined { .. })),
                "{mode:?}"
            );
            let mut restarted = world.actor();
            drive(mode, &mut restarted, start(100));
            assert_eq!(world.effects(), 1, "{mode:?}: effect repeated");
        }
    }
}

#[test]
fn failed_undo_intent_write_runs_no_undo_and_quarantines() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("CompensationStarted"),
        );
        let out = drive(mode, &mut actor, comp_request(100, &["a"]));
        assert_eq!(world.undos(), 0, "{mode:?}: undo ran without intent");
        assert!(
            has_quarantine(&out),
            "{mode:?}: obligation must not be lost"
        );
        assert!(
            !out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
            "{mode:?}"
        );
    }
}

#[test]
fn failed_undo_result_write_cannot_claim_completion() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("CompensationCompleted"),
        );
        let out = drive(mode, &mut actor, comp_request(100, &["a"]));
        assert_eq!(world.undos(), 1, "{mode:?}");
        assert!(
            !out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
            "{mode:?}: completion claimed without durable evidence"
        );
        assert!(has_quarantine(&out), "{mode:?}");
    }
}

#[test]
fn compensation_completes_and_emits_when_all_writes_succeed() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        let out = drive(mode, &mut actor, comp_request(100, &["a"]));
        assert_eq!(world.undos(), 1, "{mode:?}");
        assert!(
            out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
            "{mode:?}"
        );
    }
}

#[test]
fn legacy_multi_step_request_is_owned_only_by_the_head_step() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        // Reverse completion order: "b" is next; "a" must wait for a singleton request.
        let out = drive(mode, &mut actor, comp_request(100, &["b", "a"]));
        assert_eq!(world.undos(), 0, "{mode:?}: non-head rolled back early");
        assert!(out.is_empty(), "{mode:?}: {out:?}");
        // Later singleton request for "a" is honored.
        let out = drive(mode, &mut actor, comp_request(100, &["a"]));
        assert_eq!(world.undos(), 1, "{mode:?}");
        assert!(!out.is_empty(), "{mode:?}");
    }
}

#[test]
fn legacy_multi_step_request_runs_for_head_step() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start(100));
        drive(mode, &mut actor, comp_request(100, &["a", "b"]));
        assert_eq!(world.undos(), 1, "{mode:?}");
    }
}
