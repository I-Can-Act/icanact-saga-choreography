use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use icanact_saga_choreography::{
    JournalEntry, JournalError, ParticipantEvent, ParticipantStore, SagaContext, SagaId,
    StoreAdmission,
};

/// Named crash/fault points in the participant pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutPoint {
    BeforeInputMark,
    AfterInputMark,
    BeforeIntent,
    AfterIntent,
    AfterEffect,
    BeforeResult,
    AfterResult,
    BeforeSend,
    AfterSend,
    BeforeAck,
    BeforeGc,
    AfterGc,
    DuringRecovery,
}

/// Shared "where are we" cursor; tests set it, fault predicates read it.
#[derive(Clone, Default)]
pub struct Cuts(Arc<Mutex<Option<CutPoint>>>);

impl Cuts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enter(&self, point: CutPoint) {
        *self.0.lock().expect("cuts lock") = Some(point);
    }

    pub fn clear(&self) {
        *self.0.lock().expect("cuts lock") = None;
    }

    pub fn current(&self) -> Option<CutPoint> {
        *self.0.lock().expect("cuts lock")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalOp {
    Append,
    AppendBatch,
    AdmitAndRecordIntent,
    Read,
    ListSagas,
    UnresolvedSagas,
    CheckAndMark,
    RemoveProcessed,
    Prune,
    CompactTerminalDetail,
    PruneResolvedBefore,
}

/// When an armed fault fires.
#[derive(Clone)]
pub enum FaultTrigger {
    /// The Nth (1-based) call of the op on this wrapper.
    NthCall(usize),
    /// Single appends or batches containing this variant (e.g. `"StepExecutionCompleted"`).
    EventKind(&'static str),
    /// Any call of the op while the cursor sits at this cut point.
    AtCut(Cuts, CutPoint),
}

struct Rule {
    op: JournalOp,
    trigger: FaultTrigger,
    once: bool,
    fired: bool,
}

#[derive(Default)]
struct Calls([AtomicUsize; 11]);

fn event_kind(event: &ParticipantEvent) -> &'static str {
    match event {
        ParticipantEvent::AcceptedDeadlineRenewed { .. } => "AcceptedDeadlineRenewed",
        ParticipantEvent::StepTriggered { .. } => "StepTriggered",
        ParticipantEvent::StepExecutionStarted { .. } => "StepExecutionStarted",
        ParticipantEvent::StepExecutionCompleted { .. } => "StepExecutionCompleted",
        ParticipantEvent::StepExecutionFailed { .. } => "StepExecutionFailed",
        ParticipantEvent::CompensationStarted { .. } => "CompensationStarted",
        ParticipantEvent::CompensationCompleted { .. } => "CompensationCompleted",
        ParticipantEvent::CompensationFailed { .. } => "CompensationFailed",
        ParticipantEvent::CompensationRequestRecorded { .. } => "CompensationRequestRecorded",
        ParticipantEvent::Quarantined { .. } => "Quarantined",
        ParticipantEvent::AcceptedStepRecorded { .. } => "AcceptedStepRecorded",
        ParticipantEvent::AcceptedCompensationRecorded { .. } => "AcceptedCompensationRecorded",
        ParticipantEvent::ParticipantRunRecorded { .. } => "ParticipantRunRecorded",
        ParticipantEvent::ParticipantTerminalRecorded { .. } => "ParticipantTerminalRecorded",
        ParticipantEvent::ParticipantReconciliationEvidence { .. } => {
            "ParticipantReconciliationEvidence"
        }
        ParticipantEvent::ParticipantForwardOutcomeRecorded { .. } => {
            "ParticipantForwardOutcomeRecorded"
        }
        ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => {
            "ParticipantDependencyCompletedRecorded"
        }
    }
}

/// Unified participant-store wrapper injecting failures. Cloning shares the inner store, rules
/// and counters, so a "reopened" actor can be given a clone.
pub struct FaultStore<S: ParticipantStore> {
    inner: Arc<S>,
    rules: Arc<Mutex<Vec<Rule>>>,
    calls: Arc<Calls>,
}

impl<S: ParticipantStore> Clone for FaultStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            rules: Arc::clone(&self.rules),
            calls: Arc::clone(&self.calls),
        }
    }
}

impl<S: ParticipantStore> FaultStore<S> {
    pub fn new(inner: S) -> Self {
        Self::shared(Arc::new(inner))
    }

    /// New wrapper (fresh rules and counters) over an existing shared store.
    pub fn shared(inner: Arc<S>) -> Self {
        Self {
            inner,
            rules: Arc::default(),
            calls: Arc::default(),
        }
    }

    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }

    /// Arm a one-shot fault.
    pub fn fail_once(&self, op: JournalOp, trigger: FaultTrigger) {
        self.arm(op, trigger, true);
    }

    /// Arm a fault that fires on every match.
    pub fn fail_always(&self, op: JournalOp, trigger: FaultTrigger) {
        self.arm(op, trigger, false);
    }

    pub fn disarm(&self) {
        self.rules.lock().expect("rules lock").clear();
    }

    pub fn call_count(&self, op: JournalOp) -> usize {
        self.counter(op).load(Ordering::SeqCst)
    }

    fn arm(&self, op: JournalOp, trigger: FaultTrigger, once: bool) {
        self.rules.lock().expect("rules lock").push(Rule {
            op,
            trigger,
            once,
            fired: false,
        });
    }

    fn counter(&self, op: JournalOp) -> &AtomicUsize {
        &self.calls.0[op as usize]
    }

    fn check(&self, op: JournalOp, kinds: &[&str]) -> Result<(), JournalError> {
        let n = self.counter(op).fetch_add(1, Ordering::SeqCst) + 1;
        let mut rules = self.rules.lock().expect("rules lock");
        for rule in rules
            .iter_mut()
            .filter(|r| r.op == op && !(r.once && r.fired))
        {
            let hit = match &rule.trigger {
                FaultTrigger::NthCall(target) => *target == n,
                FaultTrigger::EventKind(want) => kinds.iter().any(|kind| kind == want),
                FaultTrigger::AtCut(cuts, point) => cuts.current() == Some(*point),
            };
            if hit {
                rule.fired = true;
                return Err(JournalError::Storage(
                    format!("injected {op:?} failure (call {n})").into(),
                ));
            }
        }
        Ok(())
    }
}

impl<S: ParticipantStore> ParticipantStore for FaultStore<S> {
    fn max_run_start_millis(&self) -> Result<u64, JournalError> {
        self.inner.max_run_start_millis()
    }

    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        self.check(JournalOp::Append, &[event_kind(&event)])?;
        self.inner.append(saga_id, event)
    }

    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        self.check(JournalOp::Read, &[])?;
        self.inner.read(saga_id)
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        self.check(JournalOp::ListSagas, &[])?;
        self.inner.list_sagas()
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        self.check(JournalOp::Prune, &[])?;
        self.inner.prune(saga_id)
    }
    fn append_batch(
        &self,
        saga_id: SagaId,
        events: Vec<ParticipantEvent>,
    ) -> Result<Vec<u64>, JournalError> {
        let kinds: Vec<_> = events.iter().map(event_kind).collect();
        self.check(JournalOp::AppendBatch, &kinds)?;
        self.inner.append_batch(saga_id, events)
    }
    fn admit_and_record_intent(
        &self,
        context: &SagaContext,
        dedupe_key: &str,
        triggering_event: &str,
    ) -> Result<StoreAdmission, JournalError> {
        self.check(JournalOp::AdmitAndRecordIntent, &[])?;
        self.inner
            .admit_and_record_intent(context, dedupe_key, triggering_event)
    }
    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, JournalError> {
        self.check(JournalOp::CheckAndMark, &[])?;
        self.inner.check_and_mark(saga_id, key)
    }
    fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), JournalError> {
        self.check(JournalOp::RemoveProcessed, &[])?;
        self.inner.remove_processed(saga_id, key)
    }
    fn unresolved_saga_ids(&self) -> Result<Vec<SagaId>, JournalError> {
        self.check(JournalOp::UnresolvedSagas, &[])?;
        self.inner.unresolved_saga_ids()
    }
    fn compact_terminal_detail(&self, batch_limit: usize) -> Result<usize, JournalError> {
        self.check(JournalOp::CompactTerminalDetail, &[])?;
        self.inner.compact_terminal_detail(batch_limit)
    }
    fn prune_resolved_before(
        &self,
        cutoff_millis: u64,
        batch_limit: usize,
    ) -> Result<usize, JournalError> {
        self.check(JournalOp::PruneResolvedBefore, &[])?;
        self.inner.prune_resolved_before(cutoff_millis, batch_limit)
    }
}

/// Manually advanced millisecond clock for `ingest_at`-style APIs.
#[derive(Clone, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    pub fn new(start_millis: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start_millis)))
    }

    pub fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    pub fn advance(&self, millis: u64) -> u64 {
        self.0.fetch_add(millis, Ordering::SeqCst) + millis
    }

    pub fn set(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}
