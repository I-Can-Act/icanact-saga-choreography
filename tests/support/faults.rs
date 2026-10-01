use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use icanact_saga_choreography::{
    DedupeError, JournalEntry, JournalError, ParticipantDedupeStore, ParticipantEvent,
    ParticipantJournal, RunIncarnation, RunKey, RunTombstone, SagaId,
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
    Read,
    ListSagas,
    Prune,
}

/// When an armed fault fires.
#[derive(Clone)]
pub enum FaultTrigger {
    /// The Nth (1-based) call of the op on this wrapper.
    NthCall(usize),
    /// Appends whose event variant name equals this (e.g. `"StepExecutionCompleted"`).
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
struct Calls {
    append: AtomicUsize,
    read: AtomicUsize,
    list: AtomicUsize,
    prune: AtomicUsize,
}

fn event_kind(event: &ParticipantEvent) -> String {
    let dbg = format!("{:?}", event.transition());
    dbg.split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Journal wrapper injecting failures. Cloning shares the inner store, rules
/// and counters, so a "reopened" actor can be given a clone.
pub struct FaultJournal<J: ParticipantJournal> {
    inner: Arc<J>,
    rules: Arc<Mutex<Vec<Rule>>>,
    calls: Arc<Calls>,
}

impl<J: ParticipantJournal> Clone for FaultJournal<J> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            rules: Arc::clone(&self.rules),
            calls: Arc::clone(&self.calls),
        }
    }
}

impl<J: ParticipantJournal> FaultJournal<J> {
    pub fn new(inner: J) -> Self {
        Self::shared(Arc::new(inner))
    }

    /// New wrapper (fresh rules and counters) over an existing shared store.
    pub fn shared(inner: Arc<J>) -> Self {
        Self {
            inner,
            rules: Arc::default(),
            calls: Arc::default(),
        }
    }

    pub fn inner(&self) -> &Arc<J> {
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
        match op {
            JournalOp::Append => &self.calls.append,
            JournalOp::Read => &self.calls.read,
            JournalOp::ListSagas => &self.calls.list,
            JournalOp::Prune => &self.calls.prune,
        }
    }

    fn check(&self, op: JournalOp, kind: Option<&str>) -> Result<(), JournalError> {
        let n = self.counter(op).fetch_add(1, Ordering::SeqCst) + 1;
        let mut rules = self.rules.lock().expect("rules lock");
        for rule in rules
            .iter_mut()
            .filter(|r| r.op == op && !(r.once && r.fired))
        {
            let hit = match &rule.trigger {
                FaultTrigger::NthCall(target) => *target == n,
                FaultTrigger::EventKind(want) => kind == Some(*want),
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

impl<J: ParticipantJournal> ParticipantJournal for FaultJournal<J> {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        self.check(JournalOp::Append, Some(&event_kind(&event)))?;
        self.inner.append(saga_id, event)
    }

    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        self.check(JournalOp::Read, None)?;
        self.inner.read(saga_id)
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        self.check(JournalOp::ListSagas, None)?;
        self.inner.list_sagas()
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        self.check(JournalOp::Prune, None)?;
        self.inner.prune(saga_id)
    }

    fn append_run(&self, run: &RunKey, event: ParticipantEvent) -> Result<u64, JournalError> {
        self.check(JournalOp::Append, Some(&event_kind(&event)))?;
        self.inner.append_run(run, event)
    }

    fn read_run(&self, run: &RunKey) -> Result<Vec<JournalEntry>, JournalError> {
        self.check(JournalOp::Read, None)?;
        self.inner.read_run(run)
    }

    fn list_runs(&self) -> Result<Vec<RunKey>, JournalError> {
        self.check(JournalOp::ListSagas, None)?;
        self.inner.list_runs()
    }

    fn finalize_run(
        &self,
        tombstone: &RunTombstone,
        cutoff: RunIncarnation,
    ) -> Result<(), JournalError> {
        self.check(JournalOp::Prune, None)?;
        self.inner.finalize_run(tombstone, cutoff)
    }

    fn run_tombstones(
        &self,
        saga_type: &str,
        saga_id: SagaId,
    ) -> Result<Vec<RunTombstone>, JournalError> {
        self.check(JournalOp::Read, None)?;
        self.inner.run_tombstones(saga_type, saga_id)
    }

    fn prune_expired_tombstones(&self, cutoff: RunIncarnation) -> Result<u64, JournalError> {
        self.check(JournalOp::Prune, None)?;
        self.inner.prune_expired_tombstones(cutoff)
    }
}

/// Dedupe wrapper failing `check_and_mark` / `prune` on the Nth call or at a cut.
/// Cloning shares the inner store, triggers and counters.
pub struct FaultDedupe<D: ParticipantDedupeStore> {
    inner: Arc<D>,
    mark_trigger: Arc<Mutex<Option<FaultTrigger>>>,
    prune_trigger: Arc<Mutex<Option<FaultTrigger>>>,
    marks: Arc<AtomicUsize>,
    prunes: Arc<AtomicUsize>,
}

impl<D: ParticipantDedupeStore> Clone for FaultDedupe<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            mark_trigger: Arc::clone(&self.mark_trigger),
            prune_trigger: Arc::clone(&self.prune_trigger),
            marks: Arc::clone(&self.marks),
            prunes: Arc::clone(&self.prunes),
        }
    }
}

impl<D: ParticipantDedupeStore> FaultDedupe<D> {
    pub fn new(inner: D) -> Self {
        Self::shared(Arc::new(inner))
    }

    pub fn shared(inner: Arc<D>) -> Self {
        Self {
            inner,
            mark_trigger: Arc::default(),
            prune_trigger: Arc::default(),
            marks: Arc::default(),
            prunes: Arc::default(),
        }
    }

    pub fn inner(&self) -> &Arc<D> {
        &self.inner
    }

    pub fn fail_check_and_mark(&self, trigger: FaultTrigger) {
        *self.mark_trigger.lock().expect("trigger lock") = Some(trigger);
    }

    pub fn fail_prune(&self, trigger: FaultTrigger) {
        *self.prune_trigger.lock().expect("trigger lock") = Some(trigger);
    }

    pub fn disarm(&self) {
        *self.mark_trigger.lock().expect("trigger lock") = None;
        *self.prune_trigger.lock().expect("trigger lock") = None;
    }

    pub fn mark_calls(&self) -> usize {
        self.marks.load(Ordering::SeqCst)
    }

    pub fn prune_calls(&self) -> usize {
        self.prunes.load(Ordering::SeqCst)
    }

    fn fires(trigger: &Mutex<Option<FaultTrigger>>, counter: &AtomicUsize) -> bool {
        let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
        match trigger.lock().expect("trigger lock").as_ref() {
            Some(FaultTrigger::NthCall(target)) => *target == n,
            Some(FaultTrigger::AtCut(cuts, point)) => cuts.current() == Some(*point),
            // Event kinds are meaningless for dedupe.
            Some(FaultTrigger::EventKind(_)) | None => false,
        }
    }
}

impl<D: ParticipantDedupeStore> ParticipantDedupeStore for FaultDedupe<D> {
    fn check_and_mark(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        if Self::fires(&self.mark_trigger, &self.marks) {
            return Err(DedupeError::Storage(
                "injected check_and_mark failure".into(),
            ));
        }
        self.inner.check_and_mark(saga_id, key)
    }

    fn contains(&self, saga_id: SagaId, key: &str) -> Result<bool, DedupeError> {
        self.inner.contains(saga_id, key)
    }

    fn mark_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        self.inner.mark_processed(saga_id, key)
    }

    fn remove_processed(&self, saga_id: SagaId, key: &str) -> Result<(), DedupeError> {
        self.inner.remove_processed(saga_id, key)
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), DedupeError> {
        if Self::fires(&self.prune_trigger, &self.prunes) {
            return Err(DedupeError::Storage("injected prune failure".into()));
        }
        self.inner.prune(saga_id)
    }

    fn check_and_mark_run(&self, run: &RunKey, key: &str) -> Result<bool, DedupeError> {
        if Self::fires(&self.mark_trigger, &self.marks) {
            return Err(DedupeError::Storage(
                "injected check_and_mark failure".into(),
            ));
        }
        self.inner.check_and_mark_run(run, key)
    }

    fn contains_run(&self, run: &RunKey, key: &str) -> Result<bool, DedupeError> {
        self.inner.contains_run(run, key)
    }

    fn mark_processed_run(&self, run: &RunKey, key: &str) -> Result<(), DedupeError> {
        self.inner.mark_processed_run(run, key)
    }

    fn remove_processed_run(&self, run: &RunKey, key: &str) -> Result<(), DedupeError> {
        self.inner.remove_processed_run(run, key)
    }

    fn prune_run(&self, run: &RunKey) -> Result<(), DedupeError> {
        if Self::fires(&self.prune_trigger, &self.prunes) {
            return Err(DedupeError::Storage("injected prune failure".into()));
        }
        self.inner.prune_run(run)
    }

    fn list_runs(&self) -> Result<Vec<RunKey>, DedupeError> {
        self.inner.list_runs()
    }

    fn keys_run(&self, run: &RunKey) -> Result<Vec<Box<str>>, DedupeError> {
        self.inner.keys_run(run)
    }

    fn prune_expired(&self, cutoff: RunIncarnation) -> Result<u64, DedupeError> {
        if Self::fires(&self.prune_trigger, &self.prunes) {
            return Err(DedupeError::Storage("injected prune failure".into()));
        }
        self.inner.prune_expired(cutoff)
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
