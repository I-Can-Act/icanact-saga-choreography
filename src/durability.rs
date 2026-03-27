use std::path::{Path, PathBuf};

use crate::{
    handle_async_saga_event_with_emit, handle_saga_event_with_emit, AsyncSagaParticipant,
    DedupeError, HasSagaParticipantSupport, JournalEntry, JournalError, ParticipantDedupeStore,
    ParticipantEvent, ParticipantJournal, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt,
};

pub const PANIC_QUARANTINE_REASON_PREFIX: &str = "panic_during_active_";
pub const PANIC_QUARANTINE_PUBLISH_KEY: &str = "panic_quarantine_published";
pub const DEFAULT_RECOVERY_SAGA_TYPE: &str = "order_lifecycle";

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
            let _ = bus.publish(next_event);
        }
    }
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
            let _ = bus.publish(next_event);
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

    let _ = saga.journal.append(
        context.saga_id,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );

    let emitted = SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(step_name.into()),
        reason,
        step: step_name.to_string().into_boxed_str(),
        participant_id,
    };

    if let Some(bus) = &saga.bus {
        let stats = bus.publish(emitted);
        if stats.delivered > 0 {
            let _ = saga
                .dedupe
                .mark_processed(context.saga_id, PANIC_QUARANTINE_PUBLISH_KEY);
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
        let stale_after_ms = std::env::var("SAGA_RECOVERY_MAX_AGE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5 * 60 * 1000);
        Self { stale_after_ms }
    }
}

pub fn classify_recovery(
    entries: &[JournalEntry],
    now_ms: u64,
    policy: RecoveryPolicy,
) -> RecoveryDecision {
    if entries.is_empty() {
        return RecoveryDecision::TerminalNoAction;
    }
    let last = entries.last().expect("non-empty entries");
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
) -> Vec<SagaChoreographyEvent> {
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
) -> Vec<SagaChoreographyEvent> {
    let mut out = Vec::new();
    let policy = RecoveryPolicy::default();
    let now = SagaContext::now_millis();
    let saga_ids = journal.list_sagas().unwrap_or_default();
    for saga_id in saga_ids {
        let entries = journal.read(saga_id).unwrap_or_default();
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
                let should_emit = dedupe
                    .check_and_mark(saga_id, PANIC_QUARANTINE_PUBLISH_KEY)
                    .unwrap_or(false);
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
    out
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

    use super::{
        collect_startup_recovery_events, collect_startup_recovery_events_for_saga_type,
        DEFAULT_RECOVERY_SAGA_TYPE,
    };
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
        let startup_recovery_events = if saga_type == DEFAULT_RECOVERY_SAGA_TYPE {
            collect_startup_recovery_events(&journal, &dedupe, step_name)
        } else {
            collect_startup_recovery_events_for_saga_type(&journal, &dedupe, step_name, saga_type)
        };
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
    use super::default_runtime_dir;

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
}
