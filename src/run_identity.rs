//! Run identity, admission and replay horizon (ADR-0001).

use std::time::Duration;

use crate::{SagaChoreographyEvent, SagaContext, SagaId};

/// Incarnation of a run: the saga start time in epoch millis (ADR-0001).
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RunIncarnation(u64);

impl RunIncarnation {
    /// Incarnation from a saga start time in epoch millis (ADR-0001).
    pub const fn new(started_at_millis: u64) -> Self {
        Self(started_at_millis)
    }

    /// Raw epoch millis (ADR-0001).
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RunIncarnation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// `(saga_type, saga_id, incarnation)`; reuse of a `SagaId` requires a strictly greater
/// `saga_started_at_millis` (ADR-0001).
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RunKey {
    saga_type: Box<str>,
    saga_id: SagaId,
    incarnation: RunIncarnation,
}

impl RunKey {
    /// Build a run key (ADR-0001).
    pub fn new(
        saga_type: impl Into<Box<str>>,
        saga_id: SagaId,
        incarnation: RunIncarnation,
    ) -> Self {
        Self {
            saga_type: saga_type.into(),
            saga_id,
            incarnation,
        }
    }

    /// Saga type of the run (ADR-0001).
    pub fn saga_type(&self) -> &str {
        &self.saga_type
    }

    /// Saga id of the run (ADR-0001).
    pub fn saga_id(&self) -> SagaId {
        self.saga_id
    }

    /// Incarnation of the run (ADR-0001).
    pub fn incarnation(&self) -> RunIncarnation {
        self.incarnation
    }

    /// Non-allocating `self == context.run_key()` (ADR-0001).
    pub fn is_run_of(&self, context: &SagaContext) -> bool {
        self.saga_id == context.saga_id
            && self.incarnation.get() == context.saga_started_at_millis
            && self.saga_type.as_ref() == context.saga_type.as_ref()
    }

    /// `"{len:010}:{saga_type}:"` (ADR-0001).
    pub fn type_prefix(saga_type: &str) -> String {
        format!("{:010}:{}:", saga_type.len(), saga_type)
    }

    /// `type_prefix + "{saga_id:020}:"` (ADR-0001).
    pub fn saga_prefix(saga_type: &str, saga_id: SagaId) -> String {
        format!("{}{:020}:", Self::type_prefix(saga_type), saga_id.get())
    }

    /// `saga_prefix + "{incarnation:020}:"` (ADR-0001).
    pub fn storage_prefix(&self) -> String {
        format!(
            "{}{:020}:",
            Self::saga_prefix(&self.saga_type, self.saga_id),
            self.incarnation.get()
        )
    }

    /// Inverse of [`RunKey::storage_prefix`]; returns the key and the remainder (ADR-0001).
    pub fn parse_storage_prefix(encoded: &str) -> Option<(RunKey, &str)> {
        let len: usize = encoded.get(0..10)?.parse().ok()?;
        let rest = encoded.get(10..)?.strip_prefix(':')?;
        let saga_type = rest.get(..len)?;
        let rest = rest.get(len..)?.strip_prefix(':')?;
        let saga_id: u64 = rest.get(0..20)?.parse().ok()?;
        let rest = rest.get(20..)?.strip_prefix(':')?;
        let incarnation: u64 = rest.get(0..20)?.parse().ok()?;
        let rest = rest.get(20..)?.strip_prefix(':')?;
        Some((
            RunKey::new(
                saga_type,
                SagaId::new(saga_id),
                RunIncarnation::new(incarnation),
            ),
            rest,
        ))
    }
}

impl std::fmt::Display for RunKey {
    /// `{saga_type}/{saga_id}@{incarnation}` (ADR-0001).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}@{}",
            self.saga_type,
            self.saga_id.get(),
            self.incarnation
        )
    }
}

/// Local knowledge of the exact incoming run (ADR-0001).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStatus {
    /// No active state and no tombstone for the exact run (ADR-0001).
    Unknown,
    /// The run is active (ADR-0001).
    Active,
    /// The run is finalized (ADR-0001).
    Terminal,
}

/// What is known about other runs of the same `(saga_type, saga_id)` (ADR-0001).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KnownRuns {
    /// Newest incarnation among active runs and unexpired tombstones (ADR-0001).
    pub newest: Option<RunIncarnation>,
    /// Number of other active runs (ADR-0001).
    pub other_active: usize,
}

/// Admission decision (ADR-0001).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunAdmission {
    /// Admit as a new run; `concurrent` when another run of the saga id is active (ADR-0001).
    NewRun {
        /// Another run of the same saga id is active (ADR-0001).
        concurrent: bool,
    },
    /// Event belongs to the active run (ADR-0001).
    CurrentRun,
    /// Start replay for a run that is already known (ADR-0001).
    DuplicateStart,
    /// Non-start event for a finalized run (ADR-0001).
    TerminalRun,
}

/// Run-identity fence violation (ADR-0001).
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RunIdentityError {
    /// A newer incarnation of the saga id is already known (ADR-0001).
    #[error("stale run incarnation: incoming={incoming} newest_known={newest_known}")]
    StaleIncarnation {
        /// The rejected run (ADR-0001).
        incoming: RunKey,
        /// Newest known incarnation (ADR-0001).
        newest_known: RunIncarnation,
    },
    /// The incarnation is older than the replay horizon (ADR-0001).
    #[error("expired run incarnation: incoming={incoming} cutoff={cutoff}")]
    ExpiredIncarnation {
        /// The rejected run (ADR-0001).
        incoming: RunKey,
        /// Replay cutoff (ADR-0001).
        cutoff: RunIncarnation,
    },
}

/// ADR-0001 §2.2 admission table.
pub fn admit_run(
    exact: RunStatus,
    known: KnownRuns,
    incoming: &RunKey,
    is_start: bool,
    cutoff: RunIncarnation,
) -> Result<RunAdmission, RunIdentityError> {
    match exact {
        RunStatus::Active if is_start => Ok(RunAdmission::DuplicateStart),
        RunStatus::Active => Ok(RunAdmission::CurrentRun),
        RunStatus::Terminal if is_start => Ok(RunAdmission::DuplicateStart),
        RunStatus::Terminal => Ok(RunAdmission::TerminalRun),
        RunStatus::Unknown if incoming.incarnation() < cutoff => {
            Err(RunIdentityError::ExpiredIncarnation {
                incoming: incoming.clone(),
                cutoff,
            })
        }
        RunStatus::Unknown => match known.newest {
            Some(newest) if incoming.incarnation() < newest => {
                Err(RunIdentityError::StaleIncarnation {
                    incoming: incoming.clone(),
                    newest_known: newest,
                })
            }
            _ => Ok(RunAdmission::NewRun {
                concurrent: known.other_active > 0,
            }),
        },
    }
}

/// Stable identity of an event within its run; never contains `trace_id` (ADR-0001 §2.3).
pub fn event_identity(event: &SagaChoreographyEvent) -> String {
    const SEP: char = '\u{1f}';
    let context = event.context();
    let discriminator = match event {
        SagaChoreographyEvent::CompensationRequested {
            failed_step,
            steps_to_compensate,
            ..
        } => {
            let steps: Vec<&str> = steps_to_compensate.iter().map(AsRef::as_ref).collect();
            format!("{failed_step}\u{1e}{}", steps.join(","))
        }
        SagaChoreographyEvent::StepAccepted { execution_id, .. }
        | SagaChoreographyEvent::CompensationAccepted { execution_id, .. } => {
            execution_id.to_string()
        }
        SagaChoreographyEvent::StepAck { status, .. } => format!("{status:?}"),
        SagaChoreographyEvent::SagaAbortRequested { source, .. } => format!("{source:?}"),
        SagaChoreographyEvent::SagaEffectsRetained {
            steps, disposition, ..
        } => {
            let steps: Vec<&str> = steps.iter().map(AsRef::as_ref).collect();
            format!("{disposition:?}\u{1e}{}", steps.join(","))
        }
        _ => String::new(),
    };
    format!(
        "{}{SEP}{}{SEP}{}{SEP}{}",
        event.event_type(),
        context.step_name,
        context.attempt,
        discriminator
    )
}

/// Outcome recorded in a durable run tombstone; quarantined runs are never finalized (ADR-0001).
#[derive(Clone, Copy, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RunTerminalOutcome {
    /// The run completed (ADR-0001).
    Completed,
    /// The run failed and rolled back (ADR-0001).
    Failed,
}

/// Durable proof that a run was finalized, kept until the replay horizon expires (ADR-0001).
#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct RunTombstone {
    run: RunKey,
    outcome: RunTerminalOutcome,
    finalized_at_millis: u64,
}

impl RunTombstone {
    /// Build a tombstone (ADR-0001).
    pub fn new(run: RunKey, outcome: RunTerminalOutcome, finalized_at_millis: u64) -> Self {
        Self {
            run,
            outcome,
            finalized_at_millis,
        }
    }

    /// The finalized run (ADR-0001).
    pub fn run(&self) -> &RunKey {
        &self.run
    }

    /// Terminal outcome of the run (ADR-0001).
    pub fn outcome(&self) -> RunTerminalOutcome {
        self.outcome
    }

    /// Finalize time in epoch millis (ADR-0001).
    pub fn finalized_at_millis(&self) -> u64 {
        self.finalized_at_millis
    }
}

/// Documented floor of every replay horizon (ADR-0001 §2.5).
pub const MIN_REPLAY_HORIZON: Duration = Duration::from_secs(60 * 60);
/// Participant horizon when the host configures none (ADR-0001 §2.5).
pub const DEFAULT_PARTICIPANT_REPLAY_HORIZON: Duration = Duration::from_secs(24 * 60 * 60);

/// How long finalized-run tombstones are kept; older runs are rejected as expired (ADR-0001).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplayHorizon(Duration);

/// A requested horizon is below [`MIN_REPLAY_HORIZON`] (ADR-0001).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("replay horizon {requested:?} is below the minimum {minimum:?}")]
pub struct ReplayHorizonError {
    /// The rejected horizon (ADR-0001).
    pub requested: Duration,
    /// The floor (ADR-0001).
    pub minimum: Duration,
}

impl ReplayHorizon {
    /// Participant default (ADR-0001).
    pub const PARTICIPANT_DEFAULT: Self = Self(DEFAULT_PARTICIPANT_REPLAY_HORIZON);

    /// Explicit horizon; never clamped (ADR-0001).
    pub fn new(horizon: Duration) -> Result<Self, ReplayHorizonError> {
        if horizon < MIN_REPLAY_HORIZON {
            return Err(ReplayHorizonError {
                requested: horizon,
                minimum: MIN_REPLAY_HORIZON,
            });
        }
        Ok(Self(horizon))
    }

    /// `max(2 × overall_timeout, MIN_REPLAY_HORIZON)` (ADR-0001).
    pub fn for_overall_timeout(overall_timeout: Duration) -> Self {
        Self(overall_timeout.saturating_mul(2).max(MIN_REPLAY_HORIZON))
    }

    /// The horizon duration (ADR-0001).
    pub const fn get(self) -> Duration {
        self.0
    }

    /// Runs with an incarnation below the cutoff are expired (ADR-0001).
    pub fn cutoff(self, now_millis: u64) -> RunIncarnation {
        let horizon_millis = u64::try_from(self.0.as_millis()).unwrap_or(u64::MAX);
        RunIncarnation::new(now_millis.saturating_sub(horizon_millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AckStatus, DeterministicContextBuilder};

    fn key(incarnation: u64) -> RunKey {
        RunKey::new("order", SagaId::new(7), RunIncarnation::new(incarnation))
    }

    #[test]
    fn admit_run_follows_adr_0001_table() {
        let none = KnownRuns::default();
        let cutoff = RunIncarnation::new(0);
        let k = key(100);
        for (exact, start, expected) in [
            (RunStatus::Active, true, RunAdmission::DuplicateStart),
            (RunStatus::Active, false, RunAdmission::CurrentRun),
            (RunStatus::Terminal, true, RunAdmission::DuplicateStart),
            (RunStatus::Terminal, false, RunAdmission::TerminalRun),
        ] {
            assert_eq!(admit_run(exact, none, &k, start, cutoff), Ok(expected));
        }
        assert_eq!(
            admit_run(RunStatus::Unknown, none, &k, true, cutoff),
            Ok(RunAdmission::NewRun { concurrent: false })
        );
        let others = KnownRuns {
            newest: Some(RunIncarnation::new(50)),
            other_active: 1,
        };
        assert_eq!(
            admit_run(RunStatus::Unknown, others, &k, true, cutoff),
            Ok(RunAdmission::NewRun { concurrent: true })
        );
        for newest in [100, 99] {
            let known = KnownRuns {
                newest: Some(RunIncarnation::new(newest)),
                other_active: 0,
            };
            assert_eq!(
                admit_run(RunStatus::Unknown, known, &k, false, cutoff),
                Ok(RunAdmission::NewRun { concurrent: false })
            );
        }
        let newer = KnownRuns {
            newest: Some(RunIncarnation::new(101)),
            other_active: 0,
        };
        assert_eq!(
            admit_run(RunStatus::Unknown, newer, &k, true, cutoff),
            Err(RunIdentityError::StaleIncarnation {
                incoming: k.clone(),
                newest_known: RunIncarnation::new(101),
            })
        );
        let high_cutoff = RunIncarnation::new(200);
        for known in [none, newer] {
            assert_eq!(
                admit_run(RunStatus::Unknown, known, &k, true, high_cutoff),
                Err(RunIdentityError::ExpiredIncarnation {
                    incoming: k.clone(),
                    cutoff: high_cutoff,
                })
            );
        }
        for (exact, start, expected) in [
            (RunStatus::Active, false, RunAdmission::CurrentRun),
            (RunStatus::Terminal, false, RunAdmission::TerminalRun),
            (RunStatus::Terminal, true, RunAdmission::DuplicateStart),
        ] {
            assert_eq!(admit_run(exact, none, &k, start, high_cutoff), Ok(expected));
        }
    }

    #[test]
    fn storage_prefix_roundtrips_and_is_unambiguous() {
        let k = key(42);
        let probe = format!("{}suffix", k.storage_prefix());
        let (parsed, rest) = RunKey::parse_storage_prefix(&probe).expect("parses");
        assert_eq!(parsed, k);
        assert_eq!(rest, "suffix");
        let numeric = format!("{}00000000000000000001", k.storage_prefix());
        let (parsed, rest) = RunKey::parse_storage_prefix(&numeric).expect("parses numeric suffix");
        assert_eq!(parsed, k);
        assert_eq!(rest, "00000000000000000001");

        let colon = RunKey::new("a:b", SagaId::new(1), RunIncarnation::new(2));
        let plain = RunKey::new("a", SagaId::new(1), RunIncarnation::new(2));
        assert_ne!(colon.storage_prefix(), plain.storage_prefix());
        for key in [&colon, &plain] {
            let p = format!("{}x", key.storage_prefix());
            assert_eq!(RunKey::parse_storage_prefix(&p).expect("parses").0, *key);
        }
        assert!(!RunKey::type_prefix("ab").starts_with(&RunKey::type_prefix("a")));
        assert_eq!(RunKey::parse_storage_prefix("garbage"), None);
        assert_eq!(k.to_string(), "order/7@42");
    }

    #[test]
    fn event_identity_ignores_trace_and_keeps_attempt_and_frontier() {
        let ctx = DeterministicContextBuilder::default().build();
        let mut other_trace = ctx.clone();
        other_trace.trace_id = ctx.trace_id.wrapping_add(99);
        let ack = |context: SagaContext| SagaChoreographyEvent::StepAck {
            context,
            participant_id: "p".into(),
            status: AckStatus::Accepted,
        };
        assert_eq!(
            event_identity(&ack(ctx.clone())),
            event_identity(&ack(other_trace))
        );
        let mut retry = ctx.clone();
        retry.attempt += 1;
        assert_ne!(
            event_identity(&ack(ctx.clone())),
            event_identity(&ack(retry))
        );

        let comp = |steps: &[&str]| SagaChoreographyEvent::CompensationRequested {
            context: ctx.clone(),
            failed_step: "f".into(),
            reason: "r".into(),
            failure: crate::SagaFailureDetails {
                step_name: "f".into(),
                participant_id: "p".into(),
                error_code: None,
                error_message: "m".into(),
                at_millis: 0,
            },
            steps_to_compensate: steps.iter().map(|s| (*s).into()).collect(),
        };
        assert_ne!(event_identity(&comp(&["a"])), event_identity(&comp(&["b"])));
    }

    #[test]
    fn run_key_is_run_of_context() {
        let ctx = DeterministicContextBuilder::default().build();
        assert!(ctx.run_key().is_run_of(&ctx));
        let mut changed = ctx.clone();
        changed.saga_type = "other".into();
        assert!(!ctx.run_key().is_run_of(&changed));
        let mut changed = ctx.clone();
        changed.saga_id = SagaId::new(ctx.saga_id.get() + 1);
        assert!(!ctx.run_key().is_run_of(&changed));
        let mut changed = ctx.clone();
        changed.saga_started_at_millis += 1;
        assert!(!ctx.run_key().is_run_of(&changed));
    }

    #[test]
    fn replay_horizon_floor_default_and_cutoff() {
        let sec = Duration::from_secs(1);
        assert!(ReplayHorizon::new(MIN_REPLAY_HORIZON - sec).is_err());
        assert!(ReplayHorizon::new(MIN_REPLAY_HORIZON).is_ok());
        assert_eq!(
            ReplayHorizon::for_overall_timeout(Duration::from_secs(10)).get(),
            MIN_REPLAY_HORIZON
        );
        assert_eq!(
            ReplayHorizon::for_overall_timeout(Duration::from_secs(2 * 3600)).get(),
            Duration::from_secs(4 * 3600)
        );
        let horizon = ReplayHorizon::new(MIN_REPLAY_HORIZON).expect("floor is valid");
        assert_eq!(horizon.cutoff(10_000_000).get(), 10_000_000 - 3_600_000);
        assert_eq!(horizon.cutoff(5).get(), 0);
    }
}
