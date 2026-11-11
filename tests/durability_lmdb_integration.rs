use std::path::PathBuf;

use heed::EnvOpenOptions;
use heed::types::{Bytes, Str};
use icanact_saga_choreography::LmdbParticipantStore;
use icanact_saga_choreography::durability::{ActiveSagaExecutionPhase, panic_quarantine_reason};
use icanact_saga_choreography::{
    HasSagaParticipantSupport, ParticipantEvent, ParticipantStore, SagaChoreographyEvent,
    SagaContext, SagaId, SagaParticipantSupport, SagaStateExt,
};

#[test]
fn lmdb_envs_close_on_drop_allowing_many_sequential_participants() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    for iteration in 0..300 {
        let journal = LmdbParticipantStore::open(temp.path(), "test_participant")
            .unwrap_or_else(|err| panic!("journal open failed at iteration {iteration}: {err}"));
        let dedupe = LmdbParticipantStore::open(temp.path(), "test_participant")
            .unwrap_or_else(|err| panic!("dedupe open failed at iteration {iteration}: {err}"));
        journal
            .append(
                SagaId::new(iteration),
                ParticipantEvent::ParticipantRunRecorded {
                    saga_type: "probe".into(),
                    saga_started_at_millis: iteration + 1,
                    recorded_at_millis: iteration + 1,
                },
            )
            .unwrap();
        assert!(
            dedupe
                .check_and_mark(SagaId::new(iteration), "probe")
                .unwrap_or_else(|err| panic!(
                    "check_and_mark failed at iteration {iteration}: {err}"
                ))
        );
        drop(journal);
        drop(dedupe);
    }
}

#[test]
fn lmdb_journal_and_dedupe_preserve_rows_after_drop_and_reopen() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().to_path_buf();
    let dedupe_path = temp.path().to_path_buf();
    let saga_id = SagaId::new(100);
    let journal =
        LmdbParticipantStore::open(&journal_path, "test_participant").expect("journal should open");
    let dedupe =
        LmdbParticipantStore::open(&dedupe_path, "test_participant").expect("dedupe should open");
    let sequence = journal
        .append(
            saga_id,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1000,
            },
        )
        .expect("append should succeed");
    assert!(dedupe.check_and_mark(saga_id, "probe").unwrap());
    drop(journal);
    drop(dedupe);

    let journal = LmdbParticipantStore::open(&journal_path, "test_participant")
        .expect("journal should reopen");
    let dedupe =
        LmdbParticipantStore::open(&dedupe_path, "test_participant").expect("dedupe should reopen");
    let rows = journal.read(saga_id).expect("rows should read back");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sequence, sequence);
    assert!(matches!(
        rows[0].event,
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 1000,
        }
    ));
    assert_eq!(journal.list_sagas().unwrap(), vec![saga_id]);
    assert!(!dedupe.check_and_mark(saga_id, "probe").unwrap());
}

#[test]
fn lmdb_journal_and_dedupe_double_open_drop_and_reopen() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().to_path_buf();
    let dedupe_path = temp.path().to_path_buf();
    let journal = LmdbParticipantStore::open(&journal_path, "test_participant")
        .expect("first journal should open");
    let second_journal = LmdbParticipantStore::open(&journal_path, "test_participant")
        .expect("second journal should open");
    let dedupe = LmdbParticipantStore::open(&dedupe_path, "test_participant")
        .expect("first dedupe should open");
    let second_dedupe = LmdbParticipantStore::open(&dedupe_path, "test_participant")
        .expect("second dedupe should open");
    drop(journal);
    drop(second_journal);
    drop(dedupe);
    drop(second_dedupe);

    let journal = LmdbParticipantStore::open(&journal_path, "test_participant")
        .expect("journal should reopen");
    let dedupe =
        LmdbParticipantStore::open(&dedupe_path, "test_participant").expect("dedupe should reopen");
    assert!(journal.list_sagas().unwrap().is_empty());
    journal
        .append(
            SagaId::new(100),
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1000,
            },
        )
        .unwrap();
    assert!(dedupe.check_and_mark(SagaId::new(100), "probe").unwrap());
}

#[test]
fn lmdb_journal_and_dedupe_drop_preserves_other_same_path_owners() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().to_path_buf();
    let dedupe_path = temp.path().to_path_buf();
    let first_journal = LmdbParticipantStore::open(&journal_path, "test_participant").unwrap();
    let first_dedupe = LmdbParticipantStore::open(&dedupe_path, "test_participant").unwrap();
    let second_journal =
        LmdbParticipantStore::open(&journal_path.join("."), "test_participant").unwrap();
    let second_dedupe =
        LmdbParticipantStore::open(&dedupe_path.join("."), "test_participant").unwrap();
    drop(first_journal);
    drop(first_dedupe);

    let saga_id = SagaId::new(100);
    second_journal
        .append(
            saga_id,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1000,
            },
        )
        .unwrap();
    assert_eq!(second_journal.read(saga_id).unwrap().len(), 1);
    assert!(second_dedupe.check_and_mark(saga_id, "probe").unwrap());
    assert!(!second_dedupe.check_and_mark(saga_id, "probe").unwrap());

    let third_journal = LmdbParticipantStore::open(&journal_path, "test_participant")
        .expect("third journal should open while the second owner is alive");
    let third_dedupe = LmdbParticipantStore::open(&dedupe_path, "test_participant")
        .expect("third dedupe should open while the second owner is alive");
    drop(second_journal);
    drop(second_dedupe);
    assert_eq!(third_journal.read(saga_id).unwrap().len(), 1);
    assert!(!third_dedupe.check_and_mark(saga_id, "probe").unwrap());
    drop(third_journal);
    drop(third_dedupe);

    let reopened_journal = LmdbParticipantStore::open(&journal_path, "test_participant").unwrap();
    let reopened_dedupe = LmdbParticipantStore::open(&dedupe_path, "test_participant").unwrap();
    assert_eq!(reopened_journal.read(saga_id).unwrap().len(), 1);
    assert!(!reopened_dedupe.check_and_mark(saga_id, "probe").unwrap());
}

#[test]
fn lmdb_failed_opens_release_env_ownership() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    for (name, max_dbs) in [("participant-a", 16), ("participant-b", 16)] {
        let path = temp.path().join(name);
        std::fs::create_dir_all(&path).unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(max_dbs)
                .map_size(1024 * 1024 * 1024)
                .open(&path)
        }
        .unwrap();
        let mut wtxn = env.write_txn().unwrap();
        for index in 0..max_dbs {
            env.create_database::<Str, Str>(&mut wtxn, Some(&format!("occupied-{index}")))
                .unwrap();
        }
        wtxn.commit().unwrap();

        // Keep the seeded env live so all database slots remain occupied.
        LmdbParticipantStore::open(&path, "test_participant")
            .expect_err("database creation must fail");
        drop(env);
        assert!(
            heed::env_closing_event(path.canonicalize().unwrap()).is_none(),
            "failed {name} open must release and close its env"
        );
    }
}

#[test]
fn lmdb_journal_and_dedupe_roundtrip() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().to_path_buf();
    let dedupe_path = temp.path().to_path_buf();

    let journal =
        LmdbParticipantStore::open(&journal_path, "test_participant").expect("journal should open");
    let dedupe =
        LmdbParticipantStore::open(&dedupe_path, "test_participant").expect("dedupe should open");

    let saga_a = SagaId::new(100);
    let saga_b = SagaId::new(101);

    journal
        .append(
            saga_a,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1000,
            },
        )
        .expect("append for saga_a should succeed");
    journal
        .append(
            saga_b,
            ParticipantEvent::StepExecutionCompleted {
                output: b"ok".to_vec(),
                compensation_data: vec![],
                completed_at_millis: 1100,
            },
        )
        .expect("append for saga_b should succeed");

    let read_a = journal.read(saga_a).expect("read saga_a should succeed");
    assert_eq!(read_a.len(), 1);
    assert!(matches!(
        read_a[0].event,
        ParticipantEvent::StepExecutionStarted { .. }
    ));

    let mut saga_ids = journal.list_sagas().expect("list_sagas should succeed");
    saga_ids.sort_by_key(|id| id.get());
    assert_eq!(saga_ids, vec![saga_a, saga_b]);

    journal.prune(saga_a).expect("journal prune should succeed");
    assert!(
        journal
            .read(saga_a)
            .expect("read pruned saga should succeed")
            .is_empty(),
        "journal prune must remove saga rows"
    );
    assert_eq!(
        journal
            .list_sagas()
            .expect("list after prune should succeed"),
        vec![saga_b],
        "journal prune must remove saga index entry"
    );

    assert!(
        dedupe.check_and_mark(saga_a, "probe").is_err(),
        "pruned run has no dedupe namespace"
    );
    journal
        .append(
            saga_a,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 2000,
            },
        )
        .unwrap();
    assert!(
        dedupe
            .check_and_mark(saga_a, "probe")
            .expect("first check_and_mark should succeed")
    );
    assert!(
        !dedupe
            .check_and_mark(saga_a, "probe")
            .expect("second check_and_mark should succeed")
    );
    assert!(
        !dedupe
            .check_and_mark(saga_a, "probe")
            .expect("contains should succeed")
    );

    dedupe
        .mark_processed(saga_b, "manual")
        .expect("mark_processed should succeed");
    assert!(
        !dedupe
            .check_and_mark(saga_b, "manual")
            .expect("contains should succeed")
    );

    dedupe.prune(saga_a).expect("prune should succeed");
    assert!(
        dedupe.check_and_mark(saga_a, "probe").is_err(),
        "administrative prune removes the run and its dedupe namespace"
    );
}

struct LmdbParticipant {
    saga: SagaParticipantSupport<LmdbParticipantStore>,
}

impl HasSagaParticipantSupport for LmdbParticipant {
    type Store = LmdbParticipantStore;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Store> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Store> {
        &mut self.saga
    }
}

#[test]
fn prune_saga_removes_lmdb_journal_and_dedupe_state() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal =
        LmdbParticipantStore::open(temp.path(), "test_participant").expect("journal should open");
    let dedupe =
        LmdbParticipantStore::open(temp.path(), "test_participant").expect("dedupe should open");
    let saga_a = SagaId::new(300);
    let saga_b = SagaId::new(301);

    journal
        .append(
            saga_a,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1200,
            },
        )
        .expect("append saga_a should succeed");
    journal
        .append(
            saga_b,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 1300,
            },
        )
        .expect("append saga_b should succeed");
    dedupe
        .mark_processed(saga_a, "started")
        .expect("dedupe mark should succeed");

    let mut actor = LmdbParticipant {
        saga: SagaParticipantSupport::new(journal),
    };
    actor
        .prune_saga_strict(saga_a)
        .expect("terminal saga prune should succeed");

    assert!(
        actor
            .saga
            .store
            .read(saga_a)
            .expect("read pruned saga should succeed")
            .is_empty(),
        "terminal cleanup must remove durable journal rows"
    );
    assert_eq!(
        actor
            .saga
            .store
            .list_sagas()
            .expect("list after prune should succeed"),
        vec![saga_b],
        "terminal cleanup must remove only the pruned saga index"
    );
    assert!(
        actor.saga.store.check_and_mark(saga_a, "started").is_err(),
        "pruned run cannot retain a dedupe namespace"
    );
    actor
        .saga
        .store
        .append(
            saga_a,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: 2000,
            },
        )
        .unwrap();
    assert!(
        actor.saga.store.check_and_mark(saga_a, "started").unwrap(),
        "explicit reuse after administrative prune must not retain old dedupe rows"
    );
}

#[test]
fn open_support_replays_panic_quarantine_once() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let base: PathBuf = temp.path().join("support");

    let journal = LmdbParticipantStore::open(&base, "risk_gate").expect("journal should open");
    let saga_id = SagaId::new(202);
    journal
        .append(
            saga_id,
            ParticipantEvent::ParticipantRunRecorded {
                saga_type: "mature_pool_refresh".into(),
                saga_started_at_millis: 2_020,
                recorded_at_millis: 2_020,
            },
        )
        .expect("append run record should succeed");
    journal
        .append(
            saga_id,
            ParticipantEvent::Quarantined {
                reason: panic_quarantine_reason(ActiveSagaExecutionPhase::StepExecution, "boom"),
                quarantined_at_millis: SagaContext::now_millis(),
            },
        )
        .expect("append quarantined event should succeed");

    let mut first =
        open_lmdb_participant_support_for_saga_type(&base, "risk_gate", "mature_pool_refresh")
            .expect("support should open");
    let first_events = first.take_startup_recovery_events();
    assert_eq!(first_events.len(), 1);
    assert!(matches!(
        &first_events[0],
        SagaChoreographyEvent::SagaQuarantined { context, .. }
            if context.saga_type.as_ref() == "mature_pool_refresh"
                && context.saga_id == saga_id
                && context.saga_started_at_millis == 2_020
    ));

    let mut second =
        open_lmdb_participant_support_for_saga_type(&base, "risk_gate", "mature_pool_refresh")
            .expect("support should reopen");
    let second_events = second.take_startup_recovery_events();
    assert!(
        second_events.is_empty(),
        "dedupe should prevent replaying panic quarantine more than once"
    );

    let mut default_support =
        open_lmdb_participant_support(&base, "risk_gate").expect("default support should open");
    let default_events = default_support.take_startup_recovery_events();
    assert!(
        default_events.is_empty(),
        "panic replay should remain deduped through default open helper"
    );
}

#[test]
fn lmdb_open_fails_for_file_paths() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_file = temp.path().join("journal-file");
    let dedupe_file = temp.path().join("dedupe-file");
    std::fs::write(&journal_file, b"not-a-directory").expect("journal file should be created");
    std::fs::write(&dedupe_file, b"not-a-directory").expect("dedupe file should be created");

    let journal_err = LmdbParticipantStore::open(&journal_file, "test_participant")
        .expect_err("file path must fail");
    assert!(
        !journal_err.to_string().is_empty(),
        "journal open error should include a storage message"
    );

    let dedupe_err = LmdbParticipantStore::open(&dedupe_file, "test_participant")
        .expect_err("file path must fail");
    assert!(
        !dedupe_err.to_string().is_empty(),
        "dedupe open error should include a storage message"
    );
}

#[test]
fn lmdb_journal_rejects_unversioned_persisted_rows() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().join("legacy-journal");
    std::fs::create_dir_all(&journal_path).expect("legacy journal directory should exist");
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(16)
                .map_size(1024 * 1024 * 1024)
                .open(&journal_path)
        }
        .expect("legacy environment should open");
        let mut wtxn = env
            .write_txn()
            .expect("legacy write transaction should open");
        let rows = env
            .create_database::<Str, Bytes>(&mut wtxn, Some("participant_events"))
            .expect("legacy rows database should open");
        rows.put(
            &mut wtxn,
            "00000000000000000001:00000000000000000001",
            b"legacy",
        )
        .expect("legacy row should be persisted");
        wtxn.commit().expect("legacy row should commit");
    }

    let err = LmdbParticipantStore::open(&journal_path, "test_participant")
        .expect_err("unversioned persisted rows must fail closed before decoding");
    assert!(
        err.to_string().contains("required=3"),
        "unexpected schema error: {err}"
    );
}

// ---- U6: reopened LMDB workflow replay safety ------------------------------------

mod workflow_reopen {
    use std::cell::RefCell;
    use std::path::Path;

    use super::*;
    use icanact_saga_choreography::durability::apply_sync_workflow_participant_saga_ingress_with_hooks;
    use icanact_saga_choreography::{
        CompensationError, CompensationOutput, HasSagaWorkflowParticipants,
        ParticipantTerminalKind, PeerId, SagaWorkflowParticipant, StepError, StepOutput,
    };

    struct Actor {
        saga: SagaParticipantSupport<LmdbParticipantStore>,
        pay_calls: usize,
        declared_effect: bool,
        comp_calls: usize,
        last_comp_data: Vec<u8>,
    }

    impl Actor {
        fn open(base: &Path) -> Self {
            Self {
                saga: open_lmdb_participant_support_for_saga_type(base, "pay", "wf_pay")
                    .expect("support should open"),
                pay_calls: 0,
                declared_effect: false,
                comp_calls: 0,
                last_comp_data: Vec::new(),
            }
        }
    }

    impl HasSagaParticipantSupport for Actor {
        type Store = LmdbParticipantStore;
        fn saga_support(&self) -> &SagaParticipantSupport<LmdbParticipantStore> {
            &self.saga
        }
        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<LmdbParticipantStore> {
            &mut self.saga
        }
    }

    struct Pay;
    static PAY: Pay = Pay;
    static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 1] = [&PAY];

    impl HasSagaWorkflowParticipants for Actor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &WORKFLOWS
        }
    }

    impl SagaWorkflowParticipant<Actor> for Pay {
        fn step_name(&self) -> &'static str {
            "pay"
        }
        fn saga_types(&self) -> &[&'static str] {
            &["wf_pay"]
        }
        fn execute_step(
            &self,
            actor: &mut Actor,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            actor.pay_calls += 1;
            if actor.declared_effect {
                return Ok(StepOutput::CompletedWithEffect {
                    output: b"paid".to_vec(),
                    compensation_data: b"refund".to_vec(),
                    effect: "payment-outbox".into(),
                });
            }
            Ok(StepOutput::Completed {
                output: b"paid".to_vec(),
                compensation_data: b"refund".to_vec(),
            })
        }
        fn dispatch_effect(
            &self,
            actor: &mut Actor,
            request: &icanact_saga_choreography::EffectDispatchRequest<'_>,
        ) -> Result<
            icanact_saga_choreography::EffectDispatchOutcome,
            icanact_saga_choreography::EffectDispatchError,
        > {
            let rows = actor.saga.store.read(request.context.saga_id).unwrap();
            assert!(
                rows.iter().any(|row| matches!(&row.event,
                ParticipantEvent::StepExecutionCompleted { output, compensation_data, .. }
                    if output == b"paid" && compensation_data == b"refund")),
                "declared effect bytes must be durable before dispatch"
            );
            assert!(!rows.iter().any(|row| matches!(
                row.event,
                ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
            )));
            Ok(icanact_saga_choreography::EffectDispatchOutcome::Durable {
                receipt: "outbox-1".into(),
            })
        }
        fn compensate_step(
            &self,
            actor: &mut Actor,
            _context: &SagaContext,
            data: &[u8],
        ) -> Result<CompensationOutput, CompensationError> {
            actor.comp_calls += 1;
            actor.last_comp_data = data.to_vec();
            Ok(CompensationOutput::Completed)
        }
    }

    fn ctx(started_at: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(61),
            saga_type: "wf_pay".into(),
            step_name: "pay".into(),
            correlation_id: 61,
            causation_id: 61,
            trace_id: 61,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: PeerId::default(),
            saga_started_at_millis: started_at,
            event_timestamp_millis: started_at,
        }
    }

    fn deliver(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
        let out = RefCell::new(Vec::new());
        apply_sync_workflow_participant_saga_ingress_with_hooks(
            actor,
            event,
            |_actor, _event| {},
            |event| panic!("valid transition rejected: {event:?}"),
            |_actor, event| out.borrow_mut().push(event.clone()),
        );
        out.into_inner()
    }

    fn start(context: &SagaContext) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaStarted {
            context: context.clone(),
            payload: b"in".to_vec(),
        }
    }

    fn undo_request(run: &SagaContext) -> SagaChoreographyEvent {
        SagaChoreographyEvent::CompensationRequested {
            context: run.next_step("terminal_resolver".into()),
            failed_step: "later".into(),
            reason: "rollback".into(),
            failure: icanact_saga_choreography::SagaFailureDetails {
                step_name: "later".into(),
                participant_id: "p".into(),
                error_code: None,
                error_message: "rollback".into(),
                at_millis: 100,
            },
            steps_to_compensate: vec!["pay".into()],
        }
    }

    #[test]
    fn confirmed_forward_proof_resends_original_completion_after_lmdb_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        let original = {
            let mut actor = Actor::open(temp.path());
            let out = deliver(&mut actor, start(&run));
            assert_eq!(actor.pay_calls, 1);
            out.into_iter()
                .find(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
                .expect("original completion")
        };

        let mut reopened = Actor::open(temp.path());
        let resent = reopened.saga.take_startup_recovery_events();
        assert_eq!(resent.len(), 1, "{resent:?}");
        match (&resent[0], &original) {
            (
                SagaChoreographyEvent::StepCompleted {
                    context: replay,
                    output: out,
                    saga_input: input,
                    compensation_available: comp,
                },
                SagaChoreographyEvent::StepCompleted {
                    context: first,
                    output,
                    saga_input,
                    compensation_available,
                },
            ) => {
                assert_eq!(
                    (out, input, comp),
                    (output, saga_input, compensation_available)
                );
                assert_eq!(
                    (
                        replay.saga_id,
                        &replay.saga_type,
                        &replay.step_name,
                        replay.saga_started_at_millis,
                        replay.event_timestamp_millis,
                        replay.trace_id,
                        replay.causation_id,
                        replay.correlation_id,
                        replay.step_index,
                        replay.attempt,
                        replay.initiator_peer_id
                    ),
                    (
                        first.saga_id,
                        &first.saga_type,
                        &first.step_name,
                        first.saga_started_at_millis,
                        first.event_timestamp_millis,
                        first.trace_id,
                        first.causation_id,
                        first.correlation_id,
                        first.step_index,
                        first.attempt,
                        first.initiator_peer_id
                    )
                );
            }
            _ => panic!("confirmed recovery must preserve the exact completion"),
        }
        assert_eq!(reopened.pay_calls, 0, "recovery must never re-run the step");
        assert!(matches!(
            reopened.saga.saga_states.get(&run.saga_id),
            Some(icanact_saga_choreography::SagaStateEntry::Completed(_))
        ));
        assert!(reopened.saga.dependency_fired.contains(&run.saga_id));

        // A re-delivered start (new trace) still cannot repeat the effect.
        let mut retry = run.clone();
        retry.trace_id = 999;
        let replay = deliver(&mut reopened, start(&retry));
        assert_eq!(reopened.pay_calls, 0, "{replay:?}");
    }

    #[test]
    fn undo_after_lmdb_cold_restart_uses_journaled_compensation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            let mut actor = Actor::open(temp.path());
            deliver(&mut actor, start(&run));
        }
        let mut reopened = Actor::open(temp.path());
        let out = deliver(&mut reopened, undo_request(&run));
        assert_eq!(reopened.comp_calls, 1, "{out:?}");
        assert_eq!(reopened.last_comp_data, b"refund");
        assert!(
            out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
            "{out:?}"
        );
    }

    #[test]
    fn open_intent_and_unconfirmed_result_quarantine_visibly_after_lmdb_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            // Crash between durable intent and result: nothing else was written.
            let actor = Actor::open(temp.path());
            actor.admit_participant_event_strict(&run).expect("admit");
            actor
                .record_event_strict(
                    run.saga_id,
                    ParticipantEvent::StepExecutionStarted {
                        attempt: 1,
                        started_at_millis: SagaContext::now_millis(),
                    },
                )
                .expect("intent");
        }
        let mut reopened = Actor::open(temp.path());
        let events = reopened.saga.take_startup_recovery_events();
        assert!(
            matches!(events.as_slice(), [SagaChoreographyEvent::SagaQuarantined { context, .. }]
                if context.saga_started_at_millis == 100),
            "{events:?}"
        );
        assert_eq!(reopened.pay_calls, 0);
        // Drop this owner to exercise a cold reopen; overlapping owners keep the env open.
        drop(reopened);

        // Partial failure: the result landed but the confirming proof did not.
        let other = SagaContext {
            saga_id: SagaId::new(62),
            ..ctx(300)
        };
        {
            let actor = Actor::open(temp.path());
            actor.admit_participant_event_strict(&other).expect("admit");
            for event in [
                ParticipantEvent::StepExecutionStarted {
                    attempt: 1,
                    started_at_millis: 1,
                },
                ParticipantEvent::StepExecutionCompleted {
                    output: b"paid".to_vec(),
                    compensation_data: b"refund".to_vec(),
                    completed_at_millis: 2,
                },
            ] {
                actor
                    .record_event_strict(other.saga_id, event)
                    .expect("append");
            }
        }
        let mut again = Actor::open(temp.path());
        let events = again.saga.take_startup_recovery_events();
        assert!(
            events.iter().any(
                |e| matches!(e, SagaChoreographyEvent::SagaQuarantined { context, .. }
                if context.saga_id == SagaId::new(62) && context.saga_started_at_millis == 300)
            ),
            "{events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { saga_input, .. } if saga_input == b"input")),
            "an unconfirmed result must never be resent as success: {events:?}"
        );
    }

    #[test]
    fn idle_participant_is_not_quarantined_by_lmdb_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            let actor = Actor::open(temp.path());
            // Only the run record: this participant is still waiting on dependencies.
            actor.admit_participant_event_strict(&run).expect("admit");
        }
        let mut reopened = Actor::open(temp.path());
        let events = reopened.saga.take_startup_recovery_events();
        assert!(events.is_empty(), "{events:?}");
    }

    #[test]
    fn completed_run_does_not_repeat_effect_after_lmdb_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            let mut actor = Actor::open(temp.path());
            deliver(&mut actor, start(&run));
            deliver(
                &mut actor,
                SagaChoreographyEvent::SagaCompleted {
                    context: run.clone(),
                },
            );
            assert_eq!(actor.pay_calls, 1);
        }

        let mut reopened = Actor::open(temp.path());
        let replay = deliver(&mut reopened, start(&run));
        assert_eq!(reopened.pay_calls, 0, "terminal run repeated its effect");
        assert!(replay.is_empty(), "{replay:?}");

        let later = ctx(200);
        deliver(&mut reopened, start(&later));
        assert_eq!(
            reopened.pay_calls, 1,
            "a later valid run must still execute"
        );
    }

    #[test]
    fn quarantined_run_keeps_evidence_and_blocks_reuse_after_lmdb_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            let mut actor = Actor::open(temp.path());
            deliver(&mut actor, start(&run));
            deliver(
                &mut actor,
                SagaChoreographyEvent::SagaQuarantined {
                    context: run.clone(),
                    reason: "operator review".into(),
                    step: "pay".into(),
                    participant_id: "pay".into(),
                },
            );
        }

        let mut reopened = Actor::open(temp.path());
        assert!(
            reopened.saga.take_startup_recovery_events().is_empty(),
            "restart must not auto-resolve quarantined work"
        );
        let entries = reopened.saga.store.read(run.saga_id).expect("read");
        // A plain completion's single proof append retains the compensation bytes.
        assert!(entries.iter().any(|e| matches!(
            &e.event,
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome }
                if outcome.compensation_data == b"refund"
        )));
        assert!(entries.iter().any(|e| matches!(
            &e.event,
            ParticipantEvent::ParticipantTerminalRecorded {
                outcome: ParticipantTerminalKind::Quarantined,
                ..
            }
        )));
        let reuse = deliver(&mut reopened, start(&ctx(200)));
        assert_eq!(reopened.pay_calls, 0);
        assert!(
            reuse.is_empty(),
            "refusal must not create a foreign quarantine: {reuse:?}"
        );
        assert!(
            reopened
                .saga
                .store
                .read(run.saga_id)
                .unwrap()
                .iter()
                .any(|entry| matches!(
                    &entry.event,
                    ParticipantEvent::ParticipantTerminalRecorded {
                        outcome: ParticipantTerminalKind::Quarantined,
                        saga_started_at_millis: 100,
                        ..
                    }
                )),
            "retained original quarantine must continue fencing reuse"
        );
    }

    #[test]
    fn declared_effect_uses_three_commits_and_preserves_predispatch_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let run = ctx(100);
        {
            let mut actor = Actor::open(temp.path());
            actor.declared_effect = true;
            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(16)
                    .map_size(1024 * 1024 * 1024)
                    .open(temp.path())
            }
            .unwrap();
            let before = env.info().last_txn_id;
            let emitted = deliver(&mut actor, start(&run));
            assert!(
                emitted
                    .iter()
                    .any(|event| matches!(event, SagaChoreographyEvent::StepCompleted { .. }))
            );
            assert_eq!(env.info().last_txn_id - before, 3);
        }
        let reopened = Actor::open(temp.path());
        let rows = reopened.saga.store.read(run.saga_id).unwrap();
        let raw = rows
            .iter()
            .position(|row| matches!(row.event, ParticipantEvent::StepExecutionCompleted { .. }))
            .unwrap();
        let proof = rows.iter().position(|row| matches!(&row.event,
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome }
                if outcome.effect.as_deref() == Some("payment-outbox") && outcome.receipt.as_deref() == Some("outbox-1"))).unwrap();
        assert!(raw < proof);
    }

    #[test]
    fn plain_completion_is_one_proof_append_that_survives_lmdb_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            let mut actor = Actor::open(temp.path());
            let env = unsafe {
                EnvOpenOptions::new()
                    .max_dbs(16)
                    .map_size(1024 * 1024 * 1024)
                    .open(temp.path())
            }
            .unwrap();
            let before = env.info().last_txn_id;
            deliver(&mut actor, start(&run));
            assert_eq!(
                env.info().last_txn_id - before,
                2,
                "atomic admission plus proof, not an extra raw completion commit"
            );
        }
        let reopened = Actor::open(temp.path());
        let rows = reopened.saga.store.read(run.saga_id).expect("read");
        assert!(
            !rows
                .iter()
                .any(|e| matches!(e.event, ParticipantEvent::StepExecutionCompleted { .. })),
            "a plain completion has no raw result row: {rows:?}"
        );
        let proof = reopened
            .participant_run_evidence_strict(&run)
            .expect("evidence")
            .forward_outcome
            .expect("the proof alone confirms the completion");
        assert_eq!(proof.output, b"paid");
        assert_eq!(proof.compensation_data, b"refund");
    }

    #[test]
    fn lost_undo_acknowledgement_is_resent_after_lmdb_reopen_without_a_new_undo() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run = ctx(100);
        {
            let mut actor = Actor::open(temp.path());
            deliver(&mut actor, start(&run));
            deliver(&mut actor, undo_request(&run));
            assert_eq!(actor.comp_calls, 1);
            // The acknowledgement published by that undo is lost with the process.
        }

        let mut reopened = Actor::open(temp.path());
        let resent = reopened.saga.take_startup_recovery_events();
        assert!(
            matches!(resent.as_slice(), [SagaChoreographyEvent::CompensationCompleted { context }]
                if context.saga_id == run.saga_id
                    && context.saga_type.as_ref() == "wf_pay"
                    && context.saga_started_at_millis == 100
                    && context.step_name.as_ref() == "pay"),
            "{resent:?}"
        );
        assert_eq!(reopened.comp_calls, 0, "startup must not undo again");
        let rows = reopened.saga.store.read(run.saga_id).expect("read");
        let count = |kind: &str| {
            rows.iter()
                .filter(|e| format!("{:?}", e.event).starts_with(kind))
                .count()
        };
        assert_eq!(count("CompensationRequestRecorded"), 1);
        assert_eq!(count("CompensationStarted"), 1);
        assert_eq!(count("CompensationCompleted"), 1);

        // Once the resolver's terminal failure lands, nothing is resent again.
        deliver(
            &mut reopened,
            SagaChoreographyEvent::SagaFailed {
                context: run.next_step("terminal_resolver".into()),
                reason: "rolled back".into(),
                failure: None,
            },
        );
        drop(reopened);
        let mut after_terminal = Actor::open(temp.path());
        assert!(
            after_terminal
                .saga
                .take_startup_recovery_events()
                .is_empty(),
            "terminally resolved history must not fabricate a rollback"
        );
    }

    icanact_saga_choreography::define_saga_workflow_contract! {
        struct ReopenContract {
            saga_type: "wf_pay", first_step: pay, failure_authority: any (),
            required_steps: [pay], overall_timeout_ms: 60_000, stalled_timeout_ms: 60_000,
            steps: { pay => { participant: "pay", depends_on: on_start () } }
        }
    }

    #[test]
    fn resolver_and_participant_lmdb_reopen_fence_replay_before_fanout() {
        use icanact_saga_choreography::{
            LmdbTerminalResolverJournal, SagaBusPublishError, SagaChoreographyBus,
            TerminalResolverJournal,
        };
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};
        let temp = tempfile::tempdir().unwrap();
        let participant_path = temp.path().join("participant");
        let resolver_path = participant_path.clone();
        let run = ctx(SagaContext::now_millis());
        {
            let mut actor = Actor::open(&participant_path);
            let journal = Arc::new(LmdbTerminalResolverJournal::open(&resolver_path).unwrap());
            let bus = SagaChoreographyBus::new();
            bus.register_workflow_contract_provider::<ReopenContract>()
                .unwrap();
            let inbox = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&inbox);
            bus.subscribe_workflow_step_fn("wf_pay", "pay", move |event| {
                sink.lock().unwrap().push(event.clone());
                true
            })
            .unwrap();
            bus.attach_durable_terminal_resolver_for_contract::<ReopenContract, _>(
                "qa",
                Arc::clone(&journal),
            )
            .unwrap();
            bus.activate_terminal_resolver_recovery("wf_pay").unwrap();
            bus.publish_strict(start(&run)).unwrap();
            let admitted_start = inbox.lock().unwrap().remove(0);
            for event in deliver(&mut actor, admitted_start) {
                bus.publish_strict(event).unwrap();
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            let terminal = loop {
                if let Some(event) = journal.read_all().unwrap().into_iter().find(|entry| {
                    matches!(entry.event, SagaChoreographyEvent::SagaCompleted { .. })
                }) {
                    break event.event;
                }
                assert!(
                    Instant::now() < deadline,
                    "resolver did not durably record completion"
                );
                std::thread::sleep(Duration::from_millis(10));
            };
            deliver(&mut actor, terminal);
            assert_eq!(actor.pay_calls, 1);
        }
        // Both LMDB environments and their owning actors/bus are really reopened.
        let mut actor = Actor::open(&participant_path);
        let journal = Arc::new(LmdbTerminalResolverJournal::open(&resolver_path).unwrap());
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<ReopenContract>()
            .unwrap();
        let fanout = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&fanout);
        bus.subscribe_workflow_step_fn("wf_pay", "pay", move |event| {
            sink.lock().unwrap().push(event.clone());
            true
        })
        .unwrap();
        bus.attach_durable_terminal_resolver_for_contract::<ReopenContract, _>("qa", journal)
            .unwrap();
        bus.activate_terminal_resolver_recovery("wf_pay").unwrap();
        assert!(matches!(
            bus.publish_strict(start(&run)),
            Err(SagaBusPublishError::AdmissionRejected { .. })
        ));
        assert!(
            !fanout
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, SagaChoreographyEvent::SagaStarted { .. }))
        );
        assert!(
            deliver(&mut actor, start(&run)).is_empty(),
            "participant fence independently survives reopen"
        );
        assert_eq!(actor.pay_calls, 0, "terminal effect must not run again");
    }
}

#[test]
fn lmdb_journal_reads_varied_payload_lengths_and_survives_reopen() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("journal");
    let saga = SagaId::new(70);
    {
        let journal = LmdbParticipantStore::open(&path, "test_participant").expect("open");
        for len in 0..9usize {
            journal
                .append(
                    saga,
                    ParticipantEvent::StepExecutionCompleted {
                        output: vec![7; len],
                        compensation_data: vec![9; len],
                        completed_at_millis: len as u64,
                    },
                )
                .expect("append");
        }
        assert_eq!(journal.read(saga).expect("live read").len(), 9);
    }
    let reopened = LmdbParticipantStore::open(&path, "test_participant").expect("reopen");
    let entries = reopened.read(saga).expect("reopened read");
    assert_eq!(entries.len(), 9);
    assert!(entries.iter().enumerate().all(|(len, e)| matches!(
        &e.event,
        ParticipantEvent::StepExecutionCompleted { output, .. } if output.len() == len
    )));
}

#[test]
fn lmdb_journal_rejects_malformed_archive_rows_instead_of_decoding_them() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("journal");
    let saga = SagaId::new(71);
    {
        let journal = LmdbParticipantStore::open(&path, "test_participant").expect("open");
        journal
            .append(
                saga,
                ParticipantEvent::StepExecutionStarted {
                    attempt: 1,
                    started_at_millis: 1,
                },
            )
            .expect("append");
    }
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(16)
                .map_size(1024 * 1024 * 1024)
                .open(&path)
        }
        .expect("raw env");
        let mut wtxn = env.write_txn().expect("write txn");
        let rows = env
            .create_database::<Str, Bytes>(&mut wtxn, Some("participant_events"))
            .expect("rows db");
        let key = rows
            .iter(&wtxn)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0
            .to_string();
        rows.put(&mut wtxn, &key, &[0xAB; 37]).expect("corrupt row");
        wtxn.commit().expect("commit");
    }
    let reopened = LmdbParticipantStore::open(&path, "test_participant").expect("reopen");
    let err = reopened
        .read(saga)
        .expect_err("a malformed archive must fail validation");
    assert!(!err.to_string().is_empty());
}

// ---- AllOf liveness across a real LMDB close/reopen -------------------------------

mod allof_reopen {
    use std::cell::RefCell;
    use std::path::Path;

    use super::*;
    use icanact_saga_choreography::durability::{
        apply_sync_workflow_participant_saga_ingress_with_hooks,
        recover_accepted_workflow_steps_for_saga_type,
    };
    use icanact_saga_choreography::{
        CompensationError, CompensationOutput, DependencySpec, HasSagaWorkflowParticipants, PeerId,
        SagaWorkflowParticipant, StepError, StepOutput,
    };

    struct Actor {
        saga: SagaParticipantSupport<LmdbParticipantStore>,
        joins: usize,
    }

    impl Actor {
        fn open(base: &Path) -> Self {
            Self {
                saga: open_lmdb_participant_support_for_saga_type(base, "join", "wf_join")
                    .expect("support should open"),
                joins: 0,
            }
        }
    }

    impl HasSagaParticipantSupport for Actor {
        type Store = LmdbParticipantStore;
        fn saga_support(&self) -> &SagaParticipantSupport<LmdbParticipantStore> {
            &self.saga
        }
        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<LmdbParticipantStore> {
            &mut self.saga
        }
    }

    struct Join;
    static JOIN: Join = Join;
    static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 1] = [&JOIN];

    impl HasSagaWorkflowParticipants for Actor {
        fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
            &WORKFLOWS
        }
    }

    impl SagaWorkflowParticipant<Actor> for Join {
        fn step_name(&self) -> &'static str {
            "join"
        }
        fn saga_types(&self) -> &[&'static str] {
            &["wf_join"]
        }
        fn depends_on(&self) -> DependencySpec {
            DependencySpec::AllOf(&["a", "b"])
        }
        fn execute_step(
            &self,
            actor: &mut Actor,
            _context: &SagaContext,
            input: &[u8],
        ) -> Result<StepOutput, StepError> {
            assert_eq!(
                input, b"input",
                "AllOf must receive immutable SagaStarted payload, not predecessor output"
            );
            actor.joins += 1;
            Ok(StepOutput::Completed {
                output: b"joined".to_vec(),
                compensation_data: Vec::new(),
            })
        }
        fn compensate_step(
            &self,
            _actor: &mut Actor,
            _context: &SagaContext,
            _data: &[u8],
        ) -> Result<CompensationOutput, CompensationError> {
            Ok(CompensationOutput::Completed)
        }
    }

    fn ctx(started_at: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(81),
            saga_type: "wf_join".into(),
            step_name: "join".into(),
            correlation_id: 81,
            causation_id: 81,
            trace_id: 81,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: PeerId::default(),
            saga_started_at_millis: started_at,
            event_timestamp_millis: started_at,
        }
    }

    fn deliver(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
        let out = RefCell::new(Vec::new());
        apply_sync_workflow_participant_saga_ingress_with_hooks(
            actor,
            event,
            |_actor, _event| {},
            |event| panic!("valid transition rejected: {event:?}"),
            |_actor, event| out.borrow_mut().push(event.clone()),
        );
        out.into_inner()
    }

    fn dependency(run: &SagaContext, step: &str) -> SagaChoreographyEvent {
        let mut context = run.clone();
        context.step_name = step.into();
        SagaChoreographyEvent::StepCompleted {
            context,
            output: b"in".to_vec(),
            saga_input: b"input".to_vec(),
            compensation_available: false,
        }
    }

    #[test]
    fn allof_input_seen_before_an_lmdb_reopen_still_fires_the_join() {
        for (first, second) in [("a", "b"), ("b", "a")] {
            let temp = tempfile::tempdir().expect("tempdir");
            let run = ctx(100);
            {
                let mut actor = Actor::open(temp.path());
                deliver(
                    &mut actor,
                    SagaChoreographyEvent::SagaStarted {
                        context: run.clone(),
                        payload: b"input".to_vec(),
                    },
                );
                deliver(&mut actor, dependency(&run, first));
                assert_eq!(actor.joins, 0);
            }

            // Real close/reopen between the two inputs; the replayed first input is a
            // persistent-dedupe duplicate, so only the durable observation can carry it.
            let mut reopened = Actor::open(temp.path());
            recover_accepted_workflow_steps_for_saga_type(&mut reopened.saga, "join", "wf_join")
                .expect("recovery hydrates");
            assert!(reopened.saga.take_startup_recovery_events().is_empty());
            deliver(&mut reopened, dependency(&run, first));
            assert_eq!(reopened.joins, 0, "a duplicate cannot fire the join alone");
            let out = deliver(&mut reopened, dependency(&run, second));
            assert_eq!(reopened.joins, 1, "{out:?}");
            assert!(
            out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { saga_input, .. } if saga_input == b"input"))
        );

            // A second reopen after the join confirmed never re-executes it.
            drop(reopened);
            let mut again = Actor::open(temp.path());
            deliver(&mut again, dependency(&run, second));
            assert_eq!(again.joins, 0);
        }
    }
}

fn open_lmdb_participant_support(
    base: &std::path::Path,
    key: &str,
) -> Result<SagaParticipantSupport<LmdbParticipantStore>, String> {
    open_lmdb_participant_support_for_saga_type(
        base,
        key,
        icanact_saga_choreography::durability::DEFAULT_RECOVERY_SAGA_TYPE,
    )
}

#[cfg(feature = "lmdb")]
fn open_lmdb_participant_support_for_saga_types(
    base: &std::path::Path,
    participant_key: &str,
    saga_types: &[&str],
) -> Result<SagaParticipantSupport<icanact_saga_choreography::LmdbParticipantStore>, String> {
    let store = icanact_saga_choreography::LmdbParticipantStore::open(base, participant_key)
        .map_err(|err| err.to_string())?;
    let mut support = SagaParticipantSupport::new(store);
    let registrations: Vec<_> = saga_types
        .iter()
        .map(|kind| (participant_key, *kind))
        .collect();
    support.startup_recovery_events =
        icanact_saga_choreography::durability::collect_startup_recovery_events_for_participants(
            &mut support,
            &registrations,
        )
        .map_err(|err| format!("{err:?}"))?;
    Ok(support)
}

#[cfg(feature = "lmdb")]
fn open_lmdb_participant_support_for_saga_type(
    base: &std::path::Path,
    participant_key: &str,
    saga_type: &str,
) -> Result<SagaParticipantSupport<icanact_saga_choreography::LmdbParticipantStore>, String> {
    open_lmdb_participant_support_for_saga_types(base, participant_key, &[saga_type])
}

#[test]
fn v3_shared_base_isolates_participants_and_resolver_through_maintenance_and_reopen() {
    use icanact_saga_choreography::{
        DeterministicContextBuilder, LmdbTerminalResolverJournal, ParticipantTerminalKind,
        StoreAdmission, TerminalResolverJournal,
    };
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path();
    let run = DeterministicContextBuilder::default()
        .with_saga_id(801)
        .build();
    let active = DeterministicContextBuilder::default()
        .with_saga_id(802)
        .build();
    let quarantined = DeterministicContextBuilder::default()
        .with_saga_id(803)
        .build();
    {
        let a = LmdbParticipantStore::open(base, "a").unwrap();
        let b = LmdbParticipantStore::open(&base.join("."), "b").unwrap();
        let resolver = LmdbTerminalResolverJournal::open(base).unwrap();
        for store in [&a, &b] {
            assert_eq!(
                store
                    .admit_and_record_intent(&run, "start", "saga_started")
                    .unwrap(),
                StoreAdmission::Admitted
            );
        }
        for context in [&run, &active, &quarantined] {
            resolver
                .append(SagaChoreographyEvent::SagaStarted {
                    context: context.clone(),
                    payload: b"original".to_vec(),
                })
                .unwrap();
        }
        for context in [&active, &quarantined] {
            a.admit_and_record_intent(context, "start", "saga_started")
                .unwrap();
        }
        for (context, outcome) in [
            (&run, ParticipantTerminalKind::Completed),
            (&quarantined, ParticipantTerminalKind::Quarantined),
        ] {
            a.append(
                context.saga_id,
                ParticipantEvent::ParticipantTerminalRecorded {
                    saga_type: context.saga_type.clone(),
                    saga_started_at_millis: context.saga_started_at_millis,
                    outcome,
                    reason: "terminal".into(),
                    recorded_at_millis: 100,
                },
            )
            .unwrap();
        }
        let mut terminal = run.clone();
        terminal.event_timestamp_millis = 100;
        resolver
            .append(SagaChoreographyEvent::SagaCompleted { context: terminal })
            .unwrap();
        resolver
            .append(SagaChoreographyEvent::SagaQuarantined {
                context: quarantined.clone(),
                reason: "uncertain".into(),
                step: "step".into(),
                participant_id: "a".into(),
            })
            .unwrap();
        assert_eq!(a.compact_terminal_detail(1).unwrap(), 1);
        assert_eq!(a.read(run.saga_id).unwrap().len(), 1);
        assert_eq!(
            b.read(run.saga_id).unwrap().len(),
            3,
            "other participant's active run survives"
        );
        assert_eq!(resolver.compact_terminal_detail().unwrap(), 1);
        assert_eq!(resolver.read_saga(run.saga_id).unwrap().len(), 1);
        assert_eq!(
            a.prune_resolved_before(100, 1).unwrap(),
            0,
            "cutoff is exclusive"
        );
        assert_eq!(resolver.prune_resolved_before(100, 1).unwrap(), 0);
        assert_eq!(a.prune_resolved_before(101, 1).unwrap(), 1);
        assert_eq!(resolver.prune_resolved_before(101, 1).unwrap(), 1);
        assert!(!a.read(active.saga_id).unwrap().is_empty());
        assert!(!a.read(quarantined.saga_id).unwrap().is_empty());
        assert_eq!(resolver.read_saga(active.saga_id).unwrap().len(), 1);
        assert_eq!(resolver.read_saga(quarantined.saga_id).unwrap().len(), 2);
        drop(a);
        // Dropping one participant must not close the common environment.
        assert_eq!(
            b.admit_and_record_intent(&run, "start", "saga_started")
                .unwrap(),
            StoreAdmission::Duplicate
        );
        resolver.read_all().unwrap();
    }
    assert!(base.join("data.mdb").is_file());
    assert!(!base.join("journal").exists());
    assert!(!base.join("dedupe").exists());
    assert!(!base.join("resolver").exists());
    let a = LmdbParticipantStore::open(base, "a").unwrap();
    let b = LmdbParticipantStore::open(base, "b").unwrap();
    let resolver = LmdbTerminalResolverJournal::open(base).unwrap();
    assert!(a.read(run.saga_id).unwrap().is_empty());
    assert_eq!(b.read(run.saga_id).unwrap().len(), 3);
    assert!(resolver.read_saga(run.saga_id).unwrap().is_empty());
    let before = a.read(active.saga_id).unwrap().last().unwrap().sequence;
    let after = a
        .append(
            active.saga_id,
            ParticipantEvent::StepExecutionStarted {
                attempt: 2,
                started_at_millis: 200,
            },
        )
        .unwrap();
    assert!(
        after > before,
        "maintenance never resets sequence allocation"
    );
}

#[test]
fn accepted_step_and_compensation_renewals_survive_v3_reopen_without_payload_duplication() {
    use icanact_saga_choreography::durability::{
        collect_startup_recovery_events_for_participants,
        record_accepted_workflow_compensation_progress, record_accepted_workflow_step_progress,
        recover_accepted_workflow_steps_for_saga_type,
    };
    use icanact_saga_choreography::{
        AcceptedStepPolicy, AcceptedStepTimeoutOutcome, DeterministicContextBuilder,
        StepExecutionId, accept_workflow_step,
    };
    let temp = tempfile::tempdir().unwrap();
    let now = SagaContext::now_millis();
    let step = DeterministicContextBuilder::default()
        .with_saga_id(901)
        .with_saga_type("renewals")
        .with_step_name("work")
        .build();
    let undo = DeterministicContextBuilder::default()
        .with_saga_id(902)
        .with_saga_type("renewals")
        .with_step_name("work")
        .build();
    let forward_id = StepExecutionId::new("forward");
    let undo_id = StepExecutionId::new("undo");
    let payload = vec![7; 4096];
    let compensation = vec![9; 4096];
    let forward_deadline;
    let undo_deadline = now + 90_000;
    {
        let mut actor = LmdbParticipant {
            saga: SagaParticipantSupport::new(
                LmdbParticipantStore::open(temp.path(), "work").unwrap(),
            ),
        };
        accept_workflow_step(
            &mut actor,
            step.clone(),
            "worker".into(),
            forward_id.clone(),
            AcceptedStepPolicy {
                idle_timeout: std::time::Duration::from_secs(60),
                hard_timeout: std::time::Duration::from_secs(180),
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            },
            payload.clone(),
            compensation.clone(),
        )
        .unwrap();
        let old = actor.saga.accepted_workflow_steps[&step.saga_id].deadline_at_millis;
        forward_deadline = old + 30_000;
        let heartbeat = record_accepted_workflow_step_progress(
            &mut actor,
            step.saga_id,
            forward_id.clone(),
            old - 30_000,
        )
        .unwrap();
        assert!(
            matches!(heartbeat, SagaChoreographyEvent::StepAccepted { deadline_at_millis, .. } if deadline_at_millis == forward_deadline)
        );
        actor
            .saga
            .store
            .append_batch(
                undo.saga_id,
                vec![
                    ParticipantEvent::ParticipantRunRecorded {
                        saga_type: undo.saga_type.clone(),
                        saga_started_at_millis: undo.saga_started_at_millis,
                        recorded_at_millis: now,
                    },
                    ParticipantEvent::CompensationRequestRecorded {
                        context: undo.clone(),
                        failed_step: "work".into(),
                        reason: "rollback".into(),
                        failure: icanact_saga_choreography::SagaFailureDetails {
                            step_name: "work".into(),
                            participant_id: "worker".into(),
                            error_code: None,
                            error_message: "rollback".into(),
                            at_millis: now,
                        },
                        steps_to_compensate: vec!["work".into()],
                        requested_at_millis: now,
                    },
                    ParticipantEvent::AcceptedCompensationRecorded {
                        context: undo.clone(),
                        participant_id: "worker".into(),
                        execution_id: undo_id.clone(),
                        saga_input: payload.clone(),
                        compensation_data: compensation.clone(),
                        idle_timeout_millis: 60_000,
                        hard_timeout_millis: 180_000,
                        accepted_at_millis: now,
                        deadline_at_millis: now + 60_000,
                        hard_deadline_at_millis: now + 180_000,
                    },
                ],
            )
            .unwrap();
        recover_accepted_workflow_steps_for_saga_type(&mut actor.saga, "work", "renewals").unwrap();
        let heartbeat = record_accepted_workflow_compensation_progress(
            &mut actor,
            undo.saga_id,
            undo_id.clone(),
            now + 30_000,
        )
        .unwrap();
        assert!(
            matches!(heartbeat, SagaChoreographyEvent::CompensationAccepted { deadline_at_millis, .. } if deadline_at_millis == undo_deadline)
        );
        for (context, execution, is_compensation) in
            [(&step, &forward_id, false), (&undo, &undo_id, true)]
        {
            let rows = actor.saga.store.read(context.saga_id).unwrap();
            assert_eq!(
                rows.iter()
                    .filter(|row| matches!(
                        row.event,
                        ParticipantEvent::AcceptedStepRecorded { .. }
                            | ParticipantEvent::AcceptedCompensationRecorded { .. }
                    ))
                    .count(),
                1
            );
            assert_eq!(rows.iter().filter(|row| matches!(&row.event, ParticipantEvent::AcceptedDeadlineRenewed { context: recorded, execution_id, is_compensation: phase, .. }
                if recorded.saga_started_at_millis == context.saga_started_at_millis && execution_id == execution && *phase == is_compensation)).count(), 1);
        }
    }
    let mut support =
        SagaParticipantSupport::new(LmdbParticipantStore::open(temp.path(), "work").unwrap());
    let replay =
        collect_startup_recovery_events_for_participants(&mut support, &[("work", "renewals")])
            .unwrap();
    assert!(replay.iter().any(|event| matches!(event, SagaChoreographyEvent::StepAccepted { execution_id, deadline_at_millis, .. }
        if execution_id == &forward_id && *deadline_at_millis == forward_deadline)));
    assert!(replay.iter().any(|event| matches!(event, SagaChoreographyEvent::CompensationAccepted { execution_id, deadline_at_millis, .. }
        if execution_id == &undo_id && *deadline_at_millis == undo_deadline)));
    assert!(
        !replay
            .iter()
            .any(|event| matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }))
    );
    let forward = &support.accepted_workflow_steps[&step.saga_id];
    assert_eq!(forward.deadline_at_millis, forward_deadline);
    assert_eq!(forward.saga_input, payload);
    assert_eq!(forward.compensation_data, compensation);
    let recovered_undo = &support.accepted_workflow_compensations[&undo.saga_id];
    assert_eq!(recovered_undo.deadline_at_millis, undo_deadline);
    assert_eq!(recovered_undo.hard_deadline_at_millis, now + 180_000);
    assert_eq!(recovered_undo.saga_input, payload);
    assert_eq!(recovered_undo.compensation_data, compensation);
}
