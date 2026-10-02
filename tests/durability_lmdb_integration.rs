use std::path::PathBuf;

use heed::EnvOpenOptions;
use heed::types::{Bytes, Str};
use icanact_saga_choreography::durability::lmdb::{
    LmdbDedupe, LmdbJournal, open_lmdb_participant_support,
    open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::durability::{ActiveSagaExecutionPhase, panic_quarantine_reason};
use icanact_saga_choreography::{
    HasSagaParticipantSupport, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport, SagaStateExt,
};

#[test]
fn lmdb_journal_and_dedupe_roundtrip() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal_path = temp.path().join("journal");
    let dedupe_path = temp.path().join("dedupe");

    let journal = LmdbJournal::open(&journal_path).expect("journal should open");
    let dedupe = LmdbDedupe::open(&dedupe_path).expect("dedupe should open");

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
        dedupe
            .contains(saga_a, "probe")
            .expect("contains should succeed")
    );

    dedupe
        .mark_processed(saga_b, "manual")
        .expect("mark_processed should succeed");
    assert!(
        dedupe
            .contains(saga_b, "manual")
            .expect("contains should succeed")
    );

    dedupe.prune(saga_a).expect("prune should succeed");
    assert!(
        !dedupe
            .contains(saga_a, "probe")
            .expect("contains should succeed")
    );
}

struct LmdbParticipant {
    saga: SagaParticipantSupport<LmdbJournal, LmdbDedupe>,
}

impl HasSagaParticipantSupport for LmdbParticipant {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

#[test]
fn prune_saga_removes_lmdb_journal_and_dedupe_state() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal should open");
    let dedupe = LmdbDedupe::open(&temp.path().join("dedupe")).expect("dedupe should open");
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
        saga: SagaParticipantSupport::new(journal, dedupe),
    };
    actor
        .prune_saga_strict(saga_a)
        .expect("terminal saga prune should succeed");

    assert!(
        actor
            .saga
            .journal
            .read(saga_a)
            .expect("read pruned saga should succeed")
            .is_empty(),
        "terminal cleanup must remove durable journal rows"
    );
    assert_eq!(
        actor
            .saga
            .journal
            .list_sagas()
            .expect("list after prune should succeed"),
        vec![saga_b],
        "terminal cleanup must remove only the pruned saga index"
    );
    assert!(
        !actor
            .saga
            .dedupe
            .contains(saga_a, "started")
            .expect("contains should succeed"),
        "terminal cleanup must still prune dedupe rows"
    );
}

#[test]
fn open_support_replays_panic_quarantine_once() {
    let temp = tempfile::tempdir().expect("tempdir should open");
    let base: PathBuf = temp.path().join("support");

    let journal = LmdbJournal::open(&base.join("journal")).expect("journal should open");
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

    let journal_err = LmdbJournal::open(&journal_file).expect_err("file path must fail");
    assert!(
        !journal_err.to_string().is_empty(),
        "journal open error should include a storage message"
    );

    let dedupe_err = LmdbDedupe::open(&dedupe_file).expect_err("file path must fail");
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
            .create_database::<Str, Bytes>(&mut wtxn, Some("journal_rows"))
            .expect("legacy rows database should open");
        rows.put(
            &mut wtxn,
            "00000000000000000001:00000000000000000001",
            b"legacy",
        )
        .expect("legacy row should be persisted");
        wtxn.commit().expect("legacy row should commit");
    }

    let err = LmdbJournal::open(&journal_path)
        .expect_err("unversioned persisted rows must fail closed before decoding");
    assert!(
        err.to_string()
            .contains("unversioned saga journal contains rows incompatible"),
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
        saga: SagaParticipantSupport<LmdbJournal, LmdbDedupe>,
        pay_calls: usize,
        comp_calls: usize,
        last_comp_data: Vec<u8>,
    }

    impl Actor {
        fn open(base: &Path) -> Self {
            Self {
                saga: open_lmdb_participant_support_for_saga_type(base, "pay", "wf_pay")
                    .expect("support should open"),
                pay_calls: 0,
                comp_calls: 0,
                last_comp_data: Vec::new(),
            }
        }
    }

    impl HasSagaParticipantSupport for Actor {
        type Journal = LmdbJournal;
        type Dedupe = LmdbDedupe;
        fn saga_support(&self) -> &SagaParticipantSupport<LmdbJournal, LmdbDedupe> {
            &self.saga
        }
        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<LmdbJournal, LmdbDedupe> {
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
            Ok(StepOutput::Completed {
                output: b"paid".to_vec(),
                compensation_data: b"refund".to_vec(),
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
                .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. })),
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
        let entries = reopened.saga.journal.read(run.saga_id).expect("read");
        assert!(entries.iter().any(|e| matches!(
            &e.event,
            ParticipantEvent::StepExecutionCompleted { compensation_data, .. }
                if compensation_data == b"refund"
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
                .journal
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
        let resolver_path = temp.path().join("resolver");
        let run = ctx(SagaContext::now_millis());
        {
            let mut actor = Actor::open(&participant_path);
            let journal = Arc::new(LmdbTerminalResolverJournal::open(&resolver_path).unwrap());
            let bus = SagaChoreographyBus::new();
            bus.register_workflow_contract_provider::<ReopenContract>()
                .unwrap();
            bus.register_bound_workflow_step("wf_pay", "pay").unwrap();
            bus.subscribe_saga_type_fn("wf_pay", |_| true);
            bus.attach_durable_terminal_resolver_for_contract::<ReopenContract, _>(
                "qa",
                Arc::clone(&journal),
            )
            .unwrap();
            bus.activate_terminal_resolver_recovery("wf_pay").unwrap();
            bus.publish_strict(start(&run)).unwrap();
            for event in deliver(&mut actor, start(&run)) {
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
        bus.register_bound_workflow_step("wf_pay", "pay").unwrap();
        let fanout = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&fanout);
        bus.subscribe_saga_type_fn("wf_pay", move |event| {
            sink.lock().unwrap().push(event.clone());
            true
        });
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
        let journal = LmdbJournal::open(&path).expect("open");
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
    let reopened = LmdbJournal::open(&path).expect("reopen");
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
        let journal = LmdbJournal::open(&path).expect("open");
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
            .create_database::<Str, Bytes>(&mut wtxn, Some("journal_rows"))
            .expect("rows db");
        rows.put(
            &mut wtxn,
            &format!("{:020}:{:020}", saga.get(), 99u64),
            &[0xAB; 37],
        )
        .expect("corrupt row");
        wtxn.commit().expect("commit");
    }
    let reopened = LmdbJournal::open(&path).expect("reopen");
    let err = reopened
        .read(saga)
        .expect_err("a malformed archive must fail validation");
    assert!(!err.to_string().is_empty());
}
