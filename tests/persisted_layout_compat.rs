//! Pins the archived layout of persisted rows across the W1 skeleton (ADR-0001/0003/0004/0005).
use icanact_saga_choreography::{
    AcceptedStepTimeoutOutcome, JournalEntry, ParticipantEvent, SagaChoreographyEvent, SagaContext,
    SagaFailureDetails, SagaId, StepExecutionId, TerminalResolverJournalEntry,
};

const JOURNAL_ENTRY_ARCHIVED_SIZE: usize = 208;
const RESOLVER_ENTRY_ARCHIVED_SIZE: usize = 192;
const JOURNAL_ENTRY_PRE_TSK: &[u8] = &[
    108, 97, 121, 111, 117, 116, 95, 112, 105, 110, 114, 101, 115, 101, 114, 118, 101, 114, 101,
    115, 101, 114, 118, 101, 45, 97, 99, 116, 111, 114, 101, 120, 101, 99, 45, 49, 1, 2, 3, 4, 5,
    0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 88, 106, 229, 207, 139, 1, 0, 0, 10, 0, 0, 0, 0,
    0, 0, 0, 146, 16, 0, 0, 0, 0, 0, 0, 176, 255, 255, 255, 10, 0, 0, 0, 178, 255, 255, 255, 7, 0,
    0, 0, 146, 16, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1,
    0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    3, 3, 3, 0, 104, 229, 207, 139, 1, 0, 0, 244, 105, 229, 207, 139, 1, 0, 0, 97, 255, 255, 255,
    13, 0, 0, 0, 102, 255, 255, 255, 6, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0, 136, 19, 0, 0, 0, 0, 0,
    0, 0, 1, 0, 0, 80, 255, 255, 255, 3, 0, 0, 0, 75, 255, 255, 255, 2, 0, 0, 0, 0, 0, 0, 0, 38,
    106, 229, 207, 139, 1, 0, 0, 14, 110, 229, 207, 139, 1, 0, 0, 174, 125, 229, 207, 139, 1, 0, 0,
];
const RESOLVER_ENTRY_PRE_TSK: &[u8] = &[
    108, 97, 121, 111, 117, 116, 95, 112, 105, 110, 114, 101, 115, 101, 114, 118, 101, 99, 104, 97,
    114, 103, 101, 99, 97, 114, 100, 32, 100, 101, 99, 108, 105, 110, 101, 100, 99, 104, 97, 114,
    103, 101, 99, 104, 97, 114, 103, 101, 45, 97, 99, 116, 111, 114, 69, 52, 50, 99, 97, 114, 100,
    32, 100, 101, 99, 108, 105, 110, 101, 100, 114, 101, 115, 101, 114, 118, 101, 0, 0, 0, 246,
    255, 255, 255, 7, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 146, 16, 0, 0, 0,
    0, 0, 0, 144, 255, 255, 255, 10, 0, 0, 0, 146, 255, 255, 255, 7, 0, 0, 0, 146, 16, 0, 0, 0, 0,
    0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 3, 3, 3, 3, 3, 3,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 104, 229, 207,
    139, 1, 0, 0, 244, 105, 229, 207, 139, 1, 0, 0, 65, 255, 255, 255, 6, 0, 0, 0, 63, 255, 255,
    255, 13, 0, 0, 0, 68, 255, 255, 255, 6, 0, 0, 0, 66, 255, 255, 255, 12, 0, 0, 0, 1, 0, 0, 0,
    66, 255, 255, 255, 3, 0, 0, 0, 61, 255, 255, 255, 13, 0, 0, 0, 0, 0, 0, 0, 188, 106, 229, 207,
    139, 1, 0, 0, 64, 255, 255, 255, 1, 0, 0, 0,
];

fn golden_context() -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(4242),
        saga_type: "layout_pin".into(),
        step_name: "reserve".into(),
        correlation_id: 4242,
        causation_id: 7,
        trace_id: 9,
        step_index: 2,
        attempt: 1,
        initiator_peer_id: [3; 32],
        saga_started_at_millis: 1_700_000_000_000,
        event_timestamp_millis: 1_700_000_000_500,
    }
}

fn golden_journal_entry() -> JournalEntry {
    JournalEntry {
        sequence: 11,
        recorded_at_millis: 1_700_000_000_600,
        event: ParticipantEvent::AcceptedStepRecorded {
            context: golden_context(),
            participant_id: "reserve-actor".into(),
            execution_id: StepExecutionId::new("exec-1"),
            idle_timeout_millis: 1_000,
            hard_timeout_millis: 5_000,
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            },
            saga_input: vec![1, 2, 3],
            compensation_data: vec![4, 5],
            accepted_at_millis: 1_700_000_000_550,
            deadline_at_millis: 1_700_000_001_550,
            hard_deadline_at_millis: 1_700_000_005_550,
        },
    }
}

fn golden_resolver_entry() -> TerminalResolverJournalEntry {
    TerminalResolverJournalEntry {
        sequence: 12,
        event: SagaChoreographyEvent::CompensationRequested {
            context: golden_context(),
            failed_step: "charge".into(),
            reason: "card declined".into(),
            failure: SagaFailureDetails {
                step_name: "charge".into(),
                participant_id: "charge-actor".into(),
                error_code: Some("E42".into()),
                error_message: "card declined".into(),
                at_millis: 1_700_000_000_700,
            },
            steps_to_compensate: vec!["reserve".into()],
        },
    }
}

fn aligned(bytes: &[u8]) -> rkyv::util::AlignedVec<16> {
    let mut buf = rkyv::util::AlignedVec::<16>::new();
    buf.extend_from_slice(bytes);
    buf
}

#[test]
fn archived_layout_sizes_are_unchanged() {
    assert_eq!(
        std::mem::size_of::<rkyv::Archived<JournalEntry>>(),
        JOURNAL_ENTRY_ARCHIVED_SIZE
    );
    assert_eq!(
        std::mem::size_of::<rkyv::Archived<TerminalResolverJournalEntry>>(),
        RESOLVER_ENTRY_ARCHIVED_SIZE
    );
}

#[test]
fn pre_skeleton_rows_still_decode() {
    let resolver = rkyv::from_bytes::<TerminalResolverJournalEntry, rkyv::rancor::Error>(&aligned(
        RESOLVER_ENTRY_PRE_TSK,
    ))
    .expect("pre-skeleton resolver row decodes");
    assert_eq!(resolver.sequence, 12);
    assert_eq!(resolver.event, golden_resolver_entry().event);

    let decoded =
        rkyv::from_bytes::<JournalEntry, rkyv::rancor::Error>(&aligned(JOURNAL_ENTRY_PRE_TSK))
            .expect("pre-skeleton journal row decodes");
    let golden = golden_journal_entry();
    assert_eq!(decoded.sequence, golden.sequence);
    assert_eq!(decoded.recorded_at_millis, golden.recorded_at_millis);
    let ParticipantEvent::AcceptedStepRecorded {
        context,
        execution_id,
        compensation_data,
        hard_deadline_at_millis,
        ..
    } = decoded.event
    else {
        panic!("decoded row is not AcceptedStepRecorded");
    };
    assert_eq!(context, golden_context());
    assert_eq!(execution_id, StepExecutionId::new("exec-1"));
    assert_eq!(compensation_data, vec![4, 5]);
    assert_eq!(hard_deadline_at_millis, 1_700_000_005_550);
}

#[test]
fn transition_committed_row_roundtrips() {
    let entry = JournalEntry {
        sequence: 13,
        recorded_at_millis: 1_700_000_000_800,
        event: ParticipantEvent::TransitionCommitted {
            transition: Box::new(golden_journal_entry().event),
            outbox: vec![SagaChoreographyEvent::SagaCompleted {
                context: golden_context(),
            }],
        },
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&entry).expect("encode");
    let decoded = rkyv::from_bytes::<JournalEntry, rkyv::rancor::Error>(&aligned(bytes.as_slice()))
        .expect("decode");
    assert_eq!(decoded.sequence, 13);
    assert!(matches!(
        decoded.event.transition(),
        ParticipantEvent::AcceptedStepRecorded { .. }
    ));
    assert_eq!(decoded.event.outbox().len(), 1);
}
