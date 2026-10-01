//! Canonical participant fixture: drives events through the documented
//! ingress (`apply_sync_participant_saga_ingress_with_hooks`) and RETURNS
//! invalid-emission and publish failures instead of discarding them.

use std::collections::HashSet;
use std::time::Duration;

use super::FaultJournal;

use icanact_saga_choreography::durability::{
    ActiveSagaExecution, HasActiveSagaExecution, apply_sync_participant_saga_ingress_with_hooks,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, DependencySpec, FailureAuthority,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, IngressOutcome,
    SagaBusPublishError, SagaChoreographyBus, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaWorkflowContract, SagaWorkflowStepContract,
    StepError, StepOutput, SuccessCriteria, TerminalPolicy, WorkflowDependencySpec,
};

pub const SAGA_TYPE: &str = "canonical_fixture";
pub const STEP_A: &str = "canonical_a";
pub const STEP_B: &str = "canonical_b";

/// Contract with a required path through both steps, so an undelivered
/// emission is a detectable shortfall.
pub struct CanonicalContract;

impl SagaWorkflowContract for CanonicalContract {
    fn saga_type() -> &'static str {
        SAGA_TYPE
    }
    fn first_step() -> &'static str {
        STEP_A
    }
    fn steps() -> &'static [SagaWorkflowStepContract] {
        &[
            SagaWorkflowStepContract {
                step_name: STEP_A,
                participant_id: "canonical-a",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            },
            SagaWorkflowStepContract {
                step_name: STEP_B,
                participant_id: "canonical-b",
                depends_on: WorkflowDependencySpec::After(STEP_A),
            },
        ]
    }
    fn terminal_policy() -> TerminalPolicy {
        let required: HashSet<Box<str>> = [STEP_A.into(), STEP_B.into()].into_iter().collect();
        TerminalPolicy::new(
            SAGA_TYPE.into(),
            "canonical_fixture/test".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Self::steps(),
        )
    }
}

/// Bus with the canonical contract registered; `bind_steps` are bound steps.
pub fn canonical_bus(bind_steps: &[&'static str]) -> SagaChoreographyBus {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<CanonicalContract>()
        .expect("canonical contract registers");
    for step in bind_steps {
        bus.register_bound_workflow_step(SAGA_TYPE, step)
            .expect("canonical step binds");
    }
    bus
}

/// Canonical bus plus one subscriber that refuses every event (mailbox
/// rejection): each publish is a partial delivery. Keep the returned
/// subscription alive for the test's duration.
pub fn canonical_bus_with_rejecting_recipient() -> (
    SagaChoreographyBus,
    icanact_core::local::FirehoseSubscription,
) {
    let bus = canonical_bus(&[]);
    let sub = bus.subscribe_saga_type_fn(SAGA_TYPE, |_event: &SagaChoreographyEvent| false);
    (bus, sub)
}

pub fn canonical_context(saga_id: u64) -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: SAGA_TYPE.into(),
        step_name: "start".into(),
        correlation_id: saga_id,
        causation_id: saga_id,
        trace_id: saga_id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

/// Faults the fixture observed while ingesting one event.
#[derive(Debug)]
pub struct IngressReport {
    /// Typed outcome the ingress returned (ADR-0002); `Applied` is the only clean one.
    pub outcome: IngressOutcome,
    /// Emitted events the ingress rejected as invalid transitions.
    pub invalid_emissions: Vec<&'static str>,
    /// Emitted events whose strict publish failed.
    pub publish_errors: Vec<SagaBusPublishError>,
    /// Emitted events published successfully.
    pub published: usize,
}

impl IngressReport {
    pub fn is_clean(&self) -> bool {
        matches!(self.outcome, IngressOutcome::Applied)
            && self.invalid_emissions.is_empty()
            && self.publish_errors.is_empty()
    }

    pub fn into_result(self) -> Result<usize, Self> {
        if self.is_clean() {
            Ok(self.published)
        } else {
            Err(self)
        }
    }
}

pub struct CanonicalParticipant {
    step_name: &'static str,
    depends: DependencySpec,
    saga: SagaParticipantSupport<FaultJournal<InMemoryJournal>, InMemoryDedupe>,
    journal: FaultJournal<InMemoryJournal>,
    active: Option<ActiveSagaExecution>,
    bus: Option<SagaChoreographyBus>,
    pub executed: usize,
}

impl CanonicalParticipant {
    pub fn new(step_name: &'static str, depends: DependencySpec) -> Self {
        let journal = FaultJournal::new(InMemoryJournal::new());
        Self {
            step_name,
            depends,
            saga: SagaParticipantSupport::new(journal.clone(), InMemoryDedupe::new()),
            journal,
            active: None,
            bus: None,
            executed: 0,
        }
    }

    /// Fault-injection handle onto this participant's journal.
    pub fn journal(&self) -> &FaultJournal<InMemoryJournal> {
        &self.journal
    }

    /// The fixture owns publication (the support is left bus-less) so every
    /// publish result is returned to the test rather than only logged.
    pub fn attach_bus(&mut self, bus: SagaChoreographyBus) {
        self.bus = Some(bus);
    }

    /// Ingest `event` through the canonical ingress and report every fault.
    pub fn ingest(&mut self, event: SagaChoreographyEvent) -> IngressReport {
        let mut invalid: Vec<&'static str> = Vec::new();
        let mut publish_errors = Vec::new();
        let mut published = 0usize;
        let bus = self.bus.clone();
        let ingress = apply_sync_participant_saga_ingress_with_hooks(
            self,
            event,
            |_p, _e| {},
            |bad| invalid.push(bad.event_type()),
            |_p, emitted| {
                if let Some(bus) = &bus {
                    match bus.publish_strict(emitted.clone()) {
                        Ok(_) => published += 1,
                        Err(err) => {
                            tracing::error!(
                                target: "core::saga",
                                event = "canonical_fixture_publish_failed",
                                run_key = ?emitted.context().run_key(),
                                error = ?err
                            );
                            publish_errors.push(err);
                        }
                    }
                }
            },
        );
        IngressReport {
            outcome: ingress.outcome,
            invalid_emissions: invalid,
            publish_errors,
            published,
        }
    }
}

impl HasSagaParticipantSupport for CanonicalParticipant {
    type Journal = FaultJournal<InMemoryJournal>;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl HasActiveSagaExecution for CanonicalParticipant {
    fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
        &mut self.active
    }
}

impl SagaParticipant for CanonicalParticipant {
    type Error = String;
    fn step_name(&self) -> &str {
        self.step_name
    }
    fn saga_types(&self) -> &[&'static str] {
        &[SAGA_TYPE]
    }
    fn depends_on(&self) -> DependencySpec {
        self.depends.clone()
    }
    fn execute_step(&mut self, _c: &SagaContext, _i: &[u8]) -> Result<StepOutput, StepError> {
        self.executed += 1;
        Ok(StepOutput::Completed {
            output: vec![],
            compensation_data: vec![1],
        })
    }
    fn compensate_step(
        &mut self,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        Ok(CompensationOutput::Completed)
    }
}
