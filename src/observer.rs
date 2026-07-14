//! Saga observer trait

use super::SagaContext;

/// Observer trait for external observability.
///
/// Implement this trait to receive callbacks for saga lifecycle events.
/// Observers must be `Send + Sync + 'static` to support concurrent access
/// from multiple saga participants.
///
/// # Example
///
/// ```ignore
/// struct MyObserver;
///
/// impl SagaObserver for MyObserver {
///     fn on_saga_started(&self, context: &SagaContext) {
///         println!("Saga {} started", context.saga_id.0);
///     }
///     // ... implement other methods
/// }
/// ```
pub trait SagaObserver: Send + Sync + 'static {
    /// Called when a saga instance is started.
    ///
    /// @param context - The saga context containing saga_id, saga_type, and other metadata
    fn on_saga_started(&self, context: &SagaContext);

    /// Called when a step begins execution.
    ///
    /// @param context - The saga context
    /// @param step - The name/identifier of the step being executed
    fn on_step_started(&self, context: &SagaContext, step: &str);

    /// Called when a step completes successfully.
    ///
    /// @param context - The saga context
    /// @param step - The name/identifier of the completed step
    /// @param duration_millis - The execution time of the step in milliseconds
    fn on_step_completed(&self, context: &SagaContext, step: &str, duration_millis: u64);

    /// Called when a step fails during execution.
    ///
    /// @param context - The saga context
    /// @param step - The name/identifier of the failed step
    /// @param error - A description of the error that occurred
    fn on_step_failed(&self, context: &SagaContext, step: &str, error: &str);

    /// Called when compensation begins for a previously completed step.
    ///
    /// @param context - The saga context
    /// @param step - The name/identifier of the step being compensated
    fn on_compensation_started(&self, context: &SagaContext, step: &str);

    /// Called when compensation completes for a step.
    ///
    /// @param context - The saga context
    /// @param step - The name/identifier of the step whose compensation completed
    fn on_compensation_completed(&self, context: &SagaContext, step: &str);

    /// Called when a saga completes successfully (all steps finished).
    ///
    /// @param context - The saga context
    fn on_saga_completed(&self, context: &SagaContext);

    /// Called when a saga fails and cannot continue.
    ///
    /// @param context - The saga context
    /// @param reason - A description of why the saga failed
    fn on_saga_failed(&self, context: &SagaContext, reason: &str);

    /// Called when a saga is quarantined due to unrecoverable errors.
    ///
    /// Quarantined sagas require manual intervention to resolve.
    ///
    /// @param context - The saga context
    /// @param step - The name/identifier of the step that caused the quarantine
    /// @param reason - A description of why the saga was quarantined
    fn on_saga_quarantined(&self, context: &SagaContext, step: &str, reason: &str);
}

/// A no-operation observer that ignores all saga events.
///
/// Use this when you need an observer but don't want any observability overhead.
/// All callback methods are empty implementations.
pub struct NoOpObserver;

impl SagaObserver for NoOpObserver {
    fn on_saga_started(&self, _context: &SagaContext) {}
    fn on_step_started(&self, _context: &SagaContext, _step: &str) {}
    fn on_step_completed(&self, _context: &SagaContext, _step: &str, _duration_millis: u64) {}
    fn on_step_failed(&self, _context: &SagaContext, _step: &str, _error: &str) {}
    fn on_compensation_started(&self, _context: &SagaContext, _step: &str) {}
    fn on_compensation_completed(&self, _context: &SagaContext, _step: &str) {}
    fn on_saga_completed(&self, _context: &SagaContext) {}
    fn on_saga_failed(&self, _context: &SagaContext, _reason: &str) {}
    fn on_saga_quarantined(&self, _context: &SagaContext, _step: &str, _reason: &str) {}
}

/// An observer that emits structured log events using the `tracing` crate.
///
/// This observer logs all saga lifecycle events at appropriate log levels:
/// - `INFO`: Normal operations (saga started, step started/completed, compensation events)
/// - `WARN`: Step failures
/// - `ERROR`: Saga failures and quarantines
///
/// Each log event includes structured fields for `saga_id`, and where applicable,
/// `step`, `duration_ms`, `error`, or `reason`.
pub struct TracingObserver;

impl SagaObserver for TracingObserver {
    fn on_saga_started(&self, context: &SagaContext) {
        tracing::info!(saga_id = %context.saga_id.0, saga_type = %context.saga_type, "Saga started");
    }

    fn on_step_started(&self, context: &SagaContext, step: &str) {
        tracing::info!(saga_id = %context.saga_id.0, step = %step, "Step started");
    }

    fn on_step_completed(&self, context: &SagaContext, step: &str, duration_millis: u64) {
        tracing::info!(saga_id = %context.saga_id.0, step = %step, duration_ms = duration_millis, "Step completed");
    }

    fn on_step_failed(&self, context: &SagaContext, step: &str, error: &str) {
        tracing::warn!(saga_id = %context.saga_id.0, step = %step, error = %error, "Step failed");
    }

    fn on_saga_quarantined(&self, context: &SagaContext, step: &str, reason: &str) {
        tracing::error!(saga_id = %context.saga_id.0, step = %step, reason = %reason, "Saga quarantined");
    }

    fn on_saga_completed(&self, context: &SagaContext) {
        tracing::info!(saga_id = %context.saga_id.0, "Saga completed");
    }

    fn on_saga_failed(&self, context: &SagaContext, reason: &str) {
        tracing::error!(saga_id = %context.saga_id.0, reason = %reason, "Saga failed");
    }

    fn on_compensation_started(&self, context: &SagaContext, step: &str) {
        tracing::info!(saga_id = %context.saga_id.0, step = %step, "Compensation started");
    }

    fn on_compensation_completed(&self, context: &SagaContext, step: &str) {
        tracing::info!(saga_id = %context.saga_id.0, step = %step, "Compensation completed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SagaId;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt};

    fn ctx(saga_id: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(saga_id),
            saga_type: "obs".into(),
            step_name: "obs_step".into(),
            correlation_id: 1,
            causation_id: 1,
            trace_id: 1,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0u8; 32],
            saga_started_at_millis: 1_000,
            event_timestamp_millis: 2_000,
        }
    }

    // NOTE: PeerId shape for the context builder is `[u8; 32]`, passed inline.

    /// Capturing `tracing` layer: records each event's level, message, and
    /// field values. Scoped under `with_default` so it never pollutes the
    /// global subscriber or other tests.
    type CapturedEvent = (tracing::Level, String, Vec<(String, String)>);

    #[derive(Default)]
    struct Captured {
        events: Vec<CapturedEvent>,
    }

    struct CaptureLayer {
        inner: Arc<Mutex<Captured>>,
    }

    impl CaptureLayer {
        fn new(inner: Arc<Mutex<Captured>>) -> Self {
            Self { inner }
        }
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: tracing::Subscriber,
    {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            let mut guard = self.inner.lock().unwrap();
            guard.events.push((
                *event.metadata().level(),
                visitor.message.unwrap_or_default(),
                visitor.fields,
            ));
        }
    }

    #[derive(Default)]
    struct FieldVisitor {
        message: Option<String>,
        fields: Vec<(String, String)>,
    }

    impl tracing::field::Visit for FieldVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = Some(format!("{value:?}"));
            } else {
                self.fields
                    .push((field.name().to_string(), format!("{value:?}")));
            }
        }
    }

    fn run_observed<F: FnOnce(&TracingObserver)>(f: F) -> Vec<(tracing::Level, String)> {
        let captured = Arc::new(Mutex::new(Captured::default()));
        let layer = CaptureLayer::new(captured.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            f(&TracingObserver);
        });
        let guard = captured.lock().unwrap();
        guard
            .events
            .iter()
            .map(|(lvl, msg, _)| (*lvl, msg.clone()))
            .collect()
    }

    #[test]
    fn no_op_observer_never_panics_across_all_callbacks() {
        let obs = NoOpObserver;
        let c = ctx(1);
        // Every method must be callable and do nothing; the invariant is
        // "never panics, never side-effects".
        obs.on_saga_started(&c);
        obs.on_step_started(&c, "s");
        obs.on_step_completed(&c, "s", 42);
        obs.on_step_failed(&c, "s", "boom");
        obs.on_compensation_started(&c, "s");
        obs.on_compensation_completed(&c, "s");
        obs.on_saga_completed(&c);
        obs.on_saga_failed(&c, "reason");
        obs.on_saga_quarantined(&c, "s", "reason");
    }

    #[test]
    fn tracing_observer_logs_started_step_and_completion_at_info() {
        let levels = run_observed(|obs| {
            let c = ctx(5);
            obs.on_saga_started(&c);
            obs.on_step_started(&c, "reserve");
            obs.on_step_completed(&c, "reserve", 250);
        });
        assert!(levels.iter().all(|(lvl, _)| *lvl == tracing::Level::INFO));
        assert_eq!(levels.len(), 3);
    }

    #[test]
    fn tracing_observer_logs_step_failure_at_warn() {
        let levels = run_observed(|obs| obs.on_step_failed(&ctx(2), "pay", "declined"));
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].0, tracing::Level::WARN);
    }

    #[test]
    fn tracing_observer_logs_terminal_failures_at_error() {
        let levels = run_observed(|obs| {
            let c = ctx(3);
            obs.on_saga_failed(&c, "upstream timeout");
            obs.on_saga_quarantined(&c, "pay", "ambiguous compensation");
        });
        assert_eq!(levels.len(), 2);
        assert!(levels.iter().all(|(lvl, _)| *lvl == tracing::Level::ERROR));
    }

    #[test]
    fn tracing_observer_emits_saga_id_field_for_every_event() {
        // Every observer method must tag saga_id so logs stay correlatable.
        let captured = Arc::new(Mutex::new(Captured::default()));
        let layer = CaptureLayer::new(captured.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let obs = TracingObserver;
            let c = ctx(99);
            obs.on_saga_started(&c);
            obs.on_step_started(&c, "s");
            obs.on_step_completed(&c, "s", 1);
            obs.on_step_failed(&c, "s", "e");
            obs.on_compensation_started(&c, "s");
            obs.on_compensation_completed(&c, "s");
            obs.on_saga_completed(&c);
            obs.on_saga_failed(&c, "r");
            obs.on_saga_quarantined(&c, "s", "r");
        });
        let guard = captured.lock().unwrap();
        assert_eq!(
            guard.events.len(),
            9,
            "all 9 callbacks should emit one event"
        );
        for (_, _, fields) in &guard.events {
            let has_saga_id = fields.iter().any(|(k, v)| k == "saga_id" && v == "99");
            assert!(has_saga_id, "event missing saga_id=99 field: {fields:?}");
        }
    }
}
