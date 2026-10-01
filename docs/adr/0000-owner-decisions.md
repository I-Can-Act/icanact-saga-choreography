# Owner decisions on ADR open questions Q1–Q14 (2026-10-01)

Global rule from owner (Q5): **absolutely no silent failures.** Every failed or uncertain path must
(a) return a typed error / non-success outcome to the caller, (b) log at `error`/`warn` with the RunKey,
and (c) emit a failure/quarantine event wherever that is safe. "Emit nothing" is never an accepted outcome.
Apply this to every ADR, not only D2.

| Q | Decision | Who decided |
|---|---|---|
| Q1 | Accept: incarnation = `saga_started_at_millis`; same-ms reuse = same run. | owner |
| Q2 | Refuse to open legacy LMDB journals that still have rows (drain on old binary first). Error must be explicit and name the drain procedure. | owner |
| Q3 | **Rejected: no permanent tombstones.** Cleanup on every run; keep only what is needed. Design: tombstone per finalized RunKey retained only for a bounded *replay horizon* (policy field / config, default derived from the policy's `overall_timeout`, e.g. 2×, with a documented floor). Expired tombstones are pruned during each run's finalize/GC pass (same txn). Correctness after expiry: any event for a RunKey with no active state and no tombstone whose incarnation is older than `now − horizon` is **rejected as expired** (typed error + log), never admitted as a new run. Outbox/inbox/journal rows of a finalized run are deleted at finalize, except quarantined runs (kept, R07). | owner + parent |
| Q4 | Coexist (architect proposal). Reason: existing tests `helpers.rs:1480–1545` are satisfied by RunKey-scoped state, no active state is ever dropped, and both runs still resolve/compensate independently. Add `warn!` + a stats counter when a second active run of the same saga id is admitted. Starts older than a still-known newer incarnation stay rejected (typed error). | parent |
| Q5 | Intent-commit failure → `IngressOutcome::Failed` returned **and** a `StepFailed` emitted (no effect ran, so a non-ambiguous failure is safe and lets the saga compensate) **and** `error!` log. | owner |
| Q6 | Quarantine (no retry loop) for uncertain post-effect commit failure: `ReconciliationNeeded` returned, `SagaQuarantined` emitted where the emit path is available, evidence retained, `error!` log. | parent (per Q5 rule) |
| Q7 | Accept replay-at-startup of all non-finalized outbox obligations without delivered marks (at-least-once; receivers dedupe via durable D5 inbox). Outbox rows deleted at run finalize (Q3). | parent |
| Q8 | **Changed: no weaker capability.** Default `commit_with_outbox` must not drop the outbox. Encode outbound obligations inside the same single journal row (same approach as D5 `InboxCommitted`), e.g. a `TransitionCommitted { event, outbox }` participant-journal row, so every journal with atomic `append` gets at-least-once publication for free. `outbox_for_replay` default derives from `read`/`list`. If the architect finds this infeasible, the method becomes required (no default) — never silently dropping. | parent |
| Q9 | Non-retryable compensation failure → `SagaQuarantined` (an effect remains in the world; `SagaFailed` must mean "cleanly rolled back"). | parent |
| Q10 | No dependency graph → undo strictly serial in reverse completion order (newest first), next step only after the previous undo's durable ack. | parent |
| Q11 | **Changed: compensate losing branches by default.** AnyOf/Quorum groups get a declared loser policy `{ Compensate (default), Keep }`. With `Compensate`, completed compensable losing-branch steps are undone before `SagaCompleted`; late loser completions after the decision are treated as late effects (undo, or quarantine if not compensable). With `Keep`, kept effects are recorded in the terminal event/journal (never silent). Needs a new resolver card (`T31R`, W5). | parent |
| Q12 | Accept the break, and make it the last one: `TerminalPolicy` becomes `#[non_exhaustive]` with constructor + builder (`with_compensation_retry_limit`, `with_replay_horizon`, …); the `saga_workflow_contract!`-style macro must build via the constructor, not a struct literal. | parent |
| Q13 | Accept the single-row inbox. | owner |
| Q14 | Drain-first, enforced fail-closed: on open, an active run lacking the W5 inbox rows is not silently resumed with lost join state — it is surfaced as `ReconciliationNeeded`/quarantined with an explicit reason. Upgrade notes say: drain in-flight AllOf sagas before deploying W5. | parent |

Plan changes proposed by the architect, accepted by the parent:
1. W1 `TSK` write-set widened as listed in `TSK-signatures.md`; exit gate = all existing tests pass + the named new tests.
2. W2: T08P + T08D merged into one lane (serial: `T08D-stores` → `T08PD-rekey`).
3. W3: add `T03D` (R03 stale-recovery path). `T12P` dropped.
4. W5: add `T31R` (Q11 loser compensation) and a GC card for Q3 tombstone expiry if not absorbed by `T07D`/`T17R`.

§5 policy defaults: unchanged (owner did not override).

## Follow-up (2026-10-01)
- N1: accepted — participant replay horizon default 24 h, 1 h floor (reject shorter, no clamping). Revisit later.
- N2: accepted for now — an uncompensable late effect after SagaCompleted/SagaFailed is recorded via SagaEffectsRetained + error log (no second terminal event).
