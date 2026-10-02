# Upgrade notes

## Upgrade notes (0.x → next)

Breaking changes from the run-identity, outbox, resolver and startup-contract work:

- **Required run-scoped trait methods.** `ParticipantJournal` now requires `append_run`,
  `read_run`, `list_runs`, `finalize_run`, `run_tombstones` and `prune_expired_tombstones`
  (`commit_inbox` / `commit_with_outbox` have defaults). `ParticipantDedupeStore` now requires
  `check_and_mark_run`, `contains_run`, `mark_processed_run`, `remove_processed_run`,
  `prune_run`, `list_runs`, `keys_run` and `prune_expired`. Custom backends must implement them.
- **`TerminalPolicy` is `#[non_exhaustive]`.** Struct-literal construction no longer compiles
  outside the crate; use `TerminalPolicy::new(...)` and its builder-style setters.
- **Ingress return types.** Ingress helpers return `IngressReport { outcome: IngressOutcome,
  publish_failures }`; `IngressOutcome` is `Applied | Duplicate | Rejected | Failed |
  ReconciliationNeeded`. Callers must inspect both fields.
- **Plain binders take `steps: &[&str]`.** `bind_*_participant_*` (non-workflow) now tag
  subscriptions with the steps the actor owns; untagged (empty) subscriptions never satisfy a
  required step. Strict workflow binders are unchanged.
- **Durable resolvers must be activated.** Until `activate_terminal_resolver_recovery*` runs,
  a durable resolver publishes nothing and `SagaStarted` is rejected with `AdmissionRejected`.
- **New `SagaBusPublishError` variants / fields:** `AdmissionRejected`, `AbortNotDelivered`,
  and `RequiredPathDeliveryShortfall.missing_roles`. Exhaustive matches must be updated.
- **Legacy LMDB journals must be drained** (see [Run identity](#run-identity)).
- **New journal row variants.** The participant journal can now contain rows (for example
  `ParticipantEvent::CompensationRetryable`) that older binaries cannot decode. There is no
  downgrade once a newer binary has written to a journal.

## Run identity

Participant state is now keyed by run (`RunKey = (saga_type, saga_id, incarnation)`,
where `incarnation` is `saga_started_at_millis`; see `docs/adr/0001-run-key.md`).

### LMDB participant journal: drain before upgrading

The participant journal schema moves from `"2"` to `"3"`. Rows written by the previous
binary carry no run identity, and identity is never guessed. A schema-`"2"` journal that
still holds rows is therefore **refused** at `LmdbJournal::open` with
`JournalError::LegacyRunIdentity { legacy_rows }`:

```
participant journal holds N rows written before run identity (schema 2); drain in-flight
sagas on the previous binary until the journal is empty, then start this version
(docs/upgrade.md#run-identity)
```

Drain procedure:

1. Stop admitting new sagas to the participant (stop the saga initiators or route them away).
2. Keep running the **previous** binary until every in-flight saga that touches the
   participant has reached `SagaCompleted` or `SagaFailed`. Terminal cleanup prunes the
   participant journal rows of finished sagas, so the journal is empty when the drain is done.
   Sagas that are quarantined or awaiting reconciliation keep their rows: resolve them
   (compensate or manually reconcile) on the previous binary first.
3. Verify the journal is empty: `ParticipantJournal::list_sagas()` returns no ids (or the
   `journal_rows` LMDB database has zero entries).
4. Stop the previous binary and start this version. A schema-`"2"` journal with no rows is
   upgraded in place to `"3"`; no data migration is needed.

Do not delete or edit LMDB files to bypass the check: the rows are the only record of
in-flight effects and outbound obligations.

### LMDB dedupe store

The dedupe environment gains run-scoped databases (`dedupe_run_entries`, `dedupe_meta`,
`dedupe_schema_version = "3"`). Entries written by the previous binary stay in
`dedupe_entries`, are never consulted by run-scoped lookups, and are reported once at
`warn` level with their count when the store is first opened. This is not an error:
redelivery after a restart originates from journals, and journals with legacy rows are
refused above.

### Downgrade

There is no downgrade path once a journal has been stamped `"3"`.
