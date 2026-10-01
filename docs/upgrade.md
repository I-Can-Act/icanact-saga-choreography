# Upgrade notes

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
