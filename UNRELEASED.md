# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _V043__Add_reassignment_event_types.sql_: adds the `assign` and `unassign` labels to the `__event_type` enum. Metadata-only, no table is scanned or rewritten. **[Impact: Instantaneous]**
- _V044__Rename_domain_id_to_synchronizer_id.sql_: renames the `domain_id` column of `__transactions` to `synchronizer_id`. Metadata-only, no table is scanned or rewritten. **[Impact: Instantaneous]**
- _V045__Add_multisync_contract_columns.sql_ **[Impact: ~2 min/10M contracts]**:
  - Add the multi-sync columns (`assigned_at_ix`, `unassigned_at_ix`, `reassignment_counter`, `synchronizer_id`) to the `__contracts` table and `contract` type.
  - **Full __contracts rewrite:** Redefine the `life_ix` column to take `assigned_at_ix` and `unassigned_at_ix` into account.
  - **Rebuild GiST index on __contracts.**
  - Drop the internal `__archives` view.
  - Drop the `__tmp_archived_contracts` and replace with `__tmp_deactivated_contracts` for internal use by the PQS pipeline.
- _V046__Create_reassignments_table.sql_: creates the `__reassignment_type` enum and the list-partitioned `__reassignments` table, then creates one partition per row already present in `__contract_tpe`. **[Impact: Instantaneous /< 1 min]**
- _V047__Prunning_reassignments.sql_: drops `prune_archived_to_offset` and `prune_archived_to_offset_dry_run` so that they can be re-created with the new `deleted_reassignments` result column. **[Impact: Instantaneous]**

## What's New

### Multi-sync support

#### Ingestion of reassignment events and reassigned contracts

- PQS now subscribes to reassignments in addition to transactions.
- Every `Reassignment` received from the ledger is recorded in `__transactions`, and its `Assigned` and `Unassigned` events are recorded in `__events` with the new `assign` and `unassign` types.
- Each `Assigned` and `Unassigned` event is also recorded in the new `__reassignments` table. The table is a standalone audit log of what the ledger reported about the reassignment itself.
- An `Assigned` event creates a new row in the `__contracts` table. Instead of setting `created_at_ix` and `create_event_pk`, it sets `assigned_at_ix` and `assign_event_pk`.
- An `Unassigned` event deactivates its corresponding row from the `__contracts` table. Instead of setting `archived_at_ix` and `archive_event_pk`, it sets `unassigned_at_ix` and `unassign_event_pk`.
- `Created` and `Assigned` events from the ledger now set `reassignment_counter` and `synchronizer_id`. These columns are left empty in legacy rows (PQS 3.6 or older).
- Contracts ingested from the active contract set (when PQS starts from a non-genesis offset) keep the `reassignment_counter` reported by the ledger for their synchronizer, instead of `0`.


#### SQL API

- The SQL `contract` type, describing the output of the `active`, `creates` and `archives` SQL functions, is modified with new columns:
```sql
assign_event_pk bigint,
assign_event_id event_id,
assigned_at_ix bigint,
assigned_at_offset bigint,
unassign_event_pk bigint,
unassign_event_id event_id,
unassigned_at_ix bigint,
unassigned_at_offset bigint,
reassignment_counter bigint,
synchronizer_id text;
```
- `active` and `lookup_contract` now return a single row per `(contract_id, template_fqn)`, keeping the row with the highest `reassignment_counter`.
- `summary_active` counts each `contract_id` only once per `template_fqn`.
- *BREAKING*: The `__transactions` column `domain_id` is renamed to `synchronizer_id`, to match Canton's current vocabulary. It is now populated for every update — every transaction and every reassignment. Rows written before this release keep `NULL` and are not getting backfilled.
- The `synchronizer_id text` column is added to the `transactions` SQL view.
- The `synchronizer_id text` column is added to the output of the `exercises` and `lookup_exercise` SQL functions.


#### Pruning, Reset to offset and Redaction

- *BREAKING*: `prune_archived_to_offset` and `prune_archived_to_offset_dry_run` return an additional `deleted_reassignments integer` column. The `prune` command prints it as `Deleted reassignments`.
- Pruning takes reassignments into account - SQL functions now also delete related `__reassignments` entries.
- `redact_contract` redacts a contract once all its rows are deactivated (archived or unassigned), so reassigned and unassigned contracts can be redacted; rows inserted after a redaction (a late assign or create, or rows replayed by `reset_to_offset`) keep their payload — call `redact_contract` again once they are deactivated, and it fails with "already redacted" only when every row is redacted.
- Resetting to an offset takes reassignments into account: `reset_to_offset` and `__cleanup_transactions_after_watermark` delete the contracts assigned after the offset and the `__reassignments` entries recorded after it, and revives the contracts unassigned after it.

### Bug fixes

- Fixed silent handling of PostgreSQL commit failures that could leave gaps in `__transactions`. Commit errors now invalidate the connection and fail the pipeline so ingestion is retried.

### Minor improvements

- At startup, the PQS pipeline runs `ANALYZE` on `__contracts` and `__tmp_deactivated_contracts` to populate planner statistics and speed up ingestion.

### Helm Chart

- Allow for managing JVM `JDK_JAVA_OPTIONS` env var via values file.  Default to assigning max of 75% of available container memory to the JVM process.  Allow for the entire `JDK_JAVA_OPTIONS` to be configured via `values.yaml` - `options` to override or `extraOptions` to append an additional value.
