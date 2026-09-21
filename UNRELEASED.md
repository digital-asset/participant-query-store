# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _V043__Add_reassignment_event_types.sql_: adds the `assign` and `unassign` labels to the `__event_type` enum. Metadata-only, no table is scanned or rewritten. **[Impact: Instantaneous]**
- _V044__Rename_domain_id_to_synchronizer_id.sql_: renames the `domain_id` column of `__transactions` to `synchronizer_id`. Metadata-only, no table is scanned or rewritten. **[Impact: Instantaneous]**
- _V045__Add_multisync_contract_columns.sql_ **[Impact: ~10 min]**:
  - Add the multi-sync columns (`assigned_at_ix`, `unassigned_at_ix`, `reassignment_counter`, `synchronizer_id`) to the `__contracts` table and `contract` type.
  - **Full __contracts rewrite:** Redefine the `life_ix` column to take `assigned_at_ix` and `unassigned_at_ix` into account.
  - **Rebuild GiST index on __contracts.**
  - Drop the `__tmp_archived_contracts` and replace with `__tmp_deactivated_contracts` for internal use by the PQS pipeline.

## What's New

### Multi-sync support

- PQS now subscribes to reassignments in addition to transactions.
- Every `Reassignment` received from the ledger is recorded in `__transactions`, and its `Assigned` and `Unassigned` events are recorded in `__events` with the new `assign` and `unassign` types.
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
- An `Assigned` event creates a new row in the `__contracts` table. Instead of setting `created_at_ix` and `create_event_pk`, it sets `assigned_at_ix` and `assign_event_pk`.
- An `Unassigned` event deactivates its corresponding row from the `__contracts` table. Instead of setting `archived_at_ix` and `archive_event_pk`, it sets `unassigned_at_ix` and `unassign_event_pk`.
- `Created` and `Assigned` events from the ledger now set `reassignment_counter` and `synchronizer_id`. These columns are left empty in legacy rows (PQS 3.6 or older).
- *BREAKING*: The `__transactions` column `domain_id` is renamed to `synchronizer_id`, to match Canton's current vocabulary. It is now populated for every update — every transaction and every reassignment. Rows written before this release keep `NULL` and are not getting backfilled.
- The `synchronizer_id text` column is added to the `transactions` SQL view.
- The `synchronizer_id text` column is added to the output of the `exercises` and `lookup_exercise` SQL functions.
