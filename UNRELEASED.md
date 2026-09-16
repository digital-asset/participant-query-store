# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _V043__Add_reassignment_event_types.sql_: adds the `assign` and `unassign` labels to the `__event_type` enum. Metadata-only, no table is scanned or rewritten. **[Impact: Instantaneous]**

## What's New

### Multi-sync support

- PQS now subscribes to reassignments in addition to transactions. Every `Reassignment` received from the ledger is recorded in `__transactions`, and its `Assigned` and `Unassigned` events are recorded in `__events` with the new `assign` and `unassign` types.
