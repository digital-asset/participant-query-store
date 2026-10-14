# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _V043__Add_divulged_created_at_ix_index.sql_: adds a partial index on `__contracts` so `prune_archived_to_offset` can prune divulged-only contracts via an index scan. **[Impact: ~ 1 min]**


## What's New

### Minor Improvements

- Optimize `prune_archived_to_offset` and its dry-run to run significantly faster on large datasets. Performance gains range from 20% to 80%, depending on the pruning window and dataset size.
