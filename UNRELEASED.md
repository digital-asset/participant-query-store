# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _V042__Clear_contract_key_hash_for_interface_views.sql_: clears `contract_key_hash` on interface view rows already stored. Scans `__contracts` once and rewrites only the interface rows that still hold a hash. **[Impact: < 1 min]**


## What's New

### Minor Improvements

- *BREAKING*: PQS configuration no longer provides default Postgres credentials. It is now mandatory to supply the `--target-postgres-username` and `--target-postgres-password` command arguments, or the `PQS_TARGET_POSTGRES_USERNAME` and `PQS_TARGET_POSTGRES_PASSWORD` environment variables.

- (Helm chart) PQS pruning can now be configured via the chart values file. Runs as a cronjob with configurable schedule, disabled by default.
- Update library versions to address security vulnerabilities
- *BREAKING*: Interface view rows no longer store `contract_key_hash`. Previously the hash of the underlying template's contract key was duplicated onto every interface view row, while `contract_key` was already left empty. Both columns are now empty for interface views. Upgrading also clears the hash from interface view rows already stored. The hash remains available on the template rows.
