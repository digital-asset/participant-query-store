# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

This release includes the following SQL migrations:
- _FileName.sql_: _DESCRIPTION_ **[Impact: Instantaneous /< 1 min / ~10 mins]**


## What's New

### Renamed Scribe to Participant Query Store (PQS)

- *BREAKING*: The assembly JAR is renamed from `scribe.jar` to `pqs.jar`.
- *BREAKING*: The Docker image entrypoint is updated to run the `pqs.jar`.
- *BREAKING*: The main class is renamed from `com.digitalasset.scribe.Main` to `com.digitalasset.pqs.Main`.
- *BREAKING*: The application name property in the Postgres connection is renamed from `scribe` to `pqs`.
- Environment configuration with `PQS_` prefix (e.g. `PQS_TARGET_POSTGRES_HOST`) is now supported. The `SCRIBE_` prefix is still supported as a fallback and it prints a deprecation warning.
- *BREAKING*: The default cache directory has moved from `/tmp/scribe` to `/tmp/pqs`.
- Both `participant-query-store` and legacy `scribe` components (DPM) are published. They configure the same `pqs` command, which invokes `pqs.jar`. 
- Both `participant-query-store` and legacy `scribe` Docker images are published. The entrypoint is `java -jar pqs.jar` for both.
- *BREAKING*: The `org.opencontainers.image.ref.name` label is updated from `scribe` to `participant-query-store`.
- *BREAKING*: The `OTEL_SERVICE_NAME` environment variable is updated from `scribe` to `pqs`.
- *BREAKING*: All metrics and attributes prefixes are renamed from `scribe` to `pqs`.

### Bug fixes

- Optimize core SQL functions (`creates`, `exercises`, `active`, `archives`) to compute the nearest offset only once per query.

### Minor Improvements

- *BREAKING*: PQS configuration no longer provides default Postgres credentials. It is now mandatory to supply the `--target-postgres-username` and `--target-postgres-password` command arguments, or the `PQS_TARGET_POSTGRES_USERNAME` and `PQS_TARGET_POSTGRES_PASSWORD` environment variables.
