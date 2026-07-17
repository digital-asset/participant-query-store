# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## What's New

### Minor Improvements

- *BREAKING*: PQS configuration no longer provides default Postgres credentials. It is now mandatory to supply the `--target-postgres-username` and `--target-postgres-password` command arguments, or the `PQS_TARGET_POSTGRES_USERNAME` and `PQS_TARGET_POSTGRES_PASSWORD` environment variables.
