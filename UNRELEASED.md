# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## What's New

### Bug fixes

- Fixed silent handling of PostgreSQL commit failures that could leave gaps in `__transactions`. Commit errors now invalidate the connection and fail the pipeline so ingestion is retried.

### Minor Improvements

- Added support for `AzurePostgresqlAuthenticationPlugin` by bundling `com.azure:azure-identity-extensions:1.2.2` into assembly JAR and Docker image.
