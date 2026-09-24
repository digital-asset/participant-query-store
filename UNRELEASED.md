# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## What's New

### Bug fixes

### Minor Improvements

- Add a `--target-postgres-properties-<key>=<value>` to pass arbitrary additional pgjdbc connection properties through to the driver. Enables driver-level features such as JDBC authentication plugins (e.g. Azure Entra ID).
  Example: `--target-postgres-properties-authenticationPluginClassName=com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin`
