// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import com.digitalasset.scribe.specific.offsetScalaType
import zio.*
import zio.internal.stacktracer.SourceLocation
import zio.test.*

object NavigationHelpSpec extends ZIOSpecDefault:
  private val App             = "./scribe"
  private val HelpFlag        = "--help"
  private val HelpVerboseFlag = "--help-verbose"

  override val spec = suite("Application navigation help specification")(
    verify(App, Array(HelpFlag)) {
      """Usage: scribe COMMAND

An efficient ledger data exporting tool

Commands:
  pipeline     Initiate continuous ledger data export
  datastore    Perform operations supporting a certified data store

Run 'scribe COMMAND --help[-verbose]' for more information on a command.
"""
    },
    verify(App, Array("datastore", HelpFlag)) {
      """Usage: scribe datastore COMMAND

Perform operations supporting a certified data store

Commands:
  postgres-document    Perform operations supporting Postgres database (w/ document payload representation)

Run 'scribe datastore COMMAND --help[-verbose]' for more information on a command.
"""
    },
    verify(App, Array("datastore", "postgres-document", HelpFlag)) {
      """Usage: scribe datastore postgres-document COMMAND

Perform operations supporting Postgres database (w/ document payload representation)

Commands:
  schema    Infer or apply database schema derived from Daml package metadata
  prune     Prune transactions to a given offset inclusively

Run 'scribe datastore postgres-document COMMAND --help[-verbose]' for more information on a command.
"""
    },
    verify(App, Array("datastore", "postgres-document", "schema", HelpFlag)) {
      """Usage: scribe datastore postgres-document schema COMMAND

Infer or apply database schema derived from Daml package metadata

Commands:
  apply    Infer required database schema, apply it to data store and quit
  show     Infer required database schema, display it and quit

Run 'scribe datastore postgres-document schema COMMAND --help[-verbose]' for more information on a command.
"""
    },
    verify(App, Array("datastore", "postgres-document", "schema", "apply", HelpFlag)) {
      """Usage: scribe datastore postgres-document schema apply [OPTIONS]

Infer required database schema, apply it to data store and quit

Options:
  --config file                        Path to configuration overrides via an external HOCON file (optional)
  --ledger-host string                 Ledger API host (default: localhost)
  --ledger-cachedir file               Cache Directory (default: /tmp/scribe)
  --ledger-buffersize int              Buffer size for gRPC channel (default: 128)
  --ledger-keepalive-time string       Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)
  --ledger-keepalive-timeout string    Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)
  --ledger-auth enum                   Authorisation mode (default: NoAuth)
  --ledger-tls-cafile file             Trusted Certificate Authority (CA) certificate (optional)
  --ledger-tls-cert file               Client's certificate (leave empty if embedded into private key file) (optional)
  --ledger-tls-key file                Client's private key (leave empty for server-only TLS) (optional)
  --ledger-port int                    Ledger API port (default: 6865)
  --logger-destination file            Log output file (default: output.log)
  --logger-mappings map                Custom mappings for log levels
  --logger-format enum                 Log output format (default: Plain)
  --logger-pattern [enum | string]     Log pattern (default: Plain)
  --logger-level enum                  Log level (default: Info)
  --filter-contracts string            Filter expression determining which templates and interfaces to include (default: *)
  --schema-autoapply boolean           Apply metadata inferred schema on startup (default: true)
  --schema-baseline boolean            Baseline existing database schema during apply (default: false)
  --postgres-host string               Postgres host (default: localhost)
  --postgres-probeinterval string      Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)
  --postgres-appname string            Application name for Postgres connections (default: scribe)
  --postgres-buffersize int            Buffer size for transactions processing (default: 128)
  --postgres-tls-mode enum             SSL mode required for Postgres connectivity (default: Disable)
  --postgres-tls-cert file             Client's certificate (optional)
  --postgres-tls-key file              Client's private key (optional)
  --postgres-tls-cafile file           Trusted Certificate Authority (CA) certificate (optional)
  --postgres-keepalive boolean         Enable/disable TCP keep-alive probe (default: true)
  --postgres-maxconnections int        Maximum number of JDBC connections (default: 16)
  --postgres-password string           Postgres user password
  --postgres-username string           Postgres user name
  --postgres-schema string             Postgres schema (default: public)
  --postgres-database string           Postgres database (default: postgres)
  --postgres-port int                  Postgres port (default: 5432)
  --oauth-clientid string              Client's identifier (optional)
  --oauth-proxy-url uri                Proxy server URL (optional)
  --oauth-proxy-password string        Proxy server password (optional)
  --oauth-proxy-user string            Proxy server username (optional)
  --oauth-accesstoken string           Access token (optional)
  --oauth-scope [enum | string]        Token scope (default: Default)
  --oauth-parameters map               Custom parameters
  --oauth-preemptexpiry string         The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)
  --oauth-cafile file                  Trusted Certificate Authority (CA) certificate (optional)
  --oauth-endpoint uri                 Token endpoint URL (optional)
  --oauth-issuer uri                   OIDC-compliant issuer URL (optional)
  --oauth-clientsecret string          Client's secret (optional)
"""
    },
    verify(App, Array("datastore", "postgres-document", "schema", "apply", HelpVerboseFlag)) {
      """Usage: scribe datastore postgres-document schema apply [OPTIONS]

Infer required database schema, apply it to data store and quit

Options:
  --config file                        Path to configuration overrides via an external HOCON file (optional)
                                        + Environment variable: SCRIBE_CONFIG
                                        + System property:      config
  --ledger-host string                 Ledger API host (default: localhost)
                                        + Environment variable: SCRIBE_LEDGER_HOST
                                        + System property:      ledger.host
  --ledger-cachedir file               Cache Directory (default: /tmp/scribe)
                                        + Environment variable: SCRIBE_LEDGER_CACHEDIR
                                        + System property:      ledger.cacheDir
  --ledger-buffersize int              Buffer size for gRPC channel (default: 128)
                                        + Environment variable: SCRIBE_LEDGER_BUFFERSIZE
                                        + System property:      ledger.bufferSize
  --ledger-keepalive-time string       Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)
                                        + Environment variable: SCRIBE_LEDGER_KEEPALIVE_TIME
                                        + System property:      ledger.keepAlive.time
  --ledger-keepalive-timeout string    Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)
                                        + Environment variable: SCRIBE_LEDGER_KEEPALIVE_TIMEOUT
                                        + System property:      ledger.keepAlive.timeout
  --ledger-auth enum                   Authorisation mode (default: NoAuth)
                                        + Environment variable: SCRIBE_LEDGER_AUTH
                                        + System property:      ledger.auth
                                        + Enumeration values:   OAuth, NoAuth
  --ledger-tls-cafile file             Trusted Certificate Authority (CA) certificate (optional)
                                        + Environment variable: SCRIBE_LEDGER_TLS_CAFILE
                                        + System property:      ledger.tls.cafile
  --ledger-tls-cert file               Client's certificate (leave empty if embedded into private key file) (optional)
                                        + Environment variable: SCRIBE_LEDGER_TLS_CERT
                                        + System property:      ledger.tls.cert
  --ledger-tls-key file                Client's private key (leave empty for server-only TLS) (optional)
                                        + Environment variable: SCRIBE_LEDGER_TLS_KEY
                                        + System property:      ledger.tls.key
  --ledger-port int                    Ledger API port (default: 6865)
                                        + Environment variable: SCRIBE_LEDGER_PORT
                                        + System property:      ledger.port
  --logger-destination file            Log output file (default: output.log)
                                        + Environment variable: SCRIBE_LOGGER_DESTINATION
                                        + System property:      logger.destination
  --logger-mappings map                Custom mappings for log levels
                                        + Environment variable: SCRIBE_LOGGER_MAPPINGS
                                        + System property:      logger.mappings
  --logger-format enum                 Log output format (default: Plain)
                                        + Environment variable: SCRIBE_LOGGER_FORMAT
                                        + System property:      logger.format
                                        + Enumeration values:   Plain, PlainAsync, Json, JsonAsync
  --logger-pattern [enum | string]     Log pattern (default: Plain)
                                        + Environment variable: SCRIBE_LOGGER_PATTERN
                                        + System property:      logger.pattern
                                        + Enumeration values:   Plain, Standard, Structured
  --logger-level enum                  Log level (default: Info)
                                        + Environment variable: SCRIBE_LOGGER_LEVEL
                                        + System property:      logger.level
                                        + Enumeration values:   All, Fatal, Error, Warning, Info, Debug, Trace, None
  --filter-contracts string            Filter expression determining which templates and interfaces to include (default: *)
                                        + Environment variable: SCRIBE_FILTER_CONTRACTS
                                        + System property:      filter.contracts
  --schema-autoapply boolean           Apply metadata inferred schema on startup (default: true)
                                        + Environment variable: SCRIBE_SCHEMA_AUTOAPPLY
                                        + System property:      schema.autoApply
  --schema-baseline boolean            Baseline existing database schema during apply (default: false)
                                        + Environment variable: SCRIBE_SCHEMA_BASELINE
                                        + System property:      schema.baseline
  --postgres-host string               Postgres host (default: localhost)
                                        + Environment variable: SCRIBE_POSTGRES_HOST
                                        + System property:      postgres.host
  --postgres-probeinterval string      Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)
                                        + Environment variable: SCRIBE_POSTGRES_PROBEINTERVAL
                                        + System property:      postgres.probeInterval
  --postgres-appname string            Application name for Postgres connections (default: scribe)
                                        + Environment variable: SCRIBE_POSTGRES_APPNAME
                                        + System property:      postgres.appName
  --postgres-buffersize int            Buffer size for transactions processing (default: 128)
                                        + Environment variable: SCRIBE_POSTGRES_BUFFERSIZE
                                        + System property:      postgres.bufferSize
  --postgres-tls-mode enum             SSL mode required for Postgres connectivity (default: Disable)
                                        + Environment variable: SCRIBE_POSTGRES_TLS_MODE
                                        + System property:      postgres.tls.mode
                                        + Enumeration values:   Disable, Require, VerifyCA, VerifyFull
  --postgres-tls-cert file             Client's certificate (optional)
                                        + Environment variable: SCRIBE_POSTGRES_TLS_CERT
                                        + System property:      postgres.tls.cert
  --postgres-tls-key file              Client's private key (optional)
                                        + Environment variable: SCRIBE_POSTGRES_TLS_KEY
                                        + System property:      postgres.tls.key
  --postgres-tls-cafile file           Trusted Certificate Authority (CA) certificate (optional)
                                        + Environment variable: SCRIBE_POSTGRES_TLS_CAFILE
                                        + System property:      postgres.tls.cafile
  --postgres-keepalive boolean         Enable/disable TCP keep-alive probe (default: true)
                                        + Environment variable: SCRIBE_POSTGRES_KEEPALIVE
                                        + System property:      postgres.keepAlive
  --postgres-maxconnections int        Maximum number of JDBC connections (default: 16)
                                        + Environment variable: SCRIBE_POSTGRES_MAXCONNECTIONS
                                        + System property:      postgres.maxConnections
  --postgres-password string           Postgres user password
                                        + Environment variable: SCRIBE_POSTGRES_PASSWORD
                                        + System property:      postgres.password
  --postgres-username string           Postgres user name
                                        + Environment variable: SCRIBE_POSTGRES_USERNAME
                                        + System property:      postgres.username
  --postgres-schema string             Postgres schema (default: public)
                                        + Environment variable: SCRIBE_POSTGRES_SCHEMA
                                        + System property:      postgres.schema
  --postgres-database string           Postgres database (default: postgres)
                                        + Environment variable: SCRIBE_POSTGRES_DATABASE
                                        + System property:      postgres.database
  --postgres-port int                  Postgres port (default: 5432)
                                        + Environment variable: SCRIBE_POSTGRES_PORT
                                        + System property:      postgres.port
  --oauth-clientid string              Client's identifier (optional)
                                        + Environment variable: SCRIBE_OAUTH_CLIENTID
                                        + System property:      oauth.clientId
  --oauth-proxy-url uri                Proxy server URL (optional)
                                        + Environment variable: SCRIBE_OAUTH_PROXY_URL
                                        + System property:      oauth.proxy.url
  --oauth-proxy-password string        Proxy server password (optional)
                                        + Environment variable: SCRIBE_OAUTH_PROXY_PASSWORD
                                        + System property:      oauth.proxy.password
  --oauth-proxy-user string            Proxy server username (optional)
                                        + Environment variable: SCRIBE_OAUTH_PROXY_USER
                                        + System property:      oauth.proxy.user
  --oauth-accesstoken string           Access token (optional)
                                        + Environment variable: SCRIBE_OAUTH_ACCESSTOKEN
                                        + System property:      oauth.accessToken
  --oauth-scope [enum | string]        Token scope (default: Default)
                                        + Environment variable: SCRIBE_OAUTH_SCOPE
                                        + System property:      oauth.scope
                                        + Enumeration values:   Default, None
  --oauth-parameters map               Custom parameters
                                        + Environment variable: SCRIBE_OAUTH_PARAMETERS
                                        + System property:      oauth.parameters
  --oauth-preemptexpiry string         The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)
                                        + Environment variable: SCRIBE_OAUTH_PREEMPTEXPIRY
                                        + System property:      oauth.preemptExpiry
  --oauth-cafile file                  Trusted Certificate Authority (CA) certificate (optional)
                                        + Environment variable: SCRIBE_OAUTH_CAFILE
                                        + System property:      oauth.cafile
  --oauth-endpoint uri                 Token endpoint URL (optional)
                                        + Environment variable: SCRIBE_OAUTH_ENDPOINT
                                        + System property:      oauth.endpoint
  --oauth-issuer uri                   OIDC-compliant issuer URL (optional)
                                        + Environment variable: SCRIBE_OAUTH_ISSUER
                                        + System property:      oauth.issuer
  --oauth-clientsecret string          Client's secret (optional)
                                        + Environment variable: SCRIBE_OAUTH_CLIENTSECRET
                                        + System property:      oauth.clientSecret
"""
    },
    verify(App, Array("datastore", "postgres-document", "schema", "show", HelpFlag)) {
      """Usage: scribe datastore postgres-document schema show [OPTIONS]

Infer required database schema, display it and quit

Options:
  --config file                        Path to configuration overrides via an external HOCON file (optional)
  --ledger-host string                 Ledger API host (default: localhost)
  --ledger-cachedir file               Cache Directory (default: /tmp/scribe)
  --ledger-buffersize int              Buffer size for gRPC channel (default: 128)
  --ledger-keepalive-time string       Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)
  --ledger-keepalive-timeout string    Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)
  --ledger-auth enum                   Authorisation mode (default: NoAuth)
  --ledger-tls-cafile file             Trusted Certificate Authority (CA) certificate (optional)
  --ledger-tls-cert file               Client's certificate (leave empty if embedded into private key file) (optional)
  --ledger-tls-key file                Client's private key (leave empty for server-only TLS) (optional)
  --ledger-port int                    Ledger API port (default: 6865)
  --logger-destination file            Log output file (default: output.log)
  --logger-mappings map                Custom mappings for log levels
  --logger-format enum                 Log output format (default: Plain)
  --logger-pattern [enum | string]     Log pattern (default: Plain)
  --logger-level enum                  Log level (default: Info)
  --filter-contracts string            Filter expression determining which templates and interfaces to include (default: *)
  --oauth-clientid string              Client's identifier (optional)
  --oauth-proxy-url uri                Proxy server URL (optional)
  --oauth-proxy-password string        Proxy server password (optional)
  --oauth-proxy-user string            Proxy server username (optional)
  --oauth-accesstoken string           Access token (optional)
  --oauth-scope [enum | string]        Token scope (default: Default)
  --oauth-parameters map               Custom parameters
  --oauth-preemptexpiry string         The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)
  --oauth-cafile file                  Trusted Certificate Authority (CA) certificate (optional)
  --oauth-endpoint uri                 Token endpoint URL (optional)
  --oauth-issuer uri                   OIDC-compliant issuer URL (optional)
  --oauth-clientsecret string          Client's secret (optional)
"""
    },
    verify(App, Array("datastore", "postgres-document", "schema", "show", HelpVerboseFlag)) {
      """Usage: scribe datastore postgres-document schema show [OPTIONS]

Infer required database schema, display it and quit

Options:
  --config file                        Path to configuration overrides via an external HOCON file (optional)
                                        + Environment variable: SCRIBE_CONFIG
                                        + System property:      config
  --ledger-host string                 Ledger API host (default: localhost)
                                        + Environment variable: SCRIBE_LEDGER_HOST
                                        + System property:      ledger.host
  --ledger-cachedir file               Cache Directory (default: /tmp/scribe)
                                        + Environment variable: SCRIBE_LEDGER_CACHEDIR
                                        + System property:      ledger.cacheDir
  --ledger-buffersize int              Buffer size for gRPC channel (default: 128)
                                        + Environment variable: SCRIBE_LEDGER_BUFFERSIZE
                                        + System property:      ledger.bufferSize
  --ledger-keepalive-time string       Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)
                                        + Environment variable: SCRIBE_LEDGER_KEEPALIVE_TIME
                                        + System property:      ledger.keepAlive.time
  --ledger-keepalive-timeout string    Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)
                                        + Environment variable: SCRIBE_LEDGER_KEEPALIVE_TIMEOUT
                                        + System property:      ledger.keepAlive.timeout
  --ledger-auth enum                   Authorisation mode (default: NoAuth)
                                        + Environment variable: SCRIBE_LEDGER_AUTH
                                        + System property:      ledger.auth
                                        + Enumeration values:   OAuth, NoAuth
  --ledger-tls-cafile file             Trusted Certificate Authority (CA) certificate (optional)
                                        + Environment variable: SCRIBE_LEDGER_TLS_CAFILE
                                        + System property:      ledger.tls.cafile
  --ledger-tls-cert file               Client's certificate (leave empty if embedded into private key file) (optional)
                                        + Environment variable: SCRIBE_LEDGER_TLS_CERT
                                        + System property:      ledger.tls.cert
  --ledger-tls-key file                Client's private key (leave empty for server-only TLS) (optional)
                                        + Environment variable: SCRIBE_LEDGER_TLS_KEY
                                        + System property:      ledger.tls.key
  --ledger-port int                    Ledger API port (default: 6865)
                                        + Environment variable: SCRIBE_LEDGER_PORT
                                        + System property:      ledger.port
  --logger-destination file            Log output file (default: output.log)
                                        + Environment variable: SCRIBE_LOGGER_DESTINATION
                                        + System property:      logger.destination
  --logger-mappings map                Custom mappings for log levels
                                        + Environment variable: SCRIBE_LOGGER_MAPPINGS
                                        + System property:      logger.mappings
  --logger-format enum                 Log output format (default: Plain)
                                        + Environment variable: SCRIBE_LOGGER_FORMAT
                                        + System property:      logger.format
                                        + Enumeration values:   Plain, PlainAsync, Json, JsonAsync
  --logger-pattern [enum | string]     Log pattern (default: Plain)
                                        + Environment variable: SCRIBE_LOGGER_PATTERN
                                        + System property:      logger.pattern
                                        + Enumeration values:   Plain, Standard, Structured
  --logger-level enum                  Log level (default: Info)
                                        + Environment variable: SCRIBE_LOGGER_LEVEL
                                        + System property:      logger.level
                                        + Enumeration values:   All, Fatal, Error, Warning, Info, Debug, Trace, None
  --filter-contracts string            Filter expression determining which templates and interfaces to include (default: *)
                                        + Environment variable: SCRIBE_FILTER_CONTRACTS
                                        + System property:      filter.contracts
  --oauth-clientid string              Client's identifier (optional)
                                        + Environment variable: SCRIBE_OAUTH_CLIENTID
                                        + System property:      oauth.clientId
  --oauth-proxy-url uri                Proxy server URL (optional)
                                        + Environment variable: SCRIBE_OAUTH_PROXY_URL
                                        + System property:      oauth.proxy.url
  --oauth-proxy-password string        Proxy server password (optional)
                                        + Environment variable: SCRIBE_OAUTH_PROXY_PASSWORD
                                        + System property:      oauth.proxy.password
  --oauth-proxy-user string            Proxy server username (optional)
                                        + Environment variable: SCRIBE_OAUTH_PROXY_USER
                                        + System property:      oauth.proxy.user
  --oauth-accesstoken string           Access token (optional)
                                        + Environment variable: SCRIBE_OAUTH_ACCESSTOKEN
                                        + System property:      oauth.accessToken
  --oauth-scope [enum | string]        Token scope (default: Default)
                                        + Environment variable: SCRIBE_OAUTH_SCOPE
                                        + System property:      oauth.scope
                                        + Enumeration values:   Default, None
  --oauth-parameters map               Custom parameters
                                        + Environment variable: SCRIBE_OAUTH_PARAMETERS
                                        + System property:      oauth.parameters
  --oauth-preemptexpiry string         The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)
                                        + Environment variable: SCRIBE_OAUTH_PREEMPTEXPIRY
                                        + System property:      oauth.preemptExpiry
  --oauth-cafile file                  Trusted Certificate Authority (CA) certificate (optional)
                                        + Environment variable: SCRIBE_OAUTH_CAFILE
                                        + System property:      oauth.cafile
  --oauth-endpoint uri                 Token endpoint URL (optional)
                                        + Environment variable: SCRIBE_OAUTH_ENDPOINT
                                        + System property:      oauth.endpoint
  --oauth-issuer uri                   OIDC-compliant issuer URL (optional)
                                        + Environment variable: SCRIBE_OAUTH_ISSUER
                                        + System property:      oauth.issuer
  --oauth-clientsecret string          Client's secret (optional)
                                        + Environment variable: SCRIBE_OAUTH_CLIENTSECRET
                                        + System property:      oauth.clientSecret
"""
    },
    verify(App, Array("datastore", "postgres-document", "prune", HelpFlag)) {
      """Usage: scribe datastore postgres-document prune [OPTIONS]

Prune transactions to a given offset inclusively

Options:
  --config file                       Path to configuration overrides via an external HOCON file (optional)
  --prune-target string               Inclusive boundary up to which to prune. Can be an offset, timestamp (ISO 8601) or duration (ISO 8601)
  --prune-mode enum                   Precomputes effects of pruning through dry-run, or actually runs it (default: DryRun)
  --logger-destination file           Log output file (default: output.log)
  --logger-mappings map               Custom mappings for log levels
  --logger-format enum                Log output format (default: Plain)
  --logger-pattern [enum | string]    Log pattern (default: Plain)
  --logger-level enum                 Log level (default: Info)
  --postgres-host string              Postgres host (default: localhost)
  --postgres-probeinterval string     Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)
  --postgres-appname string           Application name for Postgres connections (default: scribe)
  --postgres-buffersize int           Buffer size for transactions processing (default: 128)
  --postgres-tls-mode enum            SSL mode required for Postgres connectivity (default: Disable)
  --postgres-tls-cert file            Client's certificate (optional)
  --postgres-tls-key file             Client's private key (optional)
  --postgres-tls-cafile file          Trusted Certificate Authority (CA) certificate (optional)
  --postgres-keepalive boolean        Enable/disable TCP keep-alive probe (default: true)
  --postgres-maxconnections int       Maximum number of JDBC connections (default: 16)
  --postgres-password string          Postgres user password
  --postgres-username string          Postgres user name
  --postgres-schema string            Postgres schema (default: public)
  --postgres-database string          Postgres database (default: postgres)
  --postgres-port int                 Postgres port (default: 5432)
"""
    },
    verify(App, Array("datastore", "postgres-document", "prune", HelpVerboseFlag)) {
      """Usage: scribe datastore postgres-document prune [OPTIONS]

Prune transactions to a given offset inclusively

Options:
  --config file                       Path to configuration overrides via an external HOCON file (optional)
                                       + Environment variable: SCRIBE_CONFIG
                                       + System property:      config
  --prune-target string               Inclusive boundary up to which to prune. Can be an offset, timestamp (ISO 8601) or duration (ISO 8601)
                                       + Environment variable: SCRIBE_PRUNE_TARGET
                                       + System property:      prune.target
  --prune-mode enum                   Precomputes effects of pruning through dry-run, or actually runs it (default: DryRun)
                                       + Environment variable: SCRIBE_PRUNE_MODE
                                       + System property:      prune.mode
                                       + Enumeration values:   DryRun, Force
  --logger-destination file           Log output file (default: output.log)
                                       + Environment variable: SCRIBE_LOGGER_DESTINATION
                                       + System property:      logger.destination
  --logger-mappings map               Custom mappings for log levels
                                       + Environment variable: SCRIBE_LOGGER_MAPPINGS
                                       + System property:      logger.mappings
  --logger-format enum                Log output format (default: Plain)
                                       + Environment variable: SCRIBE_LOGGER_FORMAT
                                       + System property:      logger.format
                                       + Enumeration values:   Plain, PlainAsync, Json, JsonAsync
  --logger-pattern [enum | string]    Log pattern (default: Plain)
                                       + Environment variable: SCRIBE_LOGGER_PATTERN
                                       + System property:      logger.pattern
                                       + Enumeration values:   Plain, Standard, Structured
  --logger-level enum                 Log level (default: Info)
                                       + Environment variable: SCRIBE_LOGGER_LEVEL
                                       + System property:      logger.level
                                       + Enumeration values:   All, Fatal, Error, Warning, Info, Debug, Trace, None
  --postgres-host string              Postgres host (default: localhost)
                                       + Environment variable: SCRIBE_POSTGRES_HOST
                                       + System property:      postgres.host
  --postgres-probeinterval string     Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)
                                       + Environment variable: SCRIBE_POSTGRES_PROBEINTERVAL
                                       + System property:      postgres.probeInterval
  --postgres-appname string           Application name for Postgres connections (default: scribe)
                                       + Environment variable: SCRIBE_POSTGRES_APPNAME
                                       + System property:      postgres.appName
  --postgres-buffersize int           Buffer size for transactions processing (default: 128)
                                       + Environment variable: SCRIBE_POSTGRES_BUFFERSIZE
                                       + System property:      postgres.bufferSize
  --postgres-tls-mode enum            SSL mode required for Postgres connectivity (default: Disable)
                                       + Environment variable: SCRIBE_POSTGRES_TLS_MODE
                                       + System property:      postgres.tls.mode
                                       + Enumeration values:   Disable, Require, VerifyCA, VerifyFull
  --postgres-tls-cert file            Client's certificate (optional)
                                       + Environment variable: SCRIBE_POSTGRES_TLS_CERT
                                       + System property:      postgres.tls.cert
  --postgres-tls-key file             Client's private key (optional)
                                       + Environment variable: SCRIBE_POSTGRES_TLS_KEY
                                       + System property:      postgres.tls.key
  --postgres-tls-cafile file          Trusted Certificate Authority (CA) certificate (optional)
                                       + Environment variable: SCRIBE_POSTGRES_TLS_CAFILE
                                       + System property:      postgres.tls.cafile
  --postgres-keepalive boolean        Enable/disable TCP keep-alive probe (default: true)
                                       + Environment variable: SCRIBE_POSTGRES_KEEPALIVE
                                       + System property:      postgres.keepAlive
  --postgres-maxconnections int       Maximum number of JDBC connections (default: 16)
                                       + Environment variable: SCRIBE_POSTGRES_MAXCONNECTIONS
                                       + System property:      postgres.maxConnections
  --postgres-password string          Postgres user password
                                       + Environment variable: SCRIBE_POSTGRES_PASSWORD
                                       + System property:      postgres.password
  --postgres-username string          Postgres user name
                                       + Environment variable: SCRIBE_POSTGRES_USERNAME
                                       + System property:      postgres.username
  --postgres-schema string            Postgres schema (default: public)
                                       + Environment variable: SCRIBE_POSTGRES_SCHEMA
                                       + System property:      postgres.schema
  --postgres-database string          Postgres database (default: postgres)
                                       + Environment variable: SCRIBE_POSTGRES_DATABASE
                                       + System property:      postgres.database
  --postgres-port int                 Postgres port (default: 5432)
                                       + Environment variable: SCRIBE_POSTGRES_PORT
                                       + System property:      postgres.port
"""
    },
    verify(App, Array("pipeline", HelpFlag)) {
      List(
        "Usage: scribe pipeline SOURCE TARGET [OPTIONS]",
        "",
        "Initiate continuous ledger data export",
        "",
        "Available sources:",
        "  ledger    Daml ledger",
        "",
        "Available targets:",
        "  postgres-document    Postgres database (w/ document payload representation)",
        "",
        "Options:",
        paddedOptionLine(
          "  --config file",
          "Path to configuration overrides via an external HOCON file (optional)"
        ),
        paddedOptionLine(
          "  --pipeline-datasource enum",
          "Ledger API service to use as data source (default: TransactionStream)"
        ),
        paddedOptionLine("  --pipeline-oauth-clientid string", "Client's identifier (optional)"),
        paddedOptionLine("  --pipeline-oauth-proxy-url uri", "Proxy server URL (optional)"),
        paddedOptionLine("  --pipeline-oauth-proxy-password string", "Proxy server password (optional)"),
        paddedOptionLine("  --pipeline-oauth-proxy-user string", "Proxy server username (optional)"),
        paddedOptionLine("  --pipeline-oauth-accesstoken string", "Access token (optional)"),
        paddedOptionLine("  --pipeline-oauth-scope [enum | string]", "Token scope (default: Default)"),
        paddedOptionLine("  --pipeline-oauth-parameters map", "Custom parameters"),
        paddedOptionLine(
          "  --pipeline-oauth-preemptexpiry string",
          "The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)"
        ),
        paddedOptionLine(
          "  --pipeline-oauth-cafile file",
          "Trusted Certificate Authority (CA) certificate (optional)"
        ),
        paddedOptionLine("  --pipeline-oauth-endpoint uri", "Token endpoint URL (optional)"),
        paddedOptionLine("  --pipeline-oauth-issuer uri", "OIDC-compliant issuer URL (optional)"),
        paddedOptionLine("  --pipeline-oauth-clientsecret string", "Client's secret (optional)"),
        paddedOptionLine(
          "  --pipeline-filter-parties string",
          "Filter expression determining Daml party identifiers to filter on (default: *)"
        ),
        paddedOptionLine(
          "  --pipeline-filter-metadata string",
          "Filter expression determining which templates and interfaces to capture metadata for (default: !*)"
        ),
        paddedOptionLine(
          "  --pipeline-filter-contracts string",
          "Filter expression determining which templates and interfaces to include (default: *)"
        ),
        paddedOptionLine(
          s"  --pipeline-ledger-start [enum | $offsetScalaType]",
          "Start offset (default: Latest)"
        ),
        paddedOptionLine(
          s"  --pipeline-ledger-stop [enum | $offsetScalaType]",
          "Stop offset (default: Never)"
        ),
        paddedOptionLine(
          "  --retry-backoff-base string",
          "Base time (ISO 8601) for backoff retry strategy (default: PT1S)"
        ),
        paddedOptionLine(
          "  --retry-backoff-cap string",
          "Max duration (ISO 8601) between attempts (default: PT1M)"
        ),
        paddedOptionLine(
          "  --retry-backoff-factor double",
          "Factor for backoff retry strategy (default: 2.0)"
        ),
        paddedOptionLine("  --retry-counter-attempts int", "Max attempts before giving up (optional)"),
        paddedOptionLine(
          "  --retry-counter-reset string",
          "Reset retry counters after period (ISO 8601) of stability (default: PT10M)"
        ),
        paddedOptionLine(
          "  --retry-counter-duration string",
          "Time limit (ISO 8601) before giving up (optional)"
        ),
        paddedOptionLine(
          "  --health-address string",
          "Hostname or IP to bind HTTP health info service to (default: 127.0.0.1)"
        ),
        paddedOptionLine(
          "  --health-port int",
          "HTTP port to use to expose application health info (default: 8080)"
        ),
        paddedOptionLine("  --logger-level enum", "Log level (default: Info)"),
        paddedOptionLine("  --logger-mappings map", "Custom mappings for log levels"),
        paddedOptionLine("  --logger-format enum", "Log output format (default: Plain)"),
        paddedOptionLine("  --logger-pattern [enum | string]", "Log pattern (default: Plain)"),
        paddedOptionLine("  --target-postgres-host string", "Postgres host (default: localhost)"),
        paddedOptionLine(
          "  --target-postgres-probeinterval string",
          "Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)"
        ),
        paddedOptionLine(
          "  --target-postgres-appname string",
          "Application name for Postgres connections (default: scribe)"
        ),
        paddedOptionLine(
          "  --target-postgres-buffersize int",
          "Buffer size for transactions processing (default: 128)"
        ),
        paddedOptionLine(
          "  --target-postgres-tls-mode enum",
          "SSL mode required for Postgres connectivity (default: Disable)"
        ),
        paddedOptionLine("  --target-postgres-tls-cert file", "Client's certificate (optional)"),
        paddedOptionLine("  --target-postgres-tls-key file", "Client's private key (optional)"),
        paddedOptionLine(
          "  --target-postgres-tls-cafile file",
          "Trusted Certificate Authority (CA) certificate (optional)"
        ),
        paddedOptionLine(
          "  --target-postgres-keepalive boolean",
          "Enable/disable TCP keep-alive probe (default: true)"
        ),
        paddedOptionLine(
          "  --target-postgres-maxconnections int",
          "Maximum number of JDBC connections (default: 16)"
        ),
        paddedOptionLine("  --target-postgres-password string", "Postgres user password"),
        paddedOptionLine("  --target-postgres-username string", "Postgres user name"),
        paddedOptionLine("  --target-postgres-schema string", "Postgres schema (default: public)"),
        paddedOptionLine("  --target-postgres-database string", "Postgres database (default: postgres)"),
        paddedOptionLine("  --target-postgres-port int", "Postgres port (default: 5432)"),
        paddedOptionLine(
          "  --target-encoding-numericasstring boolean",
          "Encode numeric as string instead of JSON number (default: true)"
        ),
        paddedOptionLine(
          "  --target-encoding-excludenulls boolean",
          "Omit trailing fields with NULL values from resulting JSON (default: false)"
        ),
        paddedOptionLine(
          "  --target-encoding-int64asstring boolean",
          "Encode int64 as string instead of JSON number (default: true)"
        ),
        paddedOptionLine(
          "  --target-schema-autoapply boolean",
          "Apply metadata inferred schema on startup (default: true)"
        ),
        paddedOptionLine(
          "  --target-schema-baseline boolean",
          "Baseline existing database schema during apply (default: false)"
        ),
        paddedOptionLine("  --source-ledger-host string", "Ledger API host (default: localhost)"),
        paddedOptionLine("  --source-ledger-cachedir file", "Cache Directory (default: /tmp/scribe)"),
        paddedOptionLine("  --source-ledger-buffersize int", "Buffer size for gRPC channel (default: 128)"),
        paddedOptionLine(
          "  --source-ledger-keepalive-time string",
          "Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)"
        ),
        paddedOptionLine(
          "  --source-ledger-keepalive-timeout string",
          "Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)"
        ),
        paddedOptionLine("  --source-ledger-auth enum", "Authorisation mode (default: NoAuth)"),
        paddedOptionLine(
          "  --source-ledger-tls-cafile file",
          "Trusted Certificate Authority (CA) certificate (optional)"
        ),
        paddedOptionLine(
          "  --source-ledger-tls-cert file",
          "Client's certificate (leave empty if embedded into private key file) (optional)"
        ),
        paddedOptionLine(
          "  --source-ledger-tls-key file",
          "Client's private key (leave empty for server-only TLS) (optional)"
        ),
        paddedOptionLine("  --source-ledger-port int", "Ledger API port (default: 6865)"),
        ""
      ).mkString("\n")
    },
    verify(App, Array("pipeline", HelpVerboseFlag)) {
      List(
        "Usage: scribe pipeline SOURCE TARGET [OPTIONS]",
        "",
        "Initiate continuous ledger data export",
        "",
        "Available sources:",
        "  ledger    Daml ledger",
        "",
        "Available targets:",
        "  postgres-document    Postgres database (w/ document payload representation)",
        "",
        "Options:",
        paddedOptionLine("  --config file", "Path to configuration overrides via an external HOCON file (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_CONFIG"),
        paddedOptionLine("", " + System property:      config"),
        paddedOptionLine(
          "  --pipeline-datasource enum",
          "Ledger API service to use as data source (default: TransactionStream)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_DATASOURCE"),
        paddedOptionLine("", " + System property:      pipeline.datasource"),
        paddedOptionLine("", " + Enumeration values:   TransactionStream, TransactionTreeStream"),
        paddedOptionLine("  --pipeline-oauth-clientid string", "Client's identifier (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_CLIENTID"),
        paddedOptionLine("", " + System property:      pipeline.oauth.clientId"),
        paddedOptionLine("  --pipeline-oauth-proxy-url uri", "Proxy server URL (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_PROXY_URL"),
        paddedOptionLine("", " + System property:      pipeline.oauth.proxy.url"),
        paddedOptionLine("  --pipeline-oauth-proxy-password string", "Proxy server password (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_PROXY_PASSWORD"),
        paddedOptionLine("", " + System property:      pipeline.oauth.proxy.password"),
        paddedOptionLine("  --pipeline-oauth-proxy-user string", "Proxy server username (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_PROXY_USER"),
        paddedOptionLine("", " + System property:      pipeline.oauth.proxy.user"),
        paddedOptionLine("  --pipeline-oauth-accesstoken string", "Access token (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_ACCESSTOKEN"),
        paddedOptionLine("", " + System property:      pipeline.oauth.accessToken"),
        paddedOptionLine("  --pipeline-oauth-scope [enum | string]", "Token scope (default: Default)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_SCOPE"),
        paddedOptionLine("", " + System property:      pipeline.oauth.scope"),
        paddedOptionLine("", " + Enumeration values:   Default, None"),
        paddedOptionLine("  --pipeline-oauth-parameters map", "Custom parameters"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_PARAMETERS"),
        paddedOptionLine("", " + System property:      pipeline.oauth.parameters"),
        paddedOptionLine(
          "  --pipeline-oauth-preemptexpiry string",
          "The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_PREEMPTEXPIRY"),
        paddedOptionLine("", " + System property:      pipeline.oauth.preemptExpiry"),
        paddedOptionLine(
          "  --pipeline-oauth-cafile file",
          "Trusted Certificate Authority (CA) certificate (optional)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_CAFILE"),
        paddedOptionLine("", " + System property:      pipeline.oauth.cafile"),
        paddedOptionLine("  --pipeline-oauth-endpoint uri", "Token endpoint URL (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_ENDPOINT"),
        paddedOptionLine("", " + System property:      pipeline.oauth.endpoint"),
        paddedOptionLine("  --pipeline-oauth-issuer uri", "OIDC-compliant issuer URL (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_ISSUER"),
        paddedOptionLine("", " + System property:      pipeline.oauth.issuer"),
        paddedOptionLine("  --pipeline-oauth-clientsecret string", "Client's secret (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_OAUTH_CLIENTSECRET"),
        paddedOptionLine("", " + System property:      pipeline.oauth.clientSecret"),
        paddedOptionLine(
          "  --pipeline-filter-parties string",
          "Filter expression determining Daml party identifiers to filter on (default: *)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_FILTER_PARTIES"),
        paddedOptionLine("", " + System property:      pipeline.filter.parties"),
        paddedOptionLine(
          "  --pipeline-filter-metadata string",
          "Filter expression determining which templates and interfaces to capture metadata for (default: !*)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_FILTER_METADATA"),
        paddedOptionLine("", " + System property:      pipeline.filter.metadata"),
        paddedOptionLine(
          "  --pipeline-filter-contracts string",
          "Filter expression determining which templates and interfaces to include (default: *)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_FILTER_CONTRACTS"),
        paddedOptionLine("", " + System property:      pipeline.filter.contracts"),
        paddedOptionLine(s"  --pipeline-ledger-start [enum | $offsetScalaType]", "Start offset (default: Latest)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_LEDGER_START"),
        paddedOptionLine("", " + System property:      pipeline.ledger.start"),
        paddedOptionLine("", " + Enumeration values:   Genesis, Oldest, Latest"),
        paddedOptionLine(s"  --pipeline-ledger-stop [enum | $offsetScalaType]", "Stop offset (default: Never)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_PIPELINE_LEDGER_STOP"),
        paddedOptionLine("", " + System property:      pipeline.ledger.stop"),
        paddedOptionLine("", " + Enumeration values:   Latest, Never"),
        paddedOptionLine(
          "  --retry-backoff-base string",
          "Base time (ISO 8601) for backoff retry strategy (default: PT1S)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_RETRY_BACKOFF_BASE"),
        paddedOptionLine("", " + System property:      retry.backoff.base"),
        paddedOptionLine("  --retry-backoff-cap string", "Max duration (ISO 8601) between attempts (default: PT1M)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_RETRY_BACKOFF_CAP"),
        paddedOptionLine("", " + System property:      retry.backoff.cap"),
        paddedOptionLine("  --retry-backoff-factor double", "Factor for backoff retry strategy (default: 2.0)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_RETRY_BACKOFF_FACTOR"),
        paddedOptionLine("", " + System property:      retry.backoff.factor"),
        paddedOptionLine("  --retry-counter-attempts int", "Max attempts before giving up (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_RETRY_COUNTER_ATTEMPTS"),
        paddedOptionLine("", " + System property:      retry.counter.attempts"),
        paddedOptionLine(
          "  --retry-counter-reset string",
          "Reset retry counters after period (ISO 8601) of stability (default: PT10M)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_RETRY_COUNTER_RESET"),
        paddedOptionLine("", " + System property:      retry.counter.reset"),
        paddedOptionLine("  --retry-counter-duration string", "Time limit (ISO 8601) before giving up (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_RETRY_COUNTER_DURATION"),
        paddedOptionLine("", " + System property:      retry.counter.duration"),
        paddedOptionLine(
          "  --health-address string",
          "Hostname or IP to bind HTTP health info service to (default: 127.0.0.1)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_HEALTH_ADDRESS"),
        paddedOptionLine("", " + System property:      health.address"),
        paddedOptionLine("  --health-port int", "HTTP port to use to expose application health info (default: 8080)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_HEALTH_PORT"),
        paddedOptionLine("", " + System property:      health.port"),
        paddedOptionLine("  --logger-level enum", "Log level (default: Info)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_LOGGER_LEVEL"),
        paddedOptionLine("", " + System property:      logger.level"),
        paddedOptionLine("", " + Enumeration values:   All, Fatal, Error, Warning, Info, Debug, Trace, None"),
        paddedOptionLine("  --logger-mappings map", "Custom mappings for log levels"),
        paddedOptionLine("", " + Environment variable: SCRIBE_LOGGER_MAPPINGS"),
        paddedOptionLine("", " + System property:      logger.mappings"),
        paddedOptionLine("  --logger-format enum", "Log output format (default: Plain)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_LOGGER_FORMAT"),
        paddedOptionLine("", " + System property:      logger.format"),
        paddedOptionLine("", " + Enumeration values:   Plain, Json"),
        paddedOptionLine("  --logger-pattern [enum | string]", "Log pattern (default: Plain)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_LOGGER_PATTERN"),
        paddedOptionLine("", " + System property:      logger.pattern"),
        paddedOptionLine("", " + Enumeration values:   Plain, Standard, Structured"),
        paddedOptionLine("  --target-postgres-host string", "Postgres host (default: localhost)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_HOST"),
        paddedOptionLine("", " + System property:      target.postgres.host"),
        paddedOptionLine(
          "  --target-postgres-probeinterval string",
          "Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_PROBEINTERVAL"),
        paddedOptionLine("", " + System property:      target.postgres.probeInterval"),
        paddedOptionLine(
          "  --target-postgres-appname string",
          "Application name for Postgres connections (default: scribe)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_APPNAME"),
        paddedOptionLine("", " + System property:      target.postgres.appName"),
        paddedOptionLine(
          "  --target-postgres-buffersize int",
          "Buffer size for transactions processing (default: 128)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_BUFFERSIZE"),
        paddedOptionLine("", " + System property:      target.postgres.bufferSize"),
        paddedOptionLine(
          "  --target-postgres-tls-mode enum",
          "SSL mode required for Postgres connectivity (default: Disable)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_TLS_MODE"),
        paddedOptionLine("", " + System property:      target.postgres.tls.mode"),
        paddedOptionLine("", " + Enumeration values:   Disable, Require, VerifyCA, VerifyFull"),
        paddedOptionLine("  --target-postgres-tls-cert file", "Client's certificate (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_TLS_CERT"),
        paddedOptionLine("", " + System property:      target.postgres.tls.cert"),
        paddedOptionLine("  --target-postgres-tls-key file", "Client's private key (optional)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_TLS_KEY"),
        paddedOptionLine("", " + System property:      target.postgres.tls.key"),
        paddedOptionLine(
          "  --target-postgres-tls-cafile file",
          "Trusted Certificate Authority (CA) certificate (optional)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_TLS_CAFILE"),
        paddedOptionLine("", " + System property:      target.postgres.tls.cafile"),
        paddedOptionLine(
          "  --target-postgres-keepalive boolean",
          "Enable/disable TCP keep-alive probe (default: true)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_KEEPALIVE"),
        paddedOptionLine("", " + System property:      target.postgres.keepAlive"),
        paddedOptionLine(
          "  --target-postgres-maxconnections int",
          "Maximum number of JDBC connections (default: 16)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_MAXCONNECTIONS"),
        paddedOptionLine("", " + System property:      target.postgres.maxConnections"),
        paddedOptionLine("  --target-postgres-password string", "Postgres user password"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_PASSWORD"),
        paddedOptionLine("", " + System property:      target.postgres.password"),
        paddedOptionLine("  --target-postgres-username string", "Postgres user name"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_USERNAME"),
        paddedOptionLine("", " + System property:      target.postgres.username"),
        paddedOptionLine("  --target-postgres-schema string", "Postgres schema (default: public)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_SCHEMA"),
        paddedOptionLine("", " + System property:      target.postgres.schema"),
        paddedOptionLine("  --target-postgres-database string", "Postgres database (default: postgres)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_DATABASE"),
        paddedOptionLine("", " + System property:      target.postgres.database"),
        paddedOptionLine("  --target-postgres-port int", "Postgres port (default: 5432)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_POSTGRES_PORT"),
        paddedOptionLine("", " + System property:      target.postgres.port"),
        paddedOptionLine(
          "  --target-encoding-numericasstring boolean",
          "Encode numeric as string instead of JSON number (default: true)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_ENCODING_NUMERICASSTRING"),
        paddedOptionLine("", " + System property:      target.encoding.numericAsString"),
        paddedOptionLine(
          "  --target-encoding-excludenulls boolean",
          "Omit trailing fields with NULL values from resulting JSON (default: false)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_ENCODING_EXCLUDENULLS"),
        paddedOptionLine("", " + System property:      target.encoding.excludeNulls"),
        paddedOptionLine(
          "  --target-encoding-int64asstring boolean",
          "Encode int64 as string instead of JSON number (default: true)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_ENCODING_INT64ASSTRING"),
        paddedOptionLine("", " + System property:      target.encoding.int64AsString"),
        paddedOptionLine(
          "  --target-schema-autoapply boolean",
          "Apply metadata inferred schema on startup (default: true)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_SCHEMA_AUTOAPPLY"),
        paddedOptionLine("", " + System property:      target.schema.autoApply"),
        paddedOptionLine(
          "  --target-schema-baseline boolean",
          "Baseline existing database schema during apply (default: false)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_TARGET_SCHEMA_BASELINE"),
        paddedOptionLine("", " + System property:      target.schema.baseline"),
        paddedOptionLine("  --source-ledger-host string", "Ledger API host (default: localhost)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_HOST"),
        paddedOptionLine("", " + System property:      source.ledger.host"),
        paddedOptionLine("  --source-ledger-cachedir file", "Cache Directory (default: /tmp/scribe)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_CACHEDIR"),
        paddedOptionLine("", " + System property:      source.ledger.cacheDir"),
        paddedOptionLine("  --source-ledger-buffersize int", "Buffer size for gRPC channel (default: 128)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_BUFFERSIZE"),
        paddedOptionLine("", " + System property:      source.ledger.bufferSize"),
        paddedOptionLine(
          "  --source-ledger-keepalive-time string",
          "Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_KEEPALIVE_TIME"),
        paddedOptionLine("", " + System property:      source.ledger.keepAlive.time"),
        paddedOptionLine(
          "  --source-ledger-keepalive-timeout string",
          "Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_KEEPALIVE_TIMEOUT"),
        paddedOptionLine("", " + System property:      source.ledger.keepAlive.timeout"),
        paddedOptionLine("  --source-ledger-auth enum", "Authorisation mode (default: NoAuth)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_AUTH"),
        paddedOptionLine("", " + System property:      source.ledger.auth"),
        paddedOptionLine("", " + Enumeration values:   OAuth, NoAuth"),
        paddedOptionLine(
          "  --source-ledger-tls-cafile file",
          "Trusted Certificate Authority (CA) certificate (optional)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_TLS_CAFILE"),
        paddedOptionLine("", " + System property:      source.ledger.tls.cafile"),
        paddedOptionLine(
          "  --source-ledger-tls-cert file",
          "Client's certificate (leave empty if embedded into private key file) (optional)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_TLS_CERT"),
        paddedOptionLine("", " + System property:      source.ledger.tls.cert"),
        paddedOptionLine(
          "  --source-ledger-tls-key file",
          "Client's private key (leave empty for server-only TLS) (optional)"
        ),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_TLS_KEY"),
        paddedOptionLine("", " + System property:      source.ledger.tls.key"),
        paddedOptionLine("  --source-ledger-port int", "Ledger API port (default: 6865)"),
        paddedOptionLine("", " + Environment variable: SCRIBE_SOURCE_LEDGER_PORT"),
        paddedOptionLine("", " + System property:      source.ledger.port"),
        ""
      ).mkString("\n")
    }
  )

  private def paddedOptionLine(option: String, description: String, width: Int = 47): String =
    option.padTo(width, ' ') + description

  private def verify(appName: String, args: Array[String], expectedExitCode: ExitCode = ExitCode.success)(
      expectedOutput: String
  )(implicit sl: SourceLocation) =
    test(args.prepended(appName).mkString(" ")) {
      TestConsole.silent {
        for
          exitCode <- Main.run(args)
          output   <- TestConsole.output.map(_.mkString(java.lang.System.lineSeparator))
        yield assertTrue(
          exitCode == expectedExitCode,
          output == expectedOutput
        )
      }
    }

end NavigationHelpSpec
