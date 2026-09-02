// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.console

import com.digitalasset.pqs.functest.FuncTestDefault
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.specific.offsetScalaType
import zio.ExitCode

object HelpSpec extends FuncTestDefault:
  def spec = suite("Help")(
    funcTest("help output by default"):
      When:
        Pqs.run()

      Then:
        Pqs.exitCode `is` ExitCode.failure
      And:
        Pqs.stderr `is` "one of pipeline, datastore, -v, --version expected"
      And:
        Pqs.stdout `is`
          """
Usage: pqs COMMAND

An efficient ledger data exporting tool

Commands:
  pipeline     Initiate continuous ledger data export
  datastore    Perform operations supporting a certified data store

Run 'pqs COMMAND --help[-verbose]' for more information on a command."""
    ,
    funcTest("help output"):
      When:
        Pqs `run` "--help"

      Then:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stderr `is` empty
      And:
        Pqs.stdout `is` """Usage: pqs COMMAND

An efficient ledger data exporting tool

Commands:
  pipeline     Initiate continuous ledger data export
  datastore    Perform operations supporting a certified data store

Run 'pqs COMMAND --help[-verbose]' for more information on a command."""
    ,
    funcTest("help output for pipeline command"):
      When:
        Pqs `run` ("pipeline", "--help")

      Then:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stderr `is` empty
      And:
        Pqs.stdout `is` List(
          "Usage: pqs pipeline SOURCE TARGET [OPTIONS]",
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
            "Application name for Postgres connections (default: pqs)"
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
          paddedOptionLine("  --source-ledger-cachedir file", "Cache Directory (default: /tmp/pqs)"),
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
          paddedOptionLine("  --source-ledger-port int", "Ledger API port (default: 6865)")
        ).mkString("\n")
    ,
    funcTest("verbose output for pipeline command"):
      When:
        Pqs `run` ("pipeline", "--help-verbose")

      Then:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stderr `is` empty
      And:
        Pqs.stdout `is` List(
          s"Usage: pqs pipeline SOURCE TARGET [OPTIONS]",
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
          paddedOptionLine("", " + Environment variable: PQS_CONFIG"),
          paddedOptionLine("", " + System property:      config"),
          paddedOptionLine(
            "  --pipeline-datasource enum",
            "Ledger API service to use as data source (default: TransactionStream)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_DATASOURCE"),
          paddedOptionLine("", " + System property:      pipeline.datasource"),
          paddedOptionLine("", " + Enumeration values:   TransactionStream, TransactionTreeStream"),
          paddedOptionLine("  --pipeline-oauth-clientid string", "Client's identifier (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_CLIENTID"),
          paddedOptionLine("", " + System property:      pipeline.oauth.clientId"),
          paddedOptionLine("  --pipeline-oauth-proxy-url uri", "Proxy server URL (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_PROXY_URL"),
          paddedOptionLine("", " + System property:      pipeline.oauth.proxy.url"),
          paddedOptionLine("  --pipeline-oauth-proxy-password string", "Proxy server password (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_PROXY_PASSWORD"),
          paddedOptionLine("", " + System property:      pipeline.oauth.proxy.password"),
          paddedOptionLine("  --pipeline-oauth-proxy-user string", "Proxy server username (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_PROXY_USER"),
          paddedOptionLine("", " + System property:      pipeline.oauth.proxy.user"),
          paddedOptionLine("  --pipeline-oauth-accesstoken string", "Access token (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_ACCESSTOKEN"),
          paddedOptionLine("", " + System property:      pipeline.oauth.accessToken"),
          paddedOptionLine("  --pipeline-oauth-scope [enum | string]", "Token scope (default: Default)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_SCOPE"),
          paddedOptionLine("", " + System property:      pipeline.oauth.scope"),
          paddedOptionLine("", " + Enumeration values:   Default, None"),
          paddedOptionLine("  --pipeline-oauth-parameters map", "Custom parameters"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_PARAMETERS"),
          paddedOptionLine("", " + System property:      pipeline.oauth.parameters"),
          paddedOptionLine(
            "  --pipeline-oauth-preemptexpiry string",
            "The duration (ISO 8601) prior to expiry of current, for a new token to be requested (default: PT1M)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_PREEMPTEXPIRY"),
          paddedOptionLine("", " + System property:      pipeline.oauth.preemptExpiry"),
          paddedOptionLine(
            "  --pipeline-oauth-cafile file",
            "Trusted Certificate Authority (CA) certificate (optional)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_CAFILE"),
          paddedOptionLine("", " + System property:      pipeline.oauth.cafile"),
          paddedOptionLine("  --pipeline-oauth-endpoint uri", "Token endpoint URL (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_ENDPOINT"),
          paddedOptionLine("", " + System property:      pipeline.oauth.endpoint"),
          paddedOptionLine("  --pipeline-oauth-issuer uri", "OIDC-compliant issuer URL (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_ISSUER"),
          paddedOptionLine("", " + System property:      pipeline.oauth.issuer"),
          paddedOptionLine("  --pipeline-oauth-clientsecret string", "Client's secret (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_OAUTH_CLIENTSECRET"),
          paddedOptionLine("", " + System property:      pipeline.oauth.clientSecret"),
          paddedOptionLine(
            "  --pipeline-filter-parties string",
            "Filter expression determining Daml party identifiers to filter on (default: *)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_FILTER_PARTIES"),
          paddedOptionLine("", " + System property:      pipeline.filter.parties"),
          paddedOptionLine(
            "  --pipeline-filter-metadata string",
            "Filter expression determining which templates and interfaces to capture metadata for (default: !*)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_FILTER_METADATA"),
          paddedOptionLine("", " + System property:      pipeline.filter.metadata"),
          paddedOptionLine(
            "  --pipeline-filter-contracts string",
            "Filter expression determining which templates and interfaces to include (default: *)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_FILTER_CONTRACTS"),
          paddedOptionLine("", " + System property:      pipeline.filter.contracts"),
          paddedOptionLine(s"  --pipeline-ledger-start [enum | $offsetScalaType]", "Start offset (default: Latest)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_LEDGER_START"),
          paddedOptionLine("", " + System property:      pipeline.ledger.start"),
          paddedOptionLine("", " + Enumeration values:   Genesis, Oldest, Latest"),
          paddedOptionLine(s"  --pipeline-ledger-stop [enum | $offsetScalaType]", "Stop offset (default: Never)"),
          paddedOptionLine("", " + Environment variable: PQS_PIPELINE_LEDGER_STOP"),
          paddedOptionLine("", " + System property:      pipeline.ledger.stop"),
          paddedOptionLine("", " + Enumeration values:   Latest, Never"),
          paddedOptionLine(
            "  --retry-backoff-base string",
            "Base time (ISO 8601) for backoff retry strategy (default: PT1S)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_RETRY_BACKOFF_BASE"),
          paddedOptionLine("", " + System property:      retry.backoff.base"),
          paddedOptionLine("  --retry-backoff-cap string", "Max duration (ISO 8601) between attempts (default: PT1M)"),
          paddedOptionLine("", " + Environment variable: PQS_RETRY_BACKOFF_CAP"),
          paddedOptionLine("", " + System property:      retry.backoff.cap"),
          paddedOptionLine("  --retry-backoff-factor double", "Factor for backoff retry strategy (default: 2.0)"),
          paddedOptionLine("", " + Environment variable: PQS_RETRY_BACKOFF_FACTOR"),
          paddedOptionLine("", " + System property:      retry.backoff.factor"),
          paddedOptionLine("  --retry-counter-attempts int", "Max attempts before giving up (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_RETRY_COUNTER_ATTEMPTS"),
          paddedOptionLine("", " + System property:      retry.counter.attempts"),
          paddedOptionLine(
            "  --retry-counter-reset string",
            "Reset retry counters after period (ISO 8601) of stability (default: PT10M)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_RETRY_COUNTER_RESET"),
          paddedOptionLine("", " + System property:      retry.counter.reset"),
          paddedOptionLine("  --retry-counter-duration string", "Time limit (ISO 8601) before giving up (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_RETRY_COUNTER_DURATION"),
          paddedOptionLine("", " + System property:      retry.counter.duration"),
          paddedOptionLine(
            "  --health-address string",
            "Hostname or IP to bind HTTP health info service to (default: 127.0.0.1)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_HEALTH_ADDRESS"),
          paddedOptionLine("", " + System property:      health.address"),
          paddedOptionLine("  --health-port int", "HTTP port to use to expose application health info (default: 8080)"),
          paddedOptionLine("", " + Environment variable: PQS_HEALTH_PORT"),
          paddedOptionLine("", " + System property:      health.port"),
          paddedOptionLine("  --logger-level enum", "Log level (default: Info)"),
          paddedOptionLine("", " + Environment variable: PQS_LOGGER_LEVEL"),
          paddedOptionLine("", " + System property:      logger.level"),
          paddedOptionLine("", " + Enumeration values:   All, Fatal, Error, Warning, Info, Debug, Trace, None"),
          paddedOptionLine("  --logger-mappings map", "Custom mappings for log levels"),
          paddedOptionLine("", " + Environment variable: PQS_LOGGER_MAPPINGS"),
          paddedOptionLine("", " + System property:      logger.mappings"),
          paddedOptionLine("  --logger-format enum", "Log output format (default: Plain)"),
          paddedOptionLine("", " + Environment variable: PQS_LOGGER_FORMAT"),
          paddedOptionLine("", " + System property:      logger.format"),
          paddedOptionLine("", " + Enumeration values:   Plain, Json"),
          paddedOptionLine("  --logger-pattern [enum | string]", "Log pattern (default: Plain)"),
          paddedOptionLine("", " + Environment variable: PQS_LOGGER_PATTERN"),
          paddedOptionLine("", " + System property:      logger.pattern"),
          paddedOptionLine("", " + Enumeration values:   Plain, Standard, Structured"),
          paddedOptionLine("  --target-postgres-host string", "Postgres host (default: localhost)"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_HOST"),
          paddedOptionLine("", " + System property:      target.postgres.host"),
          paddedOptionLine(
            "  --target-postgres-probeinterval string",
            "Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable) (default: PT30S)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_PROBEINTERVAL"),
          paddedOptionLine("", " + System property:      target.postgres.probeInterval"),
          paddedOptionLine(
            "  --target-postgres-appname string",
            "Application name for Postgres connections (default: pqs)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_APPNAME"),
          paddedOptionLine("", " + System property:      target.postgres.appName"),
          paddedOptionLine(
            "  --target-postgres-buffersize int",
            "Buffer size for transactions processing (default: 128)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_BUFFERSIZE"),
          paddedOptionLine("", " + System property:      target.postgres.bufferSize"),
          paddedOptionLine(
            "  --target-postgres-tls-mode enum",
            "SSL mode required for Postgres connectivity (default: Disable)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_TLS_MODE"),
          paddedOptionLine("", " + System property:      target.postgres.tls.mode"),
          paddedOptionLine("", " + Enumeration values:   Disable, Require, VerifyCA, VerifyFull"),
          paddedOptionLine("  --target-postgres-tls-cert file", "Client's certificate (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_TLS_CERT"),
          paddedOptionLine("", " + System property:      target.postgres.tls.cert"),
          paddedOptionLine("  --target-postgres-tls-key file", "Client's private key (optional)"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_TLS_KEY"),
          paddedOptionLine("", " + System property:      target.postgres.tls.key"),
          paddedOptionLine(
            "  --target-postgres-tls-cafile file",
            "Trusted Certificate Authority (CA) certificate (optional)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_TLS_CAFILE"),
          paddedOptionLine("", " + System property:      target.postgres.tls.cafile"),
          paddedOptionLine(
            "  --target-postgres-keepalive boolean",
            "Enable/disable TCP keep-alive probe (default: true)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_KEEPALIVE"),
          paddedOptionLine("", " + System property:      target.postgres.keepAlive"),
          paddedOptionLine(
            "  --target-postgres-maxconnections int",
            "Maximum number of JDBC connections (default: 16)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_MAXCONNECTIONS"),
          paddedOptionLine("", " + System property:      target.postgres.maxConnections"),
          paddedOptionLine("  --target-postgres-password string", "Postgres user password"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_PASSWORD"),
          paddedOptionLine("", " + System property:      target.postgres.password"),
          paddedOptionLine("  --target-postgres-username string", "Postgres user name"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_USERNAME"),
          paddedOptionLine("", " + System property:      target.postgres.username"),
          paddedOptionLine("  --target-postgres-schema string", "Postgres schema (default: public)"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_SCHEMA"),
          paddedOptionLine("", " + System property:      target.postgres.schema"),
          paddedOptionLine("  --target-postgres-database string", "Postgres database (default: postgres)"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_DATABASE"),
          paddedOptionLine("", " + System property:      target.postgres.database"),
          paddedOptionLine("  --target-postgres-port int", "Postgres port (default: 5432)"),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_POSTGRES_PORT"),
          paddedOptionLine("", " + System property:      target.postgres.port"),
          paddedOptionLine(
            "  --target-encoding-numericasstring boolean",
            "Encode numeric as string instead of JSON number (default: true)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_ENCODING_NUMERICASSTRING"),
          paddedOptionLine("", " + System property:      target.encoding.numericAsString"),
          paddedOptionLine(
            "  --target-encoding-excludenulls boolean",
            "Omit trailing fields with NULL values from resulting JSON (default: false)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_ENCODING_EXCLUDENULLS"),
          paddedOptionLine("", " + System property:      target.encoding.excludeNulls"),
          paddedOptionLine(
            "  --target-encoding-int64asstring boolean",
            "Encode int64 as string instead of JSON number (default: true)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_ENCODING_INT64ASSTRING"),
          paddedOptionLine("", " + System property:      target.encoding.int64AsString"),
          paddedOptionLine(
            "  --target-schema-autoapply boolean",
            "Apply metadata inferred schema on startup (default: true)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_SCHEMA_AUTOAPPLY"),
          paddedOptionLine("", " + System property:      target.schema.autoApply"),
          paddedOptionLine(
            "  --target-schema-baseline boolean",
            "Baseline existing database schema during apply (default: false)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_TARGET_SCHEMA_BASELINE"),
          paddedOptionLine("", " + System property:      target.schema.baseline"),
          paddedOptionLine("  --source-ledger-host string", "Ledger API host (default: localhost)"),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_HOST"),
          paddedOptionLine("", " + System property:      source.ledger.host"),
          paddedOptionLine("  --source-ledger-cachedir file", "Cache Directory (default: /tmp/pqs)"),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_CACHEDIR"),
          paddedOptionLine("", " + System property:      source.ledger.cacheDir"),
          paddedOptionLine("  --source-ledger-buffersize int", "Buffer size for gRPC channel (default: 128)"),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_BUFFERSIZE"),
          paddedOptionLine("", " + System property:      source.ledger.bufferSize"),
          paddedOptionLine(
            "  --source-ledger-keepalive-time string",
            "Duration (ISO 8601) of interval between ping frames (PT0S to disable) (default: PT40S)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_KEEPALIVE_TIME"),
          paddedOptionLine("", " + System property:      source.ledger.keepAlive.time"),
          paddedOptionLine(
            "  --source-ledger-keepalive-timeout string",
            "Duration (ISO 8601) of timeout for a ping frame to be acknowledged (default: PT20S)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_KEEPALIVE_TIMEOUT"),
          paddedOptionLine("", " + System property:      source.ledger.keepAlive.timeout"),
          paddedOptionLine("  --source-ledger-auth enum", "Authorisation mode (default: NoAuth)"),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_AUTH"),
          paddedOptionLine("", " + System property:      source.ledger.auth"),
          paddedOptionLine("", " + Enumeration values:   OAuth, NoAuth"),
          paddedOptionLine(
            "  --source-ledger-tls-cafile file",
            "Trusted Certificate Authority (CA) certificate (optional)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_TLS_CAFILE"),
          paddedOptionLine("", " + System property:      source.ledger.tls.cafile"),
          paddedOptionLine(
            "  --source-ledger-tls-cert file",
            "Client's certificate (leave empty if embedded into private key file) (optional)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_TLS_CERT"),
          paddedOptionLine("", " + System property:      source.ledger.tls.cert"),
          paddedOptionLine(
            "  --source-ledger-tls-key file",
            "Client's private key (leave empty for server-only TLS) (optional)"
          ),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_TLS_KEY"),
          paddedOptionLine("", " + System property:      source.ledger.tls.key"),
          paddedOptionLine("  --source-ledger-port int", "Ledger API port (default: 6865)"),
          paddedOptionLine("", " + Environment variable: PQS_SOURCE_LEDGER_PORT"),
          paddedOptionLine("", " + System property:      source.ledger.port")
        ).mkString("\n")
  )

  private def paddedOptionLine(option: String, description: String, width: Int = 47): String =
    option.padTo(width, ' ') + description

end HelpSpec
