// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.postgres.backend

import com.digitalasset.scribe.configuration.{ISO8601Duration, Secret}
import zio.config.magnolia.{describe, name}
import zio.durationInt

import java.io.File
import scala.language.implicitConversions

case class PostgresConfig(
    @describe("Postgres host")
    host: String = "localhost",
    @describe("Postgres port")
    port: Int = 5432,
    @describe("Postgres database")
    database: String = "postgres",
    @describe("Postgres schema")
    schema: String = "public",
    @describe("Postgres user name")
    username: String,
    @describe("Postgres user password")
    password: Secret,
    @describe("Maximum number of JDBC connections")
    maxConnections: Int = 16,
    @describe("Enable/disable TCP keep-alive probe")
    keepAlive: Boolean = true,
    tls: TlsConfig,
    @describe("Buffer size for transactions processing")
    bufferSize: Int = 128,
    @describe("Application name for Postgres connections")
    appName: String = "scribe",
    @describe("Duration (ISO 8601) of interval between database connectivity probes (PT0S to disable)")
    probeInterval: ISO8601Duration = 30.seconds
)

case class SchemaConfig(
    @describe("Apply metadata inferred schema on startup")
    autoApply: Boolean = true,
    @describe("Baseline existing database schema during apply")
    baseline: Boolean = false
)

case class EncodingConfig(
    @describe("Encode numeric as string instead of JSON number")
    numericAsString: Boolean = true,
    @describe("Encode int64 as string instead of JSON number")
    int64AsString: Boolean = true,
    @describe("Omit trailing fields with NULL values from resulting JSON")
    excludeNulls: Boolean = false
)

case class TlsConfig(
    @describe("SSL mode required for Postgres connectivity")
    mode: TlsConfig.SslMode = TlsConfig.SslMode.Disable,
    @describe("Trusted Certificate Authority (CA) certificate")
    @name("cafile")
    caCertificate: Option[File] = None,
    @describe("Client's private key")
    @name("key")
    privateKey: Option[File] = None,
    @describe("Client's certificate")
    @name("cert")
    certificate: Option[File] = None
)

object TlsConfig:
  sealed trait SslMode
  object SslMode:
    case object Disable    extends SslMode
    case object Require    extends SslMode
    case object VerifyCA   extends SslMode
    case object VerifyFull extends SslMode
  end SslMode
end TlsConfig
