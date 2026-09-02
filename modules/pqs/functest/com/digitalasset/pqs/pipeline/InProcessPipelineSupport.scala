// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.auth.Config as AuthConfig
import com.digitalasset.canonical.ContractFilter
import com.digitalasset.canonical.MetadataFilter
import com.digitalasset.pqs.configuration.Secret
import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.health
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.postgres.document.DocumentPostgres
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.daml.Ledger
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.schema.IdentifierFilter
import com.digitalasset.transcode.schema.Schema
import com.digitalasset.zio.daml.Config as DamlConfig
import com.digitalasset.zio.daml.DamlSchema
import com.digitalasset.zio.daml.FileCache
import com.digitalasset.zio.daml.KeepAlive
import com.digitalasset.zio.daml.TlsConfig as DamlTlsConfig
import zio.Scope
import zio.ZIO
import zio.ZLayer

import java.io.File
import zio.test.ZTestLogger

/** This helper enables running the pipeline in-process.
  *
  * This is contrary to the standard way in works functest - where pqs is run in docker.
  */
object InProcessPipelineSupport:
  def runPipelineWithLogCapture(
      pipelineConfig: pipeline.Config,
      healthConfig: health.Config = health.Config()
  ) = for {
    beforeLogs <- ZTestLogger.logOutput
    _          <- runPipeline(pipelineConfig, healthConfig)
    afterLogs  <- ZTestLogger.logOutput
  } yield afterLogs.drop(beforeLogs.size)

  def runPipeline(
      pipelineConfig: pipeline.Config,
      healthConfig: health.Config = health.Config()
  ): ZIO[
    Scope & Docker & Service[Ledger] & Postgres & Database,
    Throwable,
    Unit
  ] =
    for
      _ <- ZIO.logInfo(
        s"Starting in-process pipeline execution via Main.execute"
      )
      configPipeline <- buildConfigPipeline(pipelineConfig, healthConfig)
      destinationLayer = buildDestinationLayer
      configLayer      = ZLayer.succeed(configPipeline)
      _ <- Main.execute(destinationLayer, configLayer, withTelemetry = false)
      _ <- ZIO.logInfo("In-process pipeline execution completed")
    yield ()

  private def buildConfigPipeline(
      pipelineConfig: pipeline.Config,
      healthConfig: health.Config
  ): ZIO[
    Scope & Docker & Service[Ledger] & Postgres & Database,
    Throwable,
    Main.ConfigPipeline
  ] =
    for
      cantonInfo <- Docker.inspect[Ledger]
      pgInfo     <- ZIO.service[Postgres]
      dbName     <- ZIO.service[Database]
      ca         <- Docker.certificateAuthority
      clientCert <- ca.generate("pqs-inprocess-client")

      caCertFile     <- ZIO.attemptBlocking(os.temp(ca.certificate.crt))
      clientKeyFile  <- ZIO.attemptBlocking(os.temp(clientCert.certificate.pem))
      clientCertFile <- ZIO.attemptBlocking(os.temp(clientCert.certificate.crt))

      pgClientCert <- ca.generate("postgres-inprocess-client")
      pgCaCertFile <- ZIO.acquireRelease(
        ZIO.attemptBlocking(os.temp(ca.certificate.crt))
      )(f => ZIO.attemptBlocking(os.remove(f)).ignore)
      pgClientKeyFile <- ZIO.acquireRelease(
        ZIO.attemptBlocking(os.temp(pgClientCert.certificate.der))
      )(f => ZIO.attemptBlocking(os.remove(f)).ignore)
      pgClientCertFile <- ZIO.acquireRelease(
        ZIO.attemptBlocking(os.temp(pgClientCert.certificate.crt))
      )(f => ZIO.attemptBlocking(os.remove(f)).ignore)

      damlConfig = DamlConfig(
        host = cantonInfo.exposedAddress,
        port = cantonInfo.exposedPorts(Ledger.participantPort),
        tls = DamlTlsConfig(
          caCertificate = Some(caCertFile.toIO),
          privateKey = Some(clientKeyFile.toIO),
          certificate = Some(clientCertFile.toIO)
        ),
        auth = AuthConfig.AuthMode.NoAuth,
        keepAlive = KeepAlive(),
        bufferSize = 128,
        cacheDir = File("/tmp/pqs-test-cache")
      )

      postgresConfig = backend.PostgresConfig(
        host = pgInfo.exposedAddress,
        port = pgInfo.exposedPorts(Postgres.port),
        database = dbName.name,
        username = "postgres",
        password = Secret("postgres"),
        tls = backend.TlsConfig(
          mode = backend.TlsConfig.SslMode.VerifyCA,
          caCertificate = Some(pgCaCertFile.toIO),
          certificate = Some(pgClientCertFile.toIO),
          privateKey = Some(pgClientKeyFile.toIO)
        ),
        appName = "pqs-inprocess-test"
      )

      _ <- ZIO.logInfo(
        s"Built ConfigPipeline: Canton=${cantonInfo.exposedAddress}:${cantonInfo.exposedPorts(Ledger.participantPort)}, Postgres=${pgInfo.exposedAddress}:${pgInfo.exposedPorts(Postgres.port)}/${dbName.name}"
      )
    yield Main.ConfigPipeline(
      pipeline = pipelineConfig,
      source = Main.ConfigSource(ledger = damlConfig),
      target = Main.ConfigTarget(
        postgres = postgresConfig,
        schema = backend.SchemaConfig(),
        encoding = backend.EncodingConfig()
      ),
      logger = com.digitalasset.pqs.logging.ConsoleLogging.Config(),
      health = healthConfig,
      retry = Retry.Config(
        backoff = Retry.Backoff(),
        counter = Retry.Counter()
      )
    )

  private def buildDestinationLayer: ZLayer[
    Schema & Main.ConfigPipeline,
    Throwable,
    com.digitalasset.pqs.backend.Datastore
  ] =
    val c = ZLayer.service[Main.ConfigPipeline]
    c.project(_.target.postgres)
      >+> backend.instanceId
      >+> backend.connectionPool
      >+> c.project(_.pipeline.filter.contracts)
      >+> DamlSchema.produce(SqlSchema)
      >+> DamlSchema.produce(JsonCodec())
      >+> c.project(_.target.schema)
      >>> DocumentPostgres.live

  private def contractFilterLayer: ZLayer[Any, Nothing, ContractFilter] =
    ZLayer.succeed(ContractFilter(IdentifierFilter.AcceptAll))

  private def metadataFilterLayer: ZLayer[Any, Nothing, MetadataFilter] =
    ZLayer.succeed(MetadataFilter(IdentifierFilter.RejectAll))

  private def fileCacheLayer: ZLayer[Any, Throwable, FileCache] =
    ZLayer.fromZIO {
      for
        cacheDir <- ZIO.attempt(os.Path(File("/tmp/pqs-test-cache")))
        _        <- ZIO.attempt(os.makeDir.all(cacheDir))
        semaphores <- zio.Ref.Synchronized.make(
          Map.empty[String, zio.Semaphore]
        )
      yield new FileCache(cacheDir, semaphores)
    }

  private def postgresConfigLayer(
      database: String
  ): ZLayer[Docker & Postgres, Throwable, backend.PostgresConfig] =
    ZLayer.scoped {
      for
        pgInfo <- ZIO.service[Postgres]
        ca     <- Docker.certificateAuthority
        cert   <- ca.generate("postgresclient")
        rootCert <- ZIO.acquireRelease(
          ZIO.attemptBlocking(os.temp(ca.certificate.crt))
        )(f => ZIO.attemptBlocking(os.remove(f)).ignore)
        sslPem <- ZIO.acquireRelease(
          ZIO.attemptBlocking(os.temp(cert.certificate.der))
        )(f => ZIO.attemptBlocking(os.remove(f)).ignore)
        sslCrt <- ZIO.acquireRelease(
          ZIO.attemptBlocking(os.temp(cert.certificate.crt))
        )(f => ZIO.attemptBlocking(os.remove(f)).ignore)
        _ <- ZIO.logDebug(
          s"In-process PG connection: ${pgInfo.exposedAddress}:${pgInfo.exposedPorts(Postgres.port)}"
        )
      yield backend.PostgresConfig(
        host = pgInfo.exposedAddress,
        port = pgInfo.exposedPorts(Postgres.port),
        database = database,
        username = "postgres",
        password = Secret("postgres"),
        tls = backend.TlsConfig(
          mode = backend.TlsConfig.SslMode.VerifyCA,
          caCertificate = Some(rootCert.toIO),
          certificate = Some(sslCrt.toIO),
          privateKey = Some(sslPem.toIO)
        ),
        appName = "pqs-inprocess-test"
      )
    }
