// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.auth.{Auth, TokenService}
import com.digitalasset.canonical.{ContractFilter, MetadataFilter, given}
import com.digitalasset.diagnostics.DiagnosticsSocketServer
import com.digitalasset.pqs.app.*
import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.pqs.health.Health
import com.digitalasset.pqs.logging.ConsoleLogging
import com.digitalasset.pqs.pipeline.pipeline.Pipeline
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.postgres.document.{DocumentPostgres, SqlSchema}
import com.digitalasset.pqs.{app, configuration, pipeline}
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.transcode.schema.{Dictionary, Schema}
import com.digitalasset.zio.daml
import com.digitalasset.zio.daml.*
import com.digitalasset.zio.daml.ledgerapi.UnknownDamlPackageException
import fastparse.*
import io.grpc.Status.Code
import org.postgresql.util.{PSQLException, PSQLState}
import zio.*
import zio.jdbc.ZConnectionPool

import java.io.IOException

object Main extends ComposableApp:
  def app =
    "pipeline" @@ Command("Initiate continuous ledger data export")
      - source @@ Param("SOURCE", "Available sources")
      - destination @@ Param("TARGET", "Available targets")
      - cliConfig[ConfigPipeline] `map` ((d, c) => execute(d, c, withTelemetry = true))

  private def source = "ledger" @@ Variant("Daml ledger")

  private def destination =
    destinationPostgresDocument @@ Variant("Postgres database (w/ document payload representation)")

  private def destinationPostgresRelational =
    "postgres-relational" `as`
      ZLayer.fail(Exception("Unimplemented"))

  private def destinationPostgresDocument =
    "postgres-document" `as`
      (
        ZLayer.service[ConfigPipeline].project(_.target.postgres)
          >+> ZLayer.service[ConfigPipeline].project(_.pipeline.filter.contracts)
          >+> DamlSchema.produce(SqlSchema)
          >+> ZLayer
            .service[ConfigPipeline]
            .flatMap { confEnv =>
              val conf = confEnv.get
              DamlSchema.produce(
                JsonCodec(
                  encodeNumericAsString = conf.target.encoding.numericAsString,
                  encodeInt64AsString = conf.target.encoding.int64AsString,
                  removeTrailingNonesInRecords = conf.target.encoding.excludeNulls
                )
              )
            }
            .update(_.matchByPackageId)
      )
      >+> ZLayer.service[ConfigPipeline].project(_.target.schema)
      >+> DocumentPostgres.live

  def execute(
      destinationLayer: ZLayer[Schema & ConfigPipeline & backend.InstanceId & ZConnectionPool, Throwable, Datastore],
      config: ZLayer[Any, Throwable, ConfigPipeline],
      withTelemetry: Boolean
  ) =
    import scala.language.implicitConversions
    val c = ZLayer.service[ConfigPipeline]
    val loggingLayer =
      if withTelemetry then
        config
          .project(_.logger)
          .orElse(ConsoleLogging.default) >>> com.digitalasset.pqs.o11y.opentelemetry.bootstrap
      else
        config
          .project(_.logger)
          .orElse(ConsoleLogging.default)
    ZIO
      .serviceWithZIO[Pipeline](_.run)
      .provideSome[ConfigPipeline & backend.InstanceId & ZConnectionPool & ZManagedChannel & FileCache & Auth](
        c.project(_.pipeline.filter.contracts),
        c.project(_.pipeline.filter.metadata),
        DamlSchema.schema,
        Ledger.live,
        c.project(_.pipeline) >>> Pipeline.layer,
        destinationLayer
      )
      .bootstrap(
        diagnosticsLayer
          >+> loggingLayer
          >+> config.fresh
          >+> com.digitalasset.pqs.appversion.LogVersion
          >+> backend.instanceId
          >+> (c.project(_.target.postgres) >>> backend.connectionPool)
          >+> ((c.project(_.source.ledger.auth) ++ c.project(_.pipeline.oauth)) >>> Auth.live(LedgerScope))
          // TokenService has its own lifecycle management and retry, so we don't need it to be restarted on pipeline retries
          >+> TokenService.live
          >+> (c.project(_.source.ledger) >>> com.digitalasset.zio.daml.Channel.live)
          // FileCache is persistent so cached packages survive pipeline retries (avoids redundant fetches)
          >+> (c.project(_.source.ledger) >>> FileCache.live)
          >+> c.project(_.health)
          >+> Health.live,
        c.project(_.retry) >>> ZLayer.fromFunction((c: Retry.Config) => Retry.retryRecoverable(c)(isRecoverable))
      )

  private lazy val diagnosticsLayer =
    ZLayer.fromZIO(ZIO.attempt(DiagnosticsSocketServer.start(openModules = true)))

  private val isRecoverable: PartialFunction[Throwable, Throwable] =
    case ex: io.grpc.StatusException
        if Set(
          Code.CANCELLED,
          Code.DEADLINE_EXCEEDED,
          Code.NOT_FOUND,
          Code.PERMISSION_DENIED,
          Code.RESOURCE_EXHAUSTED,
          Code.FAILED_PRECONDITION,
          Code.ABORTED,
          Code.INTERNAL,
          Code.UNAVAILABLE,
          Code.DATA_LOSS,
          Code.UNAUTHENTICATED
        ).contains(ex.getStatus.getCode) =>
      Throwable("Recoverable GRPC exception.", ex)

    case ex: PSQLException if !nonRecoverableJdbcStates.contains(ex.getSQLState) =>
      Throwable(s"Recoverable JDBC exception.", ex)

    case ex: IOException =>
      Throwable(s"Recoverable IO Exception.", ex)

    case ex: UnknownDamlPackageException =>
      Throwable("Recoverable: unknown Daml package detected.", ex)
  end isRecoverable

  private val nonRecoverableJdbcStates = Set(
    PSQLState.INVALID_PARAMETER_TYPE,
    PSQLState.PROTOCOL_VIOLATION,
    PSQLState.NOT_IMPLEMENTED,
    PSQLState.INVALID_PARAMETER_VALUE,
    PSQLState.SYNTAX_ERROR,
    PSQLState.UNDEFINED_COLUMN,
    PSQLState.UNDEFINED_OBJECT,
    PSQLState.UNDEFINED_TABLE,
    PSQLState.UNDEFINED_FUNCTION,
    PSQLState.NUMERIC_CONSTANT_OUT_OF_RANGE,
    PSQLState.NUMERIC_VALUE_OUT_OF_RANGE,
    PSQLState.DATA_TYPE_MISMATCH,
    PSQLState.INVALID_NAME,
    PSQLState.CANNOT_COERCE,
    PSQLState.UNEXPECTED_ERROR
  ).map(_.getState) ++ Set(
    "P0001" // raise exception statement
  )

  final case class ConfigSource(ledger: daml.Config)
  final case class ConfigTarget(
      postgres: backend.PostgresConfig,
      schema: backend.SchemaConfig,
      encoding: backend.EncodingConfig
  )
  final case class ConfigPipeline(
      pipeline: com.digitalasset.pqs.pipeline.pipeline.Config,
      source: ConfigSource,
      target: ConfigTarget,
      logger: com.digitalasset.pqs.logging.ConsoleLogging.Config,
      health: com.digitalasset.pqs.health.Config,
      retry: Retry.Config
  )

end Main
