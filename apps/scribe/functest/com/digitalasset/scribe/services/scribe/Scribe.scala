// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.scribe

import com.digitalasset.scribe.docker
import com.digitalasset.scribe.docker.*
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.postgres.document.Prune
import com.digitalasset.scribe.services.daml.{DeployedDar, Ledger, Parties}
import com.digitalasset.scribe.services.o11y.Collector
import com.digitalasset.scribe.services.oauth.OAuth
import com.digitalasset.scribe.services.postgres.*
import com.digitalasset.scribe.utils.safeequals.=/=
import org.semver4j.Semver
import os.Shellable
import zio.stream.ZStream
import zio.test.{TestResult, assert}
import zio.test.Assertion.isRight
import zio.{ExitCode, Tag, ZEnvironment, ZIO, ZLayer, durationInt}
import scala.math.Ordered.orderingToOrdered
import zio.Duration

/** Service representing Scribe data pipeline */
trait Pipeline

object Pipeline:
  val healthPort = 8081
  val allContractsWithoutAdminWorkflows =
    "(* & !(AdminWorkflows:* | AdminWorkflowsWithVacuuming:* | canton-builtin-admin-workflow-ping:*))"

/** Result of execution of a command line interface based command */
case class CliRun(exitCode: ExitCode, io: ZStream[Any, Throwable, StdIO])

object CliRun:
  def fromSvc(svc: ZEnvironment[Service[?]]): ZLayer[Any, Throwable, CliRun] =
    ZLayer.fromZIO(for exitCode <- svc.get.exitCode yield CliRun(exitCode, svc.get.io))

  def fromSvcExpectSuccess(svc: ZEnvironment[Service[?]]): ZLayer[Any, Throwable, CliRun] =
    ZLayer.fromZIO(for
      exitCode <- svc.get.exitCode
      _ <- ZIO.when(exitCode =/= ExitCode.success)(
        svc.get.io
          .collect { case StdErr(line) => line }
          .takeRight(20)
          .runCollect
          .map(_.mkString("\n"))
          .flatMap(stderr =>
            ZIO.fail(
              RuntimeException(
                s"""|Scribe pipeline exited with non-zero exit code: $exitCode
                    |Last stderr:
                    |$stderr""".stripMargin
              )
            )
          )
      )
    yield CliRun(exitCode, svc.get.io))

object Scribe extends Scribe {
  override def version: ScribeVersion = ScribeVersion.Latest
  override def image: String          = localScribeDockerImage
  override def user: Option[Int]      = Some(65532)
}

object Scribe34 extends Scribe {
  override def version: ScribeVersion = ScribeVersion("3.4.6")
  override def image: String     = s"europe-docker.pkg.dev/da-images/public-all/docker/participant-query-store:$version"
  override def user: Option[Int] = Some(1001)
}

trait Scribe {
  def version: ScribeVersion
  def image: String
  def user: Option[Int] = None

  ////////////
  // Layers //
  ////////////

  /** Scribe Pipeline Service wrapped into Layer */
  def pipeline(extraArgs: String*): ZLayer[
    Docker & Parties & Postgres & Database & Service[Ledger] & DeployedDar,
    Throwable,
    Service[Pipeline]
  ] = attemptPipeline(extraArgs*).tap(_.get.blockUntilStdOut(_.contains("Continuing from offset")))

  def attemptPipeline(extraArgs: String*): ZLayer[
    Docker & Parties & Postgres & Database & Service[Ledger] & DeployedDar,
    Throwable,
    Service[Pipeline]
  ] = conf.flatMap(conf =>
    Docker
      .service[Pipeline](
        image = image,
        env = conf.get._1,
        prepopulateFiles = conf.get._2,
        exposePorts = Set(Pipeline.healthPort),
        user = user
      )(
        "pipeline",
        "ledger",
        "postgres-document",
        extraArgs
      )
  )

  def prune(extraArgs: String*): ZLayer[
    Docker & Parties & Postgres & Database & Service[Ledger] & DeployedDar,
    Throwable,
    Service[Prune.Service]
  ] =
    val layer = ZLayer.fromZIO {
      for
        pg                <- ZIO.service[Postgres]
        dbName            <- ZIO.service[Database]
        ca                <- Docker.certificateAuthority
        clientCertificate <- ca.generate("scribe")
        files = Seq(
          os.root / "tls" / "client.pem"  -> clientCertificate.certificate.pem,
          os.root / "tls" / "client.der"  -> clientCertificate.certificate.der,
          os.root / "tls" / "client.crt"  -> clientCertificate.certificate.crt,
          os.root / "tls" / "root-ca.crt" -> ca.certificate.crt
        )
        base = Map(
          "DA_DIAGNOSTICS_ENABLED"     -> "false",
          "SCRIBE_POSTGRES_HOST"       -> pg.container.hostName,
          "SCRIBE_POSTGRES_PORT"       -> Postgres.port,
          "SCRIBE_POSTGRES_DATABASE"   -> dbName.name,
          "SCRIBE_POSTGRES_USERNAME"   -> "postgres",
          "SCRIBE_POSTGRES_PASSWORD"   -> "postgres",
          "SCRIBE_POSTGRES_TLS_MODE"   -> "VerifyFull",
          "SCRIBE_POSTGRES_TLS_KEY"    -> "/tls/client.der",
          "SCRIBE_POSTGRES_TLS_CERT"   -> "/tls/client.crt",
          "SCRIBE_POSTGRES_TLS_CAFILE" -> "/tls/root-ca.crt"
        )
      yield base -> files
    }
    layer.flatMap(env =>
      Docker
        .service[Prune.Service](
          image = localScribeDockerImage,
          env = env.get._1,
          prepopulateFiles = env.get._2
        )(
          "datastore",
          "postgres-document",
          "prune",
          extraArgs
        )
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def scribeVersion: ZIO[Any, Nothing, ScribeVersion] =
    ZIO
      .environment[Any]
      .mapAttempt(_.asInstanceOf[ZEnvironment[ScribeVersion]].get[ScribeVersion])
      .option
      .map(_.getOrElse(ScribeVersion.Latest))

  private val conf = ZLayer.fromZIO(
    for
      ca                <- Docker.certificateAuthority
      clientCertificate <- ca.generate("scribe")
      ledger            <- Docker.inspect[Ledger]
      pg                <- ZIO.service[Postgres]
      oauthInstance     <- Docker.inspectMaybe[OAuth.Instance]
      collectorInstance <- Docker.inspectMaybe[Collector.Instance]
      dbName            <- ZIO.service[Database]
      parties           <- ZIO.service[Parties]
      dar               <- ZIO.service[DeployedDar]
      partyNames        <- ZIO.foreach(parties.get)(_.id)

      base = Map(
        "DA_DIAGNOSTICS_ENABLED"        -> "false",
        "SCRIBE_SOURCE_LEDGER_CACHEDIR" -> "/ft/scribe-cache",
        "SCRIBE_HEALTH_PORT"            -> Pipeline.healthPort,
        "SCRIBE_PIPELINE_FILTER_CONTRACTS" -> (if version.semVer >= Semver.parse("0.4.0")
                                               then dar.dar.packageInfo.map((name, _, _) => s"$name:*").mkString("|")
                                               else dar.dar.packageInfo.map((_, _, id) => s"$id:*").mkString("|")),
        "SCRIBE_SOURCE_LEDGER_HOST"         -> ledger.container.hostName,
        "SCRIBE_SOURCE_LEDGER_PORT"         -> Ledger.participantPort,
        "SCRIBE_TARGET_POSTGRES_HOST"       -> pg.container.hostName,
        "SCRIBE_TARGET_POSTGRES_PORT"       -> Postgres.port,
        "SCRIBE_TARGET_POSTGRES_DATABASE"   -> dbName.name,
        "SCRIBE_TARGET_POSTGRES_USERNAME"   -> "postgres",
        "SCRIBE_TARGET_POSTGRES_PASSWORD"   -> "postgres",
        "SCRIBE_SOURCE_LEDGER_TLS_KEY"      -> "/tls/client.pem",
        "SCRIBE_SOURCE_LEDGER_TLS_CERT"     -> "/tls/client.crt",
        "SCRIBE_TARGET_POSTGRES_TLS_MODE"   -> "VerifyFull",
        "SCRIBE_TARGET_POSTGRES_TLS_KEY"    -> "/tls/client.der",
        "SCRIBE_TARGET_POSTGRES_TLS_CERT"   -> "/tls/client.crt",
        "SCRIBE_SOURCE_LEDGER_TLS_CAFILE"   -> "/tls/root-ca.crt",
        "SCRIBE_TARGET_POSTGRES_TLS_CAFILE" -> "/tls/root-ca.crt",
        "SCRIBE_LOGGER_LEVEL"               -> "Info"
      )

      oauth = oauthInstance.fold(Map("SCRIBE_PIPELINE_FILTER_PARTIES" -> partyNames.mkString("|"))) { oauth =>
        Map(
          "SCRIBE_PIPELINE_OAUTH_CLIENTSECRET" -> "clientsecret",
          "SCRIBE_PIPELINE_OAUTH_ENDPOINT"     -> s"https://${oauth.container.hostName}:${OAuth.port}/issuer1/token",
          "SCRIBE_PIPELINE_OAUTH_CAFILE"       -> "/tls/root-ca.crt",
          "SCRIBE_SOURCE_LEDGER_AUTH"          -> "OAuth"
        )
      }
      telemetry = collectorInstance.fold(Map.empty)(collector =>
        Map(
          "JAVA_TOOL_OPTIONS"                       -> "-javaagent:/agent/otel.jar",
          "OTEL_INSTRUMENTATION_MICROMETER_ENABLED" -> "true",
          "OTEL_SERVICE_NAME"                       -> "scribe",
          "OTEL_TRACES_EXPORTER"                    -> "otlp",
          "OTEL_LOGS_EXPORTER"                      -> "otlp",
          "OTEL_METRICS_EXPORTER"                   -> "otlp",
          "OTEL_METRIC_EXPORT_INTERVAL"             -> "250",
          "OTEL_EXPORTER_OTLP_PROTOCOL"             -> "grpc",
          "OTEL_EXPORTER_OTLP_ENDPOINT" -> s"http://${collector.container.hostName}:${Collector.Instance.otlp}"
        )
      )
    yield (
      base ++ oauth ++ telemetry,
      Seq(
        os.root / "tls" / "client.pem"  -> clientCertificate.certificate.pem,
        os.root / "tls" / "client.der"  -> clientCertificate.certificate.der,
        os.root / "tls" / "client.crt"  -> clientCertificate.certificate.crt,
        os.root / "tls" / "root-ca.crt" -> ca.certificate.crt
      )
    )
  )

  /** Run pipeline and wrap the result into Layer. Fails the layer if the pipeline exits with a non-zero exit code. */
  def runPipeline(extraArgs: String*): ZLayer[
    Docker & Parties & Postgres & Database & Service[Ledger] & DeployedDar,
    Throwable,
    CliRun
  ] = attemptPipeline(extraArgs*).flatMap(CliRun.fromSvcExpectSuccess)

  def runPrune(extraArgs: String*): ZLayer[
    Docker & Parties & Postgres & Database & Service[Ledger] & DeployedDar,
    Throwable,
    CliRun
  ] = prune(extraArgs*).flatMap(CliRun.fromSvc)

  /** Run CLI command in Scribe and wrap the result into Layer */
  def run(cmd: Shellable*): ZLayer[Docker, Throwable, CliRun] =
    Docker.service[Unit](image = localScribeDockerImage)(cmd).flatMap(CliRun.fromSvc)

  val exitCode: ZIO[CliRun | Service[Pipeline], Throwable, ExitCode] =
    ZIO.serviceWithZIO[CliRun | Service[Pipeline]] {
      case s: CliRun            => ZIO.succeed(s.exitCode)
      case p: Service[Pipeline] => p.exitCode
    }
  val io: ZIO[CliRun | Service[Pipeline], Throwable, ZStream[Any, Throwable, StdIO]] =
    ZIO.serviceWith[CliRun | Service[Pipeline]] {
      case s: CliRun            => s.io
      case p: Service[Pipeline] => p.io
    }
  val stdout: ZIO[CliRun | Service[Pipeline], Throwable, String] =
    io.flatMap(_.collect { case StdOut(line) => line }.mkString("", "\n", ""))
  val stderr: ZIO[CliRun | Service[Pipeline], Throwable, String] =
    io.flatMap(_.collect { case StdErr(line) => line }.mkString("", "\n", ""))

  /** Assertion over stdout streams from a service that may still be running.
    *
    * For long-running containers, collecting full stdout (e.g. with `mkString`) can block forever because the stream
    * does not complete. This helper instead looks for a matching line in a bounded time window (up to the execution of
    * this step in the test case + `duration`), so tests can assert on logs without waiting for container shutdown.
    */
  def stdoutContainsWithin(
      substring: String,
      duration: Duration = 1.second
  ): ZIO[Service[Pipeline], Throwable, TestResult] =
    val streamEndedMessage =
      s"Failed to find a line containing '$substring' in stdout because the stream has ended"
    val timeoutMessage =
      s"No stdout line containing '$substring' was observed up to this step within $duration while stdout is still open"

    io.flatMap(
      _.collect {
        case StdOut(line) if line.contains(substring) => ()
      }.runHead
        .map(_.toRight(streamEndedMessage))
        // We give it some time for the stream processor to observe the line and emit it,
        // but if we don't see it within that time, we assume it's not coming and fail the test.
        // This is to avoid hanging tests when the expected line is never emitted in an infinite stream.
        .raceFirst(ZIO.sleep(duration).as(Left(timeoutMessage)))
        // The failure text is coming in Left.
        .map(result => assert(result)(isRight.label(result.left.getOrElse(""))))
    )

  /** Asserts that the pipeline has encountered an unknown package and is retrying to discover it.
    * @param identifier
    *   the full package identifier in the form `packageId:moduleName:entityName`
    */
  def stdoutContainsPackageReload(
      identifier: String,
      duration: Duration = 4.seconds
  ): ZIO[Service[Pipeline], Throwable, TestResult] =
    stdoutContainsWithin(
      s"No package for $identifier was seen on initialization. Retrying to discover new packages.",
      duration
    )

  def hasProcessedAtLeastTransactions(count: Int) = {
    import zio.jdbc.sqlInterpolator

    val checkpoints = Postgres.query(
      sql"select f.ix, l.ix from oldest_checkpoint() f, latest_checkpoint() l".query[(Long, Long)].selectOne
    )
    val difference = zio.test.Assertion
      .assertion("diff") {
        case Some((start: Long, end: Long)) if end - start >= count - 1 => true
        case _                                                          => false
      }
      .label(s"Difference between start and end is at least $count")
    FuncTest.retryUntilTimeout(zio.test.assertZIO(checkpoints)(difference))
  }
}
