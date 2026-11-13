// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

import com.digitalasset.pqs.configuration.Secret
import com.digitalasset.pqs.docker.*
import com.digitalasset.pqs.functest.FTEnv
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.postgres.backend.{PostgresConfig, TlsConfig}
import com.digitalasset.pqs.utils.safeequals.*
import zio.*
import zio.ZIO.{acquireRelease, attemptBlocking, logDebug}
import zio.jdbc.*
import zio.jdbc.SqlFragment.Segment.Syntax

import scala.collection.mutable

/** Docker Postgres Service */
sealed class Postgres(
    val service: Service[Postgres],
    val adminDatabase: Database
):
  export service.*

object Postgres:
  val port = 5432

  // Snapshot of the Docker image checksums
  private val imageChecksums = Map(
    "14" -> "sha256:9e279cc7fc6e908da071befe389c576bd6752dbd295a9c078f96a75bab03e54c",
    "15" -> "sha256:bcab099bfaab33333a73a2ebe8c1d615c9f4c2402dd43452f989a36c6da9a5ba",
    "16" -> "sha256:be01cf82fc7dbba824acf0a82e150b4b360f3ff93c6631d7844af431e841a95c",
    "17" -> "sha256:0af65001d05296a2ead57ac4a6412433d8913d1bb5d0c88435a7d1e1ee5cb04b",
    "18" -> "sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4"
  )

  ////////////
  // Layers //
  ////////////

  val database: ZLayer[Docker & Postgres, Throwable, Database] = ZLayer
    .scoped(
      for
        postgres  <- ZIO.service[Postgres]
        dbCounter <- Docker.share("db_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
        dbName = s"ft_db_$dbCounter"
        _ <- acquireRelease(createDatabase(dbName))(_ =>
          dropDatabase(dbName).catchAllCause(ZIO.logWarningCause(s"Failed to drop database [$dbName]", _))
        )
        _ <- logDebug(s"Using database [$dbName]")
      yield initDatabase(dbName, postgres.service)
    )
    .flatten

  /** Docker Instances Service wrapped into Layer */
  val instance: ZLayer[FTEnv & Docker, Throwable, Postgres] = startInstance(Seq.empty)

  /** Start a fresh Postgres instance and create some empty databases */
  def instance(databases: Seq[String]): ZLayer[FTEnv & Docker, Throwable, Postgres] =
    Postgres.startInstance().tap { env =>
      ZIO.foreachDiscard(databases)(createDatabase).provideEnvironment(env)
    }

  /** Restores a Postgres instance from dump files.
    */
  def restore(
      dumps: Seq[Dump],
      workMemoryMb: Long,
      limits: ContainerLimits = ContainerLimits.unlimited
  ): ZLayer[FTEnv & Docker, Throwable, Postgres] =
    Postgres
      .startInstance(
        extraVolumes = dumps.map(dump => dump.file -> dump.containerPath),
        workMemoryMb = Some(workMemoryMb),
        limits = limits
      )
      .tap { env =>
        ZIO.foreachDiscard(dumps) { dump =>
          createDatabase(dump.databaseName).provideEnvironment(env)
            *> env
              .get[Postgres]
              .exec(
                "pg_restore",
                "--username=postgres",
                s"--dbname=${dump.databaseName}",
                "--no-owner",
                "--no-privileges",
                "--jobs",
                "4",
                dump.containerPath.toString
              )
              .tapSome {
                case res if res.exitCode =/= ExitCode.success =>
                  ZIO.fail(RuntimeException(s"pg_restore failed with exit code ${res.exitCode.code}: ${res.stdErr}"))
              }
        }
      }

  def vacuumAnalyzeAll: ZIO[Postgres, Throwable, ExecResult] =
    ZIO.service[Postgres].flatMap(_.exec("vacuumdb", "--username=postgres", "--all", "--analyze", "--jobs", "4"))

  private def startInstance(
      extraVolumes: Seq[(os.Path, os.Path)] = Seq.empty,
      workMemoryMb: Option[Long] = None,
      limits: ContainerLimits = ContainerLimits.unlimited
  ): ZLayer[FTEnv & Docker, Throwable, Postgres] = ZLayer
    .fromZIO(
      for
        cnt <- Docker.share("pg_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
        hostname = s"postgres$cnt"
        ca      <- Docker.certificateAuthority
        cert    <- ca.generate(hostname, Seq(hostname, "localhost"))
        version <- ZIO.service[FTEnv].map(_.config.postgresVersion)
        checksum <- ZIO
          .fromOption(imageChecksums.get(version))
          .orElseFail(RuntimeException(s"missing checksum for postgres:$version"))
      yield Docker
        .service[Postgres](
          image = s"postgres:$version@$checksum",
          exposePorts = Set(port),
          env = Map("POSTGRES_PASSWORD" -> "postgres"),
          prepopulateFiles = Seq(
            os.root / "tls" / "root-ca.crt"  -> ca.certificate.crt,
            os.root / "tls" / "postgres.pem" -> cert.certificate.pem,
            os.root / "tls" / "postgres.crt" -> cert.certificate.crt,
            os.root / "tls" / "pg_hba.conf" ->
              """# TYPE  DATABASE        USER            ADDRESS                 METHOD
                |local   all             all                                     trust
                |hostssl postgres        postgres        all                     md5
                |hostssl all             all             all                     md5 clientcert=verify-ca
                |""".stripMargin
          ),
          volumes = extraVolumes,
          hostname = Some(hostname),
          user = Some(999),
          suppressOutput = true,
          limits = limits
        )(
          "-c",
          "ssl=on",
          "-c",
          "ssl_ca_file=/tls/root-ca.crt",
          "-c",
          "ssl_cert_file=/tls/postgres.crt",
          "-c",
          "ssl_key_file=/tls/postgres.pem",
          "-c",
          "hba_file=/tls/pg_hba.conf",
          "-c",
          "max_connections=200",
          // TODO (#883) Comment and fix leaking connections
          "-c",
          "tcp_keepalives_idle=10",
          "-c",
          "tcp_keepalives_interval=5",
          "-c",
          "tcp_keepalives_count=2",
          workMemoryMb.fold(Seq.empty)(workMem => Seq("-c", s"work_mem=${workMem}MB"))
        )
        .tap(_.get.blockUntilStdErr(_.contains("database system is ready to accept connections")))
    )
    .flatten
    .project(svc => initDatabase("postgres", svc).project(Postgres(svc, _)))
    .flatten

  /** Dumps the schema (DDL only, no data/roles/ownership) of the currently connected database to the given local file.
    * Useful to eyeball how the schema looks like once all Flyway migrations have been applied.
    *
    * Returns the SQL schema dump (DDL only, no data/roles/ownership) of the currently connected PQS database as text,
    * by running `pg_dump` inside the already running Postgres container (the equivalent of `docker exec`).
    */
  def dumpSchema: ZIO[Postgres & Database, Throwable, String] =
    for
      pg     <- ZIO.service[Postgres]
      dbName <- ZIO.serviceWith[Database](_.name)
      result <- pg.exec(
        "pg_dump",
        "--username=postgres",
        s"--dbname=$dbName",
        // Use a dummy restrict-key tag to ensure determinism in tests
        "--restrict-key=onlyfortestingpqs",
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "--no-comments"
      )
      _ <- ZIO
        .fail(RuntimeException(s"pg_dump failed with exit code ${result.exitCode.code}: ${result.stdErr}"))
        .unless(result.exitCode === ExitCode.success)
    yield result.stdOut

  /** Dumps a database schema and data using Postgres custom format. */
  def dumpDatabase(dump: Dump): ZIO[Postgres, Throwable, Unit] =
    for
      pg <- ZIO.service[Postgres]
      tmpFile = os.root / "tmp" / s"${dump.databaseName}.dump"
      result <- pg.exec(
        "pg_dump",
        "--username=postgres",
        s"--dbname=${dump.databaseName}",
        "--format=custom",
        "--no-owner",
        "--no-privileges",
        s"--file=$tmpFile"
      )
      _ <- ZIO
        .fail(RuntimeException(s"pg_dump failed with exit code ${result.exitCode.code}: ${result.stdErr}"))
        .unless(result.exitCode === ExitCode.success)
      // TODO(perf): this materializes the whole dump in memory. Consider streaming or using a bind-mount.
      bytes <- pg.getFileContents(tmpFile)
      _ <- attemptBlocking {
        if os.exists(dump.file) then os.write.over(dump.file, bytes)
        else os.write(dump.file, bytes, createFolders = true)
      }
    yield ()

  private def createDatabase(name: String): ZIO[Postgres, Throwable, Unit] =
    ZIO.service[Postgres].flatMap(_.adminDatabase.autoCommit(sql"""create database "${Syntax(name)}"""".execute))

  // `force` terminates connections still open, e.g. from a PQS container
  private def dropDatabase(name: String): ZIO[Postgres, Throwable, Unit] =
    ZIO
      .service[Postgres]
      .flatMap(_.adminDatabase.autoCommit(sql"""drop database if exists "${Syntax(name)}" with (force)""".execute))

  private def initDatabase(dbName: String, svc: Service[?]): ZLayer[Docker, Throwable, Database] =
    ZLayer
      .scoped(
        for
          ca   <- Docker.certificateAuthority
          cert <- ca.generate("postgresclient")
          rootCert <- acquireRelease(attemptBlocking(os.temp(ca.certificate.crt)))(f =>
            attemptBlocking(os.remove(f)).ignore
          )
          sslPem <- acquireRelease(attemptBlocking(os.temp(cert.certificate.der)))(f =>
            attemptBlocking(os.remove(f)).ignore
          )
          sslCrt <- acquireRelease(attemptBlocking(os.temp(cert.certificate.crt)))(f =>
            attemptBlocking(os.remove(f)).ignore
          )
          config = PostgresConfig(
            host = svc.exposedAddress,
            port = svc.exposedPorts(port),
            database = dbName,
            username = "postgres",
            password = Secret("postgres"),
            tls = TlsConfig(
              mode = TlsConfig.SslMode.VerifyCA,
              caCertificate = Some(rootCert.toIO),
              certificate = Some(sslCrt.toIO),
              privateKey = Some(sslPem.toIO)
            ),
            appName = "pqs-functest"
          )
          _ <- logDebug(s"Creating PG connection pool @${config.host}:${config.port}")
        yield ZLayer.succeed(config) ++ backend.instanceId
        // `.fresh` prevents ZLayer memoization
          >+> backend.connectionPool.fresh
          >>> ZLayer.fromFunction((instanceId, connectionPool) => Database(dbName, config, instanceId, connectionPool))
      )
      .flatten

  private implicit val rowDecoder: JdbcDecoder[Row] = (ix, rs) =>
    val cells = mutable.Buffer.empty[Cell]
    for ix <- 1 to rs.getMetaData.getColumnCount do {
      val value = rs.getMetaData.getColumnType(ix) match
        case java.sql.Types.BOOLEAN => rs.getBoolean(ix)
        case java.sql.Types.BIT     => rs.getBoolean(ix)
        case java.sql.Types.VARCHAR => rs.getString(ix)
        case java.sql.Types.INTEGER => rs.getInt(ix)
        case java.sql.Types.BIGINT  => rs.getLong(ix)
        case other                  => rs.getString(ix)
      cells.append(Cell(value))
    }
    (rs.getMetaData.getColumnCount, Row(cells.toSeq))

end Postgres
