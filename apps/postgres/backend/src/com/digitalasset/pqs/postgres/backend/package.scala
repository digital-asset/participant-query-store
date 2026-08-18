// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres

import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.postgres.backend.TlsConfig.SslMode
import org.postgresql.PGProperty
import zio.jdbc.*
import zio.stream.{ZPipeline, ZSink}
import zio.{ZIO, ZLayer}

import java.io.File

package object backend:
  /** Execute each transaction in parallel */
  def executePar[A](
      n: Int
  ): ZPipeline[ZConnectionPool & PostgresConfig, Throwable, ZIO[ZConnection, Throwable, A], A] =
    ZPipeline.serviceWithPipeline[PostgresConfig](config =>
      ZPipeline[ZIO[ZConnection, Throwable, A]].mapZIOPar(n) { call =>
        transaction(call) @@ traces.span("execute datastore transaction")
      }
    )

  /** Execute each transaction in parallel in breaking the order downstream */
  def executeParUnordered[A](
      n: Int
  ): ZPipeline[ZConnectionPool & PostgresConfig, Throwable, ZIO[ZConnection, Throwable, A], A] =
    ZPipeline.serviceWithPipeline[PostgresConfig](config =>
      ZPipeline[ZIO[ZConnection, Throwable, A]].mapZIOParUnordered(n) { call =>
        transaction(call) @@ traces.span("execute datastore transaction")
      }
    )

  /** Execute all SQL statements in one large transaction */
  // NB: Don't try to parallelize this because postgres uses one thread per connection and since this is one large
  // transaction it uses only one connection and one thread on server side.
  val executeInSingleTransaction: ZSink[ZConnectionPool, Throwable, ZIO[ZConnection, Throwable, Any], Nothing, Unit] =
    ZSink.unwrapScoped(
      transaction.build.map(connection =>
        ZSink.foreach(identity[ZIO[ZConnection, Throwable, Any]]).provideEnvironment(connection)
      )
    )

  val instanceId: ZLayer[Any, Throwable, InstanceId] = ZLayer.fromZIO(
    ZIO.attempt(InstanceId(java.util.UUID.randomUUID.toString))
  )

  val connectionPool: ZLayer[PostgresConfig & InstanceId, Throwable, ZConnectionPool] = ZLayer.fromZIO {
    for
      conf       <- ZIO.service[PostgresConfig]
      instanceId <- ZIO.service[InstanceId]
    yield zio.jdbc.shims.postgres.connectionPool(
      conf.host,
      conf.port,
      conf.database,
      Map(
        PGProperty.USER.getName             -> conf.username,
        PGProperty.PASSWORD.getName         -> conf.password.value,
        PGProperty.TCP_KEEP_ALIVE.getName   -> conf.keepAlive.toString,
        PGProperty.APPLICATION_NAME.getName -> conf.appName,
        PGProperty.CURRENT_SCHEMA.getName   -> conf.schema
      ) ++ sslprops(conf.tls) ++ instanceIdProp(instanceId)
    )
  }.flatten

  def instanceIdProp(instanceId: InstanceId): Map[String, String] = Map(
    PGProperty.OPTIONS.getName -> s"-c pqs.instance=${instanceId}"
  )

  def sslprops(conf: TlsConfig): Map[String, String] =
    sslmode(conf) ++ sslrootcert(conf) ++ sslcert(conf) ++ sslkey(conf)

  private def sslmode(conf: TlsConfig) =
    Map(
      PGProperty.SSL_MODE.getName -> (conf.mode match
        case SslMode.Disable    => "disable"
        case SslMode.Require    => "require"
        case SslMode.VerifyCA   => "verify-ca"
        case SslMode.VerifyFull => "verify-full"
      )
    )

  private def sslrootcert(conf: TlsConfig) = fromFile(conf.caCertificate, PGProperty.SSL_ROOT_CERT)
  private def sslcert(conf: TlsConfig)     = fromFile(conf.certificate, PGProperty.SSL_CERT)
  private def sslkey(conf: TlsConfig)      = fromFile(conf.privateKey, PGProperty.SSL_KEY)

  private def fromFile(file: Option[File], key: PGProperty) = file.map(f => key.getName -> f.getCanonicalPath)

end backend
