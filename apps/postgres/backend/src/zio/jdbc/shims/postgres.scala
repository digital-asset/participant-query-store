// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package zio.jdbc.shims

import com.digitalasset.pqs.o11y.metrics.latency
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.postgres.backend.PostgresConfig
import zio.jdbc.*
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.{Exit, Schedule, UIO, ZIO, ZLayer, ZPool}

import java.sql.Connection
import scala.language.implicitConversions

object postgres {
  val jdbcUpGauge = zio.metrics.Metric.gauge("jdbc_conn_pool_up", "JDBC connection pool is up")

  private val connectionIsValidLatency = latency("jdbc_conn_isvalid", "Latency of database connection validation")
  private val connectionCommitLatency  = latency("jdbc_conn_commit", "Latency of database connection commit")
  private val connectionUsageLatency =
    zio.metrics.Metric
      .histogram(
        "jdbc_conn_use",
        "Latency of database connections usage",
        Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
      )
      .contramap[Long](in => in / 1e9)
  private val trackSuccess = connectionUsageLatency.tagged("result", "success")
  private val trackFailure = connectionUsageLatency.tagged("result", "failure")

  class PGRestorableConnection(val underlying: Connection) extends ZConnection.Restorable(underlying: Connection)

  def connectionPool(
      host: String,
      port: Int,
      database: String,
      props: Map[String, String]
  ): ZLayer[PostgresConfig, Throwable, ZConnectionPool] = ZLayer.scoped(for
    config <- ZIO.service[PostgresConfig]
    _      <- ZIO.attempt(Class.forName("org.postgresql.Driver"))
    acquire = ZIO.attemptBlocking {
      val properties = new java.util.Properties
      props.foreachEntry((k, v) => properties.put(k, v))
      val con = java.sql.DriverManager
        .getConnection(s"jdbc:postgresql://${config.host}:${config.port}/${config.database}", properties)
      con.setAutoCommit(false)
      con
    }
    getConn = ZIO.acquireRelease( // acquire
      acquire.retry(Schedule.stop).flatMap(con => ZIO.attempt(ZConnection(PGRestorableConnection(con))))
    )( // release
      _.close.ignoreLogged
    )
    pool <- ZPool.make(getConn, Range(config.maxConnections, config.maxConnections), zio.Duration.Infinity)
    tx = ZLayer.scoped {
      traces.span("acquire connection") {
        for
          start      <- zio.Clock.nanoTime
          connection <- pool.get
          _ <- ZIO.addFinalizerExit { exit =>
            ZIO.ifZIO((connection.isValid() @@ connectionIsValidLatency).orElseSucceed(false))(
              onTrue = exit match {
                case Exit.Success(_) =>
                  for
                    autoCommitMode <- connection.access(_.getAutoCommit).orElseSucceed(true)
                    _ <- ZIO.unless(autoCommitMode) {
                      (connection.access(_.commit())
                        @@ connectionCommitLatency
                        @@ traces.span("commit transaction")).ignoreLogged
                    }
                    _   <- connection.restore
                    end <- zio.Clock.nanoTime
                    _   <- trackSuccess.update(end - start)
                  yield ()
                case Exit.Failure(_) =>
                  for
                    autoCommitMode <- connection.access(_.getAutoCommit).orElseSucceed(true)
                    _              <- ZIO.unless(autoCommitMode)(connection.rollback.ignoreLogged)
                    _              <- connection.restore
                    end            <- zio.Clock.nanoTime
                    _              <- trackFailure.update(end - start)
                  yield ()
              },
              onFalse = pool.invalidate(connection)
            )
          }
        yield connection
      }
    }
    _ <- tx(sql"""select 1""".query[Int].selectOne).someOrFail(Throwable("Connection not ready"))
    _ <- ZIO.acquireRelease(jdbcUpGauge.set(1))(_ => jdbcUpGauge.set(0))

    // periodic database connectivity probe
    _ <- ZIO.when(!config.probeInterval.isZero) {
      val probe = tx(sql"""select 1""".query[Int].selectOne)

      // After a probe failure, drain remaining stale connections from the pool.
      // We iterate maxConnections times, acquiring and testing one connection per iteration.
      // The pool uses a FIFO queue, so under no contention this visits each connection exactly
      // once. Under contention (other fibers also borrowing connections), we may probe some
      // connections more than once — but that is fine: those other fibers are also revalidating
      // their connections, so iterating n times still guarantees every connection is touched at
      // least once. Acquisition failures are handled gracefully (database might be down).
      // Individual connection testing failures don't stop the overall drain operation.
      // This piece is crucial to prevent the pool from repeatedly handing out stale
      // connections after a database outage while there is no ledger data in flight.
      val drainStaleConnections =
        ZIO.foreachDiscard(0 until config.maxConnections) { _ =>
          ZIO.scoped {
            pool.get
              .flatMap { conn =>
                sql"""select 1"""
                  .query[Int]
                  .selectOne
                  .provide(ZLayer.succeed(conn))
                  .foldCauseZIO(
                    cause =>
                      ZIO.logWarning(s"Connection test query failed: ${cause.squash.getMessage}") *>
                        pool.invalidate(conn),
                    _ => ZIO.logDebug("Connection test query succeeded")
                  )
              }
              // If can't acquire, skip
              .catchAll(e => ZIO.logDebug(s"Could not acquire connection from pool: ${e.getMessage}"))
          }
        }

      probe
        .foldCauseZIO(
          cause =>
            jdbcUpGauge.set(0) *>
              ZIO.logWarning(
                s"Database probe (select 1) failed: ${cause.squash.getMessage}; draining stale connections"
              ) *>
              drainStaleConnections,
          _ =>
            jdbcUpGauge.set(1) *>
              ZIO.logInfo("Database probe (select 1) successful")
        )
        .repeat(Schedule.spaced(config.probeInterval))
        .forkScoped
    }
  yield new ZConnectionPool {
    def transaction: ZLayer[Any, Throwable, ZConnection] = tx
    def invalidate(conn: ZConnection): UIO[Any]          = pool.invalidate(conn)
  })
}
