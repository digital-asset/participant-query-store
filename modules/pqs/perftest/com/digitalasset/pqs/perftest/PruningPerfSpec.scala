// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import zio.ZIO
import zio.jdbc.*

import scala.language.implicitConversions
import zio.jdbc.SqlFragment

final case class PruningOutput(contracts: Long, exercises: Long, events: Long, transactions: Long)

object PruningPerfSpec extends SharedHydratedLedgerTest with QueryPerfSpec[PruningOutput]:
  private val steps = Seq((0.005, 128), (0.01, 64), (0.05, 16), (0.10, 8), (0.25, 4), (0.5, 2))
  def spec = suite("PruningPerfSpec")(
    perfTest("prune_archive_to_offset") {
      for
        lastIx <- Database.getLastIx
        queries <- ZIO.foreach(steps) { (step, repeat) =>
          Database.getOffset((lastIx * step).toLong).map { offset =>
            val prettyStep = f"${step * 100}%.1f"
            Query(
              s"prune_${prettyStep}_percent",
              sql"select deleted_contracts, deleted_exercises, deleted_events, deleted_transactions from prune_archived_to_offset($offset)",
              repeat
            )
          }
        }
      yield Benchmark(queries*)
    }
  )

  override protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, PruningOutput] =
    Postgres
      .query {
        for
          result <- sql.query[(Long, Long, Long, Long)].selectOne
          _      <- ZIO.serviceWithZIO[ZConnection](_.access(_.rollback()))
        yield result
      }
      .someOrFailException
      .map((contracts, exercises, events, transactions) => PruningOutput(contracts, exercises, events, transactions))
