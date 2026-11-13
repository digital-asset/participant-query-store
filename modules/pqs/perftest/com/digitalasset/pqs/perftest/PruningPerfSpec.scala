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
  private case class PruningStep(ratio: Double, repeat: Int, expectedDiff: Double)
  private val steps = Seq(
    PruningStep(0.005, 128, 0.14),
    PruningStep(0.01, 64, 0.14),
    PruningStep(0.05, 16, 0.11),
    PruningStep(0.10, 8, 0.08),
    PruningStep(0.25, 4, 0.03),
    PruningStep(0.5, 2, 0.05)
  )
  def spec = suite("PruningPerfSpec")(
    perfTest("prune_archive_to_offset") {
      for
        lastIx <- Database.getLastIx
        queries <- ZIO.foreach(steps) {
          case PruningStep(ratio, repeat, expectedDiff) =>
            Database.getOffset((lastIx * ratio).toLong).map { offset =>
              val prettyRatio = f"${ratio * 100}%.1f"
              Query(
                s"prune_${prettyRatio}_percent",
                sql"select deleted_contracts, deleted_exercises, deleted_events, deleted_transactions from prune_archived_to_offset($offset)",
                repeat,
                expectedDiff
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
