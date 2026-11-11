// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.jdbc.{ZConnection, sqlInterpolator}
import zio.test.*

import scala.language.implicitConversions

object PruningPerfSpec extends SharedHydratedLedgerTest:
  def spec = suite("PruningPerfSpec")(
    funcTest("pruning") {
      val benchmarkQueries = Capture[Seq[PruningQuery]]
      val baselineStats    = Capture[Seq[PruningStats]]
      Given:
        Postgres.database
          >+> Pqs37.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        prepareBenchmark.is(benchmarkQueries.capture)
      Expect(2.minutes):
        runBenchmark(benchmarkQueries.get).is(baselineStats.capture)

      When:
        Pqs.applySchema // schema migration
      Expect(2.minutes):
        for
          stats <- runBenchmark(benchmarkQueries.get)
          _ <- ZIO.logInfo(
            s"""|Baseline stats:
                |  - ${baselineStats.get.mkString("\n  - ")}
                |Candidate stats:
                |  - ${stats.mkString("\n  - ")}
                |""".stripMargin
          )
        yield
          val results = baselineStats.get.zip(stats).map { (baseline, stats) =>
            val baselineDurationMs  = baseline.duration.toMillis
            val candidateDurationMs = stats.duration.toMillis
            val diff                = (candidateDurationMs.toDouble - baselineDurationMs) / baselineDurationMs
            assert(stats.output)(Assertion.equalTo(baseline.output))
            && assertTrue(diff < 0.3).label(
              s"Pruning by ${stats.query.step.percent} (${stats.duration.render}) is ${diff.percent} slower than baseline (${baseline.duration.render})"
            )
            && assertTrue(diff > -0.3).label(
              s"Pruning by ${stats.query.step.percent} (${stats.duration.render}) is ${(-diff).percent} faster than baseline (${baseline.duration.render})"
            )
          }
          TestResult.allSuccesses(results)
    }
  )

  private final case class PruningQuery(step: Double, offset: Long, repeat: Int)

  private final case class PruningOutput(contracts: Long, exercises: Long, events: Long, transactions: Long)
  private final case class PruningStats(query: PruningQuery, duration: Duration, output: PruningOutput):
    override def toString =
      import output.*
      s"Pruning ${query.step.percent} (${duration.render}): $contracts contracts, $exercises exercises, $events events, $transactions transactions"

  private def prepareBenchmark =
    for
      lastIx <- Database.getLastIx
      queries <- ZIO.foreach(Seq((0.005, 128), (0.01, 64), (0.05, 16), (0.10, 8), (0.25, 4), (0.5, 2))) {
        (step, repeat) =>
          Database.getOffset((lastIx * step).toLong).map(offset => PruningQuery(step, offset, repeat))
      }
    yield queries

  private def runBenchmark(queries: Seq[PruningQuery]) =
    for
      _ <- Database.settle
      stats <- ZIO.foreach(queries) { query =>
        ZIO.logInfo(s"Benchmark pruning by ${query.step.percent}")
          *> prune(query.offset)
            .repeatN(query.repeat - 1)
            .timed
            .map((duration, output) => PruningStats(query, duration, output))
      }
    yield stats

  private def prune(offset: Long) =
    Postgres
      .query {
        for
          result <- sql"""select deleted_contracts, deleted_exercises, deleted_events, deleted_transactions
                          from prune_archived_to_offset($offset)"""
            .query[(Long, Long, Long, Long)]
            .selectOne
          _ <- ZIO.serviceWithZIO[ZConnection](_.access(_.rollback()))
        yield result
      }
      .someOrFailException
      .map((contracts, exercises, events, transactions) => PruningOutput(contracts, exercises, events, transactions))
