// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.jdbc.{ZConnection, sqlInterpolator}
import zio.test.*

import scala.language.implicitConversions
import zio.Duration

object PruningPerfSpec extends SharedHydratedLedgerTest:
  private val steps = Seq(0.005, 0.01, 0.05, 0.10)

  def spec = suite("PruningPerfSpec")(
    funcTest("pruning perf") {

      val baselineStats = Capture[Seq[PruningStats]]
      Given:
        Postgres.database
          >+> Pqs36.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        runBenchmark.is(baselineStats.capture)

      When:
        Postgres.database
          >+> Pqs.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect(2.minutes):
        for
          stats <- runBenchmark
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
              s"Pruning by ${stats.step.percent} (${stats.duration}) is ${diff.percent} slower than baseline (${baseline.duration})"
            )
          }
          TestResult.allSuccesses(results)
    }
  )

  private final case class PruningOutput(contracts: Long, exercises: Long, events: Long, transactions: Long)
  private final case class PruningStats(step: Double, duration: Duration, output: PruningOutput):
    override def toString =
      import output.*
      s"Pruning ${step.percent} ($duration): $contracts contracts, $exercises exercises, $events events, $transactions transactions"

  private def runBenchmark =
    for
      lastIx <- getLastIx
      stats <- ZIO.foreach(steps) { step =>
        ZIO.logInfo(s"Benchmark pruning by ${step.percent}")
          *> getOffset((lastIx * step).toLong)
            .flatMap(prune(_).repeatN(9).timed)
            .map((duration, output) => PruningStats(step, duration, output))
      }
    yield stats

  private def getLastIx =
    Postgres.get[Long](sql"select ix from __transactions order by ix desc limit 1")

  private def getOffset(ix: Long) =
    Postgres.get[Long](sql"""select "offset" from __transactions where ix=$ix""")

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
