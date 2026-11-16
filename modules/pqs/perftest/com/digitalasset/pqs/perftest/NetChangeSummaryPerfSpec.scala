// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import com.digitalasset.pqs.utils.safeequals.=/=
import zio.*
import zio.jdbc.{SqlFragment, sqlInterpolator}
import zio.test.*

import scala.language.implicitConversions

object NetChangeSummaryPerfSpec extends SharedHydratedLedgerTest:

  def spec = suite("NetChangeSummaryPerfSpec")(
    funcTest("__net_change_summary") {
      val ranges        = Capture[Seq[NetChangeRange]]
      val baselineStats = Capture[Seq[Stats]]
      Given:
        Postgres.database
          >+> Pqs37.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        prepareRanges.is(ranges.capture)
      Expect(5.minutes):
        run("summary_updates", ranges.get).is(baselineStats.capture)

      When:
        Database.freshConnectionPool >+> Pqs.applySchema
      Expect(5.minutes):
        for
          candidateStats <- run("__net_change_summary", ranges.get)
          testResults <- ZIO.foreach(ranges.get.zip(baselineStats.get.zip(candidateStats))) {
            case (range, (baseline, stats)) =>
              val baselineDurationMs  = baseline.duration.toMillis
              val candidateDurationMs = stats.duration.toMillis
              val diff                = (candidateDurationMs.toDouble - baselineDurationMs) / baselineDurationMs
              val message = s"Net change summary ${range.name} " +
                s"(${stats.rows.size} rows) " +
                s"| Candidate: ${stats.duration.dividedBy(range.repeat).render} " +
                s"| Baseline: ${baseline.duration.dividedBy(range.repeat).render} " +
                s"| Diff: ${diff.percent}"
              ZIO
                .logInfo(message)
                .as(
                  assert(stats.rows.net)(Assertion.equalTo(baseline.rows.net)).label(message)
                    && assertTrue(diff < range.expectedDiff + range.tolerance).label(message)
                    && assertTrue(diff > range.expectedDiff - range.tolerance).label(message)
                )
          }
        yield TestResult.allSuccesses(testResults)
    }
  )

  private final case class NetChangeRange(
      name: String,
      fromOffset: Long,
      toOffset: Long,
      repeat: Int,
      expectedDiff: Double,
      tolerance: Double = 0.3
  )

  private final case class ChangeRow(templateFqn: String, payloadType: String, additions: Long, removals: Long)

  extension (rows: Seq[ChangeRow])
    private def net: Map[(String, String), Long] =
      rows
        .map(r => (r.templateFqn, r.payloadType) -> (r.additions - r.removals))
        .filter((_, change) => change =/= 0L)
        .toMap

  private final case class Stats(duration: Duration, rows: Seq[ChangeRow])

  private def prepareRanges =
    for
      lastIx <- Database.getLastIx // 29767
      ix50 = (lastIx * 0.5).toLong
      offset100    <- Database.getOffset(lastIx)
      offset50     <- Database.getOffset(ix50)
      offset10     <- Database.getOffset((lastIx * 0.1).toLong)
      offset40     <- Database.getOffset((lastIx * 0.4).toLong)
      offset60     <- Database.getOffset((lastIx * 0.6).toLong)
      offset50p10  <- Database.getOffset(ix50 + 9)
      offset50p100 <- Database.getOffset(ix50 + 99)
      oldest       <- Database.getOffset(1)
      ranges = Seq(
        // One probe per changed contract; few contracts change at a single offset
        NetChangeRange("single 10%", offset10, offset10, repeat = 1500, expectedDiff = -0.20),
        NetChangeRange("single 50%", offset50, offset50, repeat = 1500, expectedDiff = 0.0),
        NetChangeRange("single 100%", offset100, offset100, repeat = 1500, expectedDiff = 0.57),
        // One probe per changed contract, where summary_updates counts index entries per template
        NetChangeRange("10 transactions at 50%", offset50, offset50p10, repeat = 1500, expectedDiff = 1.64),
        NetChangeRange(
          "100 transactions at 50%",
          offset50,
          offset50p100,
          repeat = 1000,
          expectedDiff = 17.08,
          tolerance = 3.0 // varies by up to 4.5 between runs
        ),
        // One probe per changed contract over a fifth of the ledger
        NetChangeRange(
          "40-60%",
          offset40,
          offset60,
          repeat = 25,
          expectedDiff = 32.43,
          tolerance = 8.0
        ),
        // Nothing is active before the first transaction, so every contract active at the end is probed
        NetChangeRange("oldest to latest", oldest, offset100, repeat = 3, expectedDiff = 8.32, tolerance = 2.0)
      )
      _ <- ZIO.logInfo(
        s"""|Net change summary ranges:
            |  - ${ranges.mkString("\n  - ")}
            |""".stripMargin
      )
    yield ranges

  private def run(function: String, ranges: Seq[NetChangeRange]) =
    Postgres.vacuumAnalyzeAll
      *> Database.checkpoint
      *> ZIO.foreach(ranges) { range =>
        val query = SqlFragment(s"select * from $function(") ++ sql"${range.fromOffset}, ${range.toOffset})"
        val call  = Database.getAll[(String, String, Long, Long)](query).map(_.toSeq.map(ChangeRow.apply.tupled))
        ZIO.logInfo(s"Running $function ${range.name}")
          *> call // warm-up
          *> call.repeatN(range.repeat - 1).timed.map(Stats.apply.tupled)
      }
