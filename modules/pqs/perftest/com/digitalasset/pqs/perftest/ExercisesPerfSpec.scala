// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.jdbc.{SqlFragment, sqlInterpolator}
import zio.test.*

import scala.language.implicitConversions

object ExercisesPerfSpec extends SharedHydratedLedgerTest:
  def spec = suite("ExercisesPerfSpec")(
    funcTest("exercises and lookup_exercises") {
      val baselineStats = Capture[Seq[ExercisesStats]]
      Given:
        Postgres.database
          >+> Pqs37.runPipeline(
            "--pipeline-datasource=TransactionTreeStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect(5.minutes):
        runBenchmark.is(baselineStats.capture)

      When:
        Pqs.applySchema
      Expect(5.minutes):
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
              s"${stats.name} (${stats.duration.render}) is ${diff.percent} slower than baseline (${baseline.duration.render})"
            )
          }
          TestResult.allSuccesses(results)
    }
  )

  private final case class ExercisesOutput(rows: Long, withSignatories: Long, ixSum: Long)
  private final case class ExercisesStats(name: String, duration: Duration, output: ExercisesOutput):
    override def toString =
      import output.*
      s"$name ($duration): $rows rows, $withSignatories with signatories, ix checksum $ixSum"

  private def runBenchmark =
    for
      _      <- Postgres.call(sql"analyze")
      lastIx <- getLastIx
      from   <- getOffset((lastIx * 0.4).toLong)
      to     <- getOffset((lastIx * 0.6).toLong)
      seedId <- Postgres.get[String](sql"select contract_id from creates('Blob:Seed') order by contract_id limit 1")
      benchmarks = Seq(
        ("exercises()", 5, sql"exercises()"),
        ("exercises(CreateMany)", 200, sql"exercises('Blob:Seed:CreateMany')"),
        ("exercises() in 40-60% range", 5, sql"exercises(null, $from, $to)"),
        ("lookup_exercises(seed)", 400, sql"lookup_exercises($seedId)"),
        ("lookup_exercises(seed, ArchiveMany)", 1000, sql"lookup_exercises($seedId, 'Blob:Seed:ArchiveMany')")
      )
      stats <- ZIO.foreach(benchmarks) { (name, runs, call) =>
        ZIO.logInfo(s"Benchmark $name")
          *> summarize(call).repeatN(runs - 1)
          *> summarize(call).repeatN(runs - 1).timed.map((duration, output) => ExercisesStats(name, duration, output))
      }
    yield stats

  private def summarize(call: SqlFragment) =
    Postgres
      .get[(Long, Long, Long)](
        sql"select count(*), count(signatories), coalesce(sum(exercised_at_ix), 0)::int8 from " ++ call
      )
      .map(ExercisesOutput.apply.tupled)
