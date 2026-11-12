// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.jdbc.{SqlFragment, sqlInterpolator}
import zio.test.*

import scala.language.implicitConversions

object ExercisesPerfSpec extends SharedHydratedLedgerTest:
  def spec = suite("ExercisesPerfSpec")(
    funcTest("exercises and lookup_exercises") {
      val benchmarks    = Capture[Seq[Benchmark]]
      val baselineStats = Capture[Seq[ExercisesStats]]
      Given:
        Postgres.database
          >+> Pqs37.runPipeline(
            "--pipeline-datasource=TransactionTreeStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Then:
        prepareBenchmarks.is(benchmarks.capture)
      Expect(5.minutes):
        runBenchmarks(benchmarks.get).is(baselineStats.capture)

      When:
        Pqs.applySchema
      Expect(5.minutes):
        for
          stats <- runBenchmarks(benchmarks.get)
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
            assert(stats.output)(Assertion.equalTo(baseline.output)).label(s"${stats.name} output")
            && assertTrue(diff < 0.3).label(
              s"${stats.name} (${stats.duration.render}) is ${diff.percent} slower than baseline (${baseline.duration.render})"
            )
            && assertTrue(diff > -0.9).label(
              s"${stats.name} (${stats.duration.render}) is ${(-diff).percent} faster than baseline (${baseline.duration.render})"
            )
          }
          TestResult.allSuccesses(results)
    }
  )

  private final case class ExercisesOutput(rows: Long, withSignatories: Long, checksum: Long)
  private final case class ExercisesStats(name: String, duration: Duration, output: ExercisesOutput):
    override def toString =
      import output.*
      s"$name ($duration): $rows rows, $withSignatories with signatories, offset checksum $checksum"

  private final case class Benchmark(name: String, runs: Int, call: SqlFragment)

  private def prepareBenchmarks =
    for
      lastIx <- Database.getLastIx
      from   <- Database.getOffset((lastIx * 0.4).toLong)
      to     <- Database.getOffset((lastIx * 0.6).toLong)
      seedId <- Postgres.get[String](sql"select contract_id from creates('Blob:Seed') order by contract_id limit 1")
    yield Seq(
      Benchmark("exercises()", 5, sql"exercises()"),
      Benchmark("exercises() all columns", 5, sql"exercises() e where length(e::text) > 0"),
      Benchmark("exercises(CreateMany)", 200, sql"exercises('Blob:Seed:CreateMany')"),
      Benchmark("exercises() in 40-60% range", 5, sql"exercises(null, $from, $to)"),
      Benchmark("lookup_exercises(seed)", 400, sql"lookup_exercises($seedId)"),
      Benchmark("lookup_exercises(seed, ArchiveMany)", 1000, sql"lookup_exercises($seedId, 'Blob:Seed:ArchiveMany')")
    )

  private def runBenchmarks(benchmarks: Seq[Benchmark]) =
    Postgres.vacuumAnalyzeAll
      *> Database.checkpoint
      *> ZIO.foreach(benchmarks) { benchmark =>
        ZIO.logInfo(s"Benchmark ${benchmark.name}")
          *> summarize(benchmark.call).repeatN(benchmark.runs - 1)
          *> summarize(benchmark.call)
            .repeatN(benchmark.runs - 1)
            .timed
            .map((duration, output) => ExercisesStats(benchmark.name, duration, output))
      }

  private def summarize(call: SqlFragment) =
    Postgres
      .get[(Long, Long, Long)](
        sql"select count(*), count(signatories), coalesce(sum(exercised_at_offset), 0)::int8 from " ++ call
      )
      .map(ExercisesOutput.apply.tupled)
