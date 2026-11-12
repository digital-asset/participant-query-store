// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.{Ledger, Parties}
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.{Pqs, Pqs37}
import zio.*
import zio.test.*

import zio.jdbc.SqlFragment

trait PerfBenchmarkSpec extends FuncTest[Ledger & Postgres & Parties]:
  def perfTest(name: String)(prepareBenchmark: ZIO[Database, Throwable, PerfBenchmark]) =
    funcTest(name) {
      val benchmark = Capture[PerfBenchmark]
      val baseline  = Capture[Seq[PerfStats]]
      Given:
        Postgres.database
          >+> Pqs37.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        prepareBenchmark.is(benchmark.capture)
      Expect(4.minutes):
        benchmark.get.run.is(baseline.capture)

      When:
        Database.freshConnectionPool >+> Pqs.applySchema
      Expect(4.minutes):
        benchmark.get.runAndVerify(name, baseline.get)
    }

  final case class PerfStats(query: PerfQuery, duration: Duration, rowCount: Long):
    def avgDuration = duration.dividedBy(query.repeat)
    def diffRatio(baseline: PerfStats): Double =
      val baselineDurationMs = baseline.duration.toMillis
      (duration.toMillis.toDouble - baselineDurationMs) / baselineDurationMs

  final case class PerfQuery(tag: String, sql: SqlFragment, repeat: Int, expectedDiff: Double = 0)

  final class PerfBenchmark(queries: PerfQuery*):
    def run: ZIO[Database, Throwable, Seq[PerfStats]] =
      for
        _ <- Database.settle
        stats <- ZIO.foreach(queries) { query =>
          ZIO.logInfo(s"Running query ${query.tag}")
            *> Postgres.query(query.sql) // warm-up
            // Uncomment to debug query plans
            // *> Postgres
            //   .getAll[String](sql"EXPLAIN ANALYZE ${query.sql}")
            //   .flatMap(plan => ZIO.logInfo(s"Query ${query.step}:\n${plan.mkString("\n")}"))
            *> Postgres
              .query(query.sql)
              .repeatN(query.repeat - 1)
              .timed
              .map((duration, output) => PerfStats(query, duration, output.rows.size))
        }
      yield stats

    def runAndVerify(name: String, baseline: Seq[PerfStats]): ZIO[Database, Throwable, TestResult] =
      for
        candidate   <- run
        _           <- Datadog.submit(gauges(name, candidate, baseline))
        testResults <- testResults(candidate, baseline)
      yield TestResult.allSuccesses(testResults)

  private def gauges(name: String, candidate: Seq[PerfStats], baseline: Seq[PerfStats]): Seq[Gauge] =
    baseline.zip(candidate).flatMap { (baseline, candidate) =>
      Seq(
        Gauge(
          s"pqs.perf.$name.duration_ms",
          candidate.avgDuration.toMillis.toDouble,
          Seq("variant:candidate", s"query:${candidate.query.tag}")
        ),
        Gauge(
          s"pqs.perf.$name.duration_ms",
          baseline.avgDuration.toMillis.toDouble,
          Seq("variant:baseline", s"query:${baseline.query.tag}")
        ),
        Gauge("pqs.perf.active.diff_ratio", candidate.diffRatio(baseline), Seq(s"query:${candidate.query.tag}"))
      )
    }

  private def testResults(candidate: Seq[PerfStats], baseline: Seq[PerfStats]) =
    ZIO.foreach(baseline.zip(candidate)) { (baseline, candidate) =>
      val diff = candidate.diffRatio(baseline)
      val message = s"${candidate.query.tag} (${candidate.rowCount} rows) " +
        s"| Candidate: ${candidate.avgDuration.render} " +
        s"| Baseline: ${baseline.avgDuration.render} " +
        s"| Diff: ${diff.percent}"
      ZIO
        .logInfo(message)
        .as(
          assert(candidate.rowCount)(Assertion.equalTo(baseline.rowCount))
            && assertTrue(diff < candidate.query.expectedDiff + 0.3).label(message)
            && assertTrue(diff > candidate.query.expectedDiff - 0.3).label(message)
        )
    }
