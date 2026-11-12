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

trait QueryPerfSpec[T] extends FuncTest[Ledger & Postgres & Parties]:
  protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, T]

  def perfTest(name: String)(prepareBenchmark: ZIO[Database, Throwable, Benchmark]) =
    funcTest(name) {
      val benchmark = Capture[Benchmark]
      val baseline  = Capture[Seq[Stats]]
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

  final case class Stats(query: Query, duration: Duration, output: T):
    def avgDuration = duration.dividedBy(query.repeat)
    def diffRatio(baseline: Stats): Double =
      val baselineDurationMs = baseline.duration.toMillis
      (duration.toMillis.toDouble - baselineDurationMs) / baselineDurationMs

  final case class Query(tag: String, sql: SqlFragment, repeat: Int, expectedDiff: Double = 0)

  final class Benchmark(queries: Query*):
    def run: ZIO[Database, Throwable, Seq[Stats]] =
      for
        _ <- Database.settle
        stats <- ZIO.foreach(queries) { query =>
          ZIO.logInfo(s"Running query ${query.tag}")
            *> executeQuery(query.sql) // warm-up
            // Uncomment to debug query plans
            // *> Postgres
            //   .getAll[String](sql"EXPLAIN ANALYZE ${query.sql}")
            //   .flatMap(plan => ZIO.logInfo(s"${query.tag}:\n${plan.mkString("\n")}"))
            *> executeQuery(query.sql).timed
              .map((duration, output) => Stats(query, duration, output))
        }
      yield stats

    def runAndVerify(name: String, baseline: Seq[Stats]): ZIO[Database, Throwable, TestResult] =
      for
        candidate   <- run
        _           <- Datadog.submit(gauges(name, candidate, baseline))
        testResults <- testResults(candidate, baseline)
      yield TestResult.allSuccesses(testResults)

  private def gauges(name: String, candidate: Seq[Stats], baseline: Seq[Stats]): Seq[Gauge] =
    baseline.zip(candidate).flatMap { (baseline, candidate) =>
      Seq(
        Gauge(
          s"pqs.perf.$name.duration_ms",
          candidate.avgDuration.toNanos.toDouble / 1000_000,
          Seq("variant:candidate", s"query:${candidate.query.tag}")
        ),
        Gauge(
          s"pqs.perf.$name.duration_ms",
          baseline.avgDuration.toNanos.toDouble / 1000_000,
          Seq("variant:baseline", s"query:${baseline.query.tag}")
        ),
        Gauge(s"pqs.perf.$name.diff_ratio", candidate.diffRatio(baseline), Seq(s"query:${candidate.query.tag}"))
      )
    }

  private def testResults(candidate: Seq[Stats], baseline: Seq[Stats]) =
    ZIO.foreach(baseline.zip(candidate)) { (baseline, candidate) =>
      val diff = candidate.diffRatio(baseline)
      val message = candidate.query.tag +
        s"| Candidate: ${candidate.avgDuration.render} " +
        s"| Baseline: ${baseline.avgDuration.render} " +
        s"| Diff: ${diff.percent}" +
        s"| Output: ${candidate.output}"
      ZIO
        .logInfo(message)
        .as(
          assert(candidate.output)(Assertion.equalTo(baseline.output))
            && assertTrue(diff < candidate.query.expectedDiff + 0.3).label(message)
            && assertTrue(diff > candidate.query.expectedDiff - 0.3).label(message)
        )
    }
