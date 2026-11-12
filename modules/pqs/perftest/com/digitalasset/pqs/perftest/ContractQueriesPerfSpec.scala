// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.jdbc.*
import zio.test.*

import scala.language.implicitConversions
import zio.jdbc.SqlFragment
import com.digitalasset.pqs.services.postgres.Database

object ContractQueriesPerfSpec extends SharedHydratedLedgerTest:

  def spec = suite("ContractQueriesPerfSpec")(
    funcTest("active") {
      val benchmarkQueries = Capture[Seq[ActiveQuery]]
      val baselineStats    = Capture[Seq[ActiveStats]]
      Given:
        Postgres.database
          >+> Pqs37.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        prepareBenchmark.is(benchmarkQueries.capture)
      Expect(4.minutes):
        runBenchmark(benchmarkQueries.get).is(baselineStats.capture)

      When:
        Database.freshConnectionPool >+> Pqs.applySchema
      Expect(4.minutes):
        for
          stats <- runBenchmark(benchmarkQueries.get)
          gauges = baselineStats.get.zip(stats).flatMap { (baseline, stats) =>
            Seq(
              Gauge(
                "pqs.perf.active.duration_ms",
                stats.avgDuration.toMillis.toDouble,
                Seq("variant:candidate", s"query:${stats.query.tag}")
              ),
              Gauge(
                "pqs.perf.active.duration_ms",
                baseline.avgDuration.toMillis.toDouble,
                Seq("variant:baseline", s"query:${stats.query.tag}")
              ),
              Gauge("pqs.perf.active.diff_ratio", stats.diffRatio(baseline), Seq(s"query:${stats.query.tag}"))
            )
          }
          _ <- Datadog.submit(gauges)
          testResults <- ZIO.foreach(baselineStats.get.zip(stats)) { (baseline, stats) =>
            val diff = stats.diffRatio(baseline)
            val message = s"Active query ${stats.query.step} (${stats.rowCount} rows) " +
              s"| Candidate: ${stats.avgDuration.render} " +
              s"| Baseline: ${baseline.avgDuration.render} " +
              s"| Diff: ${diff.percent}"
            ZIO
              .logInfo(message)
              .as(
                assert(stats.rowCount)(Assertion.equalTo(baseline.rowCount))
                  && assertTrue(diff < stats.query.expectedDiff + 0.3).label(message)
                  && assertTrue(diff > stats.query.expectedDiff - 0.3).label(message)
              )
          }
        yield TestResult.allSuccesses(testResults)
    }
  )

  private case class ActiveQuery(
      step: Int,
      offset: Long,
      columns: Seq[String] = Seq("*"),
      qname: Option[String] = None,
      filter: Option[String] = None,
      repeat: Int,
      expectedDiff: Double,
      tag: String
  ):
    val sql: SqlFragment =
      val table     = qname.fold(sql"active(null, $offset)")(qname => sql"active($qname, $offset)")
      val baseQuery = SqlFragment.select(columns*).from(table)
      filter.fold(baseQuery)(p => baseQuery.where(SqlFragment(p)))

    override def toString: String = s"Query $step: $sql"
  end ActiveQuery

  private final case class ActiveStats(query: ActiveQuery, duration: Duration, rowCount: Int):
    def avgDuration: Duration = duration.dividedBy(query.repeat)
    def diffRatio(baseline: ActiveStats): Double =
      val baselineDurationMs = baseline.duration.toMillis
      (duration.toMillis.toDouble - baselineDurationMs) / baselineDurationMs

  private def prepareBenchmark =
    for
      lastIx    <- Database.getLastIx // 29767
      offset100 <- Database.getOffset(lastIx)
      offset50  <- Database.getOffset((lastIx * 0.5).toLong)
      offset10  <- Database.getOffset((lastIx * 0.1).toLong)
      queries = buildQueries(offset100, offset50, offset10)
      _ <- ZIO.logInfo(
        s"""|Active benchmark:
            |  - ${queries.mkString("\n  - ")}
            |""".stripMargin
      )
    yield queries

  private def runBenchmark(queries: Seq[ActiveQuery]) =
    Postgres.vacuumAnalyzeAll
      *> Database.checkpoint
      *> ZIO.foreach(queries) { query =>
        ZIO.logInfo(s"Running active query ${query.step}")
          *> Postgres.query(query.sql) // warm-up
          // Uncomment to debug query plans
          // *> Postgres
          //   .getAll[String](sql"EXPLAIN ANALYZE ${query.sql}")
          //   .flatMap(plan => ZIO.logInfo(s"Query ${query.step}:\n${plan.mkString("\n")}"))
          *> Postgres
            .query(query.sql)
            .repeatN(query.repeat - 1)
            .timed
            .map((duration, output) => ActiveStats(query, duration, output.rows.size))
      }

  private def buildQueries(offset100: Long, offset50: Long, offset10: Long): Seq[ActiveQuery] =
    val blobA = "Blob:BlobA"
    var step  = 0
    def nextStep: Int =
      step += 1
      step
    Seq(
      // The dedup anti-join adds a extra scan of active contracts
      // and activates JIT, a fixed cost that dominates at small offsets.
      ActiveQuery(nextStep, offset100, repeat = 2, expectedDiff = 0.48, tag = "select_star_from_all_at_offset100"),
      ActiveQuery(nextStep, offset50, repeat = 2, expectedDiff = 0.45, tag = "select_star_from_all_at_offset50"),
      ActiveQuery(nextStep, offset10, repeat = 8, expectedDiff = 0.75, tag = "select_star_from_all_at_offset10"),

      // Nested loop anti-join: one extra scan of the active contracts, no JIT.
      ActiveQuery(
        nextStep,
        offset100,
        qname = Some(blobA),
        repeat = 4,
        expectedDiff = 0.26,
        tag = "select_star_from_bloba_at_offset100"
      ),
      ActiveQuery(
        nextStep,
        offset50,
        qname = Some(blobA),
        repeat = 4,
        expectedDiff = 0.24,
        tag = "select_star_from_bloba_at_offset50"
      ),
      ActiveQuery(
        nextStep,
        offset10,
        qname = Some(blobA),
        repeat = 16,
        expectedDiff = 0.12,
        tag = "select_star_from_bloba_at_offset10"
      ),

      // Baseline is a bare scan with no joins, so the anti-join weighs more
      ActiveQuery(
        nextStep,
        offset100,
        columns = Seq("contract_id", "payload"),
        repeat = 2,
        expectedDiff = 0.73,
        tag = "select_cid_payload_from_all_at_offset100"
      ),
      ActiveQuery(
        nextStep,
        offset100,
        columns = Seq("contract_id", "payload"),
        qname = Some(blobA),
        repeat = 4,
        expectedDiff = 0.47,
        tag = "select_cid_payload_from_bloba_at_offset100"
      ),

      // Candidate filters in a parallel scan (baseline is serial)
      ActiveQuery(
        nextStep,
        offset100,
        filter = Some("payload ->> 'value' = '42'"),
        repeat = 4,
        expectedDiff = -0.50,
        tag = "select_star_from_all_at_offset100_filter_on_payload"
      ),
      ActiveQuery(
        nextStep,
        offset100,
        qname = Some(blobA),
        filter = Some("payload ->> 'value' = '42'"),
        repeat = 8,
        expectedDiff = -0.02,
        tag = "select_star_from_bloba_at_offset100_filter_on_payload"
      )
    )
