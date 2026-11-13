// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.docker.ContainerLimits
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.jdbc.*
import zio.test.*

object IngestionPerfSpec extends SharedHydratedLedgerTest:
  private val limits       = ContainerLimits(cpus = 2.0, memoryGiB = 0.8)
  private val expectedDiff = 0.18

  def spec = suite("IngestionPerfSpec")(
    funcTest("Ingestion perf") {
      val baselineSummary  = Capture[DatabaseSummary]
      val candidateSummary = Capture[DatabaseSummary]
      val baselineRun      = Capture[CliRun]
      Given:
        Postgres.database >+> Pqs37.applySchema // apply schema and warm the cache
      When:
        Postgres.vacuumAnalyzeAll
      And:
        Pqs37.runPipeline(limits)(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--logger-level=Warning"
        )
      Expect:
        getDatabaseSummary.is(baselineSummary.capture)
      And:
        baselineRun.captureFromService

      When:
        Postgres.database >+> Pqs.applySchema // apply schema and warm the cache
      When:
        Postgres.vacuumAnalyzeAll
      And:
        Pqs.runPipeline(limits)(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--logger-level=Warning"
        )
      Expect:
        getDatabaseSummary.is(candidateSummary.capture)

      Expect:
        for
          candidateRun <- ZIO.service[CliRun]
          baselineDurationMs = baselineRun.get.duration.toMillis
          diff               = (candidateRun.duration.toMillis.toDouble - baselineDurationMs) / baselineDurationMs
          gauges = Seq(
            Gauge(
              s"pqs.perf.ingestion.duration_s",
              candidateRun.duration.toSeconds.toDouble,
              Seq("variant:candidate")
            ),
            Gauge(
              s"pqs.perf.ingestion.duration_s",
              baselineRun.get.duration.toSeconds.toDouble,
              Seq("variant:baseline")
            ),
            Gauge(s"pqs.perf.ingestion.diff_ratio", diff, Seq.empty)
          )
          _ <- Datadog.submit(gauges)
        yield
          val message = s"Candidate: ${candidateRun.duration.render}" +
            s" | Baseline: ${baselineRun.get.duration.render}" +
            s" | Diff: ${diff.percent}" +
            s" | Output: ${candidateSummary.get}"
          assert(candidateSummary.get)(Assertion.equalTo(baselineSummary.get))
          && assertTrue(diff < expectedDiff + 0.3).label(message)
          && assertTrue(diff > expectedDiff - 0.3).label(message)
    }
  )

  private case class DatabaseSummary(creates: Long, archives: Long, active: Long, transactions: Long, events: Long)

  private def getDatabaseSummary =
    (Postgres.get[Long](sql"select count(*) from creates()")
      <&> Postgres.get[Long](sql"select count(*) from archives()")
      <&> Postgres.get[Long](sql"select count(*) from active()")
      <&> Postgres.get[Long](sql"select count(*) from __transactions")
      <&> Postgres.get[Long](sql"select count(*) from __events"))
      .map(DatabaseSummary.apply.tupled)
