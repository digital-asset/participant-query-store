// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.docker.ContainerLimits
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.ZIO
import zio.jdbc.*
import zio.test.*

object IngestionPerfSpec extends SharedHydratedLedgerTest:
  private val limits = ContainerLimits(cpus = 2.0, memoryGiB = 0.8)

  def spec = suite("IngestionPerfSpec")(
    funcTest("Ingestion perf") {
      val baselineSummary = Capture[DatabaseSummary]
      val baselineRun     = Capture[CliRun]
      Given:
        Postgres.database
          >+> Pqs36.applySchema // apply schema and warm the cache
          >+> Pqs36.runPipeline(limits)(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        getDatabaseSummary.is(baselineSummary.capture)
      And:
        baselineRun.captureFromService

      When:
        Postgres.database
          >+> Pqs.applySchema // apply schema and warm the cache
          >+> Pqs.runPipeline(limits)(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        getDatabaseSummary.is(baselineSummary.get)

      Expect:
        ZIO.service[CliRun].map { candidateRun =>
          val baselineDurationMs  = baselineRun.get.duration.toMillis
          val candidateDurationMs = candidateRun.duration.toMillis

          val diff = (candidateDurationMs.toDouble - baselineDurationMs) / baselineDurationMs
          assertTrue(diff < 0.4).label(
            s"Candidate (${candidateRun.duration}) is ${(diff * 100).toInt}% slower than baseline (${baselineRun.get.duration})"
          )
        }
    }
  )

  private case class DatabaseSummary(creates: Long, archives: Long, active: Long, transactions: Long, events: Long)

  private def getDatabaseSummary =
    for
      creates      <- Postgres.get[Long](sql"select count(*) from creates()")
      archives     <- Postgres.get[Long](sql"select count(*) from archives()")
      active       <- Postgres.get[Long](sql"select count(*) from active()")
      transactions <- Postgres.get[Long](sql"select count(*) from __transactions")
      events       <- Postgres.get[Long](sql"select count(*) from __events")
    yield DatabaseSummary(creates, archives, active, transactions, events)
