// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.docker.ContainerLimits
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.ZIO
import zio.test.*

object IngestionPerfSpec extends SharedHydratedLedgerTest:
  private val limits = ContainerLimits(cpus = 2.0, memoryGiB = 0.8)

  def spec = suite("IngestionPerfSpec")(
    funcTest("Ingestion perf") {
      val pqs36Database    = Capture[Database]
      val pqsLocalDatabase = Capture[Database]
      val pqs36Run         = Capture[CliRun]
      val pqsLocalRun      = Capture[CliRun]
      Given:
        Postgres.database
          >+> Pqs36.applySchema // apply schema and warm the cache
          >+> Pqs36.runPipeline(limits)(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        pqs36Database.captureFromService
      And:
        pqs36Run.captureFromService

      When:
        Postgres.database
          >+> Pqs.applySchema // apply schema and warm the cache
          >+> Pqs.runPipeline(limits)(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            "--logger-level=Warning"
          )
      Expect:
        pqsLocalDatabase.captureFromService
      And:
        pqsLocalRun.captureFromService

      Expect:
        ZIO.succeed {
          val pqsLocalDurationMs = pqsLocalRun.get.duration.toMillis
          val pqs36DurationMs    = pqs36Run.get.duration.toMillis
          val diff               = (pqsLocalDurationMs.toDouble - pqs36DurationMs) / pqs36DurationMs
          assertTrue(diff < 0.4).label(
            s"Local PQS (${pqsLocalRun.get.duration}) is ${(diff * 100).toInt}% slower than PQS ${Pqs36.version} (${pqs36Run.get.duration})"
          )
        }
    }
  )
