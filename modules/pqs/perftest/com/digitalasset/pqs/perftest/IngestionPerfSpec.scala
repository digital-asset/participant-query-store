// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.ZIO
import zio.test.*

object IngestionPerfSpec extends SharedHydratedLedgerTest:
  def spec = suite("IngestionPerfSpec")(
    funcTest("Ingestion perf") {
      val pqs36Database    = Capture[Database]
      val pqsLocalDatabase = Capture[Database]
      val pqs36Run         = Capture[CliRun]
      val pqsLocalRun      = Capture[CliRun]
      Given:
        Postgres.database
          >+> Pqs36.applySchema // apply schema and warm the cache
          >+> Pqs36.runPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        pqs36Database.captureFromService
      And:
        pqs36Run.captureFromService

      When:
        Postgres.database
          >+> Pqs.applySchema // apply schema and warm the cache
          >+> Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        pqsLocalDatabase.captureFromService
      And:
        pqsLocalRun.captureFromService

      Expect:
        ZIO.succeed {
          val pqsLocalDurationMs = pqsLocalRun.get.duration.toMillis
          val pqs36DurationMs    = pqs36Run.get.duration.toMillis
          val diff               = (pqsLocalDurationMs.toDouble - pqs36DurationMs) / pqs36DurationMs
          assertTrue(diff < 0.2).label(
            s"Diff between ${pqsLocalRun.get.duration} and ${pqs36Run.get.duration} is ${(diff * 100).toInt}%"
          )
        }
    }
  )
