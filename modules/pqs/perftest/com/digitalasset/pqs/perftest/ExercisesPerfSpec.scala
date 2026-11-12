// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import zio.jdbc.*

object ExercisesPerfSpec extends SharedHydratedLedgerTest with PerfBenchmarkSpec:
  def spec = suite("ExercisesPerfSpec")(
    perfTest("exercises") {
      for
        lastIx <- Database.getLastIx
        from   <- Database.getOffset((lastIx * 0.4).toLong)
        to     <- Database.getOffset((lastIx * 0.6).toLong)
      yield PerfBenchmark(
        PerfQuery("select_star_from_all", sql"select * from exercises()", repeat = 5),
        PerfQuery("select_star_from_createmany", sql"select * from exercises('Blob:Seed:CreateMany')", repeat = 5),
        PerfQuery("select_cid_from_all", sql"select contract_id from exercises()", repeat = 5, expectedDiff = 0),
        PerfQuery(
          "select_cid_from_createmany",
          sql"select contract_id from exercises('Blob:Seed:CreateMany')",
          repeat = 200
        ),
        PerfQuery("select_star_from_all_between_40_60", sql"select * from exercises(null, $from, $to)", repeat = 5),
        PerfQuery(
          "select_star_from_createmany_between_40_60",
          sql"select * from exercises('Blob:Seed:CreateMany', $from, $to)",
          repeat = 200
        )
      )
    },
    perfTest("lookup_exercises") {
      for seedId <- Postgres.get[String](sql"select contract_id from creates('Blob:Seed') order by contract_id limit 1")
      yield PerfBenchmark(
        PerfQuery(
          "select_star_from_all",
          sql"select * from lookup_exercises($seedId)",
          repeat = 400,
          expectedDiff = 0
        ),
        PerfQuery(
          "select_star_from_archivemany",
          sql"select * from lookup_exercises($seedId, 'Blob:Seed:ArchiveMany')",
          repeat = 1000,
          expectedDiff = 0
        )
      )
    }
  )
