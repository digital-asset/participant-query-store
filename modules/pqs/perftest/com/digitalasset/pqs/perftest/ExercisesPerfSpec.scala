// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import zio.jdbc.*
import zio.ZIO

object ExercisesPerfSpec extends SharedHydratedLedgerTest with QueryPerfSpec[Int]:

  def spec = suite("ExercisesPerfSpec")(
    perfTest("exercises") {
      for
        lastIx <- Database.getLastIx
        from   <- Database.getOffset((lastIx * 0.4).toLong)
        to     <- Database.getOffset((lastIx * 0.6).toLong)
      yield Benchmark(
        Query("select_star_from_all", sql"select * from exercises()", repeat = 100),
        Query("select_star_from_createmany", sql"select * from exercises('Blob:Seed:CreateMany')", repeat = 200),
        Query("select_cid_from_all", sql"select contract_id from exercises()", repeat = 200, expectedDiff = 0),
        Query(
          "select_cid_from_createmany",
          sql"select contract_id from exercises('Blob:Seed:CreateMany')",
          repeat = 1000
        ),
        Query("select_star_from_all_between_40_60", sql"select * from exercises(null, $from, $to)", repeat = 500),
        Query(
          "select_star_from_createmany_between_40_60",
          sql"select * from exercises('Blob:Seed:CreateMany', $from, $to)",
          repeat = 1000
        )
      )
    },
    perfTest("lookup_exercises") {
      for seedId <- Postgres.get[String](sql"select contract_id from creates('Blob:Seed') order by contract_id limit 1")
      yield Benchmark(
        Query(
          "select_star_from_all",
          sql"select * from lookup_exercises($seedId)",
          repeat = 1000,
          expectedDiff = 0
        ),
        Query(
          "select_star_from_archivemany",
          sql"select * from lookup_exercises($seedId, 'Blob:Seed:ArchiveMany')",
          repeat = 1000,
          expectedDiff = 0
        )
      )
    }
  )

  override protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, Int] =
    Postgres.query(sql).map(_.rows.size)
