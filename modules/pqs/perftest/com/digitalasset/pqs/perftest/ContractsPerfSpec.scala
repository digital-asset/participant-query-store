// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.Database
import zio.jdbc.*
import zio.ZIO

object ContractsPerfSpec extends SharedHydratedLedgerTest with QueryPerfSpec[Int]:

  def spec = suite("ContractsPerfSpec")(
    perfTest("active") {
      for
        lastIx    <- Database.getLastIx // 29767
        offset100 <- Database.getOffset(lastIx)
        offset50  <- Database.getOffset((lastIx * 0.5).toLong)
        offset10  <- Database.getOffset((lastIx * 0.1).toLong)
        blobA = "Blob:BlobA"
      yield Benchmark(
        // The dedup anti-join adds a extra scan of active contracts
        // and activates JIT, a fixed cost that dominates at small offsets.
        Query(
          "select_star_from_all_at_100",
          sql"select * from active(null, $offset100)",
          repeat = 2,
          expectedDiff = 1.17,
          tolerance = 0.4
        ),
        Query(
          "select_star_from_all_at_50",
          sql"select * from active(null, $offset50)",
          repeat = 2,
          expectedDiff = 1.20
        ),
        Query(
          "select_star_from_all_at_10",
          sql"select * from active(null, $offset10)",
          repeat = 8,
          expectedDiff = 1.63,
          tolerance = 0.4
        ),

        // Nested loop anti-join: one extra scan of the active contracts, no JIT.
        Query(
          "select_star_from_bloba_at_100",
          sql"select * from active($blobA, $offset100)",
          repeat = 4,
          expectedDiff = 0.45
        ),
        Query(
          "select_star_from_bloba_at_50",
          sql"select * from active($blobA, $offset50)",
          repeat = 4,
          expectedDiff = 0.36
        ),
        Query(
          "select_star_from_bloba_at_10",
          sql"select * from active($blobA, $offset10)",
          repeat = 16,
          expectedDiff = 0.34
        ),

        // Baseline is a bare scan with no joins, so the anti-join weighs more
        Query(
          "select_cid_from_all_at_100",
          sql"select contract_id from active(null, $offset100)",
          repeat = 2,
          expectedDiff = 1.06
        ),
        Query(
          "select_cid_from_bloba_at_100",
          sql"select contract_id from active($blobA, $offset100)",
          repeat = 4,
          expectedDiff = 0.59
        ),

        // Candidate filters in a parallel scan (baseline is serial)
        Query(
          "select_star_from_all_at_100_filter_on_payload",
          sql"select * from active(null, $offset100) where payload ->> 'value' = '42'",
          repeat = 4,
          expectedDiff = -0.48
        ),
        Query(
          "select_star_from_bloba_at_100_filter_on_payload",
          sql"select * from active($blobA, $offset100) where payload ->> 'value' = '42'",
          repeat = 8,
          expectedDiff = -0.03
        )
      )
    },
    perfTest("contracts") {
      for
        lastIx    <- Database.getLastIx
        offset100 <- Database.getOffset(lastIx)
        offset50  <- Database.getOffset((lastIx * 0.5).toLong)
        offset10  <- Database.getOffset((lastIx * 0.1).toLong)
        blobA = "Blob:BlobA"
      yield Benchmark(
        // Serial per-row probe: Postgres never runs a LATERAL subquery with LIMIT in a parallel worker
        Query(
          "select_star_from_all_at_10",
          sql"select * from contracts(null, $offset10)",
          baselineSql = Some(sql"select * from creates(null, oldest_offset(), $offset10)"),
          repeat = 8,
          expectedDiff = 0.39
        ),
        Query(
          "select_star_from_bloba_at_100",
          sql"select * from contracts($blobA, $offset100)",
          baselineSql = Some(sql"select * from creates($blobA, oldest_offset(), $offset100)"),
          repeat = 4,
          expectedDiff = 0.35
        ),
        Query(
          "select_star_from_bloba_at_50",
          sql"select * from contracts($blobA, $offset50)",
          baselineSql = Some(sql"select * from creates($blobA, oldest_offset(), $offset50)"),
          repeat = 4,
          expectedDiff = 0.29
        ),
        Query(
          "select_star_from_bloba_at_10",
          sql"select * from contracts($blobA, $offset10)",
          baselineSql = Some(sql"select * from creates($blobA, oldest_offset(), $offset10)"),
          repeat = 16,
          expectedDiff = 0.39
        ),

        // Baseline is a bare scan with no joins, so the probe weighs more
        Query(
          "select_cid_from_bloba_at_100",
          sql"select contract_id from contracts($blobA, $offset100)",
          baselineSql = Some(sql"select contract_id from creates($blobA, oldest_offset(), $offset100)"),
          repeat = 4,
          expectedDiff = 0.81
        ),
        Query(
          "count_from_all_at_100",
          sql"select count(*) from contracts(null, $offset100)",
          baselineSql = Some(sql"select count(*) from creates(null, oldest_offset(), $offset100)"),
          repeat = 2,
          expectedDiff = 3.01
        ),
        Query(
          "count_from_bloba_at_100",
          sql"select count(*) from contracts($blobA, $offset100)",
          baselineSql = Some(sql"select count(*) from creates($blobA, oldest_offset(), $offset100)"),
          repeat = 4,
          expectedDiff = 2.33
        ),

        // The filter runs before the probe, so only the matching rows are probed
        Query(
          "select_star_from_all_at_100_filter_on_payload",
          sql"select * from contracts(null, $offset100) where payload ->> 'value' = '42'",
          baselineSql =
            Some(sql"select * from creates(null, oldest_offset(), $offset100) where payload ->> 'value' = '42'"),
          repeat = 4,
          expectedDiff = 0.02
        ),
        Query(
          "select_star_from_bloba_at_100_filter_on_payload",
          sql"select * from contracts($blobA, $offset100) where payload ->> 'value' = '42'",
          baselineSql =
            Some(sql"select * from creates($blobA, oldest_offset(), $offset100) where payload ->> 'value' = '42'"),
          repeat = 8,
          expectedDiff = -0.02
        ),

        // Planning dominates this 20-row query, and the probe plans a second scan of every partition
        Query(
          "select_star_from_all_limit_20",
          sql"select * from contracts() limit 20",
          baselineSql = Some(sql"select * from creates() limit 20"),
          repeat = 50,
          expectedDiff = 0.43
        ),

        // Daml Shell's completion: only contract_id is selected, so the joins are removed and only matching rows are probed
        Query(
          "daml_shell_completion",
          sql"select contract_id from contracts() where contract_id like '%' limit 20",
          baselineSql = Some(sql"select contract_id from creates() where contract_id like '%' limit 20"),
          repeat = 100,
          expectedDiff = 0.25
        ),
        Query(
          "daml_shell_completion_no_match",
          sql"select contract_id from contracts() where contract_id like 'nonexistent%' limit 20",
          baselineSql = Some(sql"select contract_id from creates() where contract_id like 'nonexistent%' limit 20"),
          repeat = 4,
          expectedDiff = -0.06
        )
      )
    }
  )

  override protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, Int] =
    Database.query(sql).map(_.rows.size)
