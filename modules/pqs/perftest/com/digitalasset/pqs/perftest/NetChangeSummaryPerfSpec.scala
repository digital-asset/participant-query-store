// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.Database
import com.digitalasset.pqs.utils.safeequals.=/=
import zio.ZIO
import zio.jdbc.*

object NetChangeSummaryPerfSpec extends SharedHydratedLedgerTest with QueryPerfSpec[Map[(String, String), Long]]:

  def spec = suite("NetChangeSummaryPerfSpec")(
    perfTest("net_change_summary") {
      for
        lastIx <- Database.getLastIx // 29767
        ix50 = (lastIx * 0.5).toLong
        offset100    <- Database.getOffset(lastIx)
        offset50     <- Database.getOffset(ix50)
        offset10     <- Database.getOffset((lastIx * 0.1).toLong)
        offset40     <- Database.getOffset((lastIx * 0.4).toLong)
        offset60     <- Database.getOffset((lastIx * 0.6).toLong)
        offset50p10  <- Database.getOffset(ix50 + 9)
        offset50p100 <- Database.getOffset(ix50 + 99)
        oldest       <- Database.getOffset(1)
      yield Benchmark(
        // One probe per changed contract; few contracts change at a single offset
        netChange("single_at_10", offset10, offset10, repeat = 1500, expectedDiff = -0.20),
        netChange("single_at_50", offset50, offset50, repeat = 1500, expectedDiff = 0.0),
        netChange("single_at_100", offset100, offset100, repeat = 1500, expectedDiff = 0.57),
        // One probe per changed contract, where summary_updates counts index entries per template
        netChange("10_transactions_at_50", offset50, offset50p10, repeat = 1500, expectedDiff = 1.64),
        netChange(
          "100_transactions_at_50",
          offset50,
          offset50p100,
          repeat = 1000,
          expectedDiff = 17.08,
          tolerance = 3.0 // varies by up to 4.5 between runs
        ),
        // One probe per changed contract over a fifth of the ledger
        netChange("between_40_60", offset40, offset60, repeat = 25, expectedDiff = 32.43, tolerance = 8.0),
        // Nothing is active before the first transaction, so every contract active at the end is probed
        netChange("oldest_to_latest", oldest, offset100, repeat = 3, expectedDiff = 8.32, tolerance = 2.0)
      )
    }
  )

  private def netChange(
      tag: String,
      from: Long,
      to: Long,
      repeat: Int,
      expectedDiff: Double,
      tolerance: Double = 0.3
  ) =
    Query(
      tag,
      sql"select * from __net_change_summary($from, $to)",
      repeat,
      expectedDiff,
      tolerance,
      baselineSql = Some(sql"select * from summary_updates($from, $to)")
    )

  // summary_updates is the baseline: with one synchronizer and no divulgence, creates - archives = additions - removals
  override protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, Map[(String, String), Long]] =
    Database
      .getAll[(String, String, Long, Long)](sql)
      .map(_.collect {
        case (template, payloadType, added, removed) if added =/= removed =>
          (template, payloadType) -> (added - removed)
      }.toMap)
