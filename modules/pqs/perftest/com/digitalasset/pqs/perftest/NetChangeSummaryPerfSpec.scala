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
        Query(
          "single_at_10",
          sql"select * from __net_change_summary($offset10, $offset10)",
          baselineSql = Some(sql"select * from summary_updates($offset10, $offset10)"),
          repeat = 1500,
          expectedDiff = 0.08
        ),
        Query(
          "single_at_50",
          sql"select * from __net_change_summary($offset50, $offset50)",
          baselineSql = Some(sql"select * from summary_updates($offset50, $offset50)"),
          repeat = 1500,
          expectedDiff = 0.22 
        ),
        Query(
          "single_at_100",
          sql"select * from __net_change_summary($offset100, $offset100)",
          baselineSql = Some(sql"select * from summary_updates($offset100, $offset100)"),
          repeat = 1500,
          expectedDiff = 1.05,
          tolerance = 0.4
        ),

        // One probe per changed contract, where summary_updates counts index entries per template
        Query(
          "10_transactions_at_50",
          sql"select * from __net_change_summary($offset50, $offset50p10)",
          baselineSql = Some(sql"select * from summary_updates($offset50, $offset50p10)"),
          repeat = 1500,
          expectedDiff = 2.61,
          tolerance = 0.5
        ),
        Query(
          "100_transactions_at_50",
          sql"select * from __net_change_summary($offset50, $offset50p100)",
          baselineSql = Some(sql"select * from summary_updates($offset50, $offset50p100)"),
          repeat = 1000,
          expectedDiff = 17.28,
          tolerance = 1.7
        ),

        // One probe per changed contract over a fifth of the ledger
        Query(
          "between_40_60",
          sql"select * from __net_change_summary($offset40, $offset60)",
          baselineSql = Some(sql"select * from summary_updates($offset40, $offset60)"),
          repeat = 25,
          expectedDiff = 34.52,
          tolerance = 2.5
        ),

        // Nothing is active before the first transaction, so every contract active at the end is probed
        Query(
          "oldest_to_latest",
          sql"select * from __net_change_summary($oldest, $offset100)",
          baselineSql = Some(sql"select * from summary_updates($oldest, $offset100)"),
          repeat = 3,
          expectedDiff = 8.72,
          tolerance = 1.0
        )
      )
    }
  )

  // summary_updates is the baseline: with one synchronizer and no divulgence, creates - archives = activations - deactivations
  override protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, Map[(String, String), Long]] =
    Database
      .getAll[(String, String, Long, Long)](sql)
      .map(_.collect {
        case (template, payloadType, activations, deactivations) if activations =/= deactivations =>
          (template, payloadType) -> (activations - deactivations)
      }.toMap)
