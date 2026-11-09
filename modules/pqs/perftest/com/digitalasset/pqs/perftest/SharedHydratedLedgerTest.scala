// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.docker.ContainerLimits
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.{DamlSdk, Party, Ledger}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.daml.Parties
import zio.jdbc.*

object SharedHydratedLedgerTest:
  val alice = Party("alice_1")

  /** Load the checked-in Postgres dumps to rehydrate the Canton ledger. Postgres has fixed CPU/memory budgets for
    * stable repeatable benchmarks.
    */
  val shared =
    Postgres.restore(CantonDumps.all, limits = ContainerLimits(cpus = 4.0, memoryGiB = 2.0, shmGiB = 0.5))
      >+> Ledger.instanceWithPgStorage(bootstrap = false)
      >+> DamlSdk.allocatedParties(alice)

trait SharedHydratedLedgerTest extends FuncTest[Ledger & Postgres & Parties]:
  def alice: Party = SharedHydratedLedgerTest.alice
  def shared       = SharedHydratedLedgerTest.shared

  /** Flushes the ingest and leaves nothing for autovacuum, so the baseline and candidate passes measure the same state
    */
  protected def settle(analyze: Boolean = true) =
    Postgres.call(sql"checkpoint") *> Postgres.call(if analyze then sql"vacuum analyze" else sql"vacuum")

  protected def getLastIx =
    Postgres.get[Long](sql"select ix from __transactions order by ix desc limit 1")

  protected def getOffset(ix: Long) =
    Postgres.get[Long](sql"""select "offset" from __transactions where ix=$ix""")
