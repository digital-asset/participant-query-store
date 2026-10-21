// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.{DamlSdk, Party, Ledger}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.daml.Parties

object SharedHydratedLedgerTest:
  val alice = Party("alice_1")

  /** Load the checked-in Postgres dumps to rehydrate the Canton ledger. */
  val shared = Postgres.restore(CantonDumps.all) >+> Ledger.instanceWithPgStorage >+> DamlSdk.allocatedParties(alice)

trait SharedHydratedLedgerTest extends FuncTest[Ledger & Postgres & Parties]:
  def alice: Party = SharedHydratedLedgerTest.alice
  def shared       = SharedHydratedLedgerTest.shared
