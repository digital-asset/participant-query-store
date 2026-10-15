// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.Ledger
import com.digitalasset.pqs.services.postgres.Postgres

object SharedLedgerAndPostgresTest:
  val shared = Ledger.instance ++ Postgres.instance

trait SharedLedgerAndPostgresTest extends FuncTest[Ledger & Postgres]:
  def shared = SharedLedgerAndPostgresTest.shared
