// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.{DamlSdk, Ledger}
import com.digitalasset.pqs.services.postgres.Postgres

object SharedLedgerAndPostgresTest:
  val shared = DamlSdk.ledger ++ Postgres.instance

trait SharedLedgerAndPostgresTest extends FuncTest[Service[Ledger] & Postgres]:
  def shared = SharedLedgerAndPostgresTest.shared
