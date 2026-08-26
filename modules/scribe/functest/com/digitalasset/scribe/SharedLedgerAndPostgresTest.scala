// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import com.digitalasset.scribe.docker.Service
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.services.daml.{DamlSdk, Ledger}
import com.digitalasset.scribe.services.postgres.Postgres

object SharedLedgerAndPostgresTest:
  val shared = DamlSdk.ledger ++ Postgres.instance

trait SharedLedgerAndPostgresTest extends FuncTest[Service[Ledger] & Postgres]:
  def shared = SharedLedgerAndPostgresTest.shared
