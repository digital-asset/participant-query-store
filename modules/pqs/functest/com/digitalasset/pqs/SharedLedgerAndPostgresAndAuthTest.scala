// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.{Ledger, TokenService}
import com.digitalasset.pqs.services.oauth.OAuth
import com.digitalasset.pqs.services.postgres.Postgres

object SharedLedgerAndPostgresAndAuthTest:
  val auth   = OAuth.instance >+> TokenService.live
  val shared = auth >+> Ledger.instance ++ Postgres.instance

trait SharedLedgerAndPostgresAndAuthTest extends FuncTest[Ledger & Postgres & Service[OAuth.Instance] & TokenService]:
  def shared = SharedLedgerAndPostgresAndAuthTest.shared
