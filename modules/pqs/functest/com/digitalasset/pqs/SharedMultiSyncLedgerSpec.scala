// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.functest.{Dpm, FTEnv, FuncTest}
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import zio.{ZIO, ZLayer}

object SharedMultiSyncLedgerSpec:
  val pingPong: DamlSource = Dars.pingPong

  val sync1: Synchronizer = Synchronizer("synchronizer1")
  val sync2: Synchronizer = Synchronizer("synchronizer2")

  val shared: ZLayer[FTEnv & Dpm & Docker, Throwable, Ledger & Postgres & DarFile] =
    Ledger.multiSync(sync1, sync2) ++ Postgres.instance >+> DamlSdk.uploadAndVetDar(pingPong)(sync1, sync2)

trait SharedMultiSyncLedgerSpec extends FuncTest[Ledger & Postgres & DarFile]:
  export SharedMultiSyncLedgerSpec.*

  protected def createContract(alice: Party): ZIO[Docker & Ledger & DarFile, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice, sync1)
      .map(_.getTransaction.events(0).getCreated.contractId)
