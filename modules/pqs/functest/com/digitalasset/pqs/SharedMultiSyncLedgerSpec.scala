// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.{Dpm, FTEnv, FuncTest}
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import zio.{ZIO, ZLayer}

object SharedMultiSyncLedgerSpec:
  val pingPong: DamlSource = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    sender: Party
                    |  where
                    |    signatory sender
                    |""".stripMargin
  )

  val sync1: Synchronizer = Synchronizer("synchronizer1")
  val sync2: Synchronizer = Synchronizer("synchronizer2")

  val shared: ZLayer[FTEnv & Dpm & Docker, Throwable, Service[Ledger] & Postgres & DarFile] =
    DamlSdk.multiSyncLedger(sync1, sync2) ++ Postgres.instance >+> DamlSdk.uploadAndVetDar(pingPong)(sync1, sync2)

trait SharedMultiSyncLedgerSpec extends FuncTest[Service[Ledger] & Postgres & DarFile]:
  export SharedMultiSyncLedgerSpec.*

  protected def createContract(alice: Party): ZIO[Docker & Service[Ledger] & DarFile, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice, sync1)
      .map(_.getTransaction.events(0).getCreated.contractId)
