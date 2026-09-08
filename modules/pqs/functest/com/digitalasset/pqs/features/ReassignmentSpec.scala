// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.nuck

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyCantonVersion
import com.digitalasset.pqs.functest.*
import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.services.postgres.Postgres

object ReassignmentSpec extends FuncTest[Service[Ledger] & Postgres & DeployedDar]:
  private val pingPong = DamlSource(
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
  private val sync1 = Synchronizer("synchronizer1")
  private val sync2 = Synchronizer("synchronizer2")

  val shared = 
    DamlSdk.dar(pingPong) ++ DamlSdk.multiSyncLedger(sync1, sync2) ++ Postgres.instance
    >+> DamlSdk.uploadAndVetDar(sync1, sync2)

  def spec = suite("Multi-Sync")(
    funcTest("Contract is created, reassigned and archived") {
      val alice = Party("Alice")
      val contractId = Capture[String]
      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      Then:
        val args = Record.defaultInstance
          .addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
        Ledger
          .create("PingPong:Ping", args, alice, sync1)
          .map(_.getTransaction.events.head.getCreated.contractId)
          .is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)
    }
  ) @@ onlyCantonVersion(">=3.5")
