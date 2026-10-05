// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.upgrades

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.SharedMultiSyncLedgerSpec

import scala.language.{implicitConversions, postfixOps}

object ReassignedContractUpgradesSpec extends SharedMultiSyncLedgerSpec:
  val pingIface = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface Labelable
                      |  where
                      |    viewtype LabelableView
                      |
                      |data LabelableView = LabelableView with label: Text
                      |  deriving (Eq, Ord, Show)
                      |
                      |""".stripMargin
  )

  val pingV1 = DamlSource(
    "Ping" -> """module Ping where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |
                |import Interfaces
                |
                |template Ping
                |  with
                |    owner: Party
                |    label: Text
                |  where
                |    signatory owner
                |
                |    interface instance Labelable for Ping where
                |        view = LabelableView with label = label <> "V1"
                |""".stripMargin
  ).dependsOn(pingIface)

  val pingV2 = DamlSource(
    "Ping" -> """|module Ping where
                 |
                 |import Daml.Script
                 |import DA.Functor (void)
                 |
                 |import Interfaces
                 |
                 |template Ping
                 |  with
                 |    owner: Party
                 |    label: Text   
                 |  where
                 |    signatory owner
                 |
                 |    interface instance Labelable for Ping where
                 |        view = LabelableView with label = label <> "V2"
                 |""".stripMargin
  ).upgrades(pingV1).dependsOn(pingIface)

  def spec = suite("Multi-Sync Upgrade")(
    funcTest("Contract with interface is reassigned to synchronizer with upgraded package") {
      val alice      = Party("Alice")
      val pingV1Dar  = Capture[DarFile]
      val pingV2Dar  = Capture[DarFile]
      val contractId = Capture[String]
      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
          ++ DamlSdk.uploadAndVetDar(pingV1)(sync1, sync2)
      And:
        pingV1Dar.captureFromService
      Then:
        val args = Record.defaultInstance
          .addFields(RecordField("owner", Some(Value(Value.Sum.Party(alice.id)))))
          .addFields(RecordField("label", Some(Value(Value.Sum.Text("someLabel")))))
        Ledger
          .create("Ping:Ping", args, alice, sync1)
          .map(_.getTransaction.events(0).getCreated.contractId)
          .is(contractId.capture)
      When:
        // vet pingV2 only on sync2
        DamlSdk.uploadAndVetDar(pingV2)(sync2)
      And:
        pingV2Dar.captureFromService
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
      When:
        Postgres.database
          >+> Pqs.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")

      Expect:
        Database
          .active(extraColumns = Seq("payload ->> 'label'", "synchronizer_id", "reassignment_counter"))
          .returns(
            table {
              // Canton uses pingV1 to compute the interface view because TAPS require a package to be vetted on all synchronizers hosting the submitting party
              pingV1Dar.get.packageId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | contractId | "someLabelV1" | sync2.id | 1
              pingV1Dar.get.packageId | s"${pingV1.name}:Ping:Ping" | "template" | contractId | "someLabel" | sync2.id | 1
            }
          )

      When:
        // vet pingV2 on sync1 and reassign contract to sync 1
        Ledger.vetDar(pingV2Dar.get, sync1)
          *> Ledger.reassign(contractId.get, alice, sync2, sync1)

      And:
        Pqs.runPipeline("--pipeline-ledger-start=Oldest", "--pipeline-ledger-stop=Latest")

      Expect:
        Database
          .active(extraColumns = Seq("payload ->> 'label'", "synchronizer_id", "reassignment_counter"))
          .returns(
            table {
              // Canton uses pingV2 to compute the interface view since it is vetted by all synchronizers hosting Alice
              pingV1Dar.get.packageId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | contractId | "someLabelV2" | sync1.id | 2
              pingV1Dar.get.packageId | s"${pingV1.name}:Ping:Ping" | "template" | contractId | "someLabel" | sync1.id | 2
            }
          )
    }
  )
