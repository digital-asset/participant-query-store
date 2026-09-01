// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.upgrades

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Database.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import scala.language.{implicitConversions, postfixOps}

// When an interface is defined in a separate package from the template that implements it,
// the interface's own package ID is never surfaced in query results — only its fully qualified
// name (FQN) appears. The package_id column on interface rows always reflects the *template's*
// package, not the interface's package. The interface (Labelable) lives in its own "Interfaces"
// package, while the template (Ping) lives in a different package that depends on it. Interface
// rows carry the Ping package ID, not the Interfaces package ID.
//
// NOTE: It is not yet confirmed whether this is the intended long-term behaviour. This test
// documents the current behaviour so we can revisit the decision later.
object InterfaceRowsUseTemplatePackageIdSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")

  // Interface defined in a standalone package — its package ID should NOT appear on any rows
  private val pingIface = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface Labelable
                      |  where
                      |    viewtype LabelableView
                      |
                      |    setLabel : Text -> Labelable
                      |
                      |    choice SetLabelChoice : ContractId Labelable
                      |      with newLabel : Text
                      |      controller (view this).owner
                      |      do create (setLabel this newLabel)
                      |
                      |data LabelableView = LabelableView with owner: Party, label: Optional Text
                      |  deriving (Eq, Ord, Show)
                      |
                      |""".stripMargin
  )

  // Template that implements the interface from a separate package
  private val ping = DamlSource(
    "Ping" -> """module Ping where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |
                |import Interfaces
                |
                |template Ping
                |  with
                |    sender: Party
                |    receiver: Party
                |  where
                |    signatory sender
                |    observer receiver
                |
                |    interface instance Labelable for Ping where
                |        view = LabelableView with owner = sender, label = None
                |        setLabel newLabel = toInterface this
                |
                |transact: Party -> Script ()
                |transact alice = void do
                |  cid <- submit alice $ createCmd Ping with sender = alice, receiver = alice
                |  submit alice $ exerciseCmd (toInterfaceContractId @Labelable cid) SetLabelChoice with newLabel = "exercised SetLabelChoice"
                |""".stripMargin
  ).dependsOn(pingIface)

  private val interfaceQname = s"${pingIface.name}:Interfaces:Labelable"
  private val templateQname  = s"${ping.name}:Ping:Ping"

  def spec = suite("Interface rows use the underlying template package ID")(
    funcTest("public contract queries keep the template package ID on interface rows") {
      val ifaceDar = Capture[DeployedDar]
      Given:
        (DamlSdk.dar(pingIface) >+> DamlSdk.deploy)
          ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        ifaceDar.captureFromService

      val dar = Capture[DeployedDar]
      And:
        DamlSdk.dar(ping) >+> DamlSdk.deploy
      And:
        dar.captureFromService
      When:
        DamlSdk.runScript("Ping:transact", alice.id)
      And:
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )

      lazy val ifacePkgId = ifaceDar.get.dar.packageId
      lazy val pkgId      = dar.get.dar.packageId

      val cId1 = Capture[String]
      val cId2 = Capture[String]

      // Interface rows carry the *template's* package ID, NOT the interface's own package ID.
      // The interface is identified solely by its FQN.
      Expect:
        creates(Some(interfaceQname)) `returns` table {
          pkgId | interfaceQname | "interface" | cId1.capture
          pkgId | interfaceQname | "interface" | cId2.capture
        }
      And:
        creates(Some(templateQname)) `returns` table {
          pkgId | templateQname | "template" | cId1
          pkgId | templateQname | "template" | cId2
        }
      And:
        archives(Some(interfaceQname)) `returns` table {
          pkgId | interfaceQname | "interface" | cId1
        }
      And:
        archives(Some(templateQname)) `returns` table {
          pkgId | templateQname | "template" | cId1
        }
      And:
        active(Some(interfaceQname)) `returns` table {
          pkgId | interfaceQname | "interface" | cId2
        }
      And:
        active(Some(templateQname)) `returns` table {
          pkgId | templateQname | "template" | cId2
        }
    }
  )
