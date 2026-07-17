// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.upgrades

import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Database.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything

import scala.language.{implicitConversions, postfixOps}

/** This must remain standalone to ensure the package store is not polluted by other concurrent tests.
  */
object PackageNameSupportSpec extends FuncTestStandalone:
  private val alice = Party("Alice")

  private val interfaces = DamlSource(
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
                |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                |""".stripMargin
  ).dependsOn(interfaces)

  // NB These are not real upgrades, but it was the best approximation in pre-2.9.1 world
  private val pingUpgrade = DamlSource(
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
                |    label: Optional Text
                |  where
                |    signatory sender
                |    observer receiver
                |
                |    interface instance Labelable for Ping where
                |        view = LabelableView with owner = sender, ..
                |        setLabel newLabel = toInterface (this with label = Some newLabel)
                |
                |transact: Party -> Script ()
                |transact alice = void do
                |  cid <- submit alice $ createCmd Ping with sender = alice, receiver = alice, label = Some "created upgraded contract"
                |  submit alice $ exerciseCmd (toInterfaceContractId @Labelable cid) SetLabelChoice with newLabel = "exercised SetLabelChoice on upgraded contract"
                |
                |""".stripMargin
  ).upgrades(ping).dependsOn(interfaces)
  def packageName = ping.name

  def spec = suite("Package Name Support")(
    suite("transactions")(
      funcTest("Package Names are correctly handled when using transactions.") {
        val dar = Capture[DeployedDar]
        Given:
          DamlSdk.dar(ping.withVersion("0.0.0")) ++ DamlSdk.ledger ++ Postgres.instance
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        And:
          dar.captureFromService
        And:
          DamlSdk.runScript("Ping:transact", alice.id)

        // upload the second dar
        val upgradedDar = Capture[DeployedDar]
        When:
          DamlSdk.dar(pingUpgrade.withVersion("0.0.1")) >+> DamlSdk.deploy >+> DamlSdk
            .runScript("Ping:transact", alice.id)
        And:
          upgradedDar.captureFromService

        And:
          Pqs.runPipeline(
            "--pipeline-datasource=TransactionStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        lazy val pkgId    = dar.get.dar.packageId
        lazy val upgPkgId = upgradedDar.get.dar.packageId
        val pkgPk         = Capture[Int]
        val upgPkgPk      = Capture[Int]
        Expect:
          __packages(sql"""order by version, name desc""") `returns` table {
            pkgPk.capture    | packageName     | "0.0.0" | pkgId
            anything         | interfaces.name | "0.0.0" | anything
            upgPkgPk.capture | packageName     | "0.0.1" | upgPkgId
          }

        val pingCTpePk      = Capture[Int]
        val labelableCTpePk = Capture[Int]
        Expect:
          __contract_tpe() `returns` table {
            labelableCTpePk.capture | s"${interfaces.name}:Interfaces:Labelable" | "interface" | s"{${interfaces.name}:Interfaces:Labelable,Interfaces:Labelable,Labelable}"
            pingCTpePk.capture | s"$packageName:Ping:Ping" | "template" | s"{$packageName:Ping:Ping,Ping:Ping,Ping}"
          }

        val cId1 = Capture[String]
        val cId2 = Capture[String]
        val cId3 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | labelableCTpePk | anything     | "[1,)"  | null
            pkgPk    | pingCTpePk      | cId1.capture | "[1,)"  | null
            upgPkgPk | labelableCTpePk | anything     | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | anything     | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk      | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          }

        Expect:
          __contracts() `returns` table {
            pkgPk    | labelableCTpePk | cId1 | "[1,)"  | null
            pkgPk    | pingCTpePk      | cId1 | "[1,)"  | null
            upgPkgPk | labelableCTpePk | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk      | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          }

        Expect:
          __exercise_tpe() `returns` table {
            anything | s"${interfaces.name}:Interfaces:Labelable" | s"${interfaces.name}:Interfaces:Labelable:Archive" | "Archive" | true | s"{${interfaces.name}:Interfaces:Labelable:Archive,Interfaces:Labelable:Archive,Labelable:Archive,Archive}"
            anything | s"${interfaces.name}:Interfaces:Labelable" | s"${interfaces.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | true | s"{${interfaces.name}:Interfaces:Labelable:SetLabelChoice,Interfaces:Labelable:SetLabelChoice,Labelable:SetLabelChoice,SetLabelChoice}"
            anything | s"$packageName:Ping:Ping" | s"$packageName:Ping:Ping:Archive" | "Archive" | true | s"{$packageName:Ping:Ping:Archive,Ping:Ping:Archive,Ping:Archive,Archive}"
          }

        Expect:
          __exercises() `returns` Table.empty

        // Read API tests
        // active
        Expect:
          active() `returns` table {
            pkgId    | s"${interfaces.name}:Interfaces:Labelable" | "interface" | cId1
            pkgId    | s"$packageName:Ping:Ping"                  | "template"  | cId1
            upgPkgId | s"${interfaces.name}:Interfaces:Labelable" | "interface" | cId3
            upgPkgId | s"$packageName:Ping:Ping"                  | "template"  | cId3
          }
        Expect:
          active(Some(s"$packageName:Ping:Ping")) `returns` table {
            pkgId    | s"$packageName:Ping:Ping" | "template" | cId1
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId3
          }
        // archives
        Expect:
          archives() `returns` table {
            upgPkgId | s"${interfaces.name}:Interfaces:Labelable" | "interface" | cId2
            upgPkgId | s"$packageName:Ping:Ping"                  | "template"  | cId2
          }
        Expect:
          archives(Some(s"$packageName:Ping:Ping")) `returns` table {
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId2
          }
        // creates
        Expect:
          creates() `returns` table {
            pkgId    | s"${interfaces.name}:Interfaces:Labelable" | "interface" | cId1
            pkgId    | s"$packageName:Ping:Ping"                  | "template"  | cId1
            upgPkgId | s"${interfaces.name}:Interfaces:Labelable" | "interface" | cId2
            upgPkgId | s"$packageName:Ping:Ping"                  | "template"  | cId2
            upgPkgId | s"${interfaces.name}:Interfaces:Labelable" | "interface" | cId3
            upgPkgId | s"$packageName:Ping:Ping"                  | "template"  | cId3
          }
        Expect:
          creates(Some(s"$packageName:Ping:Ping")) `returns` table {
            pkgId    | s"$packageName:Ping:Ping" | "template" | cId1
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId2
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId3
          }
        // exercises
        Expect:
          exercises() `returns` Table.empty
      }
    ),
    suite("transaction trees")(
      funcTest("Package Names are correctly handled when using transaction trees."):
        val dar = Capture[DeployedDar]
        Given:
          DamlSdk.dar(ping.withVersion("0.0.0")) ++ DamlSdk.ledger ++ Postgres.instance
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        And:
          dar.captureFromService
        And:
          DamlSdk.runScript("Ping:transact", alice.id)

        // upload the second dar
        val upgradedDar = Capture[DeployedDar]
        When:
          DamlSdk.dar(pingUpgrade.withVersion("0.0.1")) >+> DamlSdk.deploy >+> DamlSdk
            .runScript("Ping:transact", alice.id)
        And:
          upgradedDar.captureFromService

        And:
          Pqs.runPipeline(
            "--pipeline-datasource=TransactionTreeStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        lazy val pkgId    = dar.get.dar.packageId
        lazy val upgPkgId = upgradedDar.get.dar.packageId
        val pkgPk         = Capture[Int]
        val upgPkgPk      = Capture[Int]
        Expect:
          __packages(sql"""order by version, name desc""") `returns` table {
            pkgPk.capture    | packageName     | "0.0.0" | pkgId
            anything         | interfaces.name | "0.0.0" | anything
            upgPkgPk.capture | packageName     | "0.0.1" | upgPkgId
          }

        val pingCTpePk      = Capture[Int]
        val labelableCTpePk = Capture[Int]
        Expect:
          __contract_tpe() `returns` table {
            labelableCTpePk.capture | s"${interfaces.name}:Interfaces:Labelable" | "interface" | s"{${interfaces.name}:Interfaces:Labelable,Interfaces:Labelable,Labelable}"
            pingCTpePk.capture | s"$packageName:Ping:Ping" | "template" | s"{$packageName:Ping:Ping,Ping:Ping,Ping}"
          }

        val cId1 = Capture[String]
        val cId2 = Capture[String]
        val cId3 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | labelableCTpePk | anything     | "[1,)"  | null
            pkgPk    | pingCTpePk      | cId1.capture | "[1,)"  | null
            upgPkgPk | labelableCTpePk | anything     | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | anything     | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk      | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          }

        Expect:
          __contracts() `returns` table {
            pkgPk    | labelableCTpePk | cId1 | "[1,)"  | null
            pkgPk    | pingCTpePk      | cId1 | "[1,)"  | null
            upgPkgPk | labelableCTpePk | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk      | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          }

        val setLabelChoiceETpePk = Capture[Int]
        Expect:
          __exercise_tpe() `returns` table {
            anything | s"${interfaces.name}:Interfaces:Labelable" | s"${interfaces.name}:Interfaces:Labelable:Archive" | "Archive" | true | s"{${interfaces.name}:Interfaces:Labelable:Archive,Interfaces:Labelable:Archive,Labelable:Archive,Archive}"
            setLabelChoiceETpePk.capture | s"${interfaces.name}:Interfaces:Labelable" | s"${interfaces.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | true | s"{${interfaces.name}:Interfaces:Labelable:SetLabelChoice,Interfaces:Labelable:SetLabelChoice,Labelable:SetLabelChoice,SetLabelChoice}"
            anything | s"$packageName:Ping:Ping" | s"$packageName:Ping:Ping:Archive" | "Archive" | true | s"{$packageName:Ping:Ping:Archive,Ping:Ping:Archive,Ping:Archive,Archive}"
          }

        Expect:
          __exercises() `returns` table {
            upgPkgPk | setLabelChoiceETpePk | labelableCTpePk | cId2 | "exercised SetLabelChoice on upgraded contract"
          }

        // Read API tests
        // active
        Expect:
          active(Some(s"$packageName:Ping:Ping")) `returns` table {
            pkgId    | s"$packageName:Ping:Ping" | "template" | cId1
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId3
          }
        // archives
        Expect:
          archives(Some(s"$packageName:Ping:Ping")) `returns` table {
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId2
          }
        // creates
        Expect:
          creates(Some(s"$packageName:Ping:Ping")) `returns` table {
            pkgId    | s"$packageName:Ping:Ping" | "template" | cId1
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId2
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId3
          }
        // exercises
        Expect:
          exercises(Some(s"${interfaces.name}:Interfaces:Labelable:SetLabelChoice")) `returns` table {
            upgPkgId | s"${interfaces.name}:Interfaces:Labelable" | s"${interfaces.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | cId2 | "exercised SetLabelChoice on upgraded contract"
          }
    )
  )
