// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.upgrades

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.daml.DamlSdk.onlyDamlLfVersion
import com.digitalasset.scribe.services.postgres.Database.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.{Pipeline, Scribe}
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything

import scala.language.{implicitConversions, postfixOps}

object AmbiguousPackageNameSupportSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")
  private val ping = DamlSource(
    "Ping" -> """module Ping where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |
                |template Ping
                |  with
                |    sender: Party
                |    receiver: Party
                |  where
                |    signatory sender
                |    observer receiver
                |
                |transact: Party -> Script ()
                |transact party = void do
                |  submit party $ createCmd Ping with sender = party, receiver = party
                |""".stripMargin
  )

  // NB These are not upgrades at all, but some of our clients misused tech by not bumping versions
  private val pingUpgrade = DamlSource(
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
                      |""".stripMargin,
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
                |transact party = void do
                |  cid <- submit party $ createCmd Ping with sender = party, receiver = party, label = Some "created upgraded contract"
                |  submit party $ exerciseCmd (toInterfaceContractId @Labelable cid) SetLabelChoice with newLabel = "exercised SetLabelChoice on upgraded contract"
                |
                |""".stripMargin
  ).withName(ping.name)
  def packageName = ping.name

  def spec = suite("Ambiguous Package Name Support")(
    suite("transactions")(
      funcTest("Ambiguous Package Names are correctly handled when using transactions.") {
        val dar = Capture[DeployedDar]
        Given:
          (DamlSdk.dar(ping.withVersion("0.0.0")) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
        And:
          dar.captureFromService
        And:
          DamlSdk.runScript("Ping:transact", alice.id)

        // upload the second dar
        val upgradedDar = Capture[DeployedDar]
        When:
          DamlSdk.dar(pingUpgrade.withVersion("0.0.0")) >+> DamlSdk.deploy >+> DamlSdk
            .runScript("Ping:transact", alice.id)
        And:
          upgradedDar.captureFromService

        And:
          Scribe.pipeline(
            "--pipeline-datasource=TransactionStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        lazy val pkgId    = dar.get.dar.packageId
        lazy val upgPkgId = upgradedDar.get.dar.packageId
        val pkgPk         = Capture[Int]
        val upgPkgPk      = Capture[Int]

        lazy val (pkgIdR1, pkgIdR2, pkgPkR1, pkgPkR2) =
          if pkgId < upgPkgId then (pkgId, upgPkgId, pkgPk, upgPkgPk) else (upgPkgId, pkgId, upgPkgPk, pkgPk)
        Expect:
          __packages(sql"""order by id""") `returns` table {
            pkgPkR1.capture | packageName | "0.0.0" | pkgIdR1
            pkgPkR2.capture | packageName | "0.0.0" | pkgIdR2
          } retryUntilTimeout

        val pingCTpePk      = Capture[Int]
        val labelableCTpePk = Capture[Int]
        Expect:
          __contract_tpe() `returns` table {
            labelableCTpePk.capture | s"$packageName:Interfaces:Labelable" | "interface" | s"{$packageName:Interfaces:Labelable,Interfaces:Labelable,Labelable}"
            pingCTpePk.capture | s"$packageName:Ping:Ping" | "template" | s"{$packageName:Ping:Ping,Ping:Ping,Ping}"
          }

        val cId1 = Capture[String]
        val cId2 = Capture[String]
        val cId3 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk      | cId1.capture | "[1,)"  | null
            upgPkgPk | labelableCTpePk | anything     | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | anything     | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk      | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          } retryUntilTimeout

        val setLabelChoiceETpePk = Capture[Int]
        Expect:
          __exercise_tpe() `returns` table {
            anything | s"$packageName:Interfaces:Labelable" | s"$packageName:Interfaces:Labelable:Archive" | "Archive" | true | s"{$packageName:Interfaces:Labelable:Archive,Interfaces:Labelable:Archive,Labelable:Archive,Archive}"
            setLabelChoiceETpePk.capture | s"$packageName:Interfaces:Labelable" | s"$packageName:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | true | s"{$packageName:Interfaces:Labelable:SetLabelChoice,Interfaces:Labelable:SetLabelChoice,Labelable:SetLabelChoice,SetLabelChoice}"
            anything | s"$packageName:Ping:Ping" | s"$packageName:Ping:Ping:Archive" | "Archive" | true | s"{$packageName:Ping:Ping:Archive,Ping:Ping:Archive,Ping:Archive,Archive}"
          }

        Expect:
          __exercises() `returns` Table.empty

        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk      | cId1 | "[1,)"  | null
            upgPkgPk | labelableCTpePk | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk      | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          } retryUntilTimeout

        Expect:
          __exercises() `returns` Table.empty

        // Read API tests
        // active
        Expect:
          active() `returns` table {
            pkgId    | s"$packageName:Ping:Ping"            | "template"  | cId1
            upgPkgId | s"$packageName:Interfaces:Labelable" | "interface" | cId3
            upgPkgId | s"$packageName:Ping:Ping"            | "template"  | cId3
          }
        Expect:
          active(Some(s"$packageName:Ping:Ping")) `returns` table {
            pkgId    | s"$packageName:Ping:Ping" | "template" | cId1
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId3
          }
        // archives
        Expect:
          archives() `returns` table {
            upgPkgId | s"$packageName:Interfaces:Labelable" | "interface" | cId2
            upgPkgId | s"$packageName:Ping:Ping"            | "template"  | cId2
          }
        Expect:
          archives(Some(s"$packageName:Ping:Ping")) `returns` table {
            upgPkgId | s"$packageName:Ping:Ping" | "template" | cId2
          }
        // creates
        Expect:
          creates() `returns` table {
            pkgId    | s"$packageName:Ping:Ping"            | "template"  | cId1
            upgPkgId | s"$packageName:Interfaces:Labelable" | "interface" | cId2
            upgPkgId | s"$packageName:Ping:Ping"            | "template"  | cId2
            upgPkgId | s"$packageName:Interfaces:Labelable" | "interface" | cId3
            upgPkgId | s"$packageName:Ping:Ping"            | "template"  | cId3
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
      funcTest("Ambiguous Package Names are correctly handled when using transaction trees."):
        val dar = Capture[DeployedDar]
        Given:
          (DamlSdk.dar(ping.withVersion("0.0.0")) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
        And:
          dar.captureFromService
        And:
          DamlSdk.runScript("Ping:transact", alice.id)

        // upload the second dar
        val upgradedDar = Capture[DeployedDar]
        When:
          DamlSdk.dar(pingUpgrade.withVersion("0.0.0")) >+> DamlSdk.deploy >+> DamlSdk
            .runScript("Ping:transact", alice.id)
        And:
          upgradedDar.captureFromService

        And:
          Scribe.pipeline(
            "--pipeline-datasource=TransactionTreeStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        lazy val pkgId    = dar.get.dar.packageId
        lazy val upgPkgId = upgradedDar.get.dar.packageId
        val pkgPk         = Capture[Int]
        val upgPkgPk      = Capture[Int]

        lazy val (pkgIdR1, pkgIdR2, pkgPkR1, pkgPkR2) =
          if pkgId < upgPkgId then (pkgId, upgPkgId, pkgPk, upgPkgPk) else (upgPkgId, pkgId, upgPkgPk, pkgPk)
        Expect:
          __packages(sql"""order by id""") `returns` table {
            pkgPkR1.capture | packageName | "0.0.0" | pkgIdR1
            pkgPkR2.capture | packageName | "0.0.0" | pkgIdR2
          } retryUntilTimeout

        val pingCTpePk      = Capture[Int]
        val labelableCTpePk = Capture[Int]
        Expect:
          __contract_tpe() `returns` table {
            labelableCTpePk.capture | s"$packageName:Interfaces:Labelable" | "interface" | s"{$packageName:Interfaces:Labelable,Interfaces:Labelable,Labelable}"
            pingCTpePk.capture | s"$packageName:Ping:Ping" | "template" | s"{$packageName:Ping:Ping,Ping:Ping,Ping}"
          }

        val cId1 = Capture[String]
        val cId2 = Capture[String]
        val cId3 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk | cId1.capture | "[1,)"  | null
            upgPkgPk | pingCTpePk | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          } retryUntilTimeout

        val setLabelChoiceETpePk = Capture[Int]
        Expect:
          __exercise_tpe() `returns` table {
            anything | s"$packageName:Interfaces:Labelable" | s"$packageName:Interfaces:Labelable:Archive" | "Archive" | true | s"{$packageName:Interfaces:Labelable:Archive,Interfaces:Labelable:Archive,Labelable:Archive,Archive}"
            setLabelChoiceETpePk.capture | s"$packageName:Interfaces:Labelable" | s"$packageName:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | true | s"{$packageName:Interfaces:Labelable:SetLabelChoice,Interfaces:Labelable:SetLabelChoice,Labelable:SetLabelChoice,SetLabelChoice}"
            anything | s"$packageName:Ping:Ping" | s"$packageName:Ping:Ping:Archive" | "Archive" | true | s"{$packageName:Ping:Ping:Archive,Ping:Ping:Archive,Ping:Archive,Archive}"
          }

        Expect:
          __exercises() `returns` table {
            upgPkgPk | setLabelChoiceETpePk | labelableCTpePk | cId2 | "exercised SetLabelChoice on upgraded contract"
          }

        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk | cId1 | "[1,)"  | null
            upgPkgPk | pingCTpePk | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          } retryUntilTimeout

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
          exercises(Some(s"$packageName:Interfaces:Labelable:SetLabelChoice")) `returns` table {
            upgPkgId | s"$packageName:Interfaces:Labelable" | s"$packageName:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | cId2 | "exercised SetLabelChoice on upgraded contract"
          }
    )
  ) @@ onlyDamlLfVersion("<1.16")
