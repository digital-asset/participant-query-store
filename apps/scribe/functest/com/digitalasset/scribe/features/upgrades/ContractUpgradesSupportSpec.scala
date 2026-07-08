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
import com.digitalasset.transcode.schema.PackageName
import zio.ZIO
import zio.jdbc.sqlInterpolator
import zio.schema.internal.SourceLocation
import zio.test.Assertion.anything

import scala.language.{implicitConversions, postfixOps}

object ContractUpgradesSupportSpec extends SharedLedgerAndPostgresTest:
  private def createPingIface(using location: SourceLocation) = DamlSource(
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
  ).withNameSuffix(s"v${location.line}")

  private def createPing(pingIface: DamlSource)(using location: SourceLocation) = DamlSource(
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
  ).withNameSuffix(s"v${location.line}").dependsOn(pingIface)

  private def createPingUpgrade(pingIface: DamlSource, ping: DamlSource) = DamlSource(
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
                |setLabel: (Party, ContractId Ping) -> Script ()
                |setLabel (alice, cid) = script do
                |  submit alice $ exerciseCmd (toInterfaceContractId @Labelable cid) SetLabelChoice with newLabel = "exercised SetLabelChoice on contract created with original template version"
                |  pure ()
                |""".stripMargin
  ).upgrades(ping).dependsOn(pingIface)

  def spec = suite("Contract Upgrades")(
    suite("transactions")(
      funcTest("Contract upgrades are correctly handled when using transactions.") {
        val pingIface   = createPingIface
        val ping        = createPing(pingIface)
        val pingUpgrade = createPingUpgrade(pingIface, ping)
        val alice       = Party("Alice")
        val dar         = Capture[DeployedDar]
        Given:
          (DamlSdk.dar(ping) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
        And:
          dar.captureFromService
        And:
          DamlSdk.runScript("Ping:transact", alice.id)

        val upgradedDar = Capture[DeployedDar]
        When:
          DamlSdk.dar(pingUpgrade) >+> DamlSdk.deploy >+> DamlSdk.runScript("Ping:transact", alice.id)
        And:
          upgradedDar.captureFromService

        And:
          Scribe.runPipeline(
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
          __packages(ping.name) `returns` table {
            pkgPk.capture    | ping.name | "0.0.0" | pkgId
            upgPkgPk.capture | ping.name | "0.0.1" | upgPkgId
          }

        val pingCTpePk      = Capture[Int]
        val labelableCTpePk = Capture[Int]
        Expect:
          __contract_tpe(ping.name, pingIface.name) `returns` table {
            pingCTpePk.capture | s"${ping.name}:Ping:Ping" | "template" | s"{${ping.name}:Ping:Ping,Ping:Ping,Ping}"
            labelableCTpePk.capture | s"${pingIface.name}:Interfaces:Labelable" | "interface" | s"{${pingIface.name}:Interfaces:Labelable,Interfaces:Labelable,Labelable}"
          }

        val cId1 = Capture[String]
        val cId2 = Capture[String]
        val cId3 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk      | cId1.capture | "[1,)"  | null
            pkgPk    | labelableCTpePk | anything     | "[1,)"  | null
            upgPkgPk | pingCTpePk      | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | anything     | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | labelableCTpePk | anything     | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          }

        val setLabelChoiceETpePk = Capture[Int]
        Expect:
          __exercise_tpe(ping.name, pingIface.name) `returns` table {
            anything | s"${ping.name}:Ping:Ping" | s"${ping.name}:Ping:Ping:Archive" | "Archive" | true | s"{${ping.name}:Ping:Ping:Archive,Ping:Ping:Archive,Ping:Archive,Archive}"
            anything | s"${pingIface.name}:Interfaces:Labelable" | s"${pingIface.name}:Interfaces:Labelable:Archive" | "Archive" | true | s"{${pingIface.name}:Interfaces:Labelable:Archive,Interfaces:Labelable:Archive,Labelable:Archive,Archive}"
            setLabelChoiceETpePk.capture | s"${pingIface.name}:Interfaces:Labelable" | s"${pingIface.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | true | s"{${pingIface.name}:Interfaces:Labelable:SetLabelChoice,Interfaces:Labelable:SetLabelChoice,Labelable:SetLabelChoice,SetLabelChoice}"
          }

        Expect:
          __exercises() `returns` Table.empty

        And:
          DamlSdk.dar(pingUpgrade) >+> DamlSdk.runScript("Ping:setLabel", alice.id <&> ZIO.succeed(cId1.get))

        And:
          Scribe.runPipeline(
            "--pipeline-datasource=TransactionStream",
            "--pipeline-ledger-start=Oldest",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        val cId4 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk      | cId1 | "[1,4)" | null
            pkgPk    | labelableCTpePk | cId1 | "[1,4)" | null
            upgPkgPk | pingCTpePk      | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | labelableCTpePk | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk | cId4.capture | "[4,)" | "exercised SetLabelChoice on contract created with original template version"
            upgPkgPk | labelableCTpePk | cId4.capture | "[4,)" | "exercised SetLabelChoice on contract created with original template version"
          }

        Expect:
          __exercises() `returns` Table.empty

        // Read API tests
        // active
        Expect:
          active() `returns` table {
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId3
            upgPkgId | s"${ping.name}:Ping:Ping"                 | "template"  | cId3
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId4
            upgPkgId | s"${ping.name}:Ping:Ping"                 | "template"  | cId4
          }
        Expect:
          active(Some(s"${ping.name}:Ping:Ping")) `returns` table {
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId3
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId4
          }
        // archives
        Expect:
          archives() `returns` table {
            pkgId    | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId1
            pkgId    | s"${ping.name}:Ping:Ping"                 | "template"  | cId1
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId2
            upgPkgId | s"${ping.name}:Ping:Ping"                 | "template"  | cId2
          }
        Expect:
          archives(Some(s"${ping.name}:Ping:Ping")) `returns` table {
            pkgId    | s"${ping.name}:Ping:Ping" | "template" | cId1
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId2
          }
        // creates
        Expect:
          creates() `returns` table {
            pkgId    | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId1
            pkgId    | s"${ping.name}:Ping:Ping"                 | "template"  | cId1
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId2
            upgPkgId | s"${ping.name}:Ping:Ping"                 | "template"  | cId2
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId3
            upgPkgId | s"${ping.name}:Ping:Ping"                 | "template"  | cId3
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | "interface" | cId4
            upgPkgId | s"${ping.name}:Ping:Ping"                 | "template"  | cId4
          }
        Expect:
          creates(Some(s"${ping.name}:Ping:Ping")) `returns` table {
            pkgId    | s"${ping.name}:Ping:Ping" | "template" | cId1
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId2
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId3
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId4
          }
        // exercises
        Expect:
          exercises() `returns` Table.empty
      }
    ),
    suite("transaction trees")(
      funcTest("Contract upgrades are correctly handled when using transaction trees."):
        val pingIface   = createPingIface
        val ping        = createPing(pingIface)
        val pingUpgrade = createPingUpgrade(pingIface, ping)
        val alice       = Party("Alice")
        val dar         = Capture[DeployedDar]
        Given:
          (DamlSdk.dar(ping) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
        And:
          dar.captureFromService
        And:
          DamlSdk.runScript("Ping:transact", alice.id)

        // upload the second dar
        val upgradedDar = Capture[DeployedDar]
        When:
          DamlSdk.dar(pingUpgrade) >+> DamlSdk.deploy >+> DamlSdk.runScript("Ping:transact", alice.id)
        And:
          upgradedDar.captureFromService

        And:
          Scribe.runPipeline(
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
          __packages(ping.name) `returns` table {
            pkgPk.capture    | ping.name | "0.0.0" | pkgId
            upgPkgPk.capture | ping.name | "0.0.1" | upgPkgId
          }

        val pingCTpePk      = Capture[Int]
        val labelableCTpePk = Capture[Int]
        Expect:
          __contract_tpe(ping.name, pingIface.name) `returns` table {
            pingCTpePk.capture | s"${ping.name}:Ping:Ping" | "template" | s"{${ping.name}:Ping:Ping,Ping:Ping,Ping}"
            labelableCTpePk.capture | s"${pingIface.name}:Interfaces:Labelable" | "interface" | s"{${pingIface.name}:Interfaces:Labelable,Interfaces:Labelable,Labelable}"
          }

        val cId1 = Capture[String]
        val cId2 = Capture[String]
        val cId3 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk      | cId1.capture | "[1,)"  | null
            pkgPk    | labelableCTpePk | cId1.capture | "[1,)"  | null
            upgPkgPk | pingCTpePk      | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | cId2.capture | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | labelableCTpePk | cId3.capture | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
          }

        val setLabelChoiceETpePk = Capture[Int]
        Expect:
          __exercise_tpe(ping.name, pingIface.name) `returns` table {
            anything | s"${ping.name}:Ping:Ping" | s"${ping.name}:Ping:Ping:Archive" | "Archive" | true | s"{${ping.name}:Ping:Ping:Archive,Ping:Ping:Archive,Ping:Archive,Archive}"
            anything | s"${pingIface.name}:Interfaces:Labelable" | s"${pingIface.name}:Interfaces:Labelable:Archive" | "Archive" | true | s"{${pingIface.name}:Interfaces:Labelable:Archive,Interfaces:Labelable:Archive,Labelable:Archive,Archive}"
            setLabelChoiceETpePk.capture | s"${pingIface.name}:Interfaces:Labelable" | s"${pingIface.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | true | s"{${pingIface.name}:Interfaces:Labelable:SetLabelChoice,Interfaces:Labelable:SetLabelChoice,Labelable:SetLabelChoice,SetLabelChoice}"
          }

        Expect:
          __exercises() `returns` table {
            upgPkgPk | setLabelChoiceETpePk | labelableCTpePk | cId2 | "exercised SetLabelChoice on upgraded contract"
          }

        And:
          DamlSdk.runScript("Ping:setLabel", alice.id <&> ZIO.succeed(cId1.get))

        And:
          Scribe.runPipeline(
            "--pipeline-datasource=TransactionTreeStream",
            "--pipeline-ledger-start=Oldest",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        val cId4 = Capture[String]
        Expect:
          __contracts() `returns` table {
            pkgPk    | pingCTpePk      | cId1 | "[1,4)" | null
            pkgPk    | labelableCTpePk | cId1 | "[1,4)" | null
            upgPkgPk | pingCTpePk      | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | labelableCTpePk | cId2 | "[2,3)" | "created upgraded contract"
            upgPkgPk | pingCTpePk      | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | labelableCTpePk | cId3 | "[3,)"  | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | pingCTpePk | cId4.capture | "[4,)" | "exercised SetLabelChoice on contract created with original template version"
            upgPkgPk | labelableCTpePk | cId4.capture | "[4,)" | "exercised SetLabelChoice on contract created with original template version"
          }

        Expect:
          __exercises() `returns` table {
            upgPkgPk | setLabelChoiceETpePk | labelableCTpePk | cId2 | "exercised SetLabelChoice on upgraded contract"
            upgPkgPk | setLabelChoiceETpePk | labelableCTpePk | cId1 | "exercised SetLabelChoice on contract created with original template version"
          }

        // Read API tests
        // active
        Expect:
          active(Some(s"${ping.name}:Ping:Ping")) `returns` table {
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId3
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId4
          }
        // archives
        Expect:
          archives(Some(s"${ping.name}:Ping:Ping")) `returns` table {
            pkgId    | s"${ping.name}:Ping:Ping" | "template" | cId1
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId2
          }
        // creates
        Expect:
          creates(Some(s"${ping.name}:Ping:Ping")) `returns` table {
            pkgId    | s"${ping.name}:Ping:Ping" | "template" | cId1
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId2
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId3
            upgPkgId | s"${ping.name}:Ping:Ping" | "template" | cId4
          }
        // exercises
        Expect:
          exercises(Some(s"${pingIface.name}:Interfaces:Labelable:SetLabelChoice")) `returns` table {
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | s"${pingIface.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | cId2 | "exercised SetLabelChoice on upgraded contract"
            upgPkgId | s"${pingIface.name}:Interfaces:Labelable" | s"${pingIface.name}:Interfaces:Labelable:SetLabelChoice" | "SetLabelChoice" | cId1 | "exercised SetLabelChoice on contract created with original template version"
          }
    )
  ) @@ onlyDamlLfVersion(">=1.17")

  private def __packages(name: PackageName) =
    Postgres.query(sql"select pk, name, version, id from __packages where name=${name.toString} order by pk")

  private def __contract_tpe(packageNames: PackageName*) =
    val filter = packageNames.map(packageName => sql"package_name=${packageName.toString}").mkFragment(sql" or ")
    Postgres.query(
      sql"select pk, template_fqn, payload_type, aliases from __contract_tpe where $filter order by pk"
    )

  private def __exercise_tpe(packageNames: PackageName*) =
    val filter = packageNames.map(packageName => sql"package_name=${packageName.toString}").mkFragment(sql" or ")
    Postgres.query(
      sql"select pk, template_fqn, choice_fqn, choice, consuming, aliases from __exercise_tpe where $filter order by pk"
    )
