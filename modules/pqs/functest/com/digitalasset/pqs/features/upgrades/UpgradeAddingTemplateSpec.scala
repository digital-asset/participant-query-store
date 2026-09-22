// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.upgrades

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Database.__contracts
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.transcode.schema.PackageName
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything

import scala.language.{implicitConversions, postfixOps}

/** An upgraded package may add entities that the version it upgrades does not have.
  *
  * `__contract_tpe` keys entities by package *name*, while `__packages` holds one row per package *id*, so the single
  * `Ping:Pong` entity row is shared by both package ids of the `Ping` package - including the one that never contained
  * `Pong`. Anything that reconstructs identifiers by joining those two tables therefore has to cope with `(<v1 package
  * id>, Ping, Pong)`, which is not an entity that exists on the ledger.
  */
object UpgradeAddingTemplateSpec extends SharedLedgerAndPostgresTest:
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
                |ping: Party -> Script ()
                |ping alice = void do
                |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                |""".stripMargin
  )

  private val pingUpgrade = DamlSource(
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
                |template Pong
                |  with
                |    sender: Party
                |    receiver: Party
                |  where
                |    signatory sender
                |    observer receiver
                |
                |pong: Party -> Script ()
                |pong alice = void do
                |  submit alice $ createCmd Pong with sender = alice, receiver = alice
                |""".stripMargin
  ).upgrades(ping)

  def spec = suite("Upgrades Adding Entities")(
    funcTest("Templates added by an upgraded package are ingested alongside the version that lacks them.") {
      val alice = Party("Alice")
      val dar   = Capture[DeployedDar]
      Given:
        DamlSdk.dar(ping) ++ DamlSdk.parties(alice) ++ Postgres.database
          >+> DamlSdk.deploy
          >+> DamlSdk.runScript("Ping:ping", alice.id)
      And:
        dar.captureFromService

      val upgradedDar = Capture[DeployedDar]
      When:
        DamlSdk.dar(pingUpgrade) >+> DamlSdk.deploy >+> DamlSdk.runScript("Ping:pong", alice.id)
      And:
        upgradedDar.captureFromService

      And:
        // --retry-counter-attempts=0 so that a failure to start is reported as a non-zero exit code
        // instead of being retried.
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--retry-counter-attempts=0",
          s"--pipeline-filter-contracts=${ping.name}:*"
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

      // One entity row per qualified name, shared by both package ids
      val pingCTpePk = Capture[Int]
      val pongCTpePk = Capture[Int]
      Expect:
        __contract_tpe(ping.name) `returns` table {
          pingCTpePk.capture | s"${ping.name}:Ping:Ping" | "template" | s"{${ping.name}:Ping:Ping,Ping:Ping,Ping}"
          pongCTpePk.capture | s"${ping.name}:Ping:Pong" | "template" | s"{${ping.name}:Ping:Pong,Ping:Pong,Pong}"
        }

      Expect:
        __contracts() `returns` table {
          pkgPk    | pingCTpePk | anything | anything
          upgPkgPk | pongCTpePk | anything | anything
        }
    }
  )

  private def __packages(name: PackageName) =
    Postgres.query(sql"select pk, name, version, id from __packages where name=${name.toString} order by version")

  private def __contract_tpe(name: PackageName) =
    Postgres.query(
      sql"select pk, template_fqn, payload_type, aliases from __contract_tpe where package_name=${name.toString} order by template_fqn"
    )
