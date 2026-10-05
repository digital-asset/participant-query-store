// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.{CliRun, Pqs}
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import zio.{ZIO, ZLayer}

import scala.language.{implicitConversions, postfixOps}

object OrphanedPackageDecodingSpec extends SharedLedgerAndPostgresTest:
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

  def spec = suite("Orphaned package decoding")(
    orphanedDecoding("Contract of an unvetted package is decoded - updates stream", acsStream = false),
    orphanedDecoding("Contract of an unvetted package is decoded - ACS stream", acsStream = true)
  )

  private def orphanedDecoding(label: String, acsStream: Boolean) = funcTest(label):
    val alice       = Party("Alice")
    val orphaned    = pingPong.withNameSuffix(if acsStream then "orphaned-acs" else "orphaned-updates")
    val dar         = Capture[DarFile]
    val contractId  = Capture[String]
    val templateFqn = s"${orphaned.name}:PingPong:Ping"

    Given:
      DamlSdk.parties(alice) >+> DamlSdk.deploy(orphaned)
    And:
      dar.captureFromService
    Expect:
      vettedIds(dar.get.packageId) `is` Seq(dar.get.packageId) retryUntilTimeout
    Then:
      createContract(alice).is(contractId.capture)
    When:
      Ledger.unvetDar(dar.get)
    Expect:
      vettedIds(dar.get.packageId) `is` Seq.empty retryUntilTimeout
    When:
      Postgres.database
        >+> Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          s"--pipeline-ledger-start=${if acsStream then "Latest" else "Genesis"}",
          "--pipeline-ledger-stop=Latest"
        )
    Expect:
      Database
        .active(Some(templateFqn), extraColumns = Seq("payload"))
        .returns(
          table {
            dar.get.packageId | templateFqn | "template" | contractId | s"""{"sender": "${alice.id}"}"""
          }
        )

  private def vettedIds(packageId: String) =
    Ledger.listVettedPackages(packageId).map(_.vettedPackages.flatMap(_.packages).map(_.packageId))

  private def createContract(alice: Party): ZIO[Docker & Service[Ledger] & DarFile, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice)
      .map(_.getTransaction.events(0).getCreated.contractId)
