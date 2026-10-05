// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.daml.ledger.api.v2.package_service.ListVettedPackagesResponse
import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.{CliRun, Pqs}
import com.digitalasset.pqs.utils.safeequals.*
import com.digitalasset.pqs.functest.{Dpm, FTEnv, FuncTest}
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import zio.{Scope, ZLayer, ZIO}

import scala.language.{implicitConversions, postfixOps}

object OrphanedPackageDecodingSpec extends FuncTest[Service[Ledger] & Postgres & DeployedDar]:
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

  val shared: ZLayer[FTEnv & Dpm & Docker, Throwable, Service[Ledger] & Postgres & DeployedDar] =
    DamlSdk.dar(pingPong) ++ DamlSdk.ledger ++ Postgres.instance >+> DamlSdk.deploy

  def spec = suite("Orphaned package decoding")(
    funcTest("Contract of an unvetted package is decoded - updates stream")(orphanedDecoding(acsStream = false)),
    funcTest("Contract of an unvetted package is decoded - ACS stream")(orphanedDecoding(acsStream = true))
  )

  private def orphanedDecoding(acsStream: Boolean)(using
      Ctx[Parties & DarFile & DeployedDar & Database & CliRun, Parties & Database & Environment & Scope]
  ): Unit =
    val alice       = Party("Alice")
    val orphaned    = pingPong.withNameSuffix(if acsStream then "orphaned-acs" else "orphaned-updates")
    val dar         = Capture[DeployedDar]
    val contractId  = Capture[String]
    val templateFqn = s"${orphaned.name}:PingPong:Ping"

    Given:
      DamlSdk.parties(alice) ++ (DamlSdk.dar(orphaned) >+> DamlSdk.deploy)
    And:
      dar.captureFromService
    Expect:
      Ledger.listVettedPackages(dar.get.packageId).map(vettedIds(_, dar.get.packageId)) `is`
        Seq(dar.get.packageId) retryUntilTimeout
    Then:
      createContract(alice).is(contractId.capture)
    When:
      Ledger.unvetDar(dar.get.dar)
    Expect:
      Ledger.listVettedPackages(dar.get.packageId).map(vettedIds(_, dar.get.packageId)) `is`
        Seq.empty[String] retryUntilTimeout
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

  private def vettedIds(response: ListVettedPackagesResponse, packageId: String) =
    response.vettedPackages.flatMap(_.packages).map(_.packageId).filter(_ === packageId)

  private def createContract(alice: Party): ZIO[Docker & Service[Ledger] & DeployedDar, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice)
      .map(_.getTransaction.events(0).getCreated.contractId)
