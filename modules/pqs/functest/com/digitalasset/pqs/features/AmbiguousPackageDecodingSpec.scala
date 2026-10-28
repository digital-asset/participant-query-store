// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.services.daml.*
import zio.ZIO
import zio.test.Assertion.hasSameElements

import scala.language.implicitConversions

object AmbiguousPackageDecodingSpec extends SharedLedgerAndPostgresTest:
  private val packageVersion = "1.0.0"

  private val pingWithNote = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |template Ping
                    |  with
                    |    sender: Party
                    |    note: Text
                    |  where
                    |    signatory sender
                    |""".stripMargin
  )

  def spec = suite("Ambiguous package decoding")(
    ambiguousDecoding(
      "Contracts of packages with the same name and version are decoded - updates stream",
      acsStream = false
    ),
    ambiguousDecoding(
      "Contracts of packages with the same name and version are decoded - ACS stream",
      acsStream = true
    )
  )

  private def ambiguousDecoding(label: String, acsStream: Boolean) = funcTest(label):
    val alice       = Party("Alice")
    val packageName = if acsStream then "ambiguous-package-acs" else "ambiguous-package-updates"
    val p1          = Dars.pingPong.withName(packageName).withVersion(packageVersion)
    val p2          = pingWithNote.withName(packageName).withVersion(packageVersion)
    val dar1        = Capture[DarFile]
    val dar2        = Capture[DarFile]
    val contractId1 = Capture[String]
    val contractId2 = Capture[String]
    val templateFqn = s"$packageName:PingPong:Ping"

    Given:
      DamlSdk.parties(alice) >+> DamlSdk.deploy(p1)
    And:
      dar1.captureFromService
    Then:
      createPing(alice).is(contractId1.capture)
    When:
      Ledger.unvetDar(dar1.get)
    Expect:
      Ledger.isPackageVetted(dar1.get.packageId).is(false)
    When:
      DamlSdk.deploy(p2)
    And:
      dar2.captureFromService
    Then:
      createPing(alice, RecordField("note", Some(Value(Value.Sum.Text("hello"))))).is(contractId2.capture)
    When:
      Postgres.database
        >+> Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          s"--pipeline-ledger-start=${if acsStream then "Latest" else "Genesis"}",
          "--pipeline-ledger-stop=Latest"
        )
    Expect:
      Database
        .active(Some(templateFqn), extraColumns = Seq("package_name", "package_version", "payload"))
        .map(_.rows)
        .is(
          hasSameElements(
            table {
              dar1.get.packageId | templateFqn | "template" | contractId1 | packageName | packageVersion | s"""{"sender": "${alice.id}"}"""
              dar2.get.packageId | templateFqn | "template" | contractId2 | packageName | packageVersion | s"""{"note": "hello", "sender": "${alice.id}"}"""
            }.rows
          )
        )

  private def createPing(
      alice: Party,
      extraFields: RecordField*
  ): ZIO[Docker & Ledger & DarFile, Throwable, String] =
    val args = Record.defaultInstance.addFields(
      RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))) +: extraFields*
    )
    Ledger
      .create("PingPong:Ping", args, alice)
      .map(_.getTransaction.events(0).getCreated.contractId)
