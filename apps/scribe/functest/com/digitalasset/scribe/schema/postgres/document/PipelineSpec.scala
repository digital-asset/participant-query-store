// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.schema.postgres.document

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.DamlSdk.onlyCantonVersion
import com.digitalasset.scribe.services.daml.{DamlSdk, DamlSource, DarFile, Party}
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.*
import zio.{Chunk, ExitCode, ZIO}

import scala.language.implicitConversions

object PipelineSpec extends SharedLedgerAndPostgresTest:
  val alice = Party("Alice")
  val bob   = Party("Bob")
  val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    sender: Party
                    |    receiver: Party
                    |    text: Text
                    |  where
                    |    signatory sender
                    |    observer receiver
                    |
                    |transact1 : (Party, Party, Text) -> Script ()
                    |transact1 (alice, bob, text) = void do
                    |  submitMulti [alice, bob] [] $ createCmd Ping with sender = alice, receiver = bob, text = text
                    |
                    |-- test Archive edge case, create another contract with implicit Archive in the same module
                    |template ArchiveTest
                    |  with nobody: Party
                    |  where signatory nobody
                    |
                    |transact2 : (Party, Party, Text) -> Script ()
                    |transact2 (alice, bob, text) = void do
                    |  cid <- submitMulti [alice, bob] [] $ createCmd Ping with sender = alice, receiver = bob, text = text
                    |  submit alice $ archiveCmd cid
                    |""".stripMargin
  )
  private val packageName = pingPong.name
  private val templateRef = s"$packageName:PingPong:Ping"
  private val archiveRef  = s"$templateRef:Archive"

  def spec = suite("pipeline")(
    funcTest("single transaction"):
      Given:
        DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice, bob) ++ Postgres.database
          >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      When:
        Scribe.runPipeline("--pipeline-ledger-stop=Latest")
      And:
        Postgres.hasTable("__transactions")
      And:
        Postgres.query(sql"select ix from latest_checkpoint()").returns(table(0L))
      And:
        Postgres.query(sql"select ix from __watermark").returns(table(0L))
      And:
        Postgres.query(sql"select instance_id from __watermark").returns(table(not(isNull)))

      lazy val aliceId = Capture[String]
      lazy val bobId   = Capture[String]
      And:
        alice.id.is(aliceId.capture)
      And:
        bob.id.is(bobId.capture)
      Then:
        Postgres
          .query(sql"""select payload from active($templateRef)""".query[String].selectOne)
          .returns(Some(s"""{"text": "test", "sender": "$aliceId", "receiver": "$bobId"}"""))
    ,
    funcTest("special characters in payload are stored"):
      //        lazy val testText         = "aя麤\t\r\n\f\u0009 \"" + (0 to 255).map(_.toChar).mkString // TODO fix this, see https://www.postgresql.org/docs/current/datatype-json.html
      lazy val testText = "aя麤\t\r\n\f\u0009 \"" + (1 to 255).map(_.toChar).mkString
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed(testText))

      When:
        Scribe.runPipeline("--pipeline-ledger-stop=Latest")

      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Postgres `query`
          sql"""select payload from active($templateRef)"""
            .query[String]
            .selectOne
            .someOrFail(Throwable("no payload"))
            .map(ujson.read(_).obj("text").str) `returns` testText
    ,
    funcTest("metadata is stored"):
      lazy val testText = (1 to 255).map(_.toChar).mkString
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed(testText))

      When:
        Scribe.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=*"
        )

      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        for
          packageId <- ZIO.service[DarFile].map(_.packageId)
          aliceId   <- alice.id
          txId <- Postgres `query` sql"select transaction_id from __transactions"
            .query[String]
            .selectAll
            .collect(Throwable("single transaction id not found")) { case Chunk(ex) => ex }
          storedMetadata <- Postgres `query` sql"""select metadata from __contracts"""
            .query[Array[Byte]]
            .selectOne
            .someOrFail(Throwable("no payload"))
          expectedMetadata <- DamlSdk.api.getSingleCreatedBlob(Seq(aliceId), txId)
        yield zio.test.assertTrue(storedMetadata.toSeq == expectedMetadata.toSeq)
    ,
    funcTest("created_at should be not null"):
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      When:
        Scribe.runPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        Postgres query {
          sql"""select count(*) from __contracts where created_at_ix is not null"""
        } `returns` table {
          1
        }
    ,
    funcTest("simple archive in tree stream"):
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact2", alice.id <&> bob.id <&> ZIO.succeed("test"))

      When:
        Scribe.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-datasource=TransactionTreeStream"
        )

      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Postgres `query` sql"""select count(*) from active($templateRef)""" `returns` table { 0 }
      And:
        Postgres `query` sql"""select count(*) from exercises($archiveRef)""" `returns` table { 1 }
    ,
    funcTest("paid_traffic_cost is populated for submitting participant") {
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      When:
        Scribe.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")
      Expect:
        Postgres query {
          sql"""select paid_traffic_cost from __transactions where transaction_id is not null limit 1"""
            .query[Long]
            .selectOne
        } `returns` isSome(isGreaterThanEqualTo(1L))
    } @@ onlyCantonVersion(">=3.5"),
    funcTest("can write to non-public schema"):
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))

      When:
        Scribe.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--target-postgres-schema=custom_schema"
        )

      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Postgres.hasTable("__transactions", "custom_schema")
      And:
        Postgres.lacksTable("__transactions", "public")
  )
