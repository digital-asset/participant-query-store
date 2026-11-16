// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.document

import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.*
import zio.{Chunk, ExitCode, ZIO}

import scala.language.implicitConversions

object PipelineSpec extends SharedLedgerAndPostgresTest:
  private val packageName = Dars.message.name
  private val templateRef = s"$packageName:Message:Message"
  private val archiveRef  = s"$templateRef:Archive"

  def context(alice: Party, bob: Party, script: String = "Message:createMessage", testText: String = "test") =
    DamlSdk.parties(alice, bob) ++ Postgres.database
      >+> DamlSdk.deploy(Dars.message)
      >+> DamlSdk.runScript(script, (alice.id, bob.id, testText))

  def spec = suite("pipeline")(
    funcTest("single transaction"):
      val alice = Party("Alice")
      val bob   = Party("Bob")
      Given:
        context(alice, bob)
      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      And:
        Database.hasTable("__transactions")
      And:
        Database.query(sql"select ix from latest_checkpoint()").returns(Table(0L))
      And:
        Database.query(sql"select ix from __watermark").returns(Table(0L))
      And:
        Database.query(sql"select instance_id from __watermark").returns(Table(not(isNull)))

      Then:
        Database
          .query(sql"""select payload from active($templateRef)""".query[String].selectOne)
          .returns(Some(s"""{"text": "test", "sender": "${alice.id}", "receiver": "${bob.id}"}"""))
    ,
    funcTest("special characters in payload are stored"):
      val alice = Party("Alice")
      val bob   = Party("Bob")
      //        lazy val testText         = "aя麤\t\r\n\f\u0009 \"" + (0 to 255).map(_.toChar).mkString // TODO fix this, see https://www.postgresql.org/docs/current/datatype-json.html
      lazy val testText = "aя麤\t\r\n\f\u0009 \"" + (1 to 255).map(_.toChar).mkString
      Given:
        context(alice, bob, testText = testText)

      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")

      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        Database `query`
          sql"""select payload from active($templateRef)"""
            .query[String]
            .selectOne
            .someOrFail(Throwable("no payload"))
            .map(ujson.read(_).obj("text").str) `returns` testText
    ,
    funcTest("metadata is stored"):
      val alice         = Party("Alice")
      val bob           = Party("Bob")
      lazy val testText = (1 to 255).map(_.toChar).mkString
      Given:
        context(alice, bob, testText = testText)

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=*"
        )

      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        for
          packageId <- ZIO.service[DarFile].map(_.packageId)
          txId <- Database `query` sql"select transaction_id from __transactions"
            .query[String]
            .selectAll
            .collect(Throwable("single transaction id not found")) { case Chunk(ex) => ex }
          storedMetadata <- Database `query` sql"""select metadata from __contracts"""
            .query[Array[Byte]]
            .selectOne
            .someOrFail(Throwable("no payload"))
          createdEvent <- Ledger.getSingleCreatedEvent(Seq(alice), txId)
        yield
          val eventBlob = createdEvent.createdEventBlob.toByteArray.toSeq
          zio.test.assert(eventBlob)(not(isEmpty) && equalTo(storedMetadata.toSeq))
    ,
    funcTest("activated_at_ix should be not null"):
      val alice = Party("Alice")
      val bob   = Party("Bob")
      Given:
        context(alice, bob)
      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        Database query {
          sql"""select count(*) from __contracts where activated_at_ix is not null"""
        } `returns` Table {
          1
        }
    ,
    funcTest("simple archive in tree stream"):
      val alice = Party("Alice")
      val bob   = Party("Bob")
      Given:
        context(alice, bob, script = "Message:createAndArchiveMessage")

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-datasource=TransactionTreeStream"
        )

      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        Database `query` sql"""select count(*) from active($templateRef)""" `returns` Table { 0 }
      And:
        Database `query` sql"""select count(*) from exercises($archiveRef)""" `returns` Table { 1 }
      And:
        for
          synchronizers <- Ledger.getAllSynchronizers
          synchronizerId <- synchronizers match
            case Seq(single) => ZIO.succeed(single.synchronizerId)
            case other =>
              ZIO.fail(Throwable(s"expected exactly one connected synchronizer, found ${other.size}"))
          synchronizerIds <- Database `query` sql"select synchronizer_id from __transactions"
            .query[String]
            .selectAll
        yield zio.test.assert(synchronizerIds)(hasSize(equalTo(2)) && forall(equalTo(synchronizerId)))
    ,
    funcTest("paid_traffic_cost is populated for submitting participant") {
      val alice = Party("Alice")
      val bob   = Party("Bob")
      Given:
        context(alice, bob)
      When:
        Pqs.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")
      Expect:
        Database query {
          sql"""select paid_traffic_cost from __transactions where transaction_id is not null limit 1"""
            .query[Long]
            .selectOne
        } `returns` isSome(isGreaterThanEqualTo(1L))
    },
    funcTest("can write to non-public schema"):
      val alice = Party("Alice")
      val bob   = Party("Bob")
      Given:
        context(alice, bob)

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--target-postgres-schema=custom_schema"
        )

      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        Database.hasTable("__transactions", "custom_schema")
      And:
        Database.lacksTable("__transactions", "public")
  )
