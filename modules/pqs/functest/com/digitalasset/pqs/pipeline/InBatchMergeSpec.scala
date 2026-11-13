// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.daml.ledger.api.v2.value.*
import com.digitalasset.canonical.{Event, Transaction}
import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.{Dars, SharedLedgerAndPostgresTest}
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.zio.daml.DamlSchema
import zio.jdbc.*
import zio.test.*
import zio.test.Assertion.*
import zio.{Chunk, ZIO}

import scala.language.implicitConversions

object InBatchMergeSpec extends SharedLedgerAndPostgresTest:
  def spec = suite("In-batch deactivation merge")(
    funcTest("writes an archive into its create's row instead of staging it") {
      val alice        = Party("Alice")
      val contractId   = Capture[String]
      val transactions = Capture[Chunk[Transaction[Event]]]

      Given:
        DamlSdk.parties(alice) >+> DamlSdk.deploy(Dars.pingPong)
      Then:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.archive("PingPong:Ping", contractId.get, alice)
      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService
      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(2)) && transactions.capture)
      When:
        Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()
      When:
        // The watermark stays below the batch, so its trigger applies nothing
        InProcessPipeline.processTransactions(Chunk(createAndArchive(transactions.get)), insertWatermark = false)
      Expect:
        Database
          .query(
            sql"""select deactivation_type::text, activated_at_ix, deactivated_at_ix
                  from __contracts where contract_id = ${contractId.get}"""
          )
          .returns(Table("archive" | 1 | 1))
      And:
        Database.query(sql"select count(*) from __tmp_deactivated_contracts").returns(Table(0))
    },
    funcTest("refuses to redact a contract archived above the watermark until the watermark passes it") {
      val alice        = Party("Alice")
      val contractId   = Capture[String]
      val transactions = Capture[Chunk[Transaction[Event]]]

      Given:
        DamlSdk.parties(alice) >+> DamlSdk.deploy(Dars.pingPong)
      Then:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.archive("PingPong:Ping", contractId.get, alice)
      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService
      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(2)) && transactions.capture)
      When:
        Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()
      When:
        // The watermark stays below the batch, as if an earlier batch were still in flight
        InProcessPipeline.processTransactions(Chunk(createAndArchive(transactions.get)), insertWatermark = false)
      Expect:
        Database
          .query(sql"select redact_contract(${contractId.get}, 'reason')")
          .exit
          .map(
            assert(_)(
              fails(
                hasMessage(startsWithString(s"ERROR: Cannot redact contract ${contractId.get} because it is active"))
              )
            )
          )
      When:
        // A restart after a crash deletes everything above the watermark, then ingests it again
        ZIO.serviceWithZIO[Datastore](_.registerActiveWriterAndCleanupTransactions)
          *> InProcessPipeline.processTransactions(Chunk(createAndArchive(transactions.get)))
      Expect:
        Database.query(sql"select redact_contract(${contractId.get}, 'reason')").returns(Table(1))
    }
  )

  private def createAndArchive(transactions: Chunk[Transaction[Event]]) =
    transactions(0).copy(events = transactions(0).events ++ transactions(1).events)

  private def createContract(alice: Party): ZIO[Docker & Ledger & DarFile, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice)
      .map(_.getTransaction.events(0).getCreated.contractId)
