// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.daml.ledger.api.v2.value.*
import com.digitalasset.canonical.specific.{Offset, Transaction, TransactionEvent}
import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.pipeline.InProcessPipeline
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyCantonVersion
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.specific.OffsetType
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.zio.daml.DamlSchema
import zio.Chunk
import zio.jdbc.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object ReassignmentSpec extends FuncTestStandalone:
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

  private def context(sync1: Synchronizer, sync2: Synchronizer, alice: Party) =
    DamlSdk.dar(pingPong) ++ DamlSdk.multiSyncLedger(sync1, sync2)
      >+> DamlSdk.uploadAndVetDar(sync1, sync2) ++ DamlSdk.allocateParties(alice -> Seq(sync1, sync2))

  def spec = suite("Multi-Sync")(
    funcTest("Contract is created, reassigned and archived") {
      val sync1      = Synchronizer("synchronizer1")
      val sync2      = Synchronizer("synchronizer2")
      val alice      = Party("Alice")
      val dar        = Capture[DeployedDar]
      val contractId = Capture[String]
      Given:
        context(sync1, sync2, alice)
      And:
        dar.captureFromService
      Then:
        val args = Record.defaultInstance
          .addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
        Ledger
          .create("PingPong:Ping", args, alice, sync1)
          .map(_.getTransaction.events.head.getCreated.contractId)
          .is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)
      When:
        Postgres.instance
          >+> Postgres.database
          >+> Pqs.runPipeline(
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest"
          )

      val createdAtOffset  = Capture[OffsetType]
      val archivedAtOffset = Capture[OffsetType]
      Expect:
        Postgres
          .query(sql"""select "offset", domain_id from __transactions order by "offset"""")
          .returns(
            table {
              // submitAndWait guarantees the causal order of these multi-sync transactions
              createdAtOffset.capture  | null
              archivedAtOffset.capture | null
            }
          )

      Expect:
        Database
          .creates(extraColumns = Seq("created_at_offset"))
          .returns(
            table(dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset)
          )
      Expect:
        Database
          .archives(extraColumns = Seq("archived_at_offset"))
          .returns(
            table(dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | archivedAtOffset)
          )
    },
    funcTest("Non-causal stream: archived is received before created") {
      val sync1      = Synchronizer("synchronizer1")
      val sync2      = Synchronizer("synchronizer2")
      val alice      = Party("Alice")
      val dar        = Capture[DeployedDar]
      val contractId = Capture[String]

      Given:
        context(sync1, sync2, alice)
      Then:
        dar.captureFromService
      And:
        val args = Record.defaultInstance
          .addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
        Ledger
          .create("PingPong:Ping", args, alice, sync1)
          .map(_.getTransaction.events.head.getCreated.contractId)
          .is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)

      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService

      val transactions     = Capture[Chunk[Transaction[TransactionEvent]]]
      val archivedAtOffset = Offset.Absolute(1)
      val createdAtOffset  = Offset.Absolute(2)
      def archiveTx        = transactions.get(1).copy(offset = archivedAtOffset)
      def createTx         = transactions.get(0).copy(offset = createdAtOffset)

      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(2)) && transactions.capture)

      When:
        Postgres.instance
          >+> Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()

      When:
        // the archived event is received first
        // the created event is received later, after watermark insertion
        InProcessPipeline.processTransactions(Chunk(archiveTx)) *>
          InProcessPipeline.processTransactions(Chunk(createTx))

      Expect:
        Postgres
          .query(sql"""select "offset", domain_id from __transactions order by "offset"""")
          .returns(
            table {
              archivedAtOffset.offset | null
              createdAtOffset.offset  | null
            }
          )

      Expect:
        Database
          .creates(extraColumns = Seq("created_at_offset"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset.offset
            }
          )
      Expect:
        // TODO #17 multi-sync support
        Database.archives().returns(Table.empty)
    }
  ) @@ onlyCantonVersion(">=3.5")
