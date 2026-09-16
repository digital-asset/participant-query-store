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

      val createdAtOffset    = Capture[OffsetType]
      val unassignedAtOffset = Capture[OffsetType]
      val assignedAtOffset   = Capture[OffsetType]
      val archivedAtOffset   = Capture[OffsetType]
      Expect:
        // `effective_at is null` rather than the timestamp itself: the value of a transaction's
        // effective time is not predictable from the test, but which rows have one is exactly the
        // decision being pinned. A reassignment has no ledger effective time and must store none.
        Postgres
          .query(sql"""select "offset", domain_id, effective_at is null
                       from __transactions order by "offset"""")
          .returns(
            table {
              // submitAndWait guarantees the causal order of these multi-sync transactions
              createdAtOffset.capture    | null | false
              unassignedAtOffset.capture | null | true
              assignedAtOffset.capture   | null | true
              archivedAtOffset.capture   | null | false
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

      Expect:
        Postgres
          .query(sql"""select e."type"::text, e.event_id::text
                       from __events e join __transactions t on e.tx_ix = t.ix
                       order by t."offset"""")
          .returns(
            table {
              "create"   | s"($createdAtOffset,0)"
              "unassign" | s"($unassignedAtOffset,0)"
              "assign"   | s"($assignedAtOffset,0)"
              "archive"  | s"($archivedAtOffset,0)"
            }
          )

      Expect:
        // A cutoff that falls between the unassign and the assign: later than the create's
        // effective time, earlier than the archive's, so the only rows at or before it are the
        // create and the two reassignments. The reassignments carry a null effective_at, so this
        // function cannot see them — its max() ignores them rather than being poisoned by them,
        // and the answer is the create rather than the newer unassign. A boundary falling in a
        // reassignment-only stretch of history is therefore not targetable, which is what
        // https://github.com/digital-asset/participant-query-store/issues/74 will revisit.
        Postgres
          .query(sql"""select nearest_offset(
                         (select min(effective_at) + (max(effective_at) - min(effective_at)) / 2
                          from __transactions)
                       )""")
          .returns(table(createdAtOffset))
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
