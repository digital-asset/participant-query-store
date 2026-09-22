// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.daml.ledger.api.v2.value.*
import com.digitalasset.canonical.{Event, Offset, Transaction}
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.pipeline.InProcessPipeline
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.OffsetType
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.zio.daml.DamlSchema
import zio.{Chunk, ZIO}
import zio.jdbc.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object ReassignmentSpec extends FuncTest[Service[Ledger] & Postgres & DeployedDar]:
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

  private val sync1 = Synchronizer("synchronizer1")
  private val sync2 = Synchronizer("synchronizer2")

  override val shared =
    DamlSdk.dar(pingPong) ++ DamlSdk.multiSyncLedger(sync1, sync2) ++ Postgres.instance
      >+> DamlSdk.uploadAndVetDar(sync1, sync2)

  def spec = suite("Multi-Sync")(
    funcTest("Contract is created, reassigned and archived") {
      val alice      = Party("Alice")
      val dar        = Capture[DeployedDar]
      val contractId = Capture[String]
      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      And:
        dar.captureFromService
      Then:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)
      When:
        Postgres.database
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
        // Every update now carries a synchronizer_id; for a reassignment it is the synchronizer
        // that synchronized it — the source synchronizer for the unassign, the target for the assign.
        Postgres
          .query(sql"""select "offset", synchronizer_id, effective_at is null
                       from __transactions order by "offset"""")
          .returns(
            table {
              // submitAndWait guarantees the causal order of these multi-sync transactions
              createdAtOffset.capture    | sync1.id | false
              unassignedAtOffset.capture | sync1.id | true
              assignedAtOffset.capture   | sync2.id | true
              archivedAtOffset.capture   | sync2.id | false
            }
          )

      Expect:
        Database
          .creates(extraColumns = Seq("created_at_offset", "synchronizer_id"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset | sync1.id
            }
          )
      Expect:
        Database
          .archives(extraColumns = Seq("archived_at_offset", "synchronizer_id"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | archivedAtOffset | sync2.id
            }
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

      val reassignmentId = Capture[String]
      Expect:
        // The unassign and assign halves share a reassignment_id and counter, so the same Capture is used on both rows.
        Database
          .__reassignments()
          .returns(
            table {
              s"${pingPong.name}:PingPong:Ping" | "unassign" | contractId | reassignmentId.capture | sync1.id | sync2.id | alice.id | 1
              s"${pingPong.name}:PingPong:Ping" | "assign" | contractId | reassignmentId.capture | sync1.id | sync2.id | alice.id | 1
            }
          )
      Expect:
        // The assign branch has no assignment_exclusivity field of its own and must store none.
        Postgres
          .query(sql"""select assignment_exclusivity from __reassignments where "type" = 'assign'""")
          .returns(table(isNull))

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
    funcTest("Non-causal stream: archived before created") {
      val alice      = Party("Alice")
      val dar        = Capture[DeployedDar]
      val contractId = Capture[String]

      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      Then:
        dar.captureFromService
      And:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)

      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService

      val transactions       = Capture[Chunk[Transaction[Event]]]
      val assignedAtOffset   = Offset.Absolute(1)
      val archivedAtOffset   = Offset.Absolute(2)
      val createdAtOffset    = Offset.Absolute(3)
      val unassignedAtOffset = Offset.Absolute(4)

      def assignTx   = transactions.get(2).copy(offset = assignedAtOffset)
      def archiveTx  = transactions.get(3).copy(offset = archivedAtOffset)
      def createTx   = transactions.get(0).copy(offset = createdAtOffset)
      def unassignTx = transactions.get(1).copy(offset = unassignedAtOffset)

      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(4)) && transactions.capture)

      When:
        Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()

      When:
        // the archived event is received first
        // the created event is received later, after watermark insertion
        InProcessPipeline.processTransactions(Chunk(assignTx, archiveTx)) *>
          InProcessPipeline.processTransactions(Chunk(createTx, unassignTx))

      Expect:
        Postgres
          .query(sql"""select "offset", synchronizer_id from __transactions order by "offset"""")
          .returns(
            table {
              assignedAtOffset.toLong   | sync2.id
              archivedAtOffset.toLong   | sync2.id
              createdAtOffset.toLong    | sync1.id
              unassignedAtOffset.toLong | sync1.id
            }
          )

      Expect:
        Database
          .creates(extraColumns = Seq("created_at_offset", "synchronizer_id"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset.toLong | sync1.id
            }
          )
      Expect:
        Database
          .archives(extraColumns = Seq("archived_at_offset", "synchronizer_id"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | archivedAtOffset.toLong | sync2.id
            }
          )
      Expect:
        Database
          .activeAtOffset(
            assignedAtOffset.toLong,
            extraColumns = Seq("created_at_offset", "assigned_at_offset", "synchronizer_id")
          )
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | 0 | assignedAtOffset.toLong | sync2.id
            }
          )
      Expect:
        Database
          .activeAtOffset(
            createdAtOffset.toLong,
            extraColumns = Seq("created_at_offset", "assigned_at_offset", "synchronizer_id")
          )
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset.toLong | 0 | sync1.id
            }
          )

      val reassignmentId = Capture[String]
      Expect:
        // Ordered by reassigned_at_ix, so with this replay order the assign row comes first, then unassign.
        Database
          .__reassignments()
          .returns(
            table {
              s"${pingPong.name}:PingPong:Ping" | "assign" | contractId | reassignmentId.capture | sync1.id | sync2.id | alice.id | 1
              s"${pingPong.name}:PingPong:Ping" | "unassign" | contractId | reassignmentId.capture | sync1.id | sync2.id | alice.id | 1
            }
          )
    },
    funcTest("Non-causal stream: assigned before unassigned") {
      val alice      = Party("Alice")
      val dar        = Capture[DeployedDar]
      val contractId = Capture[String]

      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      Then:
        dar.captureFromService
      And:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)

      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService

      val transactions       = Capture[Chunk[Transaction[Event]]]
      val createdAtOffset    = Offset.Absolute(1)
      val assignedAtOffset   = Offset.Absolute(2)
      val unassignedAtOffset = Offset.Absolute(3)
      val archivedAtOffset   = Offset.Absolute(4)

      def createTx   = transactions.get(0).copy(offset = createdAtOffset)
      def assignTx   = transactions.get(2).copy(offset = assignedAtOffset)
      def unassignTx = transactions.get(1).copy(offset = unassignedAtOffset)
      def archiveTx  = transactions.get(3).copy(offset = archivedAtOffset)

      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(4)) && transactions.capture)

      When:
        Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()

      When:
        InProcessPipeline.processTransactions(Chunk(createTx, assignTx, unassignTx, archiveTx))

      Expect:
        Postgres
          .query(sql"""select "offset", synchronizer_id from __transactions order by "offset"""")
          .returns(
            table {
              createdAtOffset.toLong    | sync1.id
              assignedAtOffset.toLong   | sync2.id
              unassignedAtOffset.toLong | sync1.id
              archivedAtOffset.toLong   | sync2.id
            }
          )

      Expect:
        Database
          .creates(extraColumns = Seq("created_at_offset", "synchronizer_id"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset.toLong | sync1.id
            }
          )
      Expect:
        Database
          .archives(extraColumns = Seq("archived_at_offset", "synchronizer_id"))
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | archivedAtOffset.toLong | sync2.id
            }
          )
      Expect:
        Database
          .activeAtOffset(
            assignedAtOffset.toLong,
            extraColumns = Seq("created_at_offset", "assigned_at_offset", "synchronizer_id")
          )
          .returns(
            table {
              // TODO #17 deduplication
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | createdAtOffset.toLong | 0 | sync1.id
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | 0 | assignedAtOffset.toLong | sync2.id
            }
          )
      Expect:
        Database
          .activeAtOffset(
            unassignedAtOffset.toLong,
            extraColumns = Seq("created_at_offset", "assigned_at_offset", "synchronizer_id")
          )
          .returns(
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | 0 | assignedAtOffset.toLong | sync2.id
            }
          )

    },
    funcTest("non-causal stream: repeated interleaved reassignments") {
      val alice      = Party("Alice")
      val dar        = Capture[DeployedDar]
      val contractId = Capture[String]

      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      Then:
        dar.captureFromService
      And:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.reassign(contractId.get, alice, sync2, sync1)
          *> Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.reassign(contractId.get, alice, sync2, sync1)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync1)

      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService

      val transactions = Capture[Chunk[Transaction[Event]]]

      // interleave reassignments: assigned before unassigned
      def reorderedTransactions = Chunk(
        transactions.get(0).copy(offset = Offset.Absolute(1)), // created on sync 1
        transactions.get(2).copy(offset = Offset.Absolute(2)), // assigned to sync 2
        transactions.get(1).copy(offset = Offset.Absolute(3)), // unassigned from sync 1
        transactions.get(4).copy(offset = Offset.Absolute(4)), // assigned to sync 1
        transactions.get(3).copy(offset = Offset.Absolute(5)), // unassigned from sync 2
        transactions.get(6).copy(offset = Offset.Absolute(6)), // assigned to sync 2
        transactions.get(5).copy(offset = Offset.Absolute(7)), // unassigned from sync 1
        transactions.get(8).copy(offset = Offset.Absolute(8)), // assigned to sync 1
        transactions.get(7).copy(offset = Offset.Absolute(9)), // unassigned from sync 2
        transactions.get(9).copy(offset = Offset.Absolute(10)) // archived on sync 1
      )

      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(10)) && transactions.capture)

      When:
        Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()

      When:
        InProcessPipeline.processTransactions(reorderedTransactions)

      Expect:
        Database
          .__contracts(extraColumns = Seq("synchronizer_id"))
          .returns(
            table {
              anything | anything | contractId | "[1,3)"  | sync1.id
              anything | anything | contractId | "[2,5)"  | sync2.id
              anything | anything | contractId | "[4,7)"  | sync1.id
              anything | anything | contractId | "[6,9)"  | sync2.id
              anything | anything | contractId | "[8,10)" | sync1.id
            }
          )
    }
  )

  private def createContract(alice: Party): ZIO[Docker & Service[Ledger] & DeployedDar, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice, sync1)
      .map(_.getTransaction.events(0).getCreated.contractId)
