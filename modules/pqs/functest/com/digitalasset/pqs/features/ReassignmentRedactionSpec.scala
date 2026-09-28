// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.digitalasset.canonical.{Event, Offset, Transaction}
import com.digitalasset.pqs.SharedMultiSyncLedgerSpec
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.pipeline.InProcessPipeline
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.zio.daml.DamlSchema
import zio.Chunk
import zio.jdbc.*
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object ReassignmentRedactionSpec extends SharedMultiSyncLedgerSpec:
  def spec = suite("Multi-Sync")(
    suite("redaction")(
      funcTest("redacts a contract archived after a reassignment") {
        val alice = Party("Alice")
        val cid   = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(cid.capture)
        When:
          Ledger.reassign(cid.get, alice, sync1, sync2)
            *> Ledger.archive("PingPong:Ping", cid.get, alice, sync2)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        Expect:
          // no interfaces, so one row per segment
          Postgres.query(sql"select redact_contract(${cid.get}, 'reason')").returns(table(2))
        And:
          Postgres
            .query(sql"select payload, redaction_id from lookup_contract(${cid.get})")
            .returns(
              table {
                isNull | "reason"
                isNull | "reason"
              }
            )
      },
      funcTest("refuses to redact a contract that is active after a reassignment") {
        val alice = Party("Alice")
        val cid   = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(cid.capture)
        When:
          Ledger.reassign(cid.get, alice, sync1, sync2)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        Expect:
          // the assigned-on-sync2 segment is still open, so the contract as a whole is active
          Postgres
            .query(sql"select redact_contract(${cid.get}, 'reason')")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot redact contract ${cid.get} because it is active"
                    )
                  )
                )
              )
            )
      },
      funcTest("redacts an unassigned contract") {
        val alice      = Party("Alice")
        val contractId = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(contractId.capture)
        When:
          Ledger.reassign(contractId.get, alice, sync1, sync2)

        And:
          Ledger.damlSchema()
            >+> DamlSchema.protobufCodecs
            >+> Ledger.updateService ++ Ledger.stateService

        val transactions       = Capture[Chunk[Transaction[Event]]]
        val createdAtOffset    = Offset.Absolute(1)
        val unassignedAtOffset = Offset.Absolute(2)

        def createTx   = transactions.get(0).copy(offset = createdAtOffset)
        def unassignTx = transactions.get(1).copy(offset = unassignedAtOffset)

        Then:
          Ledger.recordTransactionStream.is(hasSize(equalTo(3)) && transactions.capture)

        When:
          Postgres.database
            >+> DamlSchema.produce(JsonCodec())
            >+> DamlSchema.produce(SqlSchema)
            >+> InProcessPipeline.destinationLayer()

        When:
          // only the create and unassign are replayed; the assign on sync2 is dropped
          InProcessPipeline.processTransactions(Chunk(createTx, unassignTx))

        Expect:
          Postgres.query(sql"select redact_contract(${contractId.get}, 'reason')").returns(table(1))
        And:
          Postgres
            .query(sql"select payload, redaction_id from lookup_contract(${contractId.get})")
            .returns(table(isNull | "reason"))
      },
      funcTest("redacts rows inserted after a redaction on a second call") {
        val alice      = Party("Alice")
        val contractId = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
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
          // the assign->archive segment arrives and is redacted before create->unassign exists at all
          InProcessPipeline.processTransactions(Chunk(assignTx, archiveTx))
        Expect:
          Postgres.query(sql"select redact_contract(${contractId.get}, 'reason')").returns(table(1))

        When:
          // the create->unassign segment lands afterwards, as a fresh, unredacted row
          InProcessPipeline.processTransactions(Chunk(createTx, unassignTx))

        Expect:
          Postgres
            .query(
              sql"""select payload, redaction_id from lookup_contract(${contractId.get})
                   order by coalesce(created_at_ix, assigned_at_ix)"""
            )
            .returns(
              table {
                isNull      | "reason"
                not(isNull) | isNull
              }
            )
        And:
          Postgres.query(sql"select redact_contract(${contractId.get}, 'reason')").returns(table(1))
        And:
          Postgres
            .query(sql"select payload, redaction_id from lookup_contract(${contractId.get})")
            .returns(
              table {
                isNull | "reason"
                isNull | "reason"
              }
            )
        Expect:
          Postgres
            .query(sql"select redact_contract(${contractId.get}, 'reason')")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot redact contract ${contractId.get} because it is already redacted"
                    )
                  )
                )
              )
            )
      }
    )
  )
