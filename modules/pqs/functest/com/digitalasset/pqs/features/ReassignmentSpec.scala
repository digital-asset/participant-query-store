// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyCantonVersion
import com.digitalasset.pqs.specific.OffsetType
import zio.jdbc.*

import scala.language.implicitConversions

object ReassignmentSpec extends FuncTest[Service[Ledger] & Postgres & DeployedDar & Database]:
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

  val shared =
    DamlSdk.dar(pingPong) ++ DamlSdk.multiSyncLedger(sync1, sync2) ++ Postgres.instance
      >+> DamlSdk.uploadAndVetDar(sync1, sync2) ++ Postgres.database

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
        Pqs.runPipeline(
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
    }
  ) @@ onlyCantonVersion(">=3.5")
