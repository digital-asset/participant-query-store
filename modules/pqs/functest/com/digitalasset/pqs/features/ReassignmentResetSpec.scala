// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.digitalasset.pqs.{OffsetType, SharedMultiSyncLedgerSpec}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object ReassignmentResetSpec extends SharedMultiSyncLedgerSpec:
  def spec = suite("Multi-Sync")(
    suite("reset")(
      funcTest("reset_to_offset deletes assigned contracts and reassignment events, and revives unassigned ones") {
        val alice         = Party("Alice")
        val reassignedCid = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(reassignedCid.capture)
        When:
          Ledger.reassign(reassignedCid.get, alice, sync1, sync2)
        Then:
          createContract(alice).is(anything)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        val reassignedCreated = Capture[OffsetType]

        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            reassignedCreated.capture | anything | anything | anything
          }.transpose
        Expect:
          Postgres
            .query(sql"select new_latest, affected_transactions from reset_to_offset(${reassignedCreated.get})")
            .returns(table(reassignedCreated | 3))
        And:
          Postgres.query(sql"""select "offset" from __transactions""").returns(table(reassignedCreated))
        And:
          Postgres
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix""")
            .returns(table("create" | reassignedCreated))
        And:
          Postgres.query(sql"select count(*) from __reassignments").returns(table(0))
        And:
          // the assign-born row is gone and the unassigned one is open-ended again on its source synchronizer
          Database
            .__contracts(extraColumns = Seq("synchronizer_id"))
            .returns(table(anything | anything | reassignedCid | "[1,)" | sync1.id))
      },
      funcTest("reset_to_offset between the unassign and the assign keeps the contract unassigned") {
        val alice         = Party("Alice")
        val reassignedCid = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(reassignedCid.capture)
        When:
          Ledger.reassign(reassignedCid.get, alice, sync1, sync2)
        Then:
          createContract(alice).is(anything)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        val reassignedCreated  = Capture[OffsetType]
        val reassignedUnassign = Capture[OffsetType]

        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            reassignedCreated.capture | reassignedUnassign.capture | anything | anything
          }.transpose
        Expect:
          Postgres
            .query(sql"select new_latest, affected_transactions from reset_to_offset(${reassignedUnassign.get})")
            .returns(table(reassignedUnassign | 2))
        And:
          Postgres
            .query(sql"""select "offset" from __transactions order by ix""")
            .returns(table(reassignedCreated | reassignedUnassign).transpose)
        And:
          Postgres
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix
                         order by t."offset"""")
            .returns(
              table {
                "create"   | reassignedCreated
                "unassign" | reassignedUnassign
              }
            )
        And:
          // the unassign half is at the cutoff, not after it, so it survives
          Postgres.query(sql"select count(*) from __reassignments").returns(table(1))
        And:
          // the assign-born row is gone and the source-synchronizer row stays unassigned
          Database
            .__contracts(extraColumns = Seq("synchronizer_id"))
            .returns(table(anything | anything | reassignedCid | "[1,2)" | sync1.id))
      },
      funcTest("__cleanup_transactions_after_watermark discards the reassignment written past the watermark") {
        val alice         = Party("Alice")
        val reassignedCid = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(reassignedCid.capture)
        When:
          Ledger.reassign(reassignedCid.get, alice, sync1, sync2)
        Then:
          createContract(alice).is(anything)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        val reassignedCreated = Capture[OffsetType]

        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            reassignedCreated.capture | anything | anything | anything
          }.transpose
        And:
          Postgres.reverseWatermark(1) `returns` 1
        Expect:
          Postgres call {
            sql"call __cleanup_transactions_after_watermark()"
          } `returns` ()
        And:
          Postgres.query(sql"""select "offset" from __transactions""").returns(table(reassignedCreated))
        And:
          Postgres
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix""")
            .returns(table("create" | reassignedCreated))
        And:
          Postgres.query(sql"select count(*) from __reassignments").returns(table(0))
        And:
          Database
            .__contracts(extraColumns = Seq("synchronizer_id"))
            .returns(table(anything | anything | reassignedCid | "[1,)" | sync1.id))
      }
    )
  )

  private def createContract(alice: Party): ZIO[Docker & Service[Ledger] & DeployedDar, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice, sync1)
      .map(_.getTransaction.events(0).getCreated.contractId)

