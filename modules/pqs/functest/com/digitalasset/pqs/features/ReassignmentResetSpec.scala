// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.digitalasset.pqs.{OffsetType, SharedMultiSyncLedgerSpec}
import com.digitalasset.pqs.functest.matchers.*
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
        val alice           = Party("Alice")
        val reassignedCid   = Capture[String]
        val createdAtOffset = Capture[OffsetType]

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

        And:
          Database
            .query(sql"""select "offset" from __transactions order by ix""")
            .returns(Table(createdAtOffset.capture | anything | anything | anything).transpose)
        Expect:
          Database
            .query(sql"select new_latest, affected_transactions from reset_to_offset(${createdAtOffset.get})")
            .returns(Table(createdAtOffset | 3))
        And:
          Database.query(sql"""select "offset" from __transactions""").returns(Table(createdAtOffset))
        And:
          Database
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix""")
            .returns(Table("create" | createdAtOffset))
        And:
          Database.query(sql"select count(*) from __reassignments").returns(Table(0))
        And:
          // the assign-born row is gone and the unassigned one is open-ended again on its source synchronizer
          Database
            .__contracts(extraColumns = Seq("synchronizer_id"))
            .returns(Table(anything | anything | reassignedCid | "[1,)" | sync1.id))
      },
      funcTest("reset_to_offset between the unassign and the assign keeps the contract unassigned") {
        val alice              = Party("Alice")
        val reassignedCid      = Capture[String]
        val createdAtOffset    = Capture[OffsetType]
        val unassignedAtOffset = Capture[OffsetType]

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

        And:
          Database
            .query(sql"""select "offset" from __transactions order by ix""")
            .returns(Table(createdAtOffset.capture | unassignedAtOffset.capture | anything | anything).transpose)
        Expect:
          Database
            .query(sql"select new_latest, affected_transactions from reset_to_offset(${unassignedAtOffset.get})")
            .returns(Table(unassignedAtOffset | 2))
        And:
          Database
            .query(sql"""select "offset" from __transactions order by ix""")
            .returns(Table(createdAtOffset | unassignedAtOffset).transpose)
        And:
          Database
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix
                         order by t."offset"""")
            .returns(
              Table {
                "create"   | createdAtOffset
                "unassign" | unassignedAtOffset
              }
            )
        And:
          // the unassign half is at the cutoff, not after it, so it survives
          Database.query(sql"select count(*) from __reassignments").returns(Table(1))
        And:
          // the assign-born row is gone and the source-synchronizer row stays unassigned
          Database
            .__contracts(extraColumns = Seq("synchronizer_id"))
            .returns(Table(anything | anything | reassignedCid | "[1,2)" | sync1.id))
      },
      funcTest("__cleanup_transactions_after_watermark discards the reassignment written past the watermark") {
        val alice           = Party("Alice")
        val reassignedCid   = Capture[String]
        val createdAtOffset = Capture[OffsetType]

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

        And:
          Database
            .query(sql"""select "offset" from __transactions order by ix""")
            .returns(Table(createdAtOffset.capture | anything | anything | anything).transpose)
        And:
          Database.reverseWatermark(1) `returns` 1
        Expect:
          Database call {
            sql"call __cleanup_transactions_after_watermark()"
          } `returns` ()
        And:
          Database.query(sql"""select "offset" from __transactions""").returns(Table(createdAtOffset))
        And:
          Database
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix""")
            .returns(Table("create" | createdAtOffset))
        And:
          Database.query(sql"select count(*) from __reassignments").returns(Table(0))
        And:
          Database
            .__contracts(extraColumns = Seq("synchronizer_id"))
            .returns(Table(anything | anything | reassignedCid | "[1,)" | sync1.id))
      }
    )
  )
