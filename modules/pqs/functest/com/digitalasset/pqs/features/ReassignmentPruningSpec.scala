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

object ReassignmentPruningSpec extends SharedMultiSyncLedgerSpec:
  def spec = suite("Multi-Sync")(
    suite("pruning")(
      funcTest("prune_archived_to_offset deletes unassigned contracts and reassignment events") {
        val alice         = Party("Alice")
        val reassignedCid = Capture[String]
        val archivedCid   = Capture[String]
        val activeCid     = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(reassignedCid.capture)
        When:
          Ledger.reassign(reassignedCid.get, alice, sync1, sync2)
        Then:
          createContract(alice).is(archivedCid.capture)
        When:
          Ledger.archive("PingPong:Ping", archivedCid.get, alice, sync1)
        Then:
          createContract(alice).is(activeCid.capture)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        val reassignedAssigned = Capture[OffsetType]
        val archivedArchived   = Capture[OffsetType]
        val activeCreated      = Capture[OffsetType]

        And:
          // created, unassigned, assigned, created, archived, created
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            anything | anything | reassignedAssigned.capture | anything | archivedArchived.capture | activeCreated.capture
          }.transpose
        Expect:
          Postgres
            .query(sql"select * from prune_archived_to_offset_dry_run(${archivedArchived.get})")
            .returns(table(activeCreated | 2 | 0 | 4 | 4 | 1))
        And:
          Postgres
            .query(sql"select * from prune_archived_to_offset(${archivedArchived.get})")
            .returns(table(activeCreated | 2 | 0 | 4 | 4 | 1))
        And:
          // the assign of the still-active contract keeps its event and transaction
          Postgres
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix
                         order by t."offset"""")
            .returns(
              table {
                "assign" | reassignedAssigned
                "create" | activeCreated
              }
            )
        And:
          Postgres
            .query(sql"""select r."type"::text, t."offset"
                         from __reassignments r join __transactions t on r.reassigned_at_ix = t.ix""")
            .returns(table("assign" | reassignedAssigned))
        And:
          Postgres
            .query(sql"""select contract_id, created_at_offset, assigned_at_offset
                         from active()
                         order by assigned_at_offset nulls last""")
            .returns(
              table {
                reassignedCid | 0             | reassignedAssigned
                activeCid     | activeCreated | 0
              }
            )
      },
      funcTest("prune_to_offset squashes assigned contracts into the new genesis") {
        val alice         = Party("Alice")
        val reassignedCid = Capture[String]
        val archivedCid   = Capture[String]
        val activeCid     = Capture[String]

        Given:
          DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
        Then:
          createContract(alice).is(reassignedCid.capture)
        When:
          Ledger.reassign(reassignedCid.get, alice, sync1, sync2)
        Then:
          createContract(alice).is(archivedCid.capture)
        When:
          Ledger.archive("PingPong:Ping", archivedCid.get, alice, sync1)
        Then:
          createContract(alice).is(activeCid.capture)
        When:
          Postgres.database
            >+> Pqs.runPipeline(
              "--pipeline-ledger-start=Genesis",
              "--pipeline-ledger-stop=Latest"
            )

        val archivedArchived = Capture[OffsetType]
        val activeCreated    = Capture[OffsetType]

        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix """
          } `returns` table {
            anything | anything | anything | anything | archivedArchived.capture | activeCreated.capture
          }.transpose
        Expect:
          Postgres
            .query(
              sql"select affected_transactions, squash_inclusive, new_oldest from prune_to_offset(${archivedArchived.get})"
            )
            .returns(table(5 | archivedArchived | activeCreated))
        And:
          Postgres.query(sql"""select "offset" from __transactions""").returns(table(activeCreated))
        And:
          Postgres
            .query(sql"""select e."type"::text, t."offset"
                         from __events e join __transactions t on e.tx_ix = t.ix
                         order by e."type"::text""")
            .returns(
              table {
                "assign" | activeCreated
                "create" | activeCreated
              }
            )
        And:
          Postgres
            .query(sql"""select r."type"::text, t."offset"
                         from __reassignments r join __transactions t on r.reassigned_at_ix = t.ix""")
            .returns(table("assign" | activeCreated))
        And:
          Postgres
            .query(sql"""select contract_id, created_at_offset, assigned_at_offset
                         from active()
                         order by assigned_at_offset nulls last""")
            .returns(
              table {
                reassignedCid | 0             | activeCreated
                activeCid     | activeCreated | 0
              }
            )
      }
    )
  )
