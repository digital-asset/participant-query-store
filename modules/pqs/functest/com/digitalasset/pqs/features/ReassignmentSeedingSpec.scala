// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.digitalasset.pqs.SharedMultiSyncLedgerSpec
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.Chunk
import zio.jdbc.*
import zio.test.Assertion.*

import java.time.Instant
import scala.language.implicitConversions

object ReassignmentSeedingSpec extends SharedMultiSyncLedgerSpec:
  private val multiSyncColumns = Seq(
    "activated_at_ix::text",
    "activation_type",
    "deactivated_at_ix::text",
    "deactivation_type",
    "created_effective_at",
    "synchronizer_id",
    "reassignment_counter",
    "activated_at_record_time",
    "deactivated_at_record_time"
  )

  def spec = suite("Multi-Sync ACS seeding")(
    funcTest("Reassigned contract is seeded from the ACS as an assign") {
      val alice      = Party("Alice")
      val dar        = Capture[DarFile]
      val contractId = Capture[String]
      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      And:
        dar.captureFromService
      Then:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
      When:
        Postgres.database
          >+> Pqs.runPipeline(
            "--pipeline-ledger-start=Latest",
            "--pipeline-ledger-stop=Latest"
          )

      Expect:
        Database
          .active(extraColumns = multiSyncColumns)
          .returns(
            Table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | "0" | "assign" | null | null | not(
                isNull
              ) | sync2.id | 1 | null | null
            }
          )
      Expect:
        Database
          .query(sql"select record_time from transactions".query[Option[Instant]].selectAll)
          .is(equalTo(Chunk(None)))
      Expect:
        Database.creates().returns(Table.empty)
      Expect:
        Database
          .activations(extraColumns = Seq("source_synchronizer_id"))
          .returns(Table(dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | null))
      Expect:
        Database
          .query(sql"""select "type"::text from __events""")
          .returns(Table("assign"))
      Expect:
        Database
          .query(sql"select * from summary_updates()")
          .returns(Table(s"${pingPong.name}:PingPong:Ping" | "template" | 0 | 0 | 1 | 0))
    },
    funcTest("Contract seeded from the ACS as an assign can be archived") {
      val alice      = Party("Alice")
      val dar        = Capture[DarFile]
      val contractId = Capture[String]
      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      And:
        dar.captureFromService
      Then:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
      When:
        Postgres.database
          >+> Pqs.runPipeline(
            "--pipeline-ledger-start=Latest",
            "--pipeline-ledger-stop=Latest"
          )
      Then:
        Database
          .active()
          .returns(Table(dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId))
      When:
        Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)
      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")

      Expect:
        Database.active().returns(Table.empty)
      Expect:
        Database
          .archives(extraColumns = multiSyncColumns)
          .returns(
            Table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | "0" | "assign" | "1" | "archive" | not(
                isNull
              ) | sync2.id | 1 | null | not(isNull)
            }
          )
      Expect:
        Database
          .query(sql"select * from summary_updates()")
          .returns(Table(s"${pingPong.name}:PingPong:Ping" | "template" | 0 | 1 | 1 | 0))
    }
  )
