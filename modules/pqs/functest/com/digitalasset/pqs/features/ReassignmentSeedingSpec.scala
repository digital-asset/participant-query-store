// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.digitalasset.pqs.SharedMultiSyncLedgerSpec
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*

import scala.language.implicitConversions

object ReassignmentSeedingSpec extends SharedMultiSyncLedgerSpec:
  private val multiSyncColumns = Seq(
    "created_at_ix::text",
    "assigned_at_ix::text",
    "archived_at_ix::text",
    "synchronizer_id",
    "reassignment_counter"
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
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | null | "0" | null | sync2.id | 1
            }
          )
      Expect:
        Database.creates().returns(Table.empty)
      Expect:
        Postgres
          .query(sql"""select "type"::text from __events""")
          .returns(table("assign"))
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
          .returns(table(dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId))
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
            table {
              dar.get.packageId | s"${pingPong.name}:PingPong:Ping" | "template" | contractId | null | "0" | "1" | sync2.id | 1
            }
          )
    }
  )
