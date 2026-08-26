// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.errors

import com.digitalasset.scribe.docker.Service
import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.*
import com.digitalasset.scribe.services.scribe.Scribe
import com.digitalasset.scribe.specific.OffsetType
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything
import zio.{ExitCode, ZLayer}

import scala.language.implicitConversions

/** This must remain standalone because it prunes the ledger.
  */
object PrunedDataAccessSpec extends FuncTestStandalone:
  private val alice = Party("Alice")
  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    sender: Party
                    |    receiver: Party
                    |  where
                    |    signatory sender
                    |    observer receiver
                    |
                    |transact1: Party -> Script ()
                    |transact1 alice = void do
                    |  -- Create several transactions to avoid problems related to pruning too close to ledger head
                    |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                    |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                    |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                    |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                    |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                    |""".stripMargin
  )

  def spec = suite("PrunedDataAccess")(
    funcTest("pipeline should fail on pruned ledger with start = Genesis"):
      val pruneUpTo = Capture[OffsetType]
      Given:
        (DamlSdk.dar(pingPong) ++ DamlSdk.ledger ++ Postgres.instance)
          >+> (DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database)
          >+> DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        Conf.pipeline
      When:
        Scribe.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `is` table {
          pruneUpTo.capture | 1L | anything | 5L
        }
      When:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      When:
        DamlSdk.pruneLedger(pruneUpTo.get)
      When:
        runScribe(
          "pipeline",
          "ledger",
          "postgres-document",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        Scribe.stderr `is` stringContaining(
          s"Requested start 'GENESIS' is outside of ledger history '$pruneUpTo..."
        )
  )

  private val checkpointsQuery = Postgres `query` sql"select f.*, l.* from oldest_checkpoint() f, latest_checkpoint() l"
