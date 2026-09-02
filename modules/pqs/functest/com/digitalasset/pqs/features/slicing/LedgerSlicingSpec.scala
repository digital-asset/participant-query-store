// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.slicing

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.docker.Service
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.specific.OffsetType
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything
import zio.{ExitCode, ZLayer}

import scala.language.{implicitConversions, postfixOps}

object LedgerSlicingSpec extends SharedLedgerAndPostgresTest:
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
                    |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                    |""".stripMargin
  )
  private val context =
    DamlSdk.dar(pingPong) >+> DamlSdk.deploy
      ++ DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.runScript("PingPong:transact1", alice.id)

  def spec = suite("slicing")(
    funcTest("on empty datastore"):
      Given:
        context
      When:
        Pqs.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `is` table { anything | 1L | anything | 1L }
      And:
        Pqs.stdout `is` stringContaining("Starting from Genesis")
    ,
    funcTest("on empty datastore, ongoing"):
      Given:
        context >+> Pqs.pipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Never")
      Expect:
        checkpointsQuery `is` table { anything | 1L | anything | 1L } retryUntilTimeout
    ,
    funcTest("on non-empty datastore"):
      val checkpoint = Capture[OffsetType]
      Given:
        context
      When:
        Pqs.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `returns` table { anything | 1L | checkpoint.capture | 1L }

      When:
        Pqs.runPipeline(s"--pipeline-ledger-start=$checkpoint", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stdout `is` stringContaining(
          s"Continuing from offset '$checkpoint' and index '1' until offset"
        )
      And:
        checkpointsQuery `returns` table { checkpoint | 1L | checkpoint | 1L }
    ,
    funcTest("idempotent repeated run"):
      val lastCheckpoint = Capture[OffsetType]
      Given:
        context
      When:
        Pqs.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `returns` table { anything | 1L | lastCheckpoint.capture | 1L }

      When:
        Pqs.runPipeline(s"--pipeline-ledger-start=$lastCheckpoint", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `returns` table { lastCheckpoint | 1L | lastCheckpoint | 1L }

      When:
        Pqs.runPipeline(s"--pipeline-ledger-start=$lastCheckpoint", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `returns` table { lastCheckpoint | 1L | lastCheckpoint | 1L }
    ,
    funcTest("second run with new transaction in the ledger"):
      val lastCheckpoint = Capture[OffsetType]
      Given:
        context
      When:
        Pqs.runPipeline("--pipeline-ledger-start=Genesis", "--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `returns` table { anything | 1L | lastCheckpoint.capture | 1L }

      When:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        checkpointsQuery `returns` table { lastCheckpoint | 1L | anything | 2L }
      And:
        Pqs.stdout `is` stringContaining(
          s"Continuing from offset '$lastCheckpoint' and index '1' until offset"
        )
  )

  private val checkpointsQuery = Postgres `query` sql"select f.*, l.* from oldest_checkpoint() f, latest_checkpoint() l"
