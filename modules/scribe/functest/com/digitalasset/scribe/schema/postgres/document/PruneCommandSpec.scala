// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.schema.postgres.document

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.docker.Service
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import com.digitalasset.scribe.specific.{OffsetType, biggestOffset, smallestOffset}
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything
import zio.{ExitCode, ZLayer}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatterBuilder
import scala.language.implicitConversions

object PruneCommandSpec extends SharedLedgerAndPostgresTest:
  lazy val alice = Party("Alice")
  lazy val pingDaml = DamlSource(
    "Pings" -> """module Pings where
                 |
                 |import Daml.Script
                 |
                 |template Ping
                 |  with
                 |    owner : Party
                 |    label : Text
                 |  where
                 |    signatory owner
                 |
                 |template Pong
                 |  with
                 |    owner : Party
                 |    label : Text
                 |  where
                 |    signatory owner
                 |
                 |setup: Party -> Script ()
                 |setup alice = do
                 |  one   <- submit alice $ createCmd (Ping with owner = alice, label = "one")
                 |  submit alice $ exerciseCmd one Archive
                 |  two <- submit alice $ createCmd (Pong with owner = alice, label = "two")
                 |  three  <- submit alice $ createCmd (Ping with owner = alice, label = "three")
                 |  pure ()
                 |""".stripMargin
  )

  val context = DamlSdk.dar(pingDaml) ++ DamlSdk.parties(alice) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("Pings:setup", alice.id)
    >+> Scribe
      .pipeline(
        "--pipeline-datasource=TransactionTreeStream",
        "--pipeline-ledger-start=Genesis",
        "--pipeline-ledger-stop=Latest"
      )
      .tap(_.get.exitCode)

  def spec = suite("postgres-document prune")(
    funcTest("dry run with valid offset"):
      val oneCreated  = Capture[OffsetType]
      val oneArchived = Capture[OffsetType]
      val twoCreated  = Capture[OffsetType]
      Given:
        context
      And:
        Postgres query {
          sql"""select "offset" from __transactions order by ix LIMIT 3"""
        } `returns` table {
          oneCreated.capture | oneArchived.capture | twoCreated.capture
        }.transpose
      When:
        Scribe.runPrune("--prune-target", oneArchived.get.toString)
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(
          s"""Dry-run result:
             |  Pruning boundary offset: ${twoCreated.get}
             |  Deleted contracts: 1
             |  Deleted choices: 1
             |  Deleted events: 2
             |  Deleted transactions: 2""".stripMargin
        )
      And:
        Scribe.stdout `is` stringContaining("Re-run with --prune-mode Force to execute the pruning operation.")
      And:
        // dry run should NOT persist pruned_offset
        Postgres.query(sql"select pruned_offset() is null").returns(table(true))
    ,
    funcTest("force run with valid offset"):
      val oneCreated  = Capture[OffsetType]
      val oneArchived = Capture[OffsetType]
      val twoCreated  = Capture[OffsetType]
      Given:
        context
      And:
        Postgres query {
          sql"""select "offset" from __transactions order by ix LIMIT 3"""
        } `returns` table {
          oneCreated.capture | oneArchived.capture | twoCreated.capture
        }.transpose
      When:
        Scribe.runPrune("--prune-target", oneArchived.get.toString, "--prune-mode", "Force")
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(
          s"""Pruning operation result:
             |  Pruning boundary offset: ${twoCreated.get}
             |  Deleted contracts: 1
             |  Deleted choices: 1
             |  Deleted events: 2
             |  Deleted transactions: 2""".stripMargin
        )
      And:
        Postgres `query` sql"""select min("offset"), count(*) from __transactions""" `returns` table {
          twoCreated | 2
        }
      And:
        // force run should persist pruned_offset
        Postgres.query(sql"select pruned_offset()").returns(table(oneArchived))
    ,
    funcTest("dry run fails if using offset out of lower bounds"):
      Given:
        context
      When:
        Scribe.runPrune("--prune-target", smallestOffset.toString)
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        Scribe.stderr `is` stringContaining(
          s"Illegal pruning offset $smallestOffset is outside lower bounds of contiguous history"
        )
    ,
    funcTest("dry run fails with latest offset"):
      val maxOffset = Capture[OffsetType]
      Given:
        context
      And:
        Postgres query {
          sql"""select max("offset") from __transactions"""
        } `returns` table { maxOffset.capture }
      When:
        Scribe.runPrune("--prune-target", maxOffset.get.toString)
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        Scribe.stderr `is` stringContaining(
          s"Illegal pruning offset ${maxOffset.get} coincides with latest consistent checkpoint of contiguous history"
        )
    ,
    funcTest("dry run fails if using offset out of upper bounds"):
      Given:
        context
      When:
        Scribe.runPrune("--prune-target", biggestOffset.toString)
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        Scribe.stderr `is` stringContaining(
          s"Illegal pruning offset $biggestOffset is beyond upper bounds of contiguous history"
        )
    ,
    funcTest("force run with timestamp"):
      val oneCreated             = Capture[OffsetType]
      val oneArchived            = Capture[OffsetType]
      val twoCreated             = Capture[OffsetType]
      val oneArchivedEffectiveAt = Capture[String]
      Given:
        context
      And:
        Postgres query {
          sql"""select "offset", "effective_at" from __transactions order by ix LIMIT 3"""
        } `returns` table {
          oneCreated.capture  | anything
          oneArchived.capture | oneArchivedEffectiveAt.capture
          twoCreated.capture  | anything
        }
      When:
        // parse the effective_at timestamp to a format that can be used by the ledger prune command...
        val formatter = new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 6, true)
          .appendPattern("X")
          .toFormatter()
        val timestamp = OffsetDateTime
          .parse(oneArchivedEffectiveAt.get, formatter)
          .toString
        Scribe.runPrune("--prune-target", timestamp, "--prune-mode", "Force")
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(
          s"""Pruning operation result:
             |  Pruning boundary offset: ${twoCreated.get}
             |  Deleted contracts: 1
             |  Deleted choices: 1
             |  Deleted events: 2
             |  Deleted transactions: 2""".stripMargin
        )
      And:
        Postgres `query` sql"""select min("offset"), count(*) from __transactions""" `returns` table {
          twoCreated | 2
        }
    ,
    funcTest("force run is no-op when already pruned past target"):
      val oneCreated  = Capture[OffsetType]
      val oneArchived = Capture[OffsetType]
      val twoCreated  = Capture[OffsetType]
      Given:
        context
      And:
        Postgres query {
          sql"""select "offset" from __transactions order by ix LIMIT 3"""
        } `returns` table {
          oneCreated.capture | oneArchived.capture | twoCreated.capture
        }.transpose
      When:
        Scribe.runPrune("--prune-target", oneArchived.get.toString, "--prune-mode", "Force")
      And:
        Scribe.runPrune("--prune-target", oneArchived.get.toString, "--prune-mode", "Force")
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(
          s"Already pruned past ${oneArchived.get}, nothing to do."
        )
    ,
    funcTest("force run with duration"):
      val oneCreated  = Capture[OffsetType]
      val oneArchived = Capture[OffsetType]
      val twoCreated  = Capture[OffsetType]
      Given:
        context
      And:
        Postgres query {
          sql"""select "offset" from __transactions order by ix LIMIT 3"""
        } `returns` table {
          oneCreated.capture | oneArchived.capture | twoCreated.capture
        }.transpose
      And:
        Postgres `query`
          sql"""
            update __transactions
            set effective_at = effective_at - interval '3 hours'
            where "offset" <= ${oneArchived.get}
         """.update `returns` anything
      When:
        Scribe.runPrune("--prune-target", "PT2H", "--prune-mode", "Force")
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(
          s"""Pruning operation result:
             |  Pruning boundary offset: ${twoCreated.get}
             |  Deleted contracts: 1
             |  Deleted choices: 1
             |  Deleted events: 2
             |  Deleted transactions: 2""".stripMargin
        )
      And:
        Postgres `query` sql"""select min("offset"), count(*) from __transactions""" `returns` table {
          twoCreated | 2
        }
  )
