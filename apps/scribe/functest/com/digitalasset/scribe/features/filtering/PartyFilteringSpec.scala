// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.filtering

import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.jdbc.sqlInterpolator
import zio.{ExitCode, ZLayer}

import scala.language.implicitConversions

/** This test needs to be standalone because it uses a wildcard filter on parties
  */
object PartyFilteringSpec extends FuncTestStandalone:
  private val alice   = Party("Alice")
  private val bob     = Party("Bob")
  private val charlie = Party("Charlie")
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
                    |transact1 : Party -> Script ()
                    |transact1 party = void do
                    |  submit party $ createCmd Ping with sender = party, receiver = party
                    |""".stripMargin
  )

  private val context = DamlSdk.dar(pingPong) ++ DamlSdk.ledger ++ Postgres.instance
    >+> DamlSdk.deploy ++ DamlSdk.parties(alice, bob, charlie) ++ Postgres.database

  def spec = suite("filtering")(
    funcTest("with filter"):
      val aliceId     = Capture[String]
      val aliceHint   = Capture[String]
      val charlieId   = Capture[String]
      val charlieHint = Capture[String]

      Given:
        context
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)
          ++ DamlSdk.runScript("PingPong:transact1", charlie.id)
      And:
        alice.id `is` aliceId.capture

      And:
        alice.name `is` aliceHint.capture

      And:
        charlie.id `is` charlieId.capture

      And:
        charlie.name `is` charlieHint.capture

      When:
        Scribe.runPipeline(
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-parties=($aliceHint::* | $charlieHint::*)"
        )

      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId,$charlieId'")

      And:
        partiesQuery `returns` table {
          aliceId | charlieId
        }.transpose

      Expect:
        Scribe.exitCode `is` ExitCode.success
    ,
    funcTest("with wildcard (*)") {
      val aliceId   = Capture[String]
      val charlieId = Capture[String]
      val bobId     = Capture[String]

      Given:
        context
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)
          ++ DamlSdk.runScript("PingPong:transact1", charlie.id)
      And:
        alice.id `is` aliceId.capture
      And:
        bob.id `is` bobId.capture
      And:
        charlie.id `is` charlieId.capture
      When:
        Scribe.runPipeline(
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-parties=*"
        )
      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of all parties on the participant")
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        partiesQuery `returns` table {
          aliceId | bobId | charlieId
        }.transpose
      And:
        Postgres `query` sql"""select count(*) from active('PingPong:Ping')""" `returns` table { 3 }
    }
  )

  private val partiesQuery =
    Postgres `query` sql"select distinct unnest(array_cat(signatories, observers)) from __contracts order by 1"
