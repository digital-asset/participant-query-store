// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.auth

import com.digitalasset.scribe.SharedLedgerAndPostgresAndAuthTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.oauth.OAuth
import com.digitalasset.scribe.services.postgres.*
import com.digitalasset.scribe.services.scribe.Scribe
import zio.*
import zio.jdbc.sqlInterpolator
import zio.test.*

import scala.language.implicitConversions

object AuthSpec extends SharedLedgerAndPostgresAndAuthTest:
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

  private val context =
    (DamlSdk.dar(pingPong) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice, bob, charlie) ++ Postgres.database

  def spec = suite("auth")(
    funcTest("no party filter"):
      val user      = User(primaryParty = alice, canActAs = Seq(bob))
      val userId    = Capture[String]
      val aliceId   = Capture[String]
      val bobId     = Capture[String]
      val charlieId = Capture[String]

      Given:
        context

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)

      And:
        alice.id `is` aliceId.capture

      And:
        bob.id `is` bobId.capture

      And:
        user.id `is` userId.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-ledger-stop=Latest"
        )

      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId,$bobId'")

      And:
        partiesQuery `returns` table { aliceId | bobId }.transpose

      Expect:
        Scribe.exitCode `is` ExitCode.success
    ,
    funcTest("with party filter"):
      val user      = User(primaryParty = alice, canActAs = Seq(bob, charlie))
      val userId    = Capture[String]
      val aliceId   = Capture[String]
      val bobId     = Capture[String]
      val charlieId = Capture[String]

      Given:
        context

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)
          ++ DamlSdk.runScript("PingPong:transact1", charlie.id)

      And:
        alice.id `is` aliceId.capture

      And:
        charlie.id `is` charlieId.capture

      And:
        user.id `is` userId.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-parties=$aliceId | $charlieId"
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
    funcTest("static access token"):
      val user    = User(primaryParty = alice)
      val token   = Capture[String]
      val aliceId = Capture[String]

      Given:
        context

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)

      And:
        alice.id `is` aliceId.capture

      And:
        TokenService.getToken(aliceId.get) `is` token.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-accesstoken=${token.get}",
          "--pipeline-ledger-stop=Latest"
        )

      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId'")

      And:
        partiesQuery `returns` table {
          aliceId
        }

      Expect:
        Scribe.exitCode `is` ExitCode.success
    ,
    funcTest("audience based token"):
      val user          = User(primaryParty = alice)
      val userId        = Capture[String]
      val aliceId       = Capture[String]
      val participantId = Capture[String]

      Given:
        context
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        alice.id `is` aliceId.capture
      And:
        user.id `is` userId.capture
      And:
        DamlSdk.api.participantId `is` participantId.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          s"--pipeline-oauth-parameters-audience=https://daml.com/jwt/aud/participant/${participantId.get}",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-oauth-scope=None"
        )

      Then:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId'")
      And:
        partiesQuery `returns` table { aliceId }
    ,
    funcTest("scope based token - default scope"):
      val participantId = Capture[String]
      val user          = User(primaryParty = alice)
      val userId        = Capture[String]
      val aliceId       = Capture[String]

      Given:
        context
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        alice.id `is` aliceId.capture
      And:
        user.id `is` userId.capture
      And:
        DamlSdk.api.participantId `is` participantId.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-oauth-scope=Default",
          "--pipeline-ledger-stop=Latest"
        )

      Then:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId'")
      And:
        partiesQuery `returns` table { aliceId }
    ,
    funcTest("scope based token - custom scope"):
      val participantId = Capture[String]
      val user          = User(primaryParty = alice)
      val userId        = Capture[String]
      val aliceId       = Capture[String]

      Given:
        context
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        alice.id `is` aliceId.capture
      And:
        user.id `is` userId.capture
      And:
        DamlSdk.api.participantId `is` participantId.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-oauth-scope=myScope1 myScope2",
          "--pipeline-ledger-stop=Latest",
          // TODO fix mock-oauth2-server impl to allow use scope parameter in mappings
          "--pipeline-oauth-parameters-custom_scope=myScope1 myScope2"
        )

      Then:
        Scribe.exitCode `is` ExitCode.success
      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId'")
      And:
        partiesQuery `returns` table { aliceId }
    ,
    funcTest("preempt expiry"):
      val user      = User(primaryParty = alice, canActAs = Seq(bob))
      val userId    = Capture[String]
      val aliceId   = Capture[String]
      val bobId     = Capture[String]
      val charlieId = Capture[String]

      Given:
        context

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)

      And:
        alice.id `is` aliceId.capture

      And:
        bob.id `is` bobId.capture

      And:
        user.id `is` userId.capture

      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-oauth-preemptexpiry=PT30S"
        )

      And:
        Scribe.stdout `is` stringContaining(s"Starting pipeline on behalf of '$aliceId,$bobId'")

      And:
        partiesQuery `returns` table {
          aliceId | bobId
        }.transpose

      Expect:
        Scribe.exitCode `is` ExitCode.success
    ,
    funcTest("retry on AccessTokenExpired") {
      val user      = User(primaryParty = alice, canActAs = Seq(charlie))
      val userId    = Capture[String]
      val aliceId   = Capture[String]
      val charlieId = Capture[String]

      Given:
        context

      And:
        DamlSdk.users(user)

      And:
        alice.id `is` aliceId.capture

      And:
        charlie.id `is` charlieId.capture

      And:
        user.id `is` userId.capture

      When:
        Scribe.pipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Never",
          "--pipeline-oauth-preemptexpiry=PT30S"
        )

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)

      And: // wait until the initial token expires
        ZLayer.fromZIO(ZIO.sleep(OAuth.tokenExpiry.seconds + 60.seconds))

      And:
        partiesQuery `returns` table {
          aliceId
        }

      And:
        DamlSdk.runScript("PingPong:transact1", charlie.id)

      And:
        partiesQuery `returns` table {
          aliceId | charlieId
        }.transpose
    },
    funcTest("can read all parties when user rights is set as any party (sdk 3+)") {
      val user      = User(primaryParty = alice, canReadAsAnyParty = true)
      val userId    = Capture[String]
      val aliceId   = Capture[String]
      val bobId     = Capture[String]
      val charlieId = Capture[String]

      Given:
        shared >+> context
      And:
        DamlSdk.users(user)
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
      And:
        user.id `is` userId.capture
      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Scribe.exitCode `is` ExitCode.success
      And:
        partiesQuery `returns` table {
          aliceId | bobId | charlieId
        }.transpose
      And:
        Postgres `query` sql"""select count(*) from active('PingPong:Ping')""" `returns` table { 3 }
    },
    funcTest("static access token can read all parties when user rights is set as any party (sdk 3+)") {
      val user      = User(primaryParty = alice, canReadAsAnyParty = true)
      val token     = Capture[String]
      val aliceId   = Capture[String]
      val bobId     = Capture[String]
      val charlieId = Capture[String]

      Given:
        shared >+> context
      And:
        DamlSdk.users(user)
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
      And:
        TokenService.getToken(aliceId.get) `is` token.capture
      When:
        Scribe.runPipeline(
          s"--pipeline-oauth-accesstoken=${token.get}",
          "--pipeline-ledger-stop=Latest"
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
