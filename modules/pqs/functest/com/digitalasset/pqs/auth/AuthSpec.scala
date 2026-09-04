// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.auth

import com.digitalasset.pqs.SharedLedgerAndPostgresAndAuthTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.oauth.OAuth
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.*
import zio.jdbc.sqlInterpolator
import zio.test.*

import scala.language.implicitConversions

object AuthSpec extends SharedLedgerAndPostgresAndAuthTest:
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

  private def context(parties: Party*) =
    (DamlSdk.dar(pingPong) >+> DamlSdk.deploy) ++ DamlSdk.parties(parties*) ++ Postgres.database

  def spec = suite("auth")(
    funcTest("no party filter"):
      val alice   = Party("Alice")
      val bob     = Party("Bob")
      val charlie = Party("Charlie")
      val user    = User(primaryParty = alice, canActAs = Seq(bob))

      Given:
        context(alice, bob, charlie)

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-ledger-stop=Latest"
        )

      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id},${bob.id}'")

      And:
        partiesQuery `returns` table { alice.id | bob.id }.transpose

      Expect:
        Pqs.exitCode `is` ExitCode.success
    ,
    funcTest("with party filter"):
      val alice   = Party("Alice")
      val bob     = Party("Bob")
      val charlie = Party("Charlie")
      val user    = User(primaryParty = alice, canActAs = Seq(bob, charlie))

      Given:
        context(alice, bob, charlie)

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)
          ++ DamlSdk.runScript("PingPong:transact1", charlie.id)

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-parties=${alice.id} | ${charlie.id}"
        )

      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id},${charlie.id}'")

      And:
        partiesQuery `returns` table {
          alice.id | charlie.id
        }.transpose

      Expect:
        Pqs.exitCode `is` ExitCode.success
    ,
    funcTest("static access token"):
      val alice = Party("Alice")
      val user  = User(primaryParty = alice)
      val token = Capture[String]

      Given:
        context(alice)

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)

      And:
        TokenService.getToken(alice.id) `is` token.capture

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-accesstoken=${token.get}",
          "--pipeline-ledger-stop=Latest"
        )

      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id}'")

      And:
        partiesQuery `returns` table {
          alice.id
        }

      Expect:
        Pqs.exitCode `is` ExitCode.success
    ,
    funcTest("audience based token"):
      val alice         = Party("Alice")
      val user          = User(primaryParty = alice)
      val participantId = Capture[String]

      Given:
        context(alice)
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        Ledger.participantId `is` participantId.capture

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          s"--pipeline-oauth-parameters-audience=https://daml.com/jwt/aud/participant/${participantId.get}",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-oauth-scope=None"
        )

      Then:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id}'")
      And:
        partiesQuery `returns` table { alice.id }
    ,
    funcTest("scope based token - default scope"):
      val alice         = Party("Alice")
      val user          = User(primaryParty = alice)
      val participantId = Capture[String]

      Given:
        context(alice)
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        Ledger.participantId `is` participantId.capture

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-oauth-scope=Default",
          "--pipeline-ledger-stop=Latest"
        )

      Then:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id}'")
      And:
        partiesQuery `returns` table { alice.id }
    ,
    funcTest("scope based token - custom scope"):
      val alice         = Party("Alice")
      val user          = User(primaryParty = alice)
      val participantId = Capture[String]

      Given:
        context(alice)
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
      And:
        Ledger.participantId `is` participantId.capture

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-oauth-scope=myScope1 myScope2",
          "--pipeline-ledger-stop=Latest",
          // TODO fix mock-oauth2-server impl to allow use scope parameter in mappings
          "--pipeline-oauth-parameters-custom_scope=myScope1 myScope2"
        )

      Then:
        Pqs.exitCode `is` ExitCode.success
      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id}'")
      And:
        partiesQuery `returns` table { alice.id }
    ,
    funcTest("preempt expiry"):
      val alice = Party("Alice")
      val bob   = Party("Bob")
      val user  = User(primaryParty = alice, canActAs = Seq(bob))

      Given:
        context(alice, bob)

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)

      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-oauth-preemptexpiry=PT30S"
        )

      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of '${alice.id},${bob.id}'")

      And:
        partiesQuery `returns` table {
          alice.id | bob.id
        }.transpose

      Expect:
        Pqs.exitCode `is` ExitCode.success
    ,
    funcTest("retry on AccessTokenExpired") {
      val alice   = Party("Alice")
      val charlie = Party("Charlie")
      val user    = User(primaryParty = alice, canActAs = Seq(charlie))

      Given:
        context(alice, charlie)

      And:
        DamlSdk.users(user)

      When:
        Pqs.pipeline(
          s"--pipeline-oauth-clientid=${user.id}",
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
          alice.id
        }

      And:
        DamlSdk.runScript("PingPong:transact1", charlie.id)

      And:
        partiesQuery `returns` table {
          alice.id | charlie.id
        }.transpose
    },
    funcTest("can read all parties when user rights is set as any party (sdk 3+)") {
      val alice   = Party("Alice")
      val bob     = Party("Bob")
      val charlie = Party("Charlie")
      val user    = User(primaryParty = alice, canReadAsAnyParty = true)

      Given:
        // Rerun the shared layer to get a fresh Canton instance for this test
        shared >+> context(alice, bob, charlie)
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)
          ++ DamlSdk.runScript("PingPong:transact1", charlie.id)
      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-clientid=${user.id}",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        partiesQuery `returns` table {
          alice.id | bob.id | charlie.id
        }.transpose
      And:
        Postgres `query` sql"""select count(*) from active('PingPong:Ping')""" `returns` table { 3 }
    },
    funcTest("static access token can read all parties when user rights is set as any party (sdk 3+)") {
      val alice   = Party("Alice")
      val bob     = Party("Bob")
      val charlie = Party("Charlie")
      val user    = User(primaryParty = alice, canReadAsAnyParty = true)
      val token   = Capture[String]

      Given:
        // Rerun the shared layer to get a fresh Canton instance for this test
        shared >+> context(alice, bob, charlie)
      And:
        DamlSdk.users(user)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)
          ++ DamlSdk.runScript("PingPong:transact1", bob.id)
          ++ DamlSdk.runScript("PingPong:transact1", charlie.id)
      And:
        TokenService.getToken(alice.id) `is` token.capture
      When:
        Pqs.runPipeline(
          s"--pipeline-oauth-accesstoken=${token.get}",
          "--pipeline-ledger-stop=Latest"
        )
      And:
        Pqs.stdout `is` stringContaining(s"Starting pipeline on behalf of all parties on the participant")
      Expect:
        Pqs.exitCode `is` ExitCode.success
      And:
        partiesQuery `returns` table {
          alice.id | bob.id | charlie.id
        }.transpose
      And:
        Postgres `query` sql"""select count(*) from active('PingPong:Ping')""" `returns` table { 3 }
    }
  )

  private val partiesQuery =
    Postgres `query` sql"select distinct unnest(array_cat(signatories, observers)) from __contracts order by 1"
