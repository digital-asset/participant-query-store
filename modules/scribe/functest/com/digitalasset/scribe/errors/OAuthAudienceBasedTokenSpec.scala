// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.errors

import com.digitalasset.scribe.SharedLedgerAndPostgresAndAuthTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.*
import com.digitalasset.scribe.services.scribe.Scribe
import zio.{ExitCode, ZLayer}

import scala.language.{implicitConversions, postfixOps}

object OAuthAudienceBasedTokenSpec extends SharedLedgerAndPostgresAndAuthTest:
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

  def spec = suite("OAuthAudienceBasedToken")(
    funcTest("pipeline should fail for audience with wrong participantId"):
      val user    = User(primaryParty = alice)
      val userId  = Capture[String]
      val aliceId = Capture[String]
      Given:
        (DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database)
          >+> DamlSdk.deploy >+> DamlSdk.runScript("PingPong:transact1", alice.id)

      And:
        Conf.pipeline

      And:
        DamlSdk.users(user)

      And:
        DamlSdk.runScript("PingPong:transact1", alice.id)

      And:
        alice.id `is` aliceId.capture

      And:
        user.id `is` userId.capture

      When:
        runScribe(
          "pipeline",
          "ledger",
          "postgres-document",
          s"--pipeline-oauth-clientid=$userId",
          "--pipeline-oauth-parameters-audience=https://daml.com/jwt/aud/participant/WRONG_PARTICIPANT",
          "--pipeline-ledger-stop=Latest"
        )

      Expect:
        Scribe.exitCode `is` ExitCode.failure

      And:
        Scribe.stderr `is` stringContaining("io.grpc.StatusException: PERMISSION_DENIED")
  )
