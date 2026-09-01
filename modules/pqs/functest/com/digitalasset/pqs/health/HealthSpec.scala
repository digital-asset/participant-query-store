// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.health

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.is
import com.digitalasset.pqs.health.HealthEndpoint.{responseBody, responseStatus}
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ZLayer
import zio.http.Status
import zio.json.ast.Json

import scala.language.{implicitConversions, postfixOps}

object HealthSpec extends SharedLedgerAndPostgresTest:
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
  private val context = DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("PingPong:transact1", alice.id)

  def spec = suite("health")(
    funcTest("livez"):
      Given:
        context
      When:
        Pqs.pipeline(
          "--health-address=0.0.0.0",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Never"
        )
      Expect:
        responseStatus("/livez") `is` Some(Status.Ok) retryUntilTimeout
    ,
    funcTest("readyz"):
      Given:
        context
      When:
        Pqs.pipeline(
          "--health-address=0.0.0.0",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Never"
        )
      Expect:
        responseStatus("/readyz") `is` Some(Status.Ok) retryUntilTimeout
      And:
        responseBody("/readyz") `is` Some(
          Json.Obj(
            "status"                  -> Json.Str("ok"),
            "grpc_up"                 -> Json.Bool(true),
            "jdbc_connection_pool_up" -> Json.Bool(true),
            "stream_up"               -> Json.Bool(true)
          )
        ) retryUntilTimeout
  )
