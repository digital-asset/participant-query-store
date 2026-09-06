// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.querying.postgres.document

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.sqlInterpolator
import zio.{ExitCode, ZLayer}

import scala.language.implicitConversions

object JsonEncodingsSpec extends SharedLedgerAndPostgresTest:
  val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    text: Optional Text
                    |    party: Party
                    |    numeric: Optional Decimal
                    |    int64: Optional Int
                    |  where
                    |    signatory party
                    |
                    |addNulls : (Party) -> Script ()
                    |addNulls (alice) = void do
                    |  submit alice $ createCmd Ping with party = alice, text = None, numeric = None, int64 = None
                    |
                    |addValues : (Party, Text, Decimal, Int) -> Script ()
                    |addValues (alice, text, numeric, int64) = void do
                    |  submit alice $ createCmd Ping with party = alice, text = Some text, numeric = Some numeric, int64 = Some int64
                    |""".stripMargin
  )

  private def context(alice: Party) =
    DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.deploy
      >+> DamlSdk.runScript("PingPong:addValues", (alice.id, "test", "1", "1"))

  def spec = suite("json encoding")(
    funcTest("include nullable fields"):
      val alice = Party("Alice")
      Given:
        context(alice)
      And:
        DamlSdk.runScript(
          "PingPong:addNulls",
          alice.id
        )

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--target-encoding-excludenulls=false"
        )
      Expect:
        Pqs.exitCode `is` ExitCode.success

      Then:
        Postgres.`query` {
          sql"""select payload from active('PingPong:Ping') order by payload"""
        } `returns` table {
          s"""{"text": null, "int64": null, "party": "${alice.id}", "numeric": null}""" |
            s"""{"text": "test", "int64": "1", "party": "${alice.id}", "numeric": "1.0000000000"}"""
        }.transpose
    ,
    funcTest("exclude nullable fields"):
      val alice = Party("Alice")
      Given:
        context(alice)
      And:
        DamlSdk.runScript(
          "PingPong:addNulls",
          alice.id
        )

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--target-encoding-excludenulls=true"
        )
      Expect:
        Pqs.exitCode `is` ExitCode.success

      Then:
        Postgres.`query` {
          sql"""select payload from active('PingPong:Ping') order by payload"""
        } `returns` table {
          s"""{"text": null, "party": "${alice.id}"}""" |
            s"""{"text": "test", "int64": "1", "party": "${alice.id}", "numeric": "1.0000000000"}"""
        }.transpose
    ,
    funcTest("encode numerics as numbers"):
      val alice = Party("Alice")
      Given:
        context(alice)

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--target-encoding-numericasstring=false"
        )
      Expect:
        Pqs.exitCode `is` ExitCode.success

      Then:
        Postgres.`query` {
          sql"""select payload from active('PingPong:Ping') order by payload"""
        } `returns` table {
          s"""{"text": "test", "int64": "1", "party": "${alice.id}", "numeric": 1}"""
        }
    ,
    funcTest("encode int64 as numbers"):
      val alice = Party("Alice")
      Given:
        context(alice)

      When:
        Pqs.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--target-encoding-int64asstring=false"
        )
      Expect:
        Pqs.exitCode `is` ExitCode.success

      Then:
        Postgres.`query` {
          sql"""select payload from active('PingPong:Ping') order by payload"""
        } `returns` table {
          s"""{"text": "test", "int64": 1, "party": "${alice.id}", "numeric": "1.0000000000"}"""
        }
  )
