// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.document

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.specific.{OffsetType, biggestOffset, smallestOffset}
import zio.ZLayer
import zio.jdbc.sqlInterpolator
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object ResetProcedureSpec extends SharedLedgerAndPostgresTest:
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
                 |  two   <- submit alice $ createCmd (Pong with owner = alice, label = "two")
                 |  three <- submit alice $ createCmd (Ping with owner = alice, label = "three")
                 |  four  <- submit alice $ createCmd (Ping with owner = alice, label = "four")
                 |  pure ()
                 |""".stripMargin
  )

  val context = DamlSdk.dar(pingDaml) ++ DamlSdk.parties(alice) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("Pings:setup", alice.id)
    >+> Pqs.runPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      "--pipeline-ledger-start=Genesis",
      "--pipeline-ledger-stop=Latest"
    )

  def spec = suite("reset procedure")(
    funcTest("validate_reset_offset"):
      val target = Capture[OffsetType]
      Given:
        context
      And:
        Postgres
          .query(sql"""select "offset" from __transactions order by ix""")
          .returns(table(anything | target.capture | anything | anything).transpose)
      Expect:
        Postgres
          .query(sql"select new_latest, affected_transactions from validate_reset_offset(${target.get})")
          .returns(table(target | 2))
    ,
    funcTest("reset_to_offset"):
      val target     = Capture[OffsetType]
      val pipelineId = Capture[String]
      Given:
        context
      And:
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(pipelineId.capture))
      And:
        Postgres
          .query(sql"""select "offset" from __transactions order by ix""")
          .returns(table(anything | target.capture | anything | anything).transpose)
      And:
        Postgres
          .query(sql"select new_latest, affected_transactions from reset_to_offset(${target.get})")
          .returns(table(target | 2))
      Expect:
        Postgres
          .query(sql"""select "offset" from latest_checkpoint()""")
          .returns(table(target))
      And:
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(not(equalTo(pipelineId.get))))
    ,
    funcTest("offset out of lower bounds"):
      Given:
        context
      Then:
        Postgres
          .query(sql"select new_latest, affected_transactions from reset_to_offset($smallestOffset)")
          .exit
          .map(
            assert(_)(
              fails(
                hasMessage(
                  containsString(
                    s"Illegal reset offset $smallestOffset is outside lower bounds of contiguous history"
                  )
                )
              )
            )
          )
    ,
    funcTest("offset out of upper bounds"):
      Given:
        context
      Then:
        Postgres
          .query(sql"select new_latest, affected_transactions from reset_to_offset($biggestOffset)")
          .exit
          .map(
            assert(_)(
              fails(
                hasMessage(
                  containsString(
                    s"Illegal reset offset $biggestOffset is beyond upper bounds of contiguous history"
                  )
                )
              )
            )
          )
  )
