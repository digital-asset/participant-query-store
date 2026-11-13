// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.document

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.{OffsetType, biggestOffset, smallestOffset}
import zio.ZLayer
import zio.jdbc.sqlInterpolator
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object ResetProcedureSpec extends SharedLedgerAndPostgresTest:
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

  def context =
    val alice = Party("Alice")
    DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.deploy(pingDaml)
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
        Database
          .query(sql"""select "offset" from __transactions order by ix""")
          .returns(Table(anything | target.capture | anything | anything).transpose)
      Expect:
        Database
          .query(sql"select new_latest, affected_transactions from validate_reset_offset(${target.get})")
          .returns(Table(target | 2))
    ,
    funcTest("reset_to_offset"):
      val target     = Capture[OffsetType]
      val pipelineId = Capture[String]
      Given:
        context
      And:
        Database
          .query(sql"select instance_id from __watermark")
          .returns(Table(pipelineId.capture))
      And:
        Database
          .query(sql"""select "offset" from __transactions order by ix""")
          .returns(Table(anything | target.capture | anything | anything).transpose)
      And:
        Database
          .query(sql"select new_latest, affected_transactions from reset_to_offset(${target.get})")
          .returns(Table(target | 2))
      Expect:
        Database
          .query(sql"""select "offset" from latest_checkpoint()""")
          .returns(Table(target))
      And:
        Database
          .query(sql"select instance_id from __watermark")
          .returns(Table(not(equalTo(pipelineId.get))))
    ,
    funcTest("__cleanup_transactions_after_watermark"):
      val target      = Capture[OffsetType]
      val staleWriter = Capture[String]
      Given:
        context
      And:
        Database
          .query(sql"""select "offset" from __transactions order by ix""")
          .returns(Table(anything | target.capture | anything | anything).transpose)
      And:
        Database.reverseWatermark(2) `returns` 1
      And:
        Database
          .query(sql"select instance_id from __watermark")
          .returns(Table(staleWriter.capture))
      Expect:
        Database call {
          sql"call __cleanup_transactions_after_watermark()"
        } `returns` ()
      And:
        Database
          .query(sql"""select "offset" from __transactions order by ix""")
          .returns(Table(anything | target).transpose)
      And:
        Database.query(sql"select count(*) from __contracts").returns(Table(2))
      And:
        Database
          .query(sql"select instance_id from __watermark")
          .returns(Table(not(equalTo(staleWriter.get))))
    ,
    funcTest("offset out of lower bounds"):
      Given:
        context
      Then:
        Database
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
        Database
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
