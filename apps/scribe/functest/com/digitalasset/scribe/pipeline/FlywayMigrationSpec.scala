// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline

import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.{Database, Postgres}
import com.digitalasset.scribe.services.scribe.{Scribe34, Scribe}
import scala.language.implicitConversions
import zio.jdbc.*
import zio.test.Assertion.*

/** This test is standalone because Scribe 3.4 does not support the Void package uploaded by DamlDecodingSpec to the
  * shared Canton instance.
  */
object FlywayMigrationSpec extends FuncTestStandalone:
  private val alice = Party("Alice")

  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    owner: Party
                    |  where
                    |    signatory owner
                    |
                    |transact : Party -> Script ()
                    |transact party = void do
                    |  submit party $ createCmd Ping with owner = party
                    |""".stripMargin
  )

  def spec = suite("FlyMigrationSpec")(
    funcTest("Migrate from 3.4.6 to main") {
      val instanceId = Capture[String]
      Given:
        DamlSdk.ledger ++ Postgres.instance
          >+> DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database
          >+> DamlSdk.deploy

      And:
        DamlSdk.runScript("PingPong:transact", alice.id)
      And:
        Scribe34.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Database
          .active(Some(s"${pingPong.name}:PingPong:Ping"))
          .returns(
            table {
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | anything
            }
          )
      Expect:
        // There was a bug in PQS 3.4 where the instance_id was only inserted on the second run.
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(isNull))
      And:
        // Run it a second time to insert the instance_id
        Scribe34.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(not(isNull) && instanceId.capture))
      And:
        DamlSdk.runScript("PingPong:transact", alice.id)
      And:
        Scribe.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Database
          .active(Some(s"${pingPong.name}:PingPong:Ping"))
          .returns(
            table {
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | anything
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | anything
            }
          )
      Expect:
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(not(isNull) && not(equalTo(instanceId.get))))
    } @@ DamlSdk.onlyDamlLfVersion("<=2.2")
  )
