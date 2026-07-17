// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.logging

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, DarFile, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.test.Assertion.not

object LoggingSpec extends SharedLedgerAndPostgresTest:
  val alice = Party("Alice")
  val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |template Ping
                    |  with
                    |    sender: Party
                    |    receiver: Party
                    |  where
                    |    signatory sender
                    |    observer receiver
                    |""".stripMargin
  )

  def spec = suite("Logging")(
    suite("console-based logging")(
      funcTest("--logger-level Info --logger-format Plain --logger-pattern Plain (defaults)"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline()
        Then:
          Pqs.stdout `is` stringMatching(
            "([\\s\\S]*)(com\\.digitalasset\\.pqs\\.postgres\\.document\\.DocumentPostgres:\\d+ Applying schema)([\\s\\S]*)"
          )
      ,
      funcTest("--logger-level Info --logger-format Plain --logger-pattern Standard"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline(
            "--logger-pattern=Standard"
          )
        Then:
          Pqs.stdout `is` stringMatching(
            "component=pqs instance_uuid=([\\s\\S]*)(description=Applying schema)([\\s\\S]*)"
          )
      ,
      funcTest("--logger-pattern Structured"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline(
            "--logger-pattern=Structured"
          )
        Then:
          Pqs.stdout `is` stringMatching(
            "([\\s\\S]*)(location=com\\.digitalasset\\.pqs\\.postgres\\.document\\.DocumentPostgres:\\d+ message=Applying schema)([\\s\\S]*)"
          )
      ,
      funcTest("--logger-pattern <custom_pattern>"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline(
            "--logger-pattern=%label{location}{%name} %label{level}{%level} %message"
          )
        Then:
          Pqs.stdout `is` stringContaining(
            "location=com.digitalasset.pqs.postgres.document.DocumentPostgres level=INFO Applying schema"
          )
      ,
      funcTest("--logger-format Json"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline(
            "--logger-format=Json"
          )
        Then:
          Pqs.stdout `is` stringContaining(
            "{\"text_content\":\""
          ) && stringMatching(
            "([\\s\\S]*)(com\\.digitalasset\\.pqs\\.postgres\\.document\\.DocumentPostgres:\\d+ Applying schema)([\\s\\S]*)"
          )
      ,
      funcTest("--logger-format Json --logger-pattern Standard"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline(
            "--logger-format=Json",
            "--logger-pattern=Standard"
          )
        Then:
          Pqs.stdout `is` stringMatching(
            "(?:[\\s\\S]*?)\\{\\\"component\\\":\\\"pqs\\\",\\\"instance_uuid\\\":\\\"[^\"]+\\\",\\\"timestamp\\\":\\\"[^\"]+\\\",\\\"level\\\":\\\"INFO\\\",\\\"description\\\":\\\"Applying schema\\\",\\\"pqs\\\":\\{\\\"trace_id\\\":\\\"[^\"]+\\\",\\\"application\\\":\\\"pqs\\\"\\}\\}(?:[\\s\\S]*)"
          )
      ,
      funcTest("--logger-format Json --logger-pattern Structured"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        When:
          runPipeline(
            "--logger-format=Json",
            "--logger-pattern=Structured"
          )
        Then:
          Pqs.stdout `is` stringMatching(
            "([\\s\\S]*)(\\\"location\\\":\\\"com\\.digitalasset\\.pqs\\.postgres\\.document\\.DocumentPostgres:\\d+\\\",\\\"message\\\":\\\"Applying schema\\\")([\\s\\S]*)"
          )
      ,
      funcTest("--logger-level Debug"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        val dar = Capture[DarFile]
        And:
          dar.captureFromService
        When:
          runPipeline(
            "--logger-level=Debug"
          )
        Then:
          Pqs.stdout `is` stringContaining(
            s"Including template ${dar.get.packageId}:PingPong:Ping"
          ) && stringContaining("com.digitalasset.pqs.grpc.")
      ,
      funcTest("--logger-level Debug --logger-mappings-com.digitalasset.pqs.grpc Info"):
        Given:
          DamlSdk.dar(pingPong)
        And:
          DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
        val dar = Capture[DarFile]
        And:
          dar.captureFromService
        When:
          runPipeline(
            "--logger-level=Debug",
            "--logger-mappings-com.digitalasset.pqs.grpc=Info"
          )
        Then:
          Pqs.stdout `is` stringContaining(
            s"Including template ${dar.get.packageId}:PingPong:Ping"
          ) && not(stringContaining("com.digitalasset.pqs.grpc."))
    )
  )

  private def runPipeline(args: String*) =
    Pqs.runPipeline(
      args ++ Seq(
        "--pipeline-ledger-stop=Latest",
        // explicitly turning off overly chatty loggers to prevent their domination of the output
        "--logger-mappings-org.flywaydb=None",
        "--logger-mappings-io.netty=None",
        "--logger-mappings-io.grpc.netty=None"
      )*
    )
end LoggingSpec
