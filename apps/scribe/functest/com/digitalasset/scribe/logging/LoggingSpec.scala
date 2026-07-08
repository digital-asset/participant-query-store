// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.logging

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.daml.{DamlSdk, DamlSource, DarFile, Party}
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
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
          Scribe.stdout `is` stringMatching(
            "([\\s\\S]*)(com\\.digitalasset\\.scribe\\.postgres\\.document\\.DocumentPostgres:\\d+ Applying schema)([\\s\\S]*)"
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
          Scribe.stdout `is` stringMatching(
            "component=scribe instance_uuid=([\\s\\S]*)(description=Applying schema)([\\s\\S]*)"
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
          Scribe.stdout `is` stringMatching(
            "([\\s\\S]*)(location=com\\.digitalasset\\.scribe\\.postgres\\.document\\.DocumentPostgres:\\d+ message=Applying schema)([\\s\\S]*)"
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
          Scribe.stdout `is` stringContaining(
            "location=com.digitalasset.scribe.postgres.document.DocumentPostgres level=INFO Applying schema"
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
          Scribe.stdout `is` stringContaining(
            "{\"text_content\":\""
          ) && stringMatching(
            "([\\s\\S]*)(com\\.digitalasset\\.scribe\\.postgres\\.document\\.DocumentPostgres:\\d+ Applying schema)([\\s\\S]*)"
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
          Scribe.stdout `is` stringMatching(
            "(?:[\\s\\S]*?)\\{\\\"component\\\":\\\"scribe\\\",\\\"instance_uuid\\\":\\\"[^\"]+\\\",\\\"timestamp\\\":\\\"[^\"]+\\\",\\\"level\\\":\\\"INFO\\\",\\\"description\\\":\\\"Applying schema\\\",\\\"scribe\\\":\\{\\\"trace_id\\\":\\\"[^\"]+\\\",\\\"application\\\":\\\"scribe\\\"\\}\\}(?:[\\s\\S]*)"
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
          Scribe.stdout `is` stringMatching(
            "([\\s\\S]*)(\\\"location\\\":\\\"com\\.digitalasset\\.scribe\\.postgres\\.document\\.DocumentPostgres:\\d+\\\",\\\"message\\\":\\\"Applying schema\\\")([\\s\\S]*)"
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
          Scribe.stdout `is` stringContaining(
            s"Including template ${dar.get.packageId}:PingPong:Ping"
          ) && stringContaining("com.digitalasset.scribe.grpc.")
      ,
      funcTest("--logger-level Debug --logger-mappings-com.digitalasset.scribe.grpc Info"):
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
            "--logger-mappings-com.digitalasset.scribe.grpc=Info"
          )
        Then:
          Scribe.stdout `is` stringContaining(
            s"Including template ${dar.get.packageId}:PingPong:Ping"
          ) && not(stringContaining("com.digitalasset.scribe.grpc."))
    )
  )

  private def runPipeline(args: String*) =
    Scribe.runPipeline(
      args ++ Seq(
        "--pipeline-ledger-stop=Latest",
        // explicitly turning off overly chatty loggers to prevent their domination of the output
        "--logger-mappings-org.flywaydb=None",
        "--logger-mappings-io.netty=None",
        "--logger-mappings-io.grpc.netty=None"
      )*
    )
end LoggingSpec
