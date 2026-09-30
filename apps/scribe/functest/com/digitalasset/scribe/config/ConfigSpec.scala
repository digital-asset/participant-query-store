// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.config

import com.digitalasset.scribe.docker.Docker
import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.scribe.*
import zio.*
import zio.test.*

object ConfigSpec extends FuncTestStandalone:
  private val notARealHost = "this-is-not-a-real-host"
  // Using "z" defers the test until the end of the group
  // This avoids timeouts caused by Docker being slow to start too many containers simultaneously.
  def spec = suite("z")(
    suite("Config")(
      funcTest("with 10k env variables should be applied in reasonable time") {
        val env: Map[String, String] = (1 to 10000).map(i => s"VAR$i" -> s"value$i").toMap
        When:
          runScribe(env ++ Map("SCRIBE_TARGET_POSTGRES_HOST" -> notARealHost, "SCRIBE_RETRY_COUNTER_ATTEMPTS" -> "0"))
        Then:
          Scribe.stdout `is` stringContaining("Applied configuration:")
        And:
          Scribe.stdout `is` stringContaining(s"host=$notARealHost")
        And:
          Scribe.stderr `is` (stringContaining(s"java.net.UnknownHostException: $notARealHost"))
        And:
          Scribe.exitCode `is` ExitCode.failure
      } @@ TestAspect.timeout(30.seconds),
      suite("target-postgres-properties")(
        funcTest("CLI options preserve entry-key case") {
          When:
            runScribe(
              Map(
                "SCRIBE_TARGET_POSTGRES_HOST"                       -> notARealHost,
                "SCRIBE_TARGET_POSTGRES_PROPERTIES_sslmode"         -> "require",
                "SCRIBE_TARGET_POSTGRES_PROPERTIES_LoginTimeout"    -> "30",
                "SCRIBE_TARGET_POSTGRES_PROPERTIES_ApplicationName" -> "myapp",
                "SCRIBE_RETRY_COUNTER_ATTEMPTS"                     -> "0"
              )
            )
          Then:
            Scribe.stdout `is` stringContaining("Applied configuration:")
          And:
            Scribe.stdout `is` (
              stringContaining("sslmode=\"********\"") &&
                stringContaining("LoginTimeout=\"********\"") &&
                stringContaining("ApplicationName=\"********\"")
            )
          And:
            Scribe.exitCode `is` ExitCode.failure
        },
        funcTest("env vars preserve entry-key case") {
          When:
            runScribe(
              env = Map(
                "SCRIBE_TARGET_POSTGRES_HOST"   -> notARealHost,
                "SCRIBE_RETRY_COUNTER_ATTEMPTS" -> "0"
              ),
              args = Seq(
                "--target-postgres-properties-sslmode=require",
                "--target-postgres-properties-LoginTimeout=30",
                "--target-postgres-properties-ApplicationName=myapp"
              )
            )
          Then:
            Scribe.stdout `is` stringContaining("Applied configuration:")
          And:
            Scribe.stdout `is` (
              stringContaining("sslmode=\"********\"") &&
                stringContaining("LoginTimeout=\"********\"") &&
                stringContaining("ApplicationName=\"********\"")
            )
          And:
            Scribe.exitCode `is` ExitCode.failure
        }
      )
    )
  )

  def runScribe(env: Map[String, Any], args: Seq[String] = Seq.empty): ZLayer[Docker, Throwable, CliRun] =
    Docker
      .service[Unit](image = localScribeDockerImage, env = env)(
        "pipeline",
        "ledger",
        "postgres-document",
        os.Shellable.IterableShellable(args)
      )
      .flatMap(CliRun.fromSvc)
