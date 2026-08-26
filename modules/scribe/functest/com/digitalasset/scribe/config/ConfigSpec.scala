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
  // Using "z" defers the test until the end of the group
  // This avoids timeouts caused by Docker being slow to start too many containers simultaneously.
  def spec = suite("z")(
    suite("Config")(
      funcTest("missing postgres username and password") {
        val env: Map[String, String] = (1 to 10000).map(i => s"VAR$i" -> s"value$i").toMap
        val myHost                   = "this-is-not-a-real-host"
        When:
          runScribe(env = Map("SCRIBE_TARGET_POSTGRES_HOST" -> myHost))
        And:
          Scribe.stderr `is` (
            stringContaining(
              s"Missing value: --target-postgres-password. Details: Postgres user password, value of type string"
            ) &&
              stringContaining(
                s"Missing value: --target-postgres-username. Details: Postgres user name, value of type string"
              )
          )
        And:
          Scribe.exitCode `is` ExitCode.failure
      } @@ TestAspect.timeout(30.seconds),
      funcTest("with 10k env variables should be applied in reasonable time") {
        val env: Map[String, String] = (1 to 10000).map(i => s"VAR$i" -> s"value$i").toMap
        val myHost                   = "this-is-not-a-real-host"
        When:
          runScribe(
            env ++ Map(
              "SCRIBE_TARGET_POSTGRES_USERNAME" -> "postgres",
              "SCRIBE_TARGET_POSTGRES_PASSWORD" -> "postgres",
              "SCRIBE_TARGET_POSTGRES_HOST"     -> myHost,
              "SCRIBE_RETRY_COUNTER_ATTEMPTS"   -> "0"
            )
          )
        Then:
          Scribe.stdout `is` stringContaining("Applied configuration:")
        And:
          Scribe.stdout `is` stringContaining(s"host=$myHost")
        And:
          Scribe.stderr `is` (stringContaining(s"java.net.UnknownHostException: $myHost"))
        And:
          Scribe.exitCode `is` ExitCode.failure
      } @@ TestAspect.timeout(30.seconds)
    )
  )

  def runScribe(env: Map[String, Any]): ZLayer[Docker, Throwable, CliRun] =
    Docker
      .service[Unit](image = localScribeDockerImage, env = env)("pipeline", "ledger", "postgres-document")
      .flatMap(CliRun.fromSvc)
