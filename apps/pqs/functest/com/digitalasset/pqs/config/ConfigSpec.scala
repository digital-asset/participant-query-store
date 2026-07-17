// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.config

import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.pqs.*
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
          runPqs(env = Map("PQS_TARGET_POSTGRES_HOST" -> myHost))
        And:
          Pqs.stderr `is` (
            stringContaining(
              s"Missing value: --target-postgres-password. Details: Postgres user password, value of type string"
            ) &&
              stringContaining(
                s"Missing value: --target-postgres-username. Details: Postgres user name, value of type string"
              )
          )
        And:
          Pqs.exitCode `is` ExitCode.failure
      } @@ TestAspect.timeout(30.seconds),
      funcTest("with 10k env variables should be applied in reasonable time") {
        val env: Map[String, String] = (1 to 10000).map(i => s"VAR$i" -> s"value$i").toMap
        val myHost                   = "this-is-not-a-real-host"
        When:
          runPqs(
            env ++ Map(
              "PQS_TARGET_POSTGRES_USERNAME" -> "postgres",
              "PQS_TARGET_POSTGRES_PASSWORD" -> "postgres",
              "PQS_TARGET_POSTGRES_HOST"     -> myHost,
              "PQS_RETRY_COUNTER_ATTEMPTS"   -> "0"
            )
          )
        Then:
          Pqs.stdout `is` stringContaining("Applied configuration:")
        And:
          Pqs.stdout `is` stringContaining(s"host=$myHost")
        And:
          Pqs.stderr `is` (stringContaining(s"java.net.UnknownHostException: $myHost"))
        And:
          Pqs.exitCode `is` ExitCode.failure
      } @@ TestAspect.timeout(30.seconds),
      funcTest("with SCRIBE prefix should warn and apply config") {
        val myHost = "scribe-prefix-test-host"
        When:
          runPqs(
            Map(
              "SCRIBE_TARGET_POSTGRES_HOST"   -> myHost,
              "PQS_TARGET_POSTGRES_USERNAME"  -> "postgres",
              "PQS_TARGET_POSTGRES_PASSWORD"  -> "postgres",
              "SCRIBE_RETRY_COUNTER_ATTEMPTS" -> "0"
            )
          )
        Then:
          Pqs.stdout `is` stringContaining("Applied configuration:")
        And:
          Pqs.stdout `is` stringContaining(s"host=$myHost")
        And:
          Pqs.stdout `is` stringContaining("level=WARN")
          && stringContaining("Environment variables with the 'SCRIBE' prefix are deprecated.")
          && stringContaining("'SCRIBE_TARGET_POSTGRES_HOST'")
          && stringContaining("'SCRIBE_RETRY_COUNTER_ATTEMPTS'")
          && stringContaining("Please use the 'PQS' prefix instead.")
        And:
          Pqs.exitCode `is` ExitCode.failure
      }
    )
  )

  def runPqs(env: Map[String, Any]): ZLayer[Docker, Throwable, CliRun] =
    Docker
      .service[Unit](image = localPqsDockerImage, env = env)("pipeline", "ledger", "postgres-document")
      .flatMap(CliRun.fromSvc)
