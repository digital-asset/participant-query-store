// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.config

import com.digitalasset.scribe.docker.Docker
import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.*
import zio.*
import zio.test.*

object AzurePostgresqlAuthenticationPluginSpec extends FuncTestStandalone:
  private val pluginClassName = "com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin"
  def spec = suite("AzurePostgresqlAuthenticationPlugin")(
    funcTest("load Azure plugin class and fail authentication") {
      Given:
        Postgres.instance
      When:
        runScribe(
          args = Seq(
            "--target-postgres-password=''",
            "--retry-counter-attempts=0",
            "--target-postgres-tls-mode=Require",
            s"--target-postgres-properties-authenticationPluginClassName=$pluginClassName"
          ),
          env = Map("AZURE_TOKEN_CREDENTIALS" -> "AzureCliCredential")
        )
      And:
        val exceptions = Seq(
          "java.lang.reflect.InvocationTargetException",
          "java.lang.IllegalStateException",
          "java.lang.ClassNotFoundException",
          "java.lang.ExceptionInInitializerError"
        )
        Scribe.stdio.is(assertAll(exceptions.map(e => Assertion.not(stringContaining(e)))))
      And:
        Scribe.stderr.is(
          (stringContaining(
            s"AzureDeveloperCliCredential authentication unavailable. Azure Developer CLI not installed."
          ))
        )
      And:
        Scribe.exitCode.is(ExitCode.failure)
    }
  )

  def runScribe(args: Seq[String], env: Map[String, String]): ZLayer[Postgres & Docker, Throwable, CliRun] =
    ZLayer
      .fromZIO(
        ZIO
          .service[Postgres]
          .map(pg =>
            Docker.service[Unit](
              image = localScribeDockerImage,
              env = env + ("SCRIBE_TARGET_POSTGRES_HOST" -> pg.container.hostName)
            )("pipeline", "ledger", "postgres-document", args)
          )
      )
      .flatten
      .flatMap(CliRun.fromSvc)
