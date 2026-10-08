// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.config

import com.digitalasset.scribe.docker.{Docker, Service}
import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.azure.EntraMock
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.*
import zio.*
import zio.test.*

object AzurePostgresqlAuthenticationPluginSpec extends FuncTestStandalone:
  private val pluginClassName = "com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin"
  def spec = suite("AzurePostgresqlAuthenticationPlugin")(
    funcTest("acquire a managed-identity token and use it as the Postgres password") {
      Given:
        Postgres.instance ++ EntraMock.instance
      When:
        runScribe(
          Seq(
            "--retry-counter-attempts=0",
            "--target-postgres-tls-mode=Require",
            s"--target-postgres-properties-authenticationPluginClassName=$pluginClassName"
          )
        )
      And:
        // The plugin is bundled and wired into the JDBC flow without classloading failures.
        val exceptions = Seq(
          "java.lang.reflect.InvocationTargetException",
          "java.lang.IllegalStateException",
          "java.lang.ClassNotFoundException",
          "java.lang.ExceptionInInitializerError"
        )
        Scribe.stdio.is(assertAll(exceptions.map(e => Assertion.not(stringContaining(e)))))
      And:
        // Token acquisition hit the mock endpoint, not the real IMDS endpoint.
        Scribe.stdio.is(Assertion.not(stringContaining("169.254.169.254")))
      And:
        // The acquired token is used as the Postgres password, which Postgres rejects.
        Scribe.stdio.is(stringContaining("password authentication failed for user \"postgres\""))
      And:
        Scribe.exitCode.is(ExitCode.failure)
    }
  )

  def runScribe(args: Seq[String]): ZLayer[Postgres & Service[EntraMock] & Docker, Throwable, CliRun] =
    ZLayer
      .fromZIO(
        for
          pg    <- ZIO.service[Postgres]
          entra <- ZIO.service[Service[EntraMock]]
        yield Docker.service[Unit](
          image = localScribeDockerImage,
          env = Map(
            "SCRIBE_TARGET_POSTGRES_HOST" -> pg.container.hostName,
            // Restrict the credential chain to managed identity and point it at the mock token endpoint.
            "AZURE_TOKEN_CREDENTIALS" -> "ManagedIdentityCredential",
            "IDENTITY_ENDPOINT"       -> s"http://${entra.container.hostName}:${EntraMock.port}/token",
            "IDENTITY_HEADER"         -> "secret"
          )
        )("pipeline", "ledger", "postgres-document", args)
      )
      .flatten
      .flatMap(CliRun.fromSvc)
