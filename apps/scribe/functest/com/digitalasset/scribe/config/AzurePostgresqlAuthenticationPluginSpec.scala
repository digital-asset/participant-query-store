// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.config

import com.digitalasset.scribe.docker.{Docker, Service}
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.azure.{EntraAuthorityMock, EntraMock}
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.*
import zio.*
import zio.test.*

object AzurePostgresqlAuthenticationPluginSpec extends FuncTest[Postgres]:
  override val shared = Postgres.instance

  private val pluginClassName = "com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin"
  def spec = suite("AzurePostgresqlAuthenticationPlugin")(
    funcTest("acquire a token via ManagedIdentityCredential and use it as the Postgres password") {
      Given:
        EntraMock.instance
      When:
        runScribeWithIdentityCredential("--retry-counter-attempts=0")
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
        // The acquired token is used as the Postgres password, which Postgres rejects.
        Scribe.stdio.is(stringContaining("password authentication failed for user \"postgres\""))
      And:
        Scribe.exitCode.is(ExitCode.failure)
    },
    funcTest("acquire a token via EnvironmentCredential and use it as the Postgres password") {
      Given:
        EntraAuthorityMock.instance
      When:
        runScribeWithEnvironmentCredential("--retry-counter-attempts=0")
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
        // The client-secret token is used as the Postgres password, which Postgres rejects.
        Scribe.stdio.is(stringContaining("password authentication failed for user \"postgres\""))
      And:
        Scribe.exitCode.is(ExitCode.failure)
    }
  )

  def runScribeWithIdentityCredential(
      args: String*
  ): ZLayer[Postgres & Service[EntraMock] & Docker, Throwable, CliRun] =
    ZLayer
      .fromZIO(
        for
          pg    <- ZIO.service[Postgres]
          entra <- ZIO.service[Service[EntraMock]]
        yield Docker.service[Unit](
          image = localScribeDockerImage,
          env = Map(
            "SCRIBE_TARGET_POSTGRES_HOST"                                     -> pg.container.hostName,
            "SCRIBE_TARGET_POSTGRES_TLS_MODE"                                 -> "Require",
            "SCRIBE_TARGET_POSTGRES_PROPERTIES_authenticationPluginClassName" -> pluginClassName,
            // Restrict the credential chain to managed identity and point it at the mock token endpoint.
            "AZURE_TOKEN_CREDENTIALS" -> "ManagedIdentityCredential",
            "IDENTITY_ENDPOINT"       -> s"http://${entra.container.hostName}:${EntraMock.port}/token",
            "IDENTITY_HEADER"         -> "secret"
          )
        )("pipeline", "ledger", "postgres-document", args)
      )
      .flatten
      .flatMap(CliRun.fromSvc)

  def runScribeWithEnvironmentCredential(
      args: String*
  ): ZLayer[Postgres & Service[EntraAuthorityMock] & Docker, Throwable, CliRun] =
    ZLayer
      .fromZIO(
        for
          pg <- ZIO.service[Postgres]
          _  <- ZIO.service[Service[EntraAuthorityMock]]
          ca <- Docker.certificateAuthority
        yield
          val trustStorePath: os.Path = os.root / "tls" / "entra-truststore.p12"
          val trustStorePassword      = "changeit"

          /** JVM options that make the client trust the mock's CA (and thus its HTTPS certificate). */
          val trustStoreJavaToolOptions: Seq[String] = Seq(
            s"-Djavax.net.ssl.trustStore=$trustStorePath",
            "-Djavax.net.ssl.trustStoreType=PKCS12",
            s"-Djavax.net.ssl.trustStorePassword=$trustStorePassword"
          )

          Docker.service[Unit](
            image = localScribeDockerImage,
            env = Map(
              "SCRIBE_TARGET_POSTGRES_HOST"                                     -> pg.container.hostName,
              "SCRIBE_TARGET_POSTGRES_TLS_MODE"                                 -> "Require",
              "SCRIBE_TARGET_POSTGRES_PROPERTIES_authenticationPluginClassName" -> pluginClassName,
              "AZURE_TOKEN_CREDENTIALS"                                         -> "EnvironmentCredential",
              "AZURE_CLIENT_ID"                                                 -> EntraAuthorityMock.clientId,
              "AZURE_TENANT_ID"                                                 -> EntraAuthorityMock.tenantId,
              "AZURE_CLIENT_SECRET"                                             -> "mock-client-secret",
              "AZURE_AUTHORITY_HOST" -> s"https://${EntraAuthorityMock.host}/",
              "JAVA_TOOL_OPTIONS"    -> trustStoreJavaToolOptions.mkString(" ")
            ),
            prepopulateFiles = Seq(trustStorePath -> ca.certificate.truststore(trustStorePassword))
          )("pipeline", "ledger", "postgres-document", args)
      )
      .flatten
      .flatMap(CliRun.fromSvc)
