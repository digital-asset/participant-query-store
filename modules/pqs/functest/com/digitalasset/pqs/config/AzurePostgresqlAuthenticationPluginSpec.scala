// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.config

import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.{CantonConf, Ledger}
import com.digitalasset.pqs.services.azure.{EntraAuthorityMock, EntraMock}
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import zio.*
import zio.test.*

object AzurePostgresqlAuthenticationPluginSpec extends SharedLedgerAndPostgresTest:
  private val pluginClassName = "com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin"
  def spec = suite("AzurePostgresqlAuthenticationPlugin")(
    funcTest("Acquire Postgres token from ManagedIdentityCredential") {
      Given:
        Postgres.database
      When:
        EntraMock.instance("invalid-token")
          >>> runPqsWithIdentityCredential("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode.is(ExitCode.failure)
      And:
        noClassLoadingErrors
      And:
        // The acquired token is used as the Postgres password, which Postgres rejects.
        Pqs.stdio.is(stringContaining("password authentication failed for user \"postgres\""))
      When:
        EntraMock.instance("postgres")
          >>> runPqsWithIdentityCredential("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode.is(ExitCode.success)
      And:
        noClassLoadingErrors
    },
    funcTest("Acquire Postgres token from EnvironmentCredential") {
      Given:
        Postgres.database
      When:
        EntraAuthorityMock.instance("invalid-token")
          >>> runPqsWithEnvironmentCredential("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode.is(ExitCode.failure)
      And:
        noClassLoadingErrors
      And:
        // The client-secret token is used as the Postgres password, which Postgres rejects.
        Pqs.stdio.is(stringContaining("password authentication failed for user \"postgres\""))
      When:
        EntraAuthorityMock.instance("postgres")
          >>> runPqsWithEnvironmentCredential("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode.is(ExitCode.success)
      And:
        noClassLoadingErrors
    }
  )

  private def runPqsWithIdentityCredential(args: String*) =
    ZLayer
      .fromZIO(
        for
          (env, tlsFiles) <- pqsConf
          entra           <- ZIO.service[Service[EntraMock]]
        yield Docker.service[Unit](
          image = localPqsDockerImage,
          env = env ++ Map(
            // Restrict the credential chain to managed identity and point it at the mock token endpoint.
            "AZURE_TOKEN_CREDENTIALS" -> "ManagedIdentityCredential",
            "IDENTITY_ENDPOINT"       -> s"http://${entra.container.hostName}:${EntraMock.port}/token",
            "IDENTITY_HEADER"         -> "secret"
          ),
          prepopulateFiles = tlsFiles
        )("pipeline", "ledger", "postgres-document", args)
      )
      .flatten
      .flatMap(CliRun.fromSvc)

  private def runPqsWithEnvironmentCredential(args: String*) =
    ZLayer
      .fromZIO(
        for
          (env, tlsFiles) <- pqsConf
          mock            <- ZIO.service[Service[EntraAuthorityMock]]
          ca              <- Docker.certificateAuthority
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
            image = localPqsDockerImage,
            env = env ++ Map(
              "AZURE_TOKEN_CREDENTIALS" -> "EnvironmentCredential",
              "AZURE_CLIENT_ID"         -> EntraAuthorityMock.clientId,
              "AZURE_TENANT_ID"         -> EntraAuthorityMock.tenantId,
              "AZURE_CLIENT_SECRET"     -> "mock-client-secret",
              "AZURE_AUTHORITY_HOST"    -> s"https://${mock.container.hostName}/",
              "JAVA_TOOL_OPTIONS"       -> trustStoreJavaToolOptions.mkString(" ")
            ),
            prepopulateFiles = tlsFiles :++ Seq(trustStorePath -> ca.certificate.truststore(trustStorePassword))
          )("pipeline", "ledger", "postgres-document", args)
      )
      .flatten
      .flatMap(CliRun.fromSvc)

  private def pqsConf =
    for
      pg                <- ZIO.service[Postgres]
      ledger            <- ZIO.service[Service[Ledger]]
      database          <- ZIO.service[Database]
      ca                <- Docker.certificateAuthority
      clientCertificate <- ca.generate("pqs")
    yield
      val env = Map(
        "PQS_RETRY_COUNTER_ATTEMPTS"                                   -> "0",
        "PQS_SOURCE_LEDGER_HOST"                                       -> ledger.container.hostName,
        "PQS_SOURCE_LEDGER_PORT"                                       -> CantonConf.participantPort,
        "PQS_SOURCE_LEDGER_TLS_KEY"                                    -> "/tls/client.pem",
        "PQS_SOURCE_LEDGER_TLS_CERT"                                   -> "/tls/client.crt",
        "PQS_SOURCE_LEDGER_TLS_CAFILE"                                 -> "/tls/root-ca.crt",
        "PQS_TARGET_POSTGRES_HOST"                                     -> pg.container.hostName,
        "PQS_TARGET_POSTGRES_PORT"                                     -> Postgres.port,
        "PQS_TARGET_POSTGRES_DATABASE"                                 -> database.name,
        "PQS_TARGET_POSTGRES_TLS_MODE"                                 -> "Require",
        "PQS_TARGET_POSTGRES_TLS_KEY"                                  -> "/tls/client.der",
        "PQS_TARGET_POSTGRES_TLS_CERT"                                 -> "/tls/client.crt",
        "PQS_TARGET_POSTGRES_TLS_CAFILE"                               -> "/tls/root-ca.crt",
        "PQS_TARGET_POSTGRES_PROPERTIES_authenticationPluginClassName" -> pluginClassName
      )
      val tlsFiles = Seq(
        os.root / "tls" / "client.pem"  -> clientCertificate.certificate.pem,
        os.root / "tls" / "client.der"  -> clientCertificate.certificate.der,
        os.root / "tls" / "client.crt"  -> clientCertificate.certificate.crt,
        os.root / "tls" / "root-ca.crt" -> ca.certificate.crt
      )
      (env, tlsFiles)

  private def noClassLoadingErrors =
    // The plugin is bundled and wired into the JDBC flow without classloading failures.
    val exceptions = Seq(
      "java.lang.reflect.InvocationTargetException",
      "java.lang.IllegalStateException",
      "java.lang.ClassNotFoundException",
      "java.lang.ExceptionInInitializerError"
    )
    Pqs.stdio.is(assertAll(exceptions.map(e => Assertion.not(stringContaining(e)))))
