// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.azure

import com.digitalasset.scribe.docker.{Docker, Service}
import zio.ZLayer

sealed trait EntraAuthorityMock

/** Mocks the Microsoft Entra token endpoint for the client-secret flow used by `EnvironmentCredential`
  * (`AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_CLIENT_SECRET`).
  *
  * The endpoint is served over HTTPS with a certificate signed by the func-test CA.
  */
object EntraAuthorityMock:
  val port: Int        = 443
  val host: String     = "login.microsoftonline.com"
  val tenantId: String = "00000000-0000-0000-0000-000000000001"
  val clientId: String = "00000000-0000-0000-0000-000000000002"

  val instance: ZLayer[Docker, Throwable, Service[EntraAuthorityMock]] = ZLayer.fromZIO {
    for
      ca   <- Docker.certificateAuthority
      cert <- ca.generate(host, Seq(host))
    yield
      // Client-credentials token response (OAuth2). Far-future expiry so the token is treated as valid.
      val tokenResponse =
        s"""{"token_type":"Bearer","expires_in":3600,"ext_expires_in":3600,"access_token":"mock-entra-access-token"}"""
      val instanceDiscovery =
        s"""|{
            |  "tenant_discovery_endpoint":"https://$host/$tenantId/v2.0/.well-known/openid-configuration",
            |  "api-version":"1.1",
            |  "metadata":[{"preferred_network":"$host","preferred_cache":"$host","aliases":["$host"]}]
            |}""".stripMargin
      val configFile =
        s"""server {
           |  listen $port ssl;
           |  ssl_certificate     /tls/server.crt;
           |  ssl_certificate_key /tls/server.key;
           |  location /common/discovery/instance {
           |    default_type application/json;
           |    return 200 '$instanceDiscovery';
           |  }
           |  location ~ "/oauth2/v2\\.0/token$$" {
           |    default_type application/json;
           |    return 200 '$tokenResponse';
           |  }
           |}
           |""".stripMargin
      val layer = Docker.service[EntraAuthorityMock](
        image = "nginx:1.27-alpine@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10",
        hostname = Some(host),
        prepopulateFiles = Seq(
          os.root / "etc" / "nginx" / "conf.d" / "default.conf" -> configFile,
          os.root / "tls" / "server.crt"                        -> cert.certificate.crt,
          os.root / "tls" / "server.key"                        -> cert.certificate.pem
        ),
        suppressOutput = true
      )()
      layer.tap(_.get.blockUntilStdOut(_.contains("Configuration complete; ready for start up")))
  }.flatten
