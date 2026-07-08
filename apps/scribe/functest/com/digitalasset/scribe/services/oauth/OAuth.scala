// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.oauth

import com.digitalasset.scribe.docker.{Docker, Service}
import zio.{Ref, ZLayer}

object OAuth:
  val port: Int   = 8080
  val tokenExpiry = 90

  trait Instance
  val instance: ZLayer[Docker, Throwable, Service[Instance]] = ZLayer.fromZIO {
    for
      ca  <- Docker.certificateAuthority
      cnt <- Docker.share("oauth_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
      hostname = s"oauth-$cnt"
      // "localhost" SAN is required: in local mode, exposedAddress resolves to "localhost"
      // so the TLS client verifies the cert against that hostname.
      sslCert    <- ca.generate(hostname, Seq(hostname, "localhost"))
      signingKey <- ca.generate("issuer1")
      escapedJwk = ujson.Str(signingKey.certificate.toJWK).render()
      svc = Docker
        .service[Instance](
          image =
            "ghcr.io/navikt/mock-oauth2-server:2.1.3@sha256:6ef46dce70332df91836e2259865f68c942b5497adde8ea9153946158ce448c6",
          hostname = Some(hostname),
          exposePorts = Set(port),
          env = Map(
            "JSON_CONFIG_PATH" -> "/data/oauth.conf",
            "LOG_LEVEL"        -> "DEBUG"
          ),
          prepopulateFiles = Seq(
            os.root / "data" / "oauth-certificate.crt" -> signingKey.certificate.crt,
            os.root / "data" / "keystore.p12"          -> sslCert.certificate.pkcs12,
            os.root / "data" / "oauth.conf" ->
              s"""{
                 |  "interactiveLogin": false,
                 |  "httpServer" : {
                 |    "type" : "NettyWrapper",
                 |    "ssl" : {
                 |      "keystoreFile" : "/data/keystore.p12",
                 |      "keystoreType" : "PKCS12"
                 |    }
                 |  },
                 |
                 |  "tokenProvider": {
                 |    "keyProvider": {
                 |      "initialKeys": $escapedJwk,
                 |      "algorithm" : "RS256"
                 |    }
                 |  },
                 |
                 |  "tokenCallbacks": [
                 |    {
                 |      "issuerId": "issuer1",
                 |      "tokenExpiry": $tokenExpiry,
                 |      "requestMappings": [
                 |        {
                 |          "requestParam": "audience",
                 |          "match": "https://daml.com/jwt/aud/participant/.*",
                 |          "claims": {
                 |            "aud": "$${audience}",
                 |            "sub": "$${clientId}"
                 |          }
                 |        },
                 |        {
                 |          "requestParam": "scope",
                 |          "match": "daml_ledger_api",
                 |          "claims": {
                 |            "sub": "$${clientId}",
                 |            "scope": "daml_ledger_api"
                 |          }
                 |        },
                 |        {
                 |          "requestParam": "custom_scope",
                 |          "match": ".*",
                 |          "claims": {
                 |            "sub": "$${clientId}",
                 |            "scope": "daml_ledger_api $${custom_scope}"
                 |          }
                 |        }
                 |      ]
                 |    }
                 |  ]
                 |}
                 |""".stripMargin
          ),
          suppressOutput = true
        )()
        .tap(_.get.blockUntilStdOut(_.contains("started server on address")))
    yield svc
  }.flatten
