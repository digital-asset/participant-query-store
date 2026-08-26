// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.auth

import com.digitalasset.scribe.configuration.Secret
import com.digitalasset.scribe.utils.safeequals.=/=
import sttp.client4.*
import sttp.client4.httpclient.zio.HttpClientZioBackend
import sttp.client4.testing.ResponseStub
import zio.ZIO.logInfo
import zio.*
import zio.test.{TestClock, ZIOSpecDefault, assertTrue}

import java.time.Instant
import scala.language.implicitConversions

object TokenServiceSpec extends ZIOSpecDefault:
  val tokenEndpointBasedAuth =
    ZLayer.succeed(
      Auth.OAuth(
        "clientId",
        Secret("secret"),
        issuer = None,
        endpoint = Some(java.net.URI("https://localhost:8080/issuer1/token")),
        scope = None,
        caCertificate = None,
        1.minute,
        parameters = Map.empty
      )
    )

  val issuerNoSlashBasedAuth =
    ZLayer.succeed(
      Auth.OAuth(
        clientId = "clientId",
        clientSecret = Secret("secret"),
        issuer = Some(java.net.URI("https://localhost:8080")),
        endpoint = None,
        scope = None,
        caCertificate = None,
        preemptExpiry = 1.minute,
        parameters = Map.empty
      )
    )

  val issuerSlashBasedAuth =
    ZLayer.succeed(
      Auth.OAuth(
        clientId = "clientId",
        clientSecret = Secret("secret"),
        issuer = Some(java.net.URI("https://localhost:8080/")),
        endpoint = None,
        scope = None,
        caCertificate = None,
        preemptExpiry = 1.minute,
        parameters = Map.empty
      )
    )

  val issuerNoSlashBasedAuthSomeScope =
    ZLayer.succeed(
      Auth.OAuth(
        clientId = "clientId",
        clientSecret = Secret("secret"),
        issuer = Some(java.net.URI("https://localhost:8080")),
        endpoint = None,
        scope = Some("scope"),
        caCertificate = None,
        preemptExpiry = 1.minute,
        parameters = Map.empty
      )
    )

  val bothIssuerAndEndpoint =
    ZLayer.succeed(
      Auth.OAuth(
        clientId = "clientId",
        clientSecret = Secret("secret"),
        issuer = Some(java.net.URI("https://localhost:8080")),
        endpoint = Some(java.net.URI("https://localhost:8080/issuer1/token")),
        scope = Some("scope"),
        caCertificate = None,
        preemptExpiry = 1.minute,
        parameters = Map.empty
      )
    )

  private val tokenJson1 =
    """{
      |  "token_type" : "Bearer",
      |  "access_token" : "eyJraWQiOiJpc3N1ZXIxIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJBbGljZSIsIm5iZiI6MTY5MjcxMDI0Niwic2NvcGUiOiJkYW1sX2xlZGdlcl9hcGkiLCJpc3MiOiJodHRwczovL2xvY2FsaG9zdDo1NDAzOC9pc3N1ZXIxIiwiZXhwIjoxNjkyNzEzODQ2LCJpYXQiOjE2OTI3MTAyNDYsImp0aSI6ImMzM2JkZmM5LTJlMTUtNDE2ZS1iMGNiLWRlNjE0OWQxMzc1ZiJ9.TzJaXSxgDr4VE0eNyfkXvPI-e2qf7Sqj0uj3GQVwtR-bU6ehw3LWsDnXhSTLbto448uM9BshkpPJzzY5_sPW2TpGY2herH2IevaWE99we16dqn9plz8vLub7BtwAIsF2DidRReP1h3Pfgp9wFvFjullJUPTHJMBafYzqLcu0Nsg1YBQrjWnWx8ANfU4DrioBwowIA5uu9rIfJVIpzIqMai86Ot1bQ_FdAPH7TTpe2fBvLlAzDWmCEmyMZvPAmc6g2aS-clTRCkTFeEryy2p-MCor8-mJ2j_4NdmjfWB19oZbArYbRP5BdJMe772ImNnmwI9CXff71U-AV2Ft7zZ2wQ",
      |  "expires_in" : 3599,
      |  "scope" : "daml_ledger_api"
      |}""".stripMargin

  private val tokenJson2 =
    """{
      |  "token_type" : "Bearer",
      |  "access_token" : "eyJraWQiOiJpc3N1ZXIxIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJBbGljZSIsIm5iZiI6MTY5MjcxMTAzMCwic2NvcGUiOiJkYW1sX2xlZGdlcl9hcGkiLCJpc3MiOiJodHRwczovL2xvY2FsaG9zdDo1NDkwOC9pc3N1ZXIxIiwiZXhwIjoxNjkyNzE0NjMwLCJpYXQiOjE2OTI3MTEwMzAsImp0aSI6IjdjMDVjNTgxLWFmNzUtNDM4Ny1hMjE4LTNlY2FmYWQyYjczYyJ9.OMczfC91-uPv2IbxheTxu_cim3B3lFbSOxSsBRwB5C9SQYpH7iyoJmU8RAfyL3VttKx2hqLzlqpyijJaY0G3SwE4Jb7B6j6Gjz6QHdfqLWvXXLPnDJCpFkyCVQU0k8pyMOcybuog0y6M_GXtdO28Sq1iZ9IBzZ1zjJN1aIQO8AZezM9tIWbLkD8TjXyU0uVFFxlhltSUpy-yO90ClWfpLKOFnyMIdah6KtCwjvdH4I4qgkaGxt5CzrVcXYRmW8za_cvOo8yiAp02lv6q6zJAMJvrMzMtm37lIQrgur00RdotM5WfZQbF_DhCpTNXyTKAQzjY2x21f-26x7_sFca1XA",
      |  "expires_in" : 3599,
      |  "scope" : "daml_ledger_api"
      |}""".stripMargin

  val mockBackend: ZLayer[Any, Nothing, Backend[Task]] = ZLayer.succeed(
    HttpClientZioBackend.stub
      .whenRequestMatches(_.uri.path.exists(_.contains("openid-configuration")))
      .thenRespondAdjust("""{"token_endpoint":"https://localhost:8080/issuer1/token"}""")
      .whenRequestMatches(_.uri.path.exists(_.contains("token")))
      .thenRespondCyclic(
        ResponseStub.adjust(tokenJson1),
        ResponseStub.adjust(tokenJson2)
      )
  )

  def spec = suite("TokenService")(
    test("retrieves and refreshes token when given an endpoint") {
      val startTime = Instant.ofEpochSecond(1692710246)
      for
        _            <- TestClock.setTime(startTime)
        _            <- logInfo(s"startTime:$startTime")
        tokenService <- ZIO.service[TokenService]
        token1       <- tokenService.getAccessToken.someOrFailException
        _            <- logInfo(s"Token1:$token1")
        _            <- TestClock.adjust(3600.seconds) // triggers refresh
        _ <- TestClock.sleeps.repeatUntilZIO { sleeps =>
          Clock.instant.map(now => sleeps.forall(_.isAfter(now)))
        }
        token2 <- tokenService.getAccessToken.someOrFailException
        _      <- logInfo(s"Token2:$token2")
      yield assertTrue(token1 =/= token2)
    }.provideSomeLayer(mockBackend ++ tokenEndpointBasedAuth >>> TokenService.auth),
    test("retrieves a token when given an issuer with no trailing slash") {
      for
        tokenService <- ZIO.service[TokenService]
        token        <- tokenService.getAccessToken.someOrFailException
      yield assertTrue(token.nonEmpty)
    }.provideSomeLayer(mockBackend ++ issuerNoSlashBasedAuth >>> TokenService.auth),
    test("retrieves a token when given an issuer with trailing slash") {
      for
        tokenService <- ZIO.service[TokenService]
        token        <- tokenService.getAccessToken.someOrFailException
      yield assertTrue(token.nonEmpty)
    }.provideSomeLayer(mockBackend ++ issuerSlashBasedAuth >>> TokenService.auth),
    test("retrieves a token when given an issuer with trailing slash and scope") {
      for
        tokenService <- ZIO.service[TokenService]
        token        <- tokenService.getAccessToken.someOrFailException
      yield assertTrue(token.nonEmpty)
    }.provideSomeLayer(mockBackend ++ issuerNoSlashBasedAuthSomeScope >>> TokenService.auth),
    test("retrieves a token when given an issuer and endpoint and scope") {
      for
        tokenService <- ZIO.service[TokenService]
        token        <- tokenService.getAccessToken.someOrFailException
      yield assertTrue(token.nonEmpty)
    }.provideSomeLayer(mockBackend ++ bothIssuerAndEndpoint >>> TokenService.auth)
  )

end TokenServiceSpec
