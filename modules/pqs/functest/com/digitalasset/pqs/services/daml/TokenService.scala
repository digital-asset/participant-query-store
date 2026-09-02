// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import com.digitalasset.auth.{AccessToken, Config, TokenService as AuthTokenService}
import com.digitalasset.pqs.configuration.Secret
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.services.oauth.OAuth
import com.digitalasset.pqs.services.proxy.ForwardProxy
import com.digitalasset.zio.daml.LedgerScope
import sttp.client4.Backend
import zio.{RIO, RLayer, Schedule, Task, ZIO, ZLayer, durationInt}

import java.net.URI
import scala.language.postfixOps

trait TokenService:
  def getParticipantAdminToken: Task[AccessToken]
  def getAccessToken(clientId: String, parameters: Map[String, String]): Task[AccessToken]
object TokenService:
  private val ClientId     = "participant_admin"
  private val ClientSecret = Secret("secret")

  private val retrySchedule =
    Schedule.exponential(500.millis, 2) && Schedule.upTo(1.minute)

  private val caCertFile: RIO[Docker, java.io.File] =
    for
      ca     <- Docker.certificateAuthority
      cacert <- ZIO.attemptBlocking(os.temp(contents = ca.certificate.crt).toIO)
    yield cacert

  private def oauthEndpoint(hostname: String, port: Int): URI =
    URI(s"https://$hostname:$port/issuer1/token")

  val live: RLayer[Docker & Service[OAuth.Instance], TokenService] =
    val clientParams = ZLayer.fromZIO(caCertFile.map(f => AuthTokenService.HttpClientParams(Some(f))))

    val endpoint = ZLayer.fromZIO {
      Docker.inspect[OAuth.Instance].map(i => oauthEndpoint(i.exposedAddress, i.exposedPorts(OAuth.port)))
    }

    buildLayer(clientParams, endpoint)
  end live

  val liveViaProxy: RLayer[Docker & Service[OAuth.Instance] & Service[ForwardProxy.Instance], TokenService] =
    val clientParams = ZLayer.fromZIO {
      for
        cacert <- caCertFile
        proxy  <- Docker.inspect[ForwardProxy.Instance]
        proxyUri = URI(s"http://${proxy.exposedAddress}:${proxy.exposedPorts(ForwardProxy.port)}")
      yield AuthTokenService.HttpClientParams(
        Some(cacert),
        Some(
          Config.ProxyConfig(
            url = Some(proxyUri),
            user = Some(ForwardProxy.defaultUser),
            password = Some(Secret(ForwardProxy.defaultPassword))
          )
        )
      )
    }

    // Use container hostname (not exposedAddress) because the proxy resolves it on the Docker network
    val endpoint = ZLayer.fromZIO {
      Docker.inspect[OAuth.Instance].map(i => oauthEndpoint(i.container.hostName, OAuth.port))
    }

    buildLayer(clientParams, endpoint)
  end liveViaProxy

  private def buildLayer[R1, R2](
      clientParams: RLayer[R1, AuthTokenService.HttpClientParams],
      endpoint: RLayer[R2, URI]
  ): RLayer[R1 & R2, TokenService] =
    val client = clientParams >>> AuthTokenService.httpClient
    (client ++ endpoint) >>> ZLayer.fromZIO {
      for
        endpoint <- ZIO.service[URI]
        backend  <- ZIO.service[Backend[Task]]
        getToken <- ZIO.succeed((clientId: String, parameters: Map[String, String]) =>
          AuthTokenService
            .acquireToken(clientId, ClientSecret, endpoint, Some(LedgerScope), parameters)
            .retry(retrySchedule)
            .map(_._1)
            .provide(ZLayer.succeed(backend))
        )
        participantAdminToken <- getToken(ClientId, Map.empty)
      yield new TokenService {
        def getParticipantAdminToken                                          = getToken(ClientId, Map.empty)
        def getAccessToken(clientId: String, parameters: Map[String, String]) = getToken(clientId, parameters)
      }
    }

  def getToken(
      clientId: String,
      parameters: Map[String, String] = Map.empty
  ): RIO[TokenService, AccessToken] = ZIO.serviceWithZIO[TokenService] {
    _.getAccessToken(clientId, parameters).map(_.drop("Bearer ".length))
  }
end TokenService
