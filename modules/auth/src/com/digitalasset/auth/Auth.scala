// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.auth

import com.digitalasset.pqs.configuration.ISO8601Duration
import com.digitalasset.auth.AccessToken as AuthAccessToken
import com.digitalasset.auth.Config.{AuthMode, Scope}
import com.digitalasset.pqs.configuration.Secret
import com.digitalasset.pqs.utils.safeequals.===
import zio.{RLayer, Task, ZIO, ZLayer}

import java.io.File
import java.net.URI

sealed trait Auth
object Auth:
  final case class OAuth(
      clientId: String,
      clientSecret: Secret,
      issuer: Option[URI],
      endpoint: Option[URI],
      scope: Option[String],
      caCertificate: Option[File] = None,
      preemptExpiry: ISO8601Duration,
      parameters: Map[String, String] = Map.empty,
      proxy: Option[Config.ProxyConfig] = None
  ) extends Auth
  final case class AccessToken(token: AuthAccessToken) extends Auth
  case object NoAuth                                   extends Auth

  def live(scope: String): RLayer[Config.AuthMode & Config.OAuth, Auth] = ZLayer.fromZIO {
    for
      mode   <- ZIO.service[Config.AuthMode]
      config <- ZIO.service[Config.OAuth]
      res    <- Auth(scope, mode, config)
    yield res
  }

  private def proxyConfig(config: Config.OAuth): Option[Config.ProxyConfig] =
    config.proxy.url.map(_ => config.proxy)

  private def apply(defaultScope: String, mode: Config.AuthMode, config: Config.OAuth): Task[Auth] =
    if mode === AuthMode.OAuth then
      config match
        /* clientId + clientSecret + at least one of issuer/endpoint */
        case Config.OAuth(
              Some(clientId),
              Some(clientSecret),
              issuer,
              endpoint,
              caCertificate,
              preemptExpiry,
              parameters,
              scope,
              _,
              _
            ) if issuer.isDefined || endpoint.isDefined =>
          ZIO.when(parameters.contains("scope"))(
            ZIO.fail(Throwable("Use `scope` configuration parameter to configure scope."))
          ) *>
            ZIO.succeed(
              Auth.OAuth(
                clientId = clientId,
                clientSecret = clientSecret,
                issuer = issuer,
                endpoint = endpoint,
                scope = makeScope(scope, defaultScope),
                caCertificate = caCertificate,
                preemptExpiry = preemptExpiry,
                parameters = parameters,
                proxy = proxyConfig(config)
              )
            )
        case Config.OAuth(_, _, _, _, _, _, _, _, Some(token), _) =>
          ZIO.succeed(Auth.AccessToken(token.value))
        case Config.OAuth(None, _, _, _, _, _, _, _, _, _) =>
          ZIO.fail(Throwable(s"Incorrect access token configuration: `clientId` is missing."))
        case Config.OAuth(_, None, _, _, _, _, _, _, _, _) =>
          ZIO.fail(Throwable(s"Incorrect access token configuration: `clientSecret` is missing."))
        case Config.OAuth(_, _, None, None, _, _, _, _, _, _) =>
          ZIO.fail(
            Throwable(
              s"Incorrect access token configuration: `endpoint`/`issuer` is missing (at least one must be specified)."
            )
          )
        case _ => ZIO.fail(Throwable(s"Missing access token configuration. config passed : ${config.toString}"))
    else ZIO.succeed(Auth.NoAuth)

  private def makeScope(scope: Scope, defaultScope: String) =
    scope match
      case Scope.Default       => Some(defaultScope)
      case Scope.None          => None
      case Scope.Custom(value) => Some(value)
end Auth
