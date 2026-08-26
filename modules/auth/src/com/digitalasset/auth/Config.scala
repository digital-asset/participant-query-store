// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.auth

import com.digitalasset.scribe.configuration.{AccessToken, ISO8601Duration, Secret}
import zio.config.magnolia.{Descriptor, describe, name}
import zio.durationInt

import java.io.File
import java.net.URI
import scala.language.implicitConversions

object Config:
  // All fields are Option because zio-config-magnolia can't handle Option[CaseClass] when the
  // inner class has required fields: it tries to read them, fails, and reports an error instead
  // of defaulting the outer Option to None. Proxy is "active" when url is Some.
  case class ProxyConfig(
      @describe("Proxy server URL")
      url: Option[URI] = None,
      @describe("Proxy server username")
      user: Option[String] = None,
      @describe("Proxy server password")
      password: Option[Secret] = None
  )
  object ProxyConfig:
    given descr: Descriptor[ProxyConfig] = Descriptor.derived[ProxyConfig]

  case class OAuth(
      @describe("Client's identifier")
      clientId: Option[String] = None,
      @describe("Client's secret")
      clientSecret: Option[Secret] = None,
      @describe("OIDC-compliant issuer URL")
      issuer: Option[URI] = None,
      @describe("Token endpoint URL")
      endpoint: Option[URI] = None,
      @describe("Trusted Certificate Authority (CA) certificate")
      @name("cafile")
      caCertificate: Option[File] = None,
      @describe("The duration (ISO 8601) prior to expiry of current, for a new token to be requested")
      preemptExpiry: ISO8601Duration = 1.minute,
      @describe("Custom parameters")
      parameters: Map[String, String] = Map.empty,
      @describe("Token scope")
      scope: Scope = Scope.Default,
      @describe("Access token")
      accessToken: Option[AccessToken] = None,
      @describe("Forward proxy for outbound OAuth HTTP requests")
      proxy: ProxyConfig = ProxyConfig()
  )

  sealed trait AuthMode
  object AuthMode:
    case object OAuth  extends AuthMode
    case object NoAuth extends AuthMode
  end AuthMode

  sealed trait Scope
  object Scope:
    case object Default                    extends Scope
    case object None                       extends Scope
    final case class Custom(scope: String) extends Scope

    given descrAbsolute: Descriptor[Scope.Custom] =
      Descriptor.from(Descriptor[String].transform[Scope.Custom](Scope.Custom.apply, _.scope))
  end Scope
end Config
