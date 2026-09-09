// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.backend

import com.digitalasset.pqs.postgres.backend.PostgresConfig.AuthMode
import org.postgresql.PGProperty
import zio.test.{ZIOSpecDefault, assertTrue}

object EntraPropsSpec extends ZIOSpecDefault:
  private val AuthPluginProp = "authenticationPluginClassName"
  private val AuthPluginClass =
    "com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin"

  val spec = suite("entraProps")(
    test("Password mode contributes no JDBC properties") {
      assertTrue(entraProps(AuthMode.Password).isEmpty)
    },
    test("Entra mode registers the azure-identity-extensions plugin") {
      val props = entraProps(AuthMode.Entra)
      assertTrue(props.get(AuthPluginProp).contains(AuthPluginClass))
    },
    test("Entra mode forces sslmode=require so the bearer token is not sent plaintext") {
      val props = entraProps(AuthMode.Entra)
      assertTrue(props.get(PGProperty.SSL_MODE.getName).contains("require"))
    }
  )
end EntraPropsSpec
