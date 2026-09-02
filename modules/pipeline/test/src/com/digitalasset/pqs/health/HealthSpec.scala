// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.health

import zio.test.*
import zio.test.Assertion.*
import zio.ZIO
import java.net.InetAddress
import zio.ZLayer

object HealthSpec extends ZIOSpecDefault:
  override def spec =
    suite("health server")(
      test("port conflict"):
        for
          server <- ZIO.acquireRelease(
            ZIO.attempt(new java.net.ServerSocket(0, 1, InetAddress.getByName("0.0.0.0")))
          )(s => ZIO.attempt(s.close()).ignoreLogged)
          healthConfig = ZLayer.succeed(Config(address = "0.0.0.0", port = server.getLocalPort))
          res <- assertZIO(
            ZIO.scoped((healthConfig >>> Health.live).build).exit
          )(fails(hasMessage(containsString("Address already in use"))))
        yield res
    )
