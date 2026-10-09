// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.azure

import com.digitalasset.scribe.docker.{Docker, Service}
import zio.{Ref, ZLayer}

sealed trait EntraMock

/** Mocks the Azure managed-identity token endpoint. */
object EntraMock:
  val port: Int = 80

  // Far-future expiry (year 2100) so the returned token is treated as valid.
  private val tokenResponse =
    """|{
       |  "access_token":"mock-entra-access-token","expires_on":"4102444800",
       |  "resource":"https://ossrdbms-aad.database.windows.net",
       |  "token_type":"Bearer"
       |}""".stripMargin

  val instance: ZLayer[Docker, Throwable, Service[EntraMock]] = ZLayer.fromZIO {
    for cnt <- Docker.share("entra_mock_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
    yield
      val configFile =
        s"""server {
           |  listen $port;
           |  location /token {
           |    default_type application/json;
           |    return 200 '$tokenResponse';
           |  }
           |}
           |""".stripMargin
      val layer = Docker.service[EntraMock](
        image = "nginx:1.27-alpine@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10",
        hostname = Some(s"entra-mock-$cnt"),
        exposePorts = Set(port),
        prepopulateFiles = Seq(os.root / "etc" / "nginx" / "conf.d" / "default.conf" -> configFile),
        suppressOutput = true
      )()
      layer.tap(_.get.blockUntilStdOut(_.contains("Configuration complete; ready for start up")))
  }.flatten
