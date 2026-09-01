// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.proxy

import com.digitalasset.pqs.docker.{Docker, Service}
import zio.{Ref, RLayer, ZLayer}

object ForwardProxy:
  val port: Int       = 8888
  val defaultUser     = "proxyuser"
  val defaultPassword = "proxypass"

  trait Instance

  def instance(connectPort: Int): RLayer[Docker, Service[Instance]] = ZLayer.fromZIO {
    for
      cnt <- Docker.share("proxy_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
      hostname = s"proxy-$cnt"
      config = Seq(
        s"Port $port",
        "Listen 0.0.0.0",
        "Timeout 600",
        "MaxClients 100",
        "Allow 0.0.0.0/0",
        "LogLevel Info",
        s"BasicAuth $defaultUser $defaultPassword",
        s"ConnectPort $connectPort"
      ).mkString("\\n")
      svc = Docker
        .service[Instance](
          image = "alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d",
          hostname = Some(hostname),
          exposePorts = Set(port),
          suppressOutput = true
        )(
          "sh",
          "-c",
          s"apk add --no-cache tinyproxy >/dev/null 2>&1 && printf '$config\\n' > /etc/tinyproxy/tinyproxy.conf && tinyproxy -d"
        )
        .tap(_.get.blockUntilStdOut(_.contains("Starting main loop")))
    yield svc
  }.flatten
