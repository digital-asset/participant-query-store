// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.o11y

import com.digitalasset.scribe.docker.{Docker, Service}
import zio.ZLayer

object Prometheus {
  trait Instance
  object Instance:
    val port = 3300

  val instance: ZLayer[Docker, Throwable, Service[Instance]] =
    Docker
      .service[Instance](
        image = "prom/prometheus:v2.54.1@sha256:f6639335d34a77d9d9db382b92eeb7fc00934be8eae81dbc03b31cfe90411a94",
        prepopulateFiles = Seq(os.root / "etc" / "prometheus" / "config.yml" -> "global:"),
        user = Some(65534)
      )(
        "--config.file=/etc/prometheus/config.yml",
        "--web.enable-remote-write-receiver",
        s"--web.listen-address=:${Instance.port}",
        "--enable-feature=native-histograms",
        "--enable-feature=exemplar-storage",
        "--enable-feature=otlp-write-receiver"
      )
      .tap(_.get.blockUntilStdErr(_.contains("msg=\"Server is ready to receive web requests.\"")))
}
