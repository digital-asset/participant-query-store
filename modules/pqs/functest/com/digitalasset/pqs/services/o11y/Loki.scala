// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.o11y

import com.digitalasset.pqs.docker.{Docker, Service}
import zio.ZLayer

object Loki {
  trait Instance
  object Instance:
    val port = 3100

  val instance: ZLayer[Docker, Throwable, Service[Instance]] =
    Docker
      .service[Instance](
        image = "grafana/loki:3.1.1@sha256:e689cc634841c937de4d7ea6157f17e29cf257d6a320f1c293ab18d46cfea986",
        prepopulateFiles = Seq(
          os.root / "etc" / "loki" / "config.yml" ->
            s"""auth_enabled: false
               |
               |server:
               |  http_listen_port: ${Instance.port}
               |
               |common:
               |  ring:
               |    instance_addr: 127.0.0.1
               |    kvstore:
               |      store: inmemory
               |  replication_factor: 1
               |  path_prefix: /tmp/loki
               |
               |schema_config:
               |  configs:
               |    - from: 2020-05-15
               |      store: tsdb
               |      object_store: filesystem
               |      schema: v13
               |      index:
               |        prefix: index_
               |        period: 24h
               |
               |storage_config:
               |  filesystem:
               |    directory: /tmp/loki/chunks
               |
               |limits_config:
               |  allow_structured_metadata: true
               |  max_query_lookback: 0s
               |""".stripMargin
        ),
        user = Some(10001)
      )(
        "--config.file=/etc/loki/config.yml"
      )
      .tap(_.get.blockUntilStdErr(_.contains("msg=\"Loki started\"")))
}
