// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.o11y

import com.digitalasset.scribe.docker.{Docker, Service}
import zio.ZLayer

object Tempo {
  trait Instance
  object Instance:
    val queryPort = 3200
    val dataPort  = 3210

  val instance: ZLayer[Docker, Throwable, Service[Instance]] =
    Docker
      .service[Instance](
        image = "grafana/tempo:2.5.0@sha256:f0200a9bff6d14eb3a4332194f7b77c37ee1a3535e7e41db024d95aab6f1b4e8",
        prepopulateFiles = Seq(
          os.root / "etc" / "tempo" / "config.yml" ->
            s"""server:
               |  http_listen_address: 0.0.0.0
               |  http_listen_port: ${Instance.queryPort}
               |
               |distributor:
               |  receivers:
               |    otlp:
               |      protocols:
               |        grpc:
               |          endpoint: "0.0.0.0:${Instance.dataPort}"
               |
               |ingester:
               |  trace_idle_period: 10s
               |  max_block_bytes: 1_000_000
               |  max_block_duration: 5m
               |
               |compactor:
               |  compaction:
               |    compaction_window: 1h
               |    max_block_bytes: 100_000_000
               |    block_retention: 1h
               |    compacted_block_retention: 10m
               |
               |storage:
               |  trace:
               |    backend: local
               |    block:
               |      bloom_filter_false_positive: .05
               |      v2_index_downsample_bytes: 1000
               |      v2_encoding: zstd
               |    wal:
               |      path: /tmp/tempo/wal
               |      v2_encoding: snappy
               |    local:
               |      path: /tmp/tempo/blocks
               |    pool:
               |      max_workers: 100
               |      queue_depth: 10000
               |
               |overrides:
               |  defaults:
               |    ingestion:
               |      rate_strategy: local
               |      rate_limit_bytes: 15000000
               |      burst_size_bytes: 20000000
               |      max_traces_per_user: 10000
               |      max_global_traces_per_user: 0
               |    read:
               |      max_bytes_per_tag_values_query: 5000000
               |    compaction:
               |      block_retention: 0s
               |    global:
               |      max_bytes_per_trace: 1000000
               |""".stripMargin
        ),
        user = Some(10001)
      )(
        "--config.file=/etc/tempo/config.yml"
      )
      .tap(_.get.blockUntilStdErr(_.contains("msg=\"Tempo started\"")))
}
