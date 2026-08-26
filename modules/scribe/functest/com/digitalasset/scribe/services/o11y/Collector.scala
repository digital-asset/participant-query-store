// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.o11y

import com.digitalasset.scribe.docker.{Docker, Service}
import zio.ZLayer

object Collector {
  trait Instance
  object Instance:
    val otlp = 3400

  def instance: ZLayer[
    Docker & Service[Tempo.Instance] & Service[Prometheus.Instance] & Service[Loki.Instance],
    Throwable,
    Service[Instance]
  ] = configFiles.flatMap(files =>
    Docker
      .service[Instance](
        image =
          "otel/opentelemetry-collector-contrib:0.108.0@sha256:923eb1cfae32fe09676cfd74762b2b237349f2273888529594f6c6ffe1fb3d7e",
        prepopulateFiles = files.get,
        user = Some(10001)
      )(
        "--config=/etc/otel-collector/config.yml"
      )
      .tap(_.get.blockUntilStdErr(_.contains("Everything is ready. Begin running and processing data.")))
  )

  private val configFiles = ZLayer.fromZIO(
    for
      tempo      <- Docker.inspect[Tempo.Instance]
      loki       <- Docker.inspect[Loki.Instance]
      prometheus <- Docker.inspect[Prometheus.Instance]
    yield Seq(
      os.root / "etc" / "otel-collector" / "config.yml" ->
        s"""|receivers:
            |  otlp:
            |    protocols:
            |      grpc:
            |        endpoint: 0.0.0.0:${Instance.otlp}
            |
            |processors:
            |  batch:
            |    timeout: 250ms
            |    send_batch_size: 1024
            |
            |exporters:
            |  debug:
            |    verbosity: detailed
            |
            |  # Metrics
            |  otlphttp/prometheus:
            |    endpoint: "http://${prometheus.container.hostName}:${Prometheus.Instance.port}/api/v1/otlp"
            |    tls:
            |      insecure: true
            |  # Traces
            |  otlp/tempo:
            |    endpoint: "http://${tempo.container.hostName}:${Tempo.Instance.dataPort}"
            |    tls:
            |      insecure: true
            |  # Logs
            |  otlphttp/loki:
            |    endpoint: "http://${loki.container.hostName}:${Loki.Instance.port}/otlp"
            |    tls:
            |      insecure: true
            |
            |service:
            |  pipelines:
            |    metrics:
            |      receivers: [ otlp ]
            |      processors: [ batch ]
            |      exporters: [ otlphttp/prometheus ]
            |    traces:
            |      receivers: [ otlp ]
            |      processors: [ batch ]
            |      exporters: [ otlp/tempo ]
            |    logs:
            |      receivers: [ otlp ]
            |      processors: [ batch ]
            |      exporters: [ otlphttp/loki ]
            |  telemetry:
            |    metrics:
            |      level: normal
            |    logs:
            |      level: info
            |""".stripMargin
    )
  )
}
