// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.o11y

import com.digitalasset.pqs.docker.{Docker, Service}
import ujson.*
import zio.http.*
import zio.{Scope, ZIO, ZLayer}

import java.time.Instant
import java.time.temporal.ChronoUnit

object Grafana:
  trait Instance
  object Instance:
    val port = 3000

  def instance: ZLayer[
    Docker & Service[Tempo.Instance] & Service[Prometheus.Instance] & Service[Loki.Instance],
    Throwable,
    Service[Instance]
  ] = configFiles.flatMap(files =>
    Docker
      .service[Instance](
        image = "grafana/grafana:11.1.5@sha256:f4796c6227f4acb0b0dfef12edc2c732f60f18f65a6cf005313700c0896aaa30",
        env = Map(
          "GF_AUTH_ANONYMOUS_ENABLED"  -> "true",
          "GF_AUTH_ANONYMOUS_ORG_ROLE" -> "Admin",
          "GF_AUTH_DISABLE_LOGIN_FORM" -> "true",
          "GF_PATHS_PROVISIONING"      -> "/data/grafana-provisioning"
        ),
        exposePorts = Set(Instance.port),
        prepopulateFiles = files.get,
        user = Some(472)
      )()
      .tap(_.get.blockUntilStdOut(_.contains("msg=\"HTTP Server Listen\"")))
  )

  def findTraceByName(name: String): ZIO[Service[Instance], Throwable, Option[String]] = client(
    ZIO.scoped(
      Client
        .streaming(
          Request.get(
            URL(
              Path.root / "api" / "datasources" / "proxy" / "uid" / "tempo" / "api" / "search",
              queryParams = QueryParams("q" -> s"{name=\"$name\"}")
            )
          )
        )
        .flatMap(_.body.asString)
        .mapAttempt(ujson.read(_))
        .flatMap(json =>
          ZIO.attempt(json.obj("traces").arr(0).obj("traceID").str.reverse.padTo(32, '0').reverse).option
        )
    )
  )

  def findTraceById(id: String): ZIO[Service[Instance], Throwable, Trace] = client(
    ZIO.scoped(
      Client
        .streaming(
          Request.post(
            URL(Path.root / "api" / "ds" / "query"),
            Body.fromString(
              s"""|{
                  |  "queries": [
                  |    {
                  |      "datasource": {
                  |        "type": "tempo",
                  |        "uid": "tempo"
                  |      },
                  |      "queryType": "traceId",
                  |      "query": "$id"
                  |    }
                  |  ]
                  |}""".stripMargin
            )
          )
        )
        .flatMap(_.body.asString)
        .mapAttempt(ujson.read(_))
        .mapAttempt(Trace(_))
    )
  )

  def getMetrics(name: String): ZIO[Service[Instance] & Scope, Throwable, Seq[Double]] = client(
    Client
      .streaming(
        Request.post(
          URL(Path.root / "api" / "ds" / "query"),
          Body.fromString(
            s"""|{
                |  "queries": [
                |    {
                |      "datasource": {
                |        "type": "prometheus",
                |        "uid": "prometheus"
                |      },
                |      "expr": "$name",
                |      "range": true,
                |      "instant": false,
                |      "exemplar": false,
                |      "intervalMs": 5000
                |    }
                |  ],
                |  "from": "${Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli}",
                |  "to":   "${Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli}"
                |}""".stripMargin
          )
        )
      )
      .flatMap(_.body.asString)
      .mapAttempt(ujson.read(_))
      .mapAttempt(
        _.obj("results")
          .obj("A")
          .obj("frames")
          .arr(0)
          .obj("data")
          .obj("values")
          .arr(1)
          .arr
          .map(_.num)
          .toSeq
      )
  )

  def getLogsForTraceId(id: String): ZIO[Service[Instance] & Scope, Throwable, Seq[String]] = client(
    Client
      .streaming(
        Request.post(
          URL(Path.root / "api" / "ds" / "query"),
          Body.fromString(
            s"""|{
                |"queries": [
                |    {
                |      "expr": "{service_name=\\"pqs\\"} | trace_id=\\"$id\\"",
                |      "datasource": {
                |        "type": "loki",
                |        "uid": "loki"
                |      }
                |    }
                |  ],
                |  "from": "${Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli}",
                |  "to":   "${Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli}"
                |}""".stripMargin
          )
        )
      )
      .flatMap(_.body.asString)
      .mapAttempt(ujson.read(_))
      .mapAttempt(
        _.obj("results")
          .obj("A")
          .obj("frames")
          .arr(0)
          .obj("data")
          .obj("values")
          .arr(2)
          .arr
          .map(_.str)
          .toSeq
      )
  )

  private val configFiles = ZLayer.fromZIO(
    for
      prometheus <- Docker.inspect[Prometheus.Instance]
      tempo      <- Docker.inspect[Tempo.Instance]
      loki       <- Docker.inspect[Loki.Instance]
    yield Seq(
      os.root / "data" / "grafana-provisioning" / "datasources" / "datasources.yaml" ->
        s"""apiVersion: 1
           |
           |datasources:
           |- name: Prometheus
           |  type: prometheus
           |  uid: prometheus
           |  access: proxy
           |  orgId: 1
           |  url: http://${prometheus.container.hostName}:${Prometheus.Instance.port}
           |  basicAuth: false
           |  isDefault: false
           |  version: 1
           |  editable: true
           |- name: Tempo
           |  type: tempo
           |  uid: tempo
           |  access: proxy
           |  orgId: 1
           |  url: http://${tempo.container.hostName}:${Tempo.Instance.queryPort}
           |  basicAuth: false
           |  isDefault: true
           |  version: 1
           |  editable: true
           |- name: Loki
           |  type: loki
           |  uid: loki
           |  access: proxy
           |  orgId: 1
           |  url: http://${loki.container.hostName}:${Loki.Instance.port}
           |  basicAuth: false
           |  editable: true
           |""".stripMargin
    )
  )

  private val client = ZLayer
    .service[Service[Instance]]
    .flatMap(s =>
      Client.default.update(
        _.host(s.get.exposedAddress)
          .port(s.get.exposedPorts(Instance.port))
          .addHeader(Header.ContentType(MediaType.application.json))
      )
    )

  class Trace(response: Value):
    import com.digitalasset.pqs.utils.safeequals.===
    type Attributes = Map[String, Value]

    private val values: Value = response
      .obj("results")
      .obj("A")
      .obj("frames")
      .arr(0)
      .obj("data")
      .obj("values")

    private def spanSection(spanIx: Int, sectionIx: Int) = values
      .arr(sectionIx)
      .arr(spanIx)
      .arr
      .map(_.obj)

    val spanNames: Seq[String] = values
      .arr(3)
      .arr
      .map(_.str)
      .toSeq

    def attributes(spanName: String): Attributes =
      spanNames.zipWithIndex
        .find(_._1 === spanName)
        .map { (name, ix) =>
          spanSection(ix, 16)
            .map(x => x("key").str -> x("value"))
            .toMap
        }
        .getOrElse(Map.empty)

    def links(spanName: String): Map[(String, String), Attributes] =
      spanNames.zipWithIndex
        .find(_._1 === spanName)
        .map { (name, ix) =>
          spanSection(ix, 15)
            .map(x =>
              val tags = x("tags").arr.map(_.obj).map(x => x("key").str -> x("value")).toMap
              x("traceID").str.reverse.padTo(32, '0').reverse -> x("spanID").str.reverse.padTo(16, '0').reverse -> tags
            )
            .toMap
        }
        .getOrElse(Map.empty)

    def events(spanName: String): Map[(String, Double), Attributes] =
      spanNames.zipWithIndex
        .find(_._1 === spanName)
        .map { (name, ix) =>
          spanSection(ix, 14)
            .map(x =>
              val fields = x("fields").arr.map(_.obj).map(x => x("key").str -> x("value")).toMap
              fields("message").str -> x("timestamp").num -> fields.removed("message")
            )
            .toMap
        }
        .getOrElse(Map.empty)
