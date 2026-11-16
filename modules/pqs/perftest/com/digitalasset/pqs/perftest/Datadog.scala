// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import zio.*
import zio.http.*
import zio.json.ast.Json

import java.util.concurrent.TimeUnit

final case class Gauge(metric: String, value: Double, tags: Seq[String] = Seq.empty)

/** Submits gauges to the Datadog v2 metrics API. A no-op unless `DATADOG_API_KEY` is set. */
object Datadog:
  private val commonTags: Seq[String] =
    "source:pqs-perftest" +: Seq(
      "postgres_version" -> "PQS_POSTGRESVERSION",
      "canton_version"   -> "PQS_CANTONVERSION",
      "git_sha"          -> "GITHUB_SHA",
      "branch"           -> "GITHUB_REF_NAME"
    ).flatMap((tag, env) => sys.env.get(env).map(value => s"$tag:$value"))

  def submit(gauges: Seq[Gauge]): Task[Unit] =
    sys.env.get("DATADOG_API_KEY") match
      case None         => ZIO.logInfo(s"DATADOG_API_KEY not set, skipping submission of ${gauges.size} metrics")
      case Some(apiKey) => Clock.currentTime(TimeUnit.SECONDS).flatMap(now => post(apiKey, payload(gauges, now)))

  private def payload(gauges: Seq[Gauge], timestamp: Long): Json =
    Json.Obj(
      "series" -> Json.Arr(
        gauges.map { gauge =>
          Json.Obj(
            "metric" -> Json.Str(gauge.metric),
            "type"   -> Json.Num(3), // gauge
            "points" -> Json.Arr(Json.Obj("timestamp" -> Json.Num(timestamp), "value" -> Json.Num(gauge.value))),
            "tags"   -> Json.Arr((commonTags ++ gauge.tags).map(Json.Str(_))*)
          )
        }*
      )
    )

  private def post(apiKey: String, body: Json): Task[Unit] =
    for
      url <- ZIO.fromEither(URL.decode(s"https://api.datadoghq.com/api/v2/series"))
      request = Request
        .post(url, Body.fromString(body.toString))
        .addHeader(Header.ContentType(MediaType.application.json))
        .addHeader("DD-API-KEY", apiKey)
      response <- ZIO
        .scoped(Client.streaming(request).flatMap(r => r.body.asString.map(r.status -> _)))
        .provide(Client.default)
      _ <- response match
        case (status, _) if status.isSuccess => ZIO.logInfo(s"Submitted metrics to Datadog")
        case (status, text) => ZIO.fail(new RuntimeException(s"Datadog rejected metrics: $status $text"))
    yield ()
