// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.health

import com.digitalasset.scribe.pipeline.pipeline.Pipeline.streamUpGauge
import com.digitalasset.zio.daml.Channel.grpcUpGauge
import zio.jdbc.shims.postgres.jdbcUpGauge
import zio.http.*
import zio.json.EncoderOps
import zio.json.ast.Json
import zio.{ZIO, RLayer, ZLayer}

import java.net.InetSocketAddress

object Health:
  val live: RLayer[Config, Unit] =
    ZLayer.scoped(
      for
        conf   <- ZIO.service[Config]
        server <- Server.defaultWith(_.copy(address = new InetSocketAddress(conf.address, conf.port))).build
        _      <- server.get.install(endpoints).resurrect.tapError(_ => ZIO.logError("Failed to start health server"))
      yield ()
    )

  val endpoints: Routes[Any, Nothing] = Routes(
    Method.GET / "livez" -> handler(Response.json("""{"status": "ok"}""")),
    Method.GET / "readyz" -> handler {
      for
        grpcUp   <- grpcUpGauge.value.map(_.value >= 1.0)
        jdbcUp   <- jdbcUpGauge.value.map(_.value >= 1.0)
        streamUp <- streamUpGauge.value.map(_.value >= 1.0)
      yield
        val ok = grpcUp && jdbcUp
        val body = Json
          .Obj(
            "status"                  -> Json.Str(if ok then "ok" else "unavailable"),
            "grpc_up"                 -> Json.Bool(grpcUp),
            "jdbc_connection_pool_up" -> Json.Bool(jdbcUp),
            "stream_up"               -> Json.Bool(streamUp)
          )
          .toJson
        if ok then Response.json(body) else Response.json(body).status(Status.ServiceUnavailable)
    }
  )
