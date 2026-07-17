// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.health

import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.services.pqs.Pipeline
import com.digitalasset.pqs.utils.safeequals.===
import zio.ZIO
import zio.http.{Request, ZClient}
import zio.json.ast.Json

object HealthEndpoint:
  private def callHealthEndpoint(path: String) =
    Docker
      .inspect[Pipeline]
      .flatMap(pipeline =>
        ZIO
          .scoped(
            ZClient
              .streaming(
                Request.get(
                  s"http://${pipeline.exposedAddress}:${pipeline.exposedPorts(Pipeline.healthPort)}$path"
                )
              )
          )
          .provide(ZClient.default)
      )

  def responseStatus(path: String) = callHealthEndpoint(path).map(_.status).option

  def responseBody(path: String) =
    callHealthEndpoint(path)
      .flatMap(_.body.asString)
      .map(Json.decoder.decodeJson(_).toOption)
      .option
      .map(_.flatten)

  def readyzStatusAndField(field: String) =
    callHealthEndpoint("/readyz").flatMap { response =>
      response.body.asString.map { body =>
        val fieldValue = Json.decoder.decodeJson(body).toOption.flatMap {
          case Json.Obj(fields) => fields.find(_._1 === field).map(_._2)
          case _                => None
        }
        (response.status, fieldValue)
      }
    }.option
