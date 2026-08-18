// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.functest

import com.digitalasset.pqs.configuration
import zio.{ZEnvironment, ZIO, ZIOAppArgs, ZLayer}

case class FTConfig(
    postgresVersion: String,
    damlSdkVersion: String,
    cantonVersion: String,
    cantonProtocolVersion: Int,
    damlLfTarget: String
)

object FTConfig {
  // Note: this approach accesses startup config (cmdline args, env vars) from the test runtime context.
  // It works when the test is started within zio.test.live context.
  // The default test context in ZIO clears the context and generates empty options.
  // See FTSpec for an example use.
  private val args: ZLayer[Any, Nothing, ZIOAppArgs] =
    ZLayer.fromZIO(ZIO.environment[Any].mapAttempt { case e: ZEnvironment[ZIOAppArgs] @unchecked => e.get[ZIOAppArgs] })
      <> ZIOAppArgs.empty

  val layer: ZLayer[Any, Nothing, FTConfig] = ZLayer.scopedEnvironment(
    (args >+> configuration.apply[FTConfig]).build.tap(ftconfig => ZIO.logInfo(s"FTCONFIG created: $ftconfig"))
  )
}
