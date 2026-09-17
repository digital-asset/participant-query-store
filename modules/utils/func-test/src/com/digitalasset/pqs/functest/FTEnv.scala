// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.functest

import com.digitalasset.zio.daml.FileCache
import os.Path
import zio.*

final class FTEnv(
    val config: FTConfig,
    val showCantonLogs: Boolean,
    val fileCache: FileCache,
    tempDirectory: os.Path,
    counter: Ref[Int]
):
  export config.*
  def createUniqueDirectory(prefix: String): Task[os.Path] =
    for
      uniqueId <- counter.getAndIncrement
      dir      <- FTEnv.makeDir(tempDirectory / s"$prefix-$uniqueId")
    yield dir

object FTEnv:
  val showCantonLogs        = ZIO.service[FTEnv].map(_.showCantonLogs)
  val cantonVersion         = ZIO.service[FTEnv].map(_.config.cantonVersion)
  val protocolVersion       = ZIO.service[FTEnv].map(_.config.cantonProtocolVersion)
  val cantonProtocolVersion = ZIO.service[FTEnv].map(_.config.cantonProtocolVersion)
  val damlSdkVersion        = ZIO.service[FTEnv].map(_.config.damlSdkVersion)
  val damlLfTarget          = ZIO.service[FTEnv].map(_.config.damlLfTarget)
  val fileCache             = ZIO.service[FTEnv].map(_.fileCache)

  def createUniqueDirectory(prefix: String) = ZIO.service[FTEnv].flatMap(_.createUniqueDirectory(prefix))

  val layer: ZLayer[Any, Throwable, FTEnv] = FTConfig.layer >>> ZLayer.scoped {
    for
      config         <- ZIO.service[FTConfig]
      showCantonLogs <- System.env("FT_CANTON_LOG").map(_.contains("true"))
      keepTemp       <- System.env("FT_KEEP_TEMP").map(_.contains("true"))
      tempDir        <- createTempDir(keepTemp)
      cacheDir       <- makeDir(tempDir / "cache")
      fileCache      <- FileCache(cacheDir)
      counter        <- Ref.make(0)
    yield FTEnv(config, showCantonLogs, fileCache, tempDir, counter)
  }

  private def createTempDir(keepTemp: Boolean): ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(os.temp.dir(prefix = "pqs-func-test", deleteOnExit = false))
    ) { tempDir =>
      if keepTemp then ZIO.log(s"Temp directory: $tempDir")
      else ZIO.attemptBlocking(os.remove.all(tempDir)).orDie
    }

  private def makeDir(dir: os.Path): ZIO[Any, Throwable, Path] =
    ZIO.attemptBlocking(os.makeDir(dir)).as(dir)
