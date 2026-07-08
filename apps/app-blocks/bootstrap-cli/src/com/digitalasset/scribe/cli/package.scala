// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import com.digitalasset.scribe.logging.{FileFormat, FileLogging}
import zio.Console.printLine
import zio.logging.*
import zio.{ZIO, ZLayer}

package object cli:
  val bootstrap: ZLayer[FileLogging.Config, Throwable, Unit] = ZLayer
    .fromZIO(
      for
        config <- ZIO.service[FileLogging.Config]
        logFormatO = config.toLogFormat
        _ <- printLine("Could not parse log format. Falling back to default.").when(logFormatO.isEmpty)
        logFormat       = logFormatO.getOrElse(LogFormat.default)
        logFilterConfig = config.toLogFilterConfig
        mkLogger = config.format match
          case FileFormat.Plain      => fileLogger
          case FileFormat.PlainAsync => fileAsyncLogger
          case FileFormat.Json       => fileJsonLogger
          case FileFormat.JsonAsync  => fileAsyncJsonLogger
      yield zio.Runtime.removeDefaultLoggers
        >>> mkLogger(
          FileLoggerConfig(
            destination = config.destination.toPath,
            format = logFormat,
            filter = logFilterConfig
          )
        )
        >>> zio.logging.slf4j.bridge.Slf4jBridge.initialize
    )
    .flatten
