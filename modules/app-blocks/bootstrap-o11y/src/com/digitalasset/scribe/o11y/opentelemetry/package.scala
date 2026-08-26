// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.o11y

import com.digitalasset.scribe.appversion.getVersion
import com.digitalasset.scribe.logging.{ConsoleFormat, ConsoleLogging}
import io.micrometer.core.instrument.Metrics
import zio.Console.printLine
import zio.logging.*
import zio.metrics.connectors.micrometer.{MicrometerConfig, micrometerLayer}
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.{ZIO, ZLayer}

package object opentelemetry:
  val bootstrap: ZLayer[ConsoleLogging.Config, Throwable, Unit] =
    logging ++ tracing ++ metrics ++ warnIfAgentAbsent

  private def logging = ZLayer
    .fromZIO(
      for
        config <- ZIO.service[ConsoleLogging.Config]
        logFormatO = config.toLogFormat
        _ <- printLine("Could not parse log format. Falling back to default.").when(logFormatO.isEmpty)
        logFormat       = logFormatO.getOrElse(LogFormat.default)
        logFilterConfig = config.toLogFilterConfig
        mkLogger = config.format match
          case ConsoleFormat.Plain => consoleLogger
          case ConsoleFormat.Json  => consoleJsonLogger
      yield zio.Runtime.removeDefaultLoggers
        >>> mkLogger(ConsoleLoggerConfig(logFormat, logFilterConfig))
        >>> zio.logging.slf4j.bridge.Slf4jBridge.initialize
        >>> LogBridge.initialize(config.toLogFilter)
    )
    .flatten

  private def tracing = ZLayer.fromZIO {
    getVersion.mapAttempt { (title, version, _) =>
      (OpenTelemetry.contextJVM ++ OpenTelemetry.global)
        >+> OpenTelemetry.tracing(title, Some(version))
        >>> TracingBridge.initialize
    }
  }.flatten

  private def metrics =
    micrometer ++ zio.Runtime.enableRuntimeMetrics

  private def micrometer =
    (ZLayer.succeed(Metrics.globalRegistry) ++ ZLayer.succeed(MicrometerConfig.default)) >>> micrometerLayer

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  private def warnIfAgentAbsent = ZLayer
    .fromZIO(
      ZIO
        .attempt {
          val file = Class
            .forName("io.opentelemetry.javaagent.bootstrap.JavaagentFileHolder")
            .getMethod("getJavaagentFile")
            .invoke(null)
          require(file != null)
        }
        .foldZIO(
          _ =>
            zio.Console.printLineError(
              s"""ATTN! OpenTelemetry Java Agent is not found.
                 |Please provide OpenTelemetry Java Agent using environment variable JAVA_TOOL_OPTIONS=-javaagent:/path/to/otel.jar to process observability signals.
                 |See also https://opentelemetry.io/docs/instrumentation/java/automatic/""".stripMargin
            ),
          _ =>
            zio.Console.printLineError(
              "OpenTelemetry Java Agent initialized."
            )
        )
    )
