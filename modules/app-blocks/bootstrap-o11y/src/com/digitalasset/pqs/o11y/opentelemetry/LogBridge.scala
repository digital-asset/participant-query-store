// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y.opentelemetry

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.logs.Severity
import zio.logging.*
import zio.logging.LoggerNameExtractor.loggerNameAnnotationOrTrace
import zio.{Cause, FiberId, FiberRefs, LogLevel, LogSpan, Trace, ZLogger}

import scala.jdk.CollectionConverters.*

object LogBridge:
  def initialize(levelFilter: LogFilter[String]) = zio.Runtime.addLogger(new ZLogger[String, Any] {
    private val logApi = GlobalOpenTelemetry.get().getLogsBridge

    def apply(
        trace: Trace,
        fiberId: FiberId,
        logLevel: LogLevel,
        message: () => String,
        cause: Cause[Any],
        context: FiberRefs,
        spans: List[LogSpan],
        annotations: Map[String, String]
    ): Any = if levelFilter(trace, fiberId, logLevel, message, cause, context, spans, annotations) then {
      val logger  = logApi.get(loggerNameAnnotationOrTrace(trace, context, annotations).getOrElse("otel_logger"))
      val builder = logger.logRecordBuilder()

      builder.setBody(message())
      builder.setSeverity(logLevel.ordinal match
        case Int.MinValue                             => Severity.UNDEFINED_SEVERITY_NUMBER
        case Int.MaxValue                             => Severity.UNDEFINED_SEVERITY_NUMBER
        case trace if trace < 10000                   => Severity.TRACE
        case debug if debug >= 10000 && debug < 20000 => Severity.DEBUG
        case info if info >= 20000 && info < 30000    => Severity.INFO
        case warn if warn >= 30000 && warn < 40000    => Severity.WARN
        case error if error >= 40000 && error < 50000 => Severity.ERROR
        case fatal if fatal >= 50000                  => Severity.FATAL
      )
      builder.setTimestamp(java.time.Instant.now())
      trace match {
        case Trace(location, _, line) => builder.setAttribute(AttributeKey.stringKey("location"), s"$location:$line")
        case _                        => // do nothing
      }
      builder.setAttribute(AttributeKey.longArrayKey("fiberId"), fiberId.ids.map(x => Long.box(x.toLong)).toSeq.asJava)
      builder.setAttribute(AttributeKey.stringKey("threadName"), fiberId.threadName)
      annotations.foreach((k, v) => builder.setAttribute(AttributeKey.stringKey(k), v))
      context.get(zio.logging.logContext).foreach { lc =>
        lc.asMap.foreach((k, v) => builder.setAttribute(AttributeKey.stringKey(k), v))
      }
      builder.emit()
    }
  })
