// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y.opentelemetry

import com.digitalasset.pqs.o11y.logs
import com.digitalasset.pqs.o11y.traces.{Attribute, DetachedSpan}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.{Span, SpanContext, SpanKind}
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.{Clock, UIO, ZIO, ZLayer}

import java.util.concurrent.TimeUnit

object TracingBridge:
  private val TraceIdLogKey = "trace_id"
  def initialize: ZLayer[Tracing & ContextStorage, Nothing, Unit] = ZLayer.scoped(
    ZIO.serviceWithZIO[Tracing](tracing =>
      ZIO.serviceWithZIO[ContextStorage](ctxStorage =>
        com.digitalasset.pqs.o11y.traces.provideImplementation(
          new com.digitalasset.pqs.o11y.traces.Api {
            def makeDetachedSpan(name: String): UIO[DetachedSpan] =
              for
                currentCtx <- ctxStorage.get
                result     <- tracing.spanUnsafe(name)
                spanCtx    <- ctxStorage.get
                (span, _) = result
                _ <- ctxStorage.set(currentCtx)
              yield new DetachedSpan:
                def end(): UIO[Unit] =
                  Clock
                    .currentTime(TimeUnit.NANOSECONDS)
                    .flatMap(nanos => ZIO.succeed(span.end(nanos, TimeUnit.NANOSECONDS)))
                def addAttributes(attrs: Attribute[?]*): UIO[Unit] =
                  ZIO.succeed(span.setAllAttributes(convertAttributes(attrs)))
                def addEvent(name: String, attrs: Attribute[?]*): UIO[Unit] =
                  ZIO.succeed(span.addEvent(name, convertAttributes(attrs)))
                def addLink(spanCtx: SpanContext, attrs: Attribute[?]*): UIO[Unit] =
                  ZIO.succeed(span.addLink(spanCtx, convertAttributes(attrs)))
                def linkFromCurrentSpan(attrs: Attribute[?]*): UIO[Unit] =
                  tracing.getCurrentSpanUnsafe.map(cs => cs.addLink(span.getSpanContext, convertAttributes(attrs))).unit
                def linkToCurrentSpan(attrs: Attribute[?]*): UIO[Unit] =
                  tracing.getCurrentSpanUnsafe.map(cs => span.addLink(cs.getSpanContext, convertAttributes(attrs))).unit
                def locally[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A] =
                  for
                    currentCtx <- ctxStorage.get
                    _          <- ctxStorage.set(spanCtx)
                    result <- (zio @@ logs.tag(TraceIdLogKey -> span.getSpanContext.getTraceId))
                      .ensuring(ctxStorage.set(currentCtx))
                  yield result

            def currentSpan(): UIO[Span] = tracing.getCurrentSpanUnsafe

            def root[R, E, A](name: String, spanKind: SpanKind)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
              tracing.root(name, spanKind) {
                currentSpan().flatMap { span =>
                  zio @@ logs.tag(TraceIdLogKey -> span.getSpanContext.getTraceId)
                }
              }

            def span[R, E, A](name: String, spanKind: SpanKind)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
              tracing.span(name, spanKind) {
                currentSpan().flatMap { span =>
                  zio @@ logs.tag(TraceIdLogKey -> span.getSpanContext.getTraceId)
                }
              }

            def attributes[R, E, A](attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
              zio <* tracing.getCurrentSpanUnsafe.map(_.setAllAttributes(convertAttributes(attrs)))

            def event[R, E, A](name: String, attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
              zio <* tracing.getCurrentSpanUnsafe.map(_.addEvent(name, convertAttributes(attrs)))

            def link[R, E, A](spanCtx: SpanContext, attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
              zio <* tracing.getCurrentSpanUnsafe.map(_.addLink(spanCtx, convertAttributes(attrs)))

            private def convertAttributes(attrs: Seq[Attribute[?]]): Attributes =
              val builder = Attributes.builder()
              attrs.foreach(a => builder.put(a.key, a.value))
              builder.build()
          }
        )
      )
    )
  )
