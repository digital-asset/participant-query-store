// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{Span, SpanContext, SpanKind}
import zio.shims.tracesApi
import zio.{Trace, UIO, ZIO, ZIOAspect}

import scala.jdk.CollectionConverters.*

package object traces:
  def makeDetachedSpan(name: String): UIO[DetachedSpan] = tracesApi.getWith(_.makeDetachedSpan(name))
  def currentSpan(): UIO[Span]                          = tracesApi.getWith(_.currentSpan())
  def root(name: String, kind: SpanKind = SpanKind.INTERNAL): OAspect =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any]:
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
        tracesApi.getWith(_.root(name, kind)(zio))
  def span(name: String, kind: SpanKind = SpanKind.INTERNAL): OAspect =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any]:
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
        tracesApi.getWith(_.span(name, kind)(zio))
  def attributes(attrs: Attribute[?]*): OAspect =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any]:
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
        tracesApi.getWith(_.attributes(attrs*)(zio))
  def event(name: String, attrs: Attribute[?]*): OAspect =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any]:
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
        tracesApi.getWith(_.event(name, attrs*)(zio))
  def link(spanCtx: SpanContext, attrs: Attribute[?]*): OAspect =
    new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any]:
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
        tracesApi.getWith(_.link(spanCtx, attrs*)(zio))

  def provideImplementation(api: Api): UIO[Unit] = zio.shims.tracesApi.set(api)

  final case class Attribute[T](key: AttributeKey[T], value: T)
  given Conversion[(String, String), Attribute[String]] =
    (k, v) => Attribute(AttributeKey.stringKey(k), v)
  given Conversion[(String, Long), Attribute[java.lang.Long]] =
    (k, v) => Attribute(AttributeKey.longKey(k), Long.box(v))
  given Conversion[(String, Double), Attribute[java.lang.Double]] =
    (k, v) => Attribute(AttributeKey.doubleKey(k), Double.box(v))
  given Conversion[(String, Boolean), Attribute[java.lang.Boolean]] =
    (k, v) => Attribute(AttributeKey.booleanKey(k), Boolean.box(v))
  given stringsToAttribute: Conversion[(String, IterableOnce[String]), Attribute[java.util.List[String]]] =
    (k, v) => Attribute(AttributeKey.stringArrayKey(k), v.iterator.toSeq.asJava)
  given longsToAttribute: Conversion[(String, IterableOnce[Long]), Attribute[java.util.List[java.lang.Long]]] =
    (k, v) => Attribute(AttributeKey.longArrayKey(k), v.iterator.map(Long.box).toSeq.asJava)
  given doublesToAttribute: Conversion[(String, IterableOnce[Double]), Attribute[java.util.List[java.lang.Double]]] =
    (k, v) => Attribute(AttributeKey.doubleArrayKey(k), v.iterator.map(Double.box).toSeq.asJava)
  given booleansToAttribute: Conversion[(String, IterableOnce[Boolean]), Attribute[java.util.List[java.lang.Boolean]]] =
    (k, v) => Attribute(AttributeKey.booleanArrayKey(k), v.iterator.map(Boolean.box).toSeq.asJava)

  trait DetachedSpan:
    def end(): UIO[Unit]
    def addAttributes(attrs: Attribute[?]*): UIO[Unit]
    def addEvent(name: String, attrs: Attribute[?]*): UIO[Unit]
    def addLink(spanCtx: SpanContext, attrs: Attribute[?]*): UIO[Unit]
    def linkFromCurrentSpan(attrs: Attribute[?]*): UIO[Unit]
    def linkToCurrentSpan(attrs: Attribute[?]*): UIO[Unit]
    def locally[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A]

  object DetachedSpan:
    private[o11y] val noop = new DetachedSpan:
      def end(): UIO[Unit]                                               = ZIO.unit
      def addAttributes(attrs: Attribute[?]*): UIO[Unit]                 = ZIO.unit
      def addEvent(name: String, attrs: Attribute[?]*): UIO[Unit]        = ZIO.unit
      def addLink(spanCtx: SpanContext, attrs: Attribute[?]*): UIO[Unit] = ZIO.unit
      def linkFromCurrentSpan(attrs: Attribute[?]*): UIO[Unit]           = ZIO.unit
      def linkToCurrentSpan(attrs: Attribute[?]*): UIO[Unit]             = ZIO.unit
      def locally[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A]              = zio

  trait Api:
    def makeDetachedSpan(name: String): UIO[DetachedSpan]
    def currentSpan(): UIO[Span]
    def root[R, E, A](name: String, spanKind: SpanKind)(zio: ZIO[R, E, A]): ZIO[R, E, A]
    def span[R, E, A](name: String, spanKind: SpanKind)(zio: ZIO[R, E, A]): ZIO[R, E, A]
    def attributes[R, E, A](value: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A]
    def event[R, E, A](name: String, attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A]
    def link[R, E, A](spanCtx: SpanContext, attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A]

  object Noop extends Api:
    override def makeDetachedSpan(name: String): UIO[DetachedSpan] = ZIO.succeed(DetachedSpan.noop)
    override def currentSpan(): UIO[Span]                          = ZIO.succeed(Span.getInvalid)
    override def root[R, E, A](name: String, spanKind: SpanKind)(zio: ZIO[R, E, A]): ZIO[R, E, A]           = zio
    override def span[R, E, A](name: String, spanKind: SpanKind)(zio: ZIO[R, E, A]): ZIO[R, E, A]           = zio
    override def attributes[R, E, A](value: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A]                 = zio
    override def event[R, E, A](name: String, attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A]        = zio
    override def link[R, E, A](spanCtx: SpanContext, attrs: Attribute[?]*)(zio: ZIO[R, E, A]): ZIO[R, E, A] = zio
