// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.grpc

import io.grpc.Metadata
import zio.{Semaphore, UIO, ZIO}

final class SafeMetadata private (private val sem: Semaphore, private[scribe] val metadata: Metadata):
  def get[T](key: Metadata.Key[T]): UIO[Option[T]] =
    wrap(m => Option(m.get(key)))

  def put[T](key: Metadata.Key[T], value: T): UIO[Unit] =
    wrap(_.put(key, value))

  def remove[T](key: Metadata.Key[T], value: T): UIO[Boolean] =
    wrap(_.remove(key, value))

  def wrap[A](f: Metadata => A): UIO[A] =
    wrapZIO(metadata => ZIO.succeed(f(metadata)))

  def wrapZIO[R, E, A](f: Metadata => ZIO[R, E, A]): ZIO[R, E, A] =
    sem.withPermit(f(metadata))

  def +=[T](keyValue: (Metadata.Key[T], T)): UIO[SafeMetadata] = updated(keyValue._1, keyValue._2)

  def updated[T](key: Metadata.Key[T], value: T): UIO[SafeMetadata] =
    put(key, value).as(this)

  def updatedZIO[R, E, T](key: Metadata.Key[T], value: ZIO[R, E, T]): ZIO[R, E, SafeMetadata] =
    value.flatMap(updated(key, _))
end SafeMetadata

object SafeMetadata:
  def make: UIO[SafeMetadata] =
    fromMetadata(new Metadata)

  def make(pairs: (String, String)*): UIO[SafeMetadata] =
    val md = new Metadata
    pairs.foreach { case (key, value) => md.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value) }
    SafeMetadata.fromMetadata(md)

  private def fromMetadata(metadata: => Metadata): UIO[SafeMetadata] =
    Semaphore.make(1).map(s => new SafeMetadata(s, metadata))
end SafeMetadata
