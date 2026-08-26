// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package zio.shim

import zio.internal.RingBuffer
import zio.stream.Take
import zio.{Chunk, Exit, Promise, UIO, Unsafe, ZIO}

import java.util.concurrent.atomic.AtomicReference

/** QueueDrainer is a bridge between GRPC callbacks happening in GRPC thread and ZIO stream. It is backed by efficient
  * block-free RingBuffer. RingBuffer will grow up to the `buffer` size. On each `drain` operation QueueDrainer will
  * maintain the requested message count to be in the range of `[buffer/2, buffer]`. `Drain` operation returns chunks of
  * accumulated messages up to `chunkSize` and optionally stream ending.
  */
class QueueDrainer[E, A](buffer: Int, chunkSize: Int, request: Int => Unit):
  private val values        = RingBuffer[A](buffer)
  private val ending        = AtomicReference(Option.empty[Take[E, Nothing]])
  private val notifyPromise = AtomicReference(Option.empty[Promise[Nothing, Unit]])

  @volatile private var drainedCount = 0

  def start(): Unit = request(buffer)

  def offer(v: A): Unit =
    values.offer(v)
    unsafeNotify()

  def end(v: Take[E, Nothing]): Unit =
    ending.set(Some(v))
    unsafeNotify()

  def drain: (Chunk[A], Option[Take[E, Nothing]]) =
    val taken = values.pollUpTo(chunkSize)
    drainedCount += taken.size
    if drainedCount >= buffer / 2 then
      request(drainedCount)
      drainedCount = 0
    (taken, ending.get().filter(_ => values.isEmpty()))

  def await: UIO[Unit] = for {
    _       <- ZIO.yieldNow
    promise <- Promise.make[Nothing, Unit]
    _ <- ZIO.succeed {
      // set promise ref and then check for emptiness in reverse order to `offer` and `end` methods - no need to
      // synchronize this block
      notifyPromise.set(Some(promise))
      if values.isEmpty() && ending.get().isEmpty
      then // still empty, waiting
        promise.await
      else // new messages appeared in-flight, skipping waiting
        notifyPromise.set(None)
        ZIO.unit
    }.flatten
  } yield {}

  inline private def unsafeNotify(): Unit =
    notifyPromise.getAndSet(None).foreach { promise => Unsafe.unsafe(promise.unsafe.done(ZIO.unit)) }
