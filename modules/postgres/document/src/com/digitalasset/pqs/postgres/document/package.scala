// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres

import zio.metrics.Metric
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.stream.{ZChannel, ZPipeline, ZStream}
import zio.{Chunk, Exit, Queue, ZIO}

import java.util.concurrent.atomic.AtomicLong

package object document {

  /** Pipeline that creates an async island with bounded queue of specified size between upstream and downstream
    * processing. This provides double back-pressure: upstream is blocked when the queue is full; downstream is blocked
    * when the queue is empty; otherwise the queue serves as a buffer. The queue size is tracked, with the `name_size`
    * metric, which is useful to identify processing bottlenecks (high average queue sizes indicate slow downstream
    * processing).
    */
  def waitPoint[A](name: String, capacity: Int = 16, chunkSize: Int = 16): ZPipeline[Any, Nothing, A, A] =
    ZPipeline.fromFunction[Any, Nothing, A, A](inStream =>
      ZStream.unwrapScoped(
        ZIO
          .acquireRelease( // acquire
            Queue.bounded[Exit[Option[Nothing], A]](capacity) <&> zio.Ref.make(0)
          )( // release
            _._1.shutdown
          )
          .flatMap { (queue, qSize) => // use
            val size = Metric.histogram(
              s"${name}_size",
              s"Number of in-flight units of work in $name wait point",
              Boundaries.linear(0, capacity.toDouble / 16, 16)
            )
            val counter           = Metric.counter(name, s"Number of units of work processed in $name wait point")
            def inc(as: Chunk[?]) = qSize.updateAndGet(_ + as.size).flatMap(size.update(_)) *> counter.modify(as.size)
            def dec(as: Chunk[?]) = qSize.updateAndGet(_ - as.size).flatMap(size.update(_))

            val outStream = ZStream.fromQueue(queue, chunkSize).flattenExitOption.mapChunksZIO(ch => dec(ch).as(ch))

            lazy val enqueue: ZChannel[Any, Nothing, Chunk[A], Any, Nothing, Nothing, Any] =
              ZChannel.readWithCause[Any, Nothing, Chunk[A], Any, Nothing, Nothing, Any](
                in => ZChannel.fromZIO(inc(in).as(in.map(Exit.succeed)).flatMap(queue.offerAll)) *> enqueue,
                err => ZChannel.fromZIO(queue.offer(Exit.failCause(err))) *> ZChannel.refailCause(err),
                done => ZChannel.fromZIO(queue.offer(Exit.fail(None))) *> ZChannel.succeedNow(done)
              )
            (inStream.channel >>> enqueue).runScoped.forkScoped.as(outStream)
          }
      )
    )

  final case class IdPlaceholder private (id: Long)
  object IdPlaceholder:
    trait Factory { def mk: IdPlaceholder }
    def factory(start: Long): Factory = new Factory:
      private val cnt       = AtomicLong(start)
      def mk: IdPlaceholder = IdPlaceholder(cnt.incrementAndGet())

}
