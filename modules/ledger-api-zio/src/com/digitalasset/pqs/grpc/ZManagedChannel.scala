// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.grpc

import io.grpc.*
import zio.shim.QueueDrainer
import zio.stream.{Stream, Take, ZChannel, ZStream}
import zio.{Cause, Chunk, ChunkBuilder, Exit, IO, Promise, UIO, ZIO, ZLayer}

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer

trait ZManagedChannel:
  def unaryCall[Req, Res](
      method: io.grpc.MethodDescriptor[Req, Res],
      request: Req
  ): IO[StatusException, Res]

  def clientStreamingCall[Req, Res](
      method: io.grpc.MethodDescriptor[Req, Res],
      request: Stream[StatusException, Req]
  ): IO[StatusException, Res]

  def serverStreamingCall[Req, Res](
      method: io.grpc.MethodDescriptor[Req, Res],
      request: Req
  ): Stream[StatusException, Res]

  def bidiCall[Req, Res](
      method: io.grpc.MethodDescriptor[Req, Res],
      request: Stream[StatusException, Req]
  ): Stream[StatusException, Res]
end ZManagedChannel

object ZManagedChannel:
  def apply(
      channelBuilder: => ManagedChannelBuilder[?],
      bufferSize: Int = 128,
      interceptors: ZClientInterceptor*
  ): ZLayer[Any, Throwable, ZManagedChannel] = mkCallStarter(channelBuilder, interceptors).project { mkCall =>
    new ZManagedChannel:
      def unaryCall[Req, Res](method: MethodDescriptor[Req, Res], request: Req): IO[StatusException, Res] =
        mkCall(method).flatMap { (call, metadata) =>
          ZIO.asyncInterrupt { callback =>
            val listener = new ClientCall.Listener[Res]:
              override def onMessage(message: Res): Unit =
                callback(ZIO.succeed(message))
              override def onClose(status: Status, trailers: Metadata): Unit = if !status.isOk then
                callback(
                  ZIO
                    .fail(status.asException(trailers))
                    .logError(s"Exception in grpc method ${method.getFullMethodName}")
                )
            call.start(listener, metadata)
            call.sendMessage(request)
            call.request(1)
            call.halfClose()
            Left(ZIO.attempt(call.cancel("Interrupted", null)).logError.ignore)
          }
        }

      def serverStreamingCall[Req, Res](
          method: MethodDescriptor[Req, Res],
          request: Req
      ): Stream[StatusException, Res] =
        val buffer    = bufferSize
        val chunkSize = 1024 min bufferSize
        type TK = Take[StatusException, Res]
        ZStream.unwrap(
          mkCall(method).map { (call, metadata) =>
            val queue = QueueDrainer[StatusException, Res](buffer, chunkSize, call.request)

            val listener = new ClientCall.Listener[Res] {
              override def onMessage(message: Res): Unit =
                queue.offer(message)
              override def onClose(status: Status, trailers: Metadata): Unit =
                queue.end(if status.isOk then Take.end else Take.fail(status.asException(trailers)))
            }

            call.start(listener, metadata)
            call.sendMessage(request)
            queue.start()
            call.halfClose()

            lazy val loop: ZChannel[Any, Any, Any, Any, StatusException, Chunk[Res], Unit] = ZChannel.suspend(
              queue.drain match
                case (chunk, None) if chunk.nonEmpty =>
                  ZChannel.write(chunk) *> loop
                case (chunk, Some(end)) =>
                  ZChannel.write(chunk) *> end.fold(
                    ZChannel.unit,
                    error =>
                      ZChannel.fromZIO(
                        ZIO.failCause(error).logError(s"Exception in grpc method ${method.getFullMethodName}")
                      ),
                    _ => ZChannel.unit
                  )
                case _ =>
                  ZChannel.fromZIO(queue.await) *> loop
            )

            def cancel(exit: Exit[Any, Any]) = for {
              _ <- zio.ZIO.logDebug(
                s"Cancelling at ${method.getFullMethodName} with " +
                  s"(success: ${exit.isSuccess}, interrupted: ${exit.isInterrupted})"
              )
              _ <- ZIO.attempt(call.cancel("Interrupted", null)).logError.ignore
            } yield ()

            ZStream.fromChannel(loop.ensuringWith(cancel))
          }
        )

      def clientStreamingCall[Req, Res](
          method: MethodDescriptor[Req, Res],
          request: Stream[StatusException, Req]
      ): IO[StatusException, Res] = ??? // unimplemented

      def bidiCall[Req, Res](
          method: MethodDescriptor[Req, Res],
          request: Stream[StatusException, Req]
      ): Stream[StatusException, Res] = ??? // unimplemented
  }

  private def mkCallStarter(builder: ManagedChannelBuilder[?], interceptors: Seq[ZClientInterceptor]) =
    ZLayer
      .scoped(ZIO.acquireRelease(ZIO.attempt(builder.build()))(ch => ZIO.attempt(ch.shutdown()).logError.ignore))
      .project(CallStarter(_, interceptors))

  private class CallStarter(channel: io.grpc.Channel, interceptors: Seq[ZClientInterceptor]):
    def apply[Req, Res](
        method: MethodDescriptor[Req, Res]
    ): ZIO[Any, StatusException, (ClientCall[Req, Res], Metadata)] = for {
      metadata <- SafeMetadata.make
      _        <- ZIO.foreachDiscard(interceptors)(_.intercept(metadata))
      call     <- ZIO.attempt(channel.newCall(method, CallOptions.DEFAULT)).orElseFail(Status.INTERNAL.asException())
    } yield (call, metadata.metadata)
end ZManagedChannel
