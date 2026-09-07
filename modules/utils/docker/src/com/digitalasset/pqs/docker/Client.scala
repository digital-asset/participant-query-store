// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.docker

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.{AsyncDockerCmd, SyncDockerCmd}
import com.github.dockerjava.api.exception.DockerException
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient
import zio.ZIO.*
import zio.stream.ZStream
import zio.{Scope, Trace, ZIO, ZLayer}

import java.io.Closeable

object Client:
  val live: ZLayer[Any, Throwable, Scope & DockerClient] = Scope.default >+> ZLayer.fromZIO(
    acquireRelease( // acquire
      attemptBlocking {
        val conf       = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
        val httpClient = ZerodepDockerHttpClient.Builder().dockerHost(conf.getDockerHost).build()
        DockerClientImpl.getInstance(conf, httpClient)
      }
    )(client => // release
      attemptBlocking { client.close() }.ignoreLogged
    ).tap(_.pingCmd().run) // health check
  )

  extension [T](cmd: SyncDockerCmd[T])
    def run(implicit trace: Trace): ZIO[Any, DockerException, T] =
      attemptBlocking(cmd.exec()).refineToOrDie[DockerException]

  extension [T <: AsyncDockerCmd[T, U], U](cmd: AsyncDockerCmd[T, U])
    /** Streams the command's results, applying `f` to each item inside the docker-java callback.
      *
      * Note: `f` must deep-copy anything docker-java may recycle, because an item is not necessarily safe to touch once
      * the callback has returned. In particular [[com.github.dockerjava.api.model.Frame]]: as of docker-java 3.4.0
      * `FramedInputStreamConsumer.accept` recycles a byte buffer passed into every forwarded Frame. If callers then
      * don't copy the frame's payload within onNext, the new onNext will override the previous frame's payload.
      */
    def streamMap[V](f: U => V)(implicit trace: Trace): ZStream[Any, Throwable, V] =
      ZStream.asyncInterrupt[Any, Throwable, V] { emit =>
        var interruptHandler = Option.empty[Closeable]
        cmd.exec(new ResultCallback[U] {
          def onStart(closeable: Closeable): Unit = interruptHandler = Some(closeable)
          def onNext(item: U): Unit               = emit single f(item)
          def onError(miserably: Throwable): Unit = emit fail miserably
          def onComplete(): Unit                  = emit.end
          def close(): Unit                       = {}
        })
        Left(attemptBlocking(interruptHandler.foreach(_.close())).ignore)
      }

end Client
