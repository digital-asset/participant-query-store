// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import zio.ZIO.{attempt, attemptBlocking, logDebug, logDebugCause, logWarningCause}
import zio.{Ref, Semaphore, ZIO, ZLayer}

object FileCache:
  val live: ZLayer[Config, Throwable, FileCache] = ZLayer.fromZIO(for
    config     <- ZIO.service[Config]
    cacheDir   <- attempt { os.Path(config.cacheDir) }
    semaphores <- Ref.Synchronized.make(Map.empty[String, Semaphore])
  yield FileCache(cacheDir, semaphores))

class FileCache(
    cacheDir: os.Path,
    semaphores: Ref.Synchronized[Map[String, Semaphore]]
):
  def cache[R, A](
      keyString: String
  )(
      unsafeDeserialize: Array[Byte] => A,
      unsafeSerialize: A => Array[Byte]
  )(
      compute: ZIO[R, Throwable, A]
  ): ZIO[R, Throwable, A] =
    val targetPath = cacheDir / keyString

    val read = for
      bytes <- attemptBlocking {
        os.read.bytes(targetPath)
      }.tapErrorCause(
        logDebugCause(s"Reading of cached data failed at $targetPath. Falling back to computing the value", _)
      )
      a <- attemptBlocking {
        unsafeDeserialize(bytes)
      }.tapErrorCause(
        logWarningCause(s"Deserialization of cached data failed at $targetPath. Falling back to computing the value", _)
      )
      _ <- logDebug(s"Successfully restored from cache at $targetPath")
    yield a

    def write(a: A) = for
      bytes <- attemptBlocking {
        unsafeSerialize(a)
      }.tapErrorCause(
        logWarningCause(s"Serialization of cached data failed at $targetPath", _)
      )
      _ <- attemptBlocking {
        os.write.over(targetPath, bytes, createFolders = true)
      }.tapErrorCause(
        logWarningCause(s"Writing of cached data failed at $targetPath", _)
      )
    yield {}

    for
      mutex  <- getSemaphore(keyString)
      result <- mutex.withPermit(read orElse compute.tap(write(_).ignore))
    yield result
  end cache

  private def getSemaphore(key: String) = semaphores.modifyZIO(map =>
    map
      .get(key)
      .fold( // make new semaphore and memoize
        Semaphore.make(1).map(semaphore => (semaphore, map.updated(key, semaphore)))
      )(semaphore => // use existing semaphore
        ZIO.succeed(semaphore, map)
      )
  )
