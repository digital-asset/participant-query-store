// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import zio.Promise
import zio.logging.LogAnnotation

package object docker:
  sealed trait StdIO:
    def line: String
  final case class StdOut(line: String) extends StdIO
  final case class StdErr(line: String) extends StdIO

  val SuiteName      = LogAnnotation[Option[Int]]("suite", (_, b) => b, _.fold("")(x => s"#$x"))
  val ContainerImage = LogAnnotation[String]("image", (_, b) => b, "[" + _.split('/').last + "]")

  sealed trait Mode
  object Mode:
    case object Local extends Mode
    case object CI    extends Mode

  /** Docker daemon resource limits (CPUs & memory), which on Docker Desktop are typically lower than the host.
    * @param cpus
    *   Number of CPUs allocated to Docker
    * @param memoryBytes
    *   Total memory allocated to Docker
    * @param availableMemoryBytes
    *   Memory available after accounting for already-running containers
    */
  case class DockerResources(cpus: Int, memoryBytes: Long, availableMemoryBytes: Long)

  /** Identifies a single functional-test Docker session.
    *
    * All Docker resources (containers, network, volume) created by one `Docker.live` instance share the same `prefix`,
    * making them easy to correlate in `docker ps`, `docker network ls`, and `docker volume ls`.
    *
    * @param prefix
    *   Resource name prefix, e.g. `"scribe-ft-3a7f2b1c0d9e8"`. Used for naming and safety-net cleanup.
    * @param dockerNetworkId
    *   Docker-assigned opaque network hash (64-char hex). Required by connect/disconnect API calls. This is NOT the
    *   user-supplied network name (`scribe-ft-network-<seed>`) -- Docker returns its own internal ID when the network
    *   is created.
    * @param volumeName
    *   The Docker volume name, e.g. `"scribe-ft-volume-3a7f2b1c0d9e8"`. Equals the user-supplied name because
    *   `createVolumeCmd.getName` returns what we passed in.
    */
  private case class DockerSession(
      prefix: String,
      dockerNetworkId: String,
      volumeName: String
  )

  /** Thrown when a container exits before the expected output predicate is satisfied in [[Service.blockUntilOutput]].
    * Used as a retry signal in the functional test framework.
    */
  final case class ContainerExitedException(message: String) extends RuntimeException(message)

  /** Wrapper for the global OOM signal promise. Failed when any managed container is OOM-killed. Tests race against
    * this to abort instantly.
    */
  private case class OomSignal(promise: Promise[Throwable, Nothing])
end docker
