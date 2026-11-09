// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.docker

/** Hard resource limits applied to a single container (the equivalent of `docker run --cpus/--memory/--shm-size`).
  *
  * @param cpus
  *   Fractional number of CPUs the container may use. `None` means unlimited.
  * @param memoryBytes
  *   Hard memory limit in bytes (e.g. `--memory`). The container is OOM-killed if it exceeds this. `None` means
  *   unlimited.
  * @param shmBytes
  *   Size of the container's `/dev/shm` tmpfs in bytes (e.g. `--shm-size`). `None` leaves Docker's 64 MiB default.
  */
case class ContainerLimits(
    cpus: Option[Double] = None,
    memoryBytes: Option[Long] = None,
    shmBytes: Option[Long] = None
)
object ContainerLimits:
  val unlimited: ContainerLimits = ContainerLimits()

  def apply(cpus: Double, memoryGiB: Double): ContainerLimits =
    new ContainerLimits(
      cpus = Some(cpus),
      memoryBytes = Some((memoryGiB * 1024 * 1024 * 1024).toLong)
    )

  def apply(cpus: Double, memoryGiB: Double, shmGiB: Double): ContainerLimits =
    new ContainerLimits(
      Some(cpus),
      Some((memoryGiB * 1024 * 1024 * 1024).toLong),
      Some((shmGiB * 1024 * 1024 * 1024).toLong)
    )
