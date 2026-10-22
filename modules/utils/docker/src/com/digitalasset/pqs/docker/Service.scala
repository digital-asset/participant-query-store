// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.docker

import os.Shellable
import zio.stream.ZStream
import zio.*

final case class ExitState(code: ExitCode, duration: Duration)

final case class Service[T](
    container: Container,
    exposedAddress: String,
    exposedPorts: Map[Int, Int],
    io: ZStream[Any, Throwable, StdIO],
    exit: Task[ExitState],
    getFileContents: os.Path => Task[Array[Byte]],
    execute: Seq[Shellable] => Task[ExecResult]
) {

  val exitCode = exit.map(_.code)
  val duration = exit.map(_.duration)

  /** Runs a command inside this (already running) container, the equivalent of `docker exec`, and collects its output
    * and exit code.
    */
  def exec(cmd: Shellable*): Task[ExecResult] = execute(cmd)

  def blockUntilStdOut(predicate: String => Boolean)(implicit trace: Trace): Task[Unit] =
    blockUntilOutput { case StdIO.Out(line) if predicate(line) => () }
  def blockUntilStdErr(predicate: String => Boolean)(implicit trace: Trace): Task[Unit] =
    blockUntilOutput { case StdIO.Err(line) if predicate(line) => () }
  def blockUntilOutput[A](p: PartialFunction[StdIO, A])(implicit trace: Trace): Task[A] =
    ContainerImage(container.image)(
      ZIO.logDebug("Blocking for output...")
        *> io
          .collect(p)
          .runHead
          .someOrElseZIO(
            for {
              code   <- exitCode.map(_.code)
              stdout <- io.takeRight(20).map(_.line).runCollect
              errorMessage =
                s"Container ${container.hostName} (${container.image}) exited (exit code $code) before expected output was observed"
              _ <- ZIO.logError(
                s"""|$errorMessage
                    |Last output:
                    |${stdout.mkString("\n")}""".stripMargin
              )
              err <- ZIO.fail(ContainerExitedException(errorMessage))
            } yield err
          )
        <* ZIO.logDebug(
          s"Predicate satisfied, unblocking. Exposed address: $exposedAddress. Exposed ports: $exposedPorts"
        )
    )
}
