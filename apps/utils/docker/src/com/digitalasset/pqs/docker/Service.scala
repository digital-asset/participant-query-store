// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.docker

import zio.ZIO.{logDebug, logError}
import zio.stream.ZStream
import zio.{ExitCode, Task, Trace, ZIO}

final case class Service[T](
    container: Container,
    exposedAddress: String,
    exposedPorts: Map[Int, Int],
    io: ZStream[Any, Throwable, StdIO],
    exitCode: Task[ExitCode],
    getFileContents: os.Path => Task[Array[Byte]]
) {
  def blockUntilStdOut(predicate: String => Boolean)(implicit trace: Trace): Task[Unit] =
    blockUntilOutput { case StdOut(line) if predicate(line) => () }
  def blockUntilStdErr(predicate: String => Boolean)(implicit trace: Trace): Task[Unit] =
    blockUntilOutput { case StdErr(line) if predicate(line) => () }
  def blockUntilOutput[A](p: PartialFunction[StdIO, A])(implicit trace: Trace): Task[A] =
    ContainerImage(container.image)(
      logDebug("Blocking for output...")
        *> io
          .collect(p)
          .runHead
          .someOrElseZIO(
            for {
              code   <- exitCode.map(_.code)
              stdout <- io.takeRight(20).map(_.line).runCollect
              errorMessage =
                s"Container ${container.hostName} (${container.image}) exited (exit code $code) before expected output was observed"
              _ <- logError(
                s"""|$errorMessage
                    |Last output:
                    |${stdout.mkString("\n")}""".stripMargin
              )
              err <- ZIO.fail(ContainerExitedException(errorMessage))
            } yield err
          )
        <* logDebug(s"Predicate satisfied, unblocking. Exposed address: $exposedAddress. Exposed ports: $exposedPorts")
    )
}
