// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.app

import com.digitalasset.scribe.app.CliTree
import zio.Console.{printLine, printLineError}
import zio.ZIO.logErrorCause
import zio.logging.LogAnnotation
import zio.{
  Duration,
  ExitCode,
  FiberRefs,
  Runtime,
  RuntimeFlag,
  RuntimeFlags,
  Schedule,
  Task,
  Unsafe,
  ZEnvironment,
  ZIO,
  ZLayer
}

trait ComposableApp { self =>
  def executableName: Option[String] = None
  def app: CliTree[Task[Unit]]

  final def main(args: Array[String]): Unit =
    val runtime = Runtime(
      ZEnvironment.empty,
      FiberRefs.empty,
      RuntimeFlags.disable(RuntimeFlags.default)(RuntimeFlag.FiberRoots)
    )
    Unsafe.unsafe { implicit unsafe =>
      java.lang.System.exit(
        runtime.unsafe
          .run(
            run(args).foldCause(
              _ => ExitCode.failure,
              _ => ExitCode.success
            )
          )
          .getOrThrow()
          .code
      )
    }

  final def run(args: Array[String]): Task[Any] =
    val Application = LogAnnotation[String]("application", (_, a) => a, identity)
    val theArgs     = executableName.toList ++ args
    app
      .parse(theArgs)
      .fold(
        identity,
        err =>
          (printLineError(err) *> printLine(app.nearestHelp(theArgs))).ignore *>
            ZIO.fail(RuntimeException(err)),
        help => printLine(help).as(ExitCode.success)
      ) @@ Application(executableName.getOrElse(""))
  end run

  extension [R](effect: ZIO[R, Throwable, Unit])
    def bootstrap(
        b: ZLayer[Any, Throwable, R],
        retrySchedule: ZLayer[R, Throwable, Schedule[Any, Throwable, Any]] = ZLayer.succeed(failfast)
    ): Task[Unit] =
      printAndLogError(ZIO.scoped(retrySchedule.build.flatMap(s => effect.retry(s.get))).provide(b))

  private def printAndLogError[R, E <: Throwable, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    effect.tapErrorCause { failure =>
      def causes(t: Throwable): List[Throwable] = t :: Option(t.getCause).toList.flatMap(causes)
      val messages = causes(failure.squash).map(e => s"${e.getClass.getCanonicalName}: ${e.getMessage}").mkString("\n")
      logErrorCause(failure).ignore *> printLineError(messages).ignore
    }

  def failfast: Schedule[Any, Any, Any] =
    Schedule.stop
  def retryForever(delay: Duration): Schedule[Any, Any, Any] =
    Schedule.spaced(delay)
}
