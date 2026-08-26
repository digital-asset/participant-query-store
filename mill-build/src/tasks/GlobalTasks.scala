// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package tasks

import mill._
import mill.api.Result
import mill.define.{Command, NamedTask, SelectMode}
import mill.eval.Evaluator
import mill.main.Tasks
import mill.resolve.Resolve
import mill.scalalib._
import mill.scalalib.scalafmt._

trait GlobalTasks extends Module {
  def reformat(ev: Evaluator): Command[Unit] = T.command {
    evaluate(ev)(ScalafmtModule.reformatAll, "__.sources")
  }

  def checkScalafmt(ev: Evaluator): Command[Unit] = T.command {
    evaluate(ev)(ScalafmtModule.checkFormatAll, "__.sources")
  }

  def showUpdates(ev: Evaluator) = T.command {
    Dependency.showUpdates(ev)
  }

  private def evaluate(ev: Evaluator)(tasks: Tasks[Seq[PathRef]] => NamedTask[_], sources: String*): Result[Unit] = {
    Resolve.Tasks.resolve(ev.rootModule, sources, SelectMode.Multi) match {
      case Right(rs: List[NamedTask[Seq[PathRef]]] @unchecked) =>
        ev.evaluate(Agg(tasks(Tasks(rs))))
          .results
          .values
          .foldLeft[Result[Unit]](Result.Success(()))((a, b) => a.flatMap(_ => b.result.map(_ => ())))
      case Left(msg) =>
        Result.Failure[Unit](msg)
    }
  }
}
