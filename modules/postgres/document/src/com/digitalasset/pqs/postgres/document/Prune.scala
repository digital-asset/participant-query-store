// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.configuration
import com.digitalasset.pqs.postgres.document.specific.*
import com.digitalasset.pqs.postgres.document.specific.PruningBoundary.*
import zio.Console.printLine
import zio.ZLayer.*
import zio.config.magnolia.{Descriptor, describe}
import zio.jdbc.*
import zio.{System as _, *}

object Prune:
  val layer = ZLayer.fromFunction(Service.apply)

  final case class Config(
      @describe(
        "Inclusive boundary up to which to prune. Can be an offset, timestamp (ISO 8601) or duration (ISO 8601)"
      )
      target: PruningBoundary,
      @describe("Precomputes effects of pruning through dry-run, or actually runs it")
      mode: PruningMode = PruningMode.DryRun
  )

  object Config:
    private val confPrune = configuration[Config]
  end Config

  sealed trait PruningMode
  object PruningMode:
    case object DryRun extends PruningMode
    case object Force  extends PruningMode
  end PruningMode

  case class Service(config: Config, connectionPool: ZConnectionPool) {

    private val env = ZEnvironment(connectionPool)

    def run: ZIO[Any, Throwable, Unit] =
      val sqlFunction = config.mode match
        case PruningMode.DryRun => sql"prune_archived_to_offset_dry_run"
        case PruningMode.Force  => sql"prune_archived_to_offset"

      val sqlArgument = config.target match
        case OffsetBoundary(offset) =>
          sql"${offset.toSqlValue}"
        case TimeBoundary(time) =>
          sql"nearest_offset(${time.toString} :: timestamp with time zone)"
        case DurationBoundary(duration) =>
          sql"nearest_offset(${duration.toString} :: interval)"

      val query =
        sql"select pruning_boundary_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions from $sqlFunction($sqlArgument)"
      for
        maybeResult <-
          transaction(query.query[(Option[String], Int, Int, Int, Int)].selectOne).provideEnvironment(env)
        _ <- maybeResult match
          // the pruning functions are STRICT: a NULL `nearest_offset` short-circuits them to an empty set
          case None =>
            printLine(s"No history older than ${config.target}, nothing to do.")
          case Some((None, _, _, _, _)) =>
            printLine(s"Already pruned past ${config.target}, nothing to do.")
          case Some((Some(boundary), deletedContracts, deletedExercises, deletedEvents, deletedTransactions)) =>
            val printResult = printLine(
              List(
                s"Pruning boundary offset: $boundary",
                s"Deleted contracts: $deletedContracts",
                s"Deleted choices: $deletedExercises",
                s"Deleted events: $deletedEvents",
                s"Deleted transactions: $deletedTransactions"
              ).map("  " + _).mkString(System.lineSeparator)
            )
            config.mode match
              case PruningMode.DryRun =>
                printLine("Dry-run result:") *> printResult *> printLine(
                  "Re-run with --prune-mode Force to execute the pruning operation."
                )
              case PruningMode.Force => printLine("Pruning operation result:") *> printResult
      yield ()
  }
