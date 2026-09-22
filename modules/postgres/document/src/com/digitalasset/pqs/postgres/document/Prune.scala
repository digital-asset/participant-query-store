// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.Offset
import com.digitalasset.pqs.configuration
import com.digitalasset.pqs.postgres.document.PruningBoundary
import zio.Console.printLine
import zio.ZLayer.*
import zio.config.magnolia.{Descriptor, describe}
import zio.jdbc.*
import zio.{System as _, *}

final case class Prune(config: PruneConfig, connectionPool: ZConnectionPool):

  private val env = ZEnvironment(connectionPool)

  def run: ZIO[Any, Throwable, Unit] =
    val sqlFunction = config.mode match
      case PruningMode.DryRun => sql"prune_archived_to_offset_dry_run"
      case PruningMode.Force  => sql"prune_archived_to_offset"

    val sqlArgument = config.target match
      case PruningBoundary.OffsetBoundary(offset) => sql"${offset.toLong}"
      case PruningBoundary.TimeBoundary(time)     => sql"nearest_offset(${time.toString} :: timestamp with time zone)"
      case PruningBoundary.DurationBoundary(duration) => sql"nearest_offset(${duration.toString} :: interval)"

    val query =
      sql"""select pruning_boundary_offset, deleted_contracts, deleted_exercises, deleted_events,
                deleted_reassignments, deleted_transactions from $sqlFunction($sqlArgument)"""
    for
      maybeResult <-
        transaction(query.query[Prune.PruningResultRow].selectOne).provideEnvironment(env)
      _ <- maybeResult match
        // the pruning functions are STRICT: a NULL `nearest_offset` short-circuits them to an empty set
        case None =>
          printLine(s"No history older than ${config.target}, nothing to do.")
        case Some(result) =>
          result.pruningBoundaryOffset match
            case None =>
              printLine(s"Already pruned past ${config.target}, nothing to do.")
            case Some(boundary) =>
              val printResult = printLine(
                List(
                  s"Pruning boundary offset: $boundary",
                  s"Deleted contracts: ${result.deletedContracts}",
                  s"Deleted choices: ${result.deletedExercises}",
                  s"Deleted events: ${result.deletedEvents}",
                  s"Deleted reassignments: ${result.deletedReassignments}",
                  s"Deleted transactions: ${result.deletedTransactions}"
                ).map("  " + _).mkString(System.lineSeparator)
              )
              config.mode match
                case PruningMode.DryRun =>
                  printLine("Dry-run result:") *> printResult *> printLine(
                    "Re-run with --prune-mode Force to execute the pruning operation."
                  )
                case PruningMode.Force => printLine("Pruning operation result:") *> printResult
    yield ()
end Prune
object Prune:
  private type PruningResultRow = (
      pruningBoundaryOffset: Option[String],
      deletedContracts: Int,
      deletedExercises: Int,
      deletedEvents: Int,
      deletedReassignments: Int,
      deletedTransactions: Int
  )

  val layer = ZLayer.fromFunction(Prune.apply)

final case class PruneConfig(
    @describe(
      "Inclusive boundary up to which to prune. Can be an offset, timestamp (ISO 8601) or duration (ISO 8601)"
    )
    target: PruningBoundary,
    @describe("Precomputes effects of pruning through dry-run, or actually runs it")
    mode: PruningMode = PruningMode.DryRun
)

object PruneConfig:
  private val confPrune = configuration[PruneConfig]

enum PruningMode:
  case DryRun, Force
