// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline.pipeline

import com.digitalasset.canonical.specific.Offset.order.*
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.canonical.UserRight
import com.digitalasset.pqs.as
import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.given
import com.digitalasset.pqs.pipeline.pipeline.Config.TransactionApi
import com.digitalasset.pqs.pipeline.pipeline.ledger.specific.Config.{CliStartOffset, CliStopOffset}
import com.digitalasset.pqs.utils.safeequals.===
import com.digitalasset.zio.daml.Ledger
import zio.ZIO.*
import zio.metrics.Metric
import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import scala.language.implicitConversions

trait Pipeline:
  def run: ZIO[Any, Throwable, Unit]

object Pipeline:
  val streamUpGauge = Metric.gauge("stream_up", "Pipeline stream is up")
  val layer         = ZLayer.fromFunction(Impl.apply).as[Pipeline]

private case class Impl(
    config: Config,
    ledger: Ledger,
    datastore: Datastore
) extends Pipeline:
  override def run =
    (for
      (rights, (continueFromOffset, continueFromIx), actualEnd) <- traces.root("initialization routine") {
        for
          rights <- ledger.getUserRights(config.filter.parties)
          _ <- rights match
            case UserRight.AsParties(parties) =>
              logInfo(s"Starting pipeline on behalf of '${parties.mkString(",")}'")
            case UserRight.AsAnyParty =>
              logInfo(s"Starting pipeline on behalf of all parties on the participant")

          ledgerStart <- ledger.getLedgerStart(rights)
          ledgerEnd   <- ledger.getLedgerEnd
          // Registering this instance as the active writer to prevent stale instances from updating the watermark
          _                <- datastore.registerActiveWriterAndCleanupTransactions
          dbEndOffsetAndIx <- datastore.getLastCheckpoint
          (dbEnd, dbEndIx)                 = dbEndOffsetAndIx
          (normalizedStart, normalizedEnd) = getNormalizedOffsetsFromConfig(ledgerStart, ledgerEnd, dbEnd)
          dbStartOffsetAndIx <- datastore.getFirstCheckpoint
          (dbStart, _) = dbStartOffsetAndIx
          _ <- ZIO.unit @@ traces.attributes(
            "pqs.init.ledger.start"     -> ledgerStart.toString,
            "pqs.init.ledger.end"       -> ledgerEnd.toString,
            "pqs.init.datastore.start"  -> dbStart.toString,
            "pqs.init.datastore.end"    -> dbEnd.toString,
            "pqs.init.normalized.start" -> normalizedStart.toString,
            "pqs.init.normalized.end"   -> normalizedEnd.toString
          )

          _ <- OffsetValidator.validate(
            normalizedStart,
            normalizedEnd,
            dbStart,
            dbEnd,
            ledgerStart,
            ledgerEnd
          )

          actualStart = dbEnd max normalizedStart
          dbIsEmpty   = dbEnd === Offset.Genesis
          _ <- Pipeline.streamUpGauge.set(1)
          offsetAndIx <- actualStart match
            case offset: Offset.Absolute if dbIsEmpty =>
              for
                _ <- logInfo(
                  s"Last checkpoint is absent. Seeding from ACS before processing transactions with starting offset: '$actualStart'"
                )
                acsEvents = ledger.getActiveContracts(rights, offset)
                allEvents = ZStream(Offset.Genesis) ++ acsEvents ++ ZStream(offset)
                _ <- allEvents.run(datastore.processAcs)
                  @@ traces.attributes("pqs.init.acs.offset" -> offset.toString)
                  @@ traces.span("seed from ACS")
                res <- datastore.getLastCheckpoint
              yield res
            case _ if dbIsEmpty =>
              logInfo(s"Starting from Genesis").as(actualStart -> dbEndIx)
            case _ =>
              logInfo(s"Last known checkpoint is at offset '$dbEnd' and index '$dbEndIx'").as(actualStart -> dbEndIx)
          (continueFromOffset, continueFromIx) = offsetAndIx
          actualEnd                            = continueFromOffset max dbEnd max normalizedEnd
          _ <- ZIO.unit @@ traces.attributes(
            "pqs.init.actual.start" -> continueFromOffset.toString,
            "pqs.init.actual.end"   -> actualEnd.toString
          )
          _ <- logInfo(
            s"Continuing from offset '$continueFromOffset' and index '$continueFromIx' until offset '$actualEnd'"
          )
        yield (rights, offsetAndIx, actualEnd)
      }
      datasource = config.datasource match
        case TransactionApi.TransactionStream     => ledger.getTransactions
        case TransactionApi.TransactionTreeStream => ledger.getTransactionTrees
      _ <- datasource(rights, continueFromOffset, actualEnd)
        .mapAccum(continueFromIx + 1)((index, a) => (index + 1, (a, index)))
        .run(datastore.processTransactions)
    yield {}).ensuring(Pipeline.streamUpGauge.set(0))

  private def getNormalizedOffsetsFromConfig(ledgerStart: Offset, ledgerEnd: Offset, dbEnd: Offset) = {
    val dbIsEmpty = dbEnd === Offset.Genesis
    val start = config.ledger.start match {
      case CliStartOffset.Genesis             => Offset.Genesis
      case CliStartOffset.Oldest if dbIsEmpty => ledgerStart
      case CliStartOffset.Oldest              => dbEnd
      case CliStartOffset.Latest if dbIsEmpty => ledgerEnd
      case CliStartOffset.Latest              => dbEnd
      case CliStartOffset.Absolute(offset)    => Offset.Absolute(offset)
    }

    val end = config.ledger.stop match {
      case CliStopOffset.Latest           => ledgerEnd
      case CliStopOffset.Never            => Offset.Infinity
      case CliStopOffset.Absolute(offset) => Offset.Absolute(offset)
    }

    start -> end
  }
