// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.trace_context.TraceContext
import com.daml.ledger.api.v2.transaction_filter.{TransactionFormat, TransactionShape, UpdateFormat}
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.daml.ledger.api.v2.update_service.{GetUpdatesRequest, GetUpdatesResponse}
import com.digitalasset.canonical.specific.{Event, Offset, Transaction, TransactionEvent}
import com.digitalasset.canonical.ReassignmentEvent
import com.digitalasset.canonical.{CommandId, TransactionId, UserRight, WorkflowId}
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import com.digitalasset.pqs.o11y.{logs, traces}
import com.digitalasset.transcode.schema.Dictionary
import com.digitalasset.zio.daml.*
import com.digitalasset.zio.daml.ledgerapi.*
import com.digitalasset.zio.daml.ledgerapi.DataAdapter.{ReassignmentAdapter, TransactionAdapter}
import com.digitalasset.zio.daml.ledgerapi.specific.{Codecs, convertEvent, convertReassignmentEvent}
import com.google.protobuf.timestamp.Timestamp
import io.opentelemetry.api.trace.*
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding
import scalapb.TimestampConverters
import zio.ZIO.{logInfo, logTrace}
import zio.metrics.Metric
import zio.stream.ZStream
import zio.{Chunk, Task, UIO, ZIO, ZLayer, stream}

import java.time.Duration
import scala.language.implicitConversions
import scala.reflect.Selectable.reflectiveSelectable

object UpdateService:
  val live: ZLayer[
    ZManagedChannel & Codecs & KnownEntityIdentifiers,
    Throwable,
    UpdateService
  ] =
    UpdateServiceClient.live
      >>> ZLayer.fromFunction(UpdateService.apply)

case class UpdateService(
    updateServiceClient: UpdateServiceClient,
    codecs: Codecs,
    identifiers: KnownEntityIdentifiers
):

  private val txLagGauge = Metric
    .gauge(
      "tx_lag_from_ledger_wallclock",
      "Lag from ledger (wall-clock delta (in ms) from command completion to receipt by pipeline)"
    )
    .contramap[Duration](_.toMillis.toDouble / 1_000)

  // TODO(record-time): lag is measured from the ledger effective time, which only a transaction
  // has, so a reassignment never contributes to it. `record_time` is present on every update and
  // would let this gauge cover them too — revisit once it is ingested. See
  // `.ai/features/multi-sync-support/2026-09-11 - TICKET - Store record_time and revisit nearest_offset.md`.
  inline private def lag(chunk: Iterable[{ def effectiveAt: Option[Timestamp] }]): UIO[Option[Duration]] =
    zio.Clock.instant.map(now =>
      // The first update that has an effective time, not simply the first update: a chunk headed by
      // a reassignment can still hold transactions, and their lag is worth reporting. A chunk with
      // no times at all yields None, and nothing is published.
      chunk.iterator
        .flatMap(_.effectiveAt)
        .nextOption()
        .map(ts => Duration.between(TimestampConverters.asJavaInstant(ts), now))
    )

  def getTransactions(
      rights: UserRight,
      beginExclusive: Offset,
      endInclusive: Offset
  ): stream.Stream[Throwable, Transaction[TransactionEvent | ReassignmentEvent]] =
    getTransactionByShape(
      rights,
      beginExclusive,
      endInclusive,
      TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
    )

  def getTransactionTrees(
      rights: UserRight,
      beginExclusive: Offset,
      endInclusive: Offset
  ): stream.Stream[Throwable, Transaction[Event | ReassignmentEvent]] =
    getTransactionByShape(
      rights,
      beginExclusive,
      endInclusive,
      TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
    )

  private def getTransactionByShape(
      rights: UserRight,
      beginExclusive: Offset,
      endInclusive: Offset,
      transactionShape: TransactionShape
  ): stream.Stream[Throwable, Transaction[TransactionEvent | ReassignmentEvent]] =
    ZStream.unwrap(
      for _ <- logFilterContents(identifiers)
      yield getTransactionStream(
        beginExclusive,
        offset =>
          GetUpdatesRequest(
            beginExclusive = offset.toBeginLedgerOffset,
            endInclusive = endInclusive.toEndLedgerOffset,
            updateFormat = Some(
              UpdateFormat.defaultInstance
                .withIncludeTransactions(
                  TransactionFormat(
                    eventFormat = Some(mkEventFormat(rights, identifiers)),
                    transactionShape = transactionShape
                  )
                )
                .withIncludeReassignments(mkEventFormat(rights, identifiers))
            ),
            descendingOrder = false
          ),
        req =>
          updateServiceClient
            .getUpdates(req)
            .map(_.update)
            .collect[DataAdapter] {
              case GetUpdatesResponse.Update.Transaction(value)  => TransactionAdapter(value, transactionShape)
              case GetUpdatesResponse.Update.Reassignment(value) => ReassignmentAdapter(value)
            }
            .mapChunksZIO { chunk =>
              logInfo(s"Received update responses at offsets: ${offsets(chunk)}") *>
                lag(chunk).flatMap(latest => ZIO.foreachDiscard(latest.toList)(txLagGauge.update(_))) *>
                zio.Clock.nanoTime.map(now => chunk.map(_ -> now))
            }
            .mapZIO { (update, seenAt) =>
              consumerSpan("com.daml.ledger.api.v2.UpdateService/GetUpdates") {
                traces.makeDetachedSpan(s"export ${update.sourceType}").map(span => (update, span, seenAt))
              }
            }
            .mapZIOPar(16) { (update, updateSpan, seenAt) =>
              updateSpan.locally(process(update, updateSpan, seenAt)(convertEvents(_, rights)))
            }
      )
    )

  private def convertEvents(
      update: DataAdapter,
      rights: UserRight
  ): Task[Chunk[TransactionEvent | ReassignmentEvent]] =
    update match
      case TransactionAdapter(tx, _) =>
        ZIO.foreach(tx.events.to(Chunk))(convertEvent(_, tx.offset, rights)(using codecs, identifiers))
      case ReassignmentAdapter(rs) =>
        ZIO.foreach(rs.events.to(Chunk))(convertReassignmentEvent(_, rs.offset)(using identifiers))

  private def consumerSpan(name: String) =
    traces.attributes(
      "messaging.system"           -> "canton",
      "messaging.destination.name" -> name,
      "messaging.operation.name"   -> "consume",
      "messaging.operation.type"   -> "process"
    ) @@ traces.root(s"consume $name", SpanKind.CONSUMER)

  private def process[E](tx: DataAdapter, txSpan: DetachedSpan, seenAt: Long)(
      eventsConverter: DataAdapter => Task[Chunk[E]]
  ) =
    val remoteSpan = tx.traceContext.remoteSpanContext
    logs.tag("correlation_id" -> remoteSpan.getOrElse(SpanContext.getInvalid).getTraceId) {
      for
        _ <- logTrace(s"Ledger ${tx.sourceType}: ${pprint(tx, height = Int.MaxValue)}")
        _ <- txSpan.addAttributes(
          "daml.command_id"     -> tx.commandId,
          "daml.events_count"   -> tx.eventsSize.toLong,
          "daml.offset"         -> tx.offset,
          "daml.transaction_id" -> tx.transactionId,
          "daml.workflow_id"    -> tx.workflowId
        )
        // TODO(record-time): a reassignment has no ledger effective time, so its span carries no
        // time at all. Both update kinds do carry `record_time` and `synchronizer_id` on the wire;
        // add them here as `daml.record_time` and `daml.synchronizer_id` once those are ingested,
        // so every update kind is traceable against the database. See
        // `.ai/features/multi-sync-support/2026-09-11 - TICKET - Store record_time and revisit nearest_offset.md`.
        _ <- ZIO.foreachDiscard(tx.effectiveAt.toList) { ts =>
          txSpan.addAttributes("daml.effective_at" -> TimestampConverters.asJavaInstant(ts).toString)
        }
        logAttrs = (Seq("offset" -> tx.offset, "events" -> tx.eventsSize)
          ++ remoteSpan.map("remote trace" -> _.getTraceId))
          .map(_.productIterator.mkString(": "))
          .mkString("(", ", ", ")")
        _ <- logInfo(s"Converting ${tx.sourceType} ${tx.transactionId} $logAttrs")
        _ <- ZIO.whenCase(remoteSpan) { case Some(rs) => txSpan.addLink(rs, "target" -> "↥ ledger submission") }
        _ <- txSpan.addEvent(s"canonicalizing ${tx.sourceType}")
        convertedEvents <- eventsConverter(tx)
        canonicalTx <- ZIO.attempt {
          Transaction(
            transactionId = TransactionId(tx.transactionId),
            commandId = CommandId(tx.commandId),
            workflowId = WorkflowId(tx.workflowId),
            effectiveAt = tx.effectiveAt.map(TimestampConverters.asJavaInstant),
            offset = tx.offset.toOffset,
            events = convertedEvents,
            externalTransactionHash = tx.externalTransactionHash,
            paidTrafficCost = tx.paidTrafficCost,
            seenAt = seenAt,
            span = txSpan,
            remoteSpan = remoteSpan.map(_.asTuple)
          )
        }
        _ <- txSpan.addEvent(s"canonicalized ${tx.sourceType}")
        _ <- logTrace(s"Canonical ${tx.sourceType}: ${pprint(canonicalTx, height = Int.MaxValue)}")
      yield canonicalTx
    }

  extension (ctx: TraceContext)
    private def remoteSpanContext = ctx.traceparent.collect {
      _.split("-") match {
        case Array(version, traceId, spanId, flags) =>
          SpanContext.createFromRemoteParent(
            traceId,
            spanId,
            TraceFlags.fromHex(flags, 0),
            ctx.tracestate.map(W3CTraceContextEncoding.decodeTraceState).getOrElse(TraceState.getDefault)
          )
      }
    }

  extension (ctx: SpanContext)
    private def asTuple = (
      s"00-${ctx.getTraceId}-${ctx.getSpanId}-${ctx.getTraceFlags.asHex}",
      W3CTraceContextEncoding.encodeTraceState(ctx.getTraceState)
    )
