// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.trace_context.TraceContext
import com.daml.ledger.api.v2.transaction_filter.{TransactionFormat, TransactionShape, UpdateFormat}
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.daml.ledger.api.v2.update_service.{GetUpdatesRequest, GetUpdatesResponse}
import com.digitalasset.canonical.specific.{Event, Offset, Transaction, TransactionEvent}
import com.digitalasset.canonical.{CommandId, TransactionId, UserRight, WorkflowId}
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import com.digitalasset.pqs.o11y.{logs, traces}
import com.digitalasset.transcode.schema.Dictionary
import com.digitalasset.zio.daml.*
import com.digitalasset.zio.daml.ledgerapi.*
import com.digitalasset.zio.daml.ledgerapi.DataAdapter.TransactionAdapter
import com.digitalasset.zio.daml.ledgerapi.specific.{Codecs, convertEvent}
import com.google.protobuf.timestamp.Timestamp
import io.opentelemetry.api.trace.*
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding
import scalapb.TimestampConverters
import zio.ZIO.{logInfo, logTrace}
import zio.metrics.Metric
import zio.stream.ZStream
import zio.{Chunk, Task, ZIO, ZLayer, stream}

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

  inline private def lag(chunk: Iterable[{ def effectiveAt: Option[Timestamp] }]) =
    zio.Clock.instant
      .map(now =>
        for {
          transaction   <- chunk.headOption
          effectiveAtTS <- transaction.effectiveAt
          effectiveAt = TimestampConverters.asJavaInstant(effectiveAtTS)
        } yield Duration.between(effectiveAt, now)
      )
      .someOrElse(Duration.ZERO)

  def getTransactions(
      rights: UserRight,
      beginExclusive: Offset,
      endInclusive: Offset
  ): stream.Stream[Throwable, Transaction[TransactionEvent]] =
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
  ): stream.Stream[Throwable, Transaction[Event]] =
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
  ): stream.Stream[Throwable, Transaction[TransactionEvent]] =
    ZStream.unwrap(
      for _ <- logFilterContents(identifiers)
      yield getTransactionStream(
        beginExclusive,
        offset =>
          GetUpdatesRequest(
            beginExclusive = offset.toBeginLedgerOffset,
            endInclusive = endInclusive.toEndLedgerOffset,
            updateFormat = Some(
              UpdateFormat.defaultInstance.withIncludeTransactions(
                TransactionFormat(
                  eventFormat = Some(mkEventFormat(rights, identifiers)),
                  transactionShape = transactionShape
                )
              )
            ),
            descendingOrder = false
          ),
        req =>
          updateServiceClient
            .getUpdates(req)
            .map(_.update)
            .collect {
              case GetUpdatesResponse.Update.Transaction(value) => value
              // TODO other cases
            }
            .mapChunksZIO { chunk =>
              logInfo(s"Received transactions responses at offsets: ${offsets(chunk)}") *>
                (lag(chunk) @@ txLagGauge) *>
                zio.Clock.nanoTime.map(now => chunk.map(_ -> now))
            }
            .mapZIO { (tx, seenAt) =>
              consumerSpan("com.daml.ledger.api.v2.UpdateService/GetUpdates") {
                val adaptedTx = TransactionAdapter(tx, transactionShape)
                traces.makeDetachedSpan(s"export ${adaptedTx.sourceType}").map(span => (adaptedTx, span, seenAt))
              }
            }
            .mapZIOPar(16) { (tx, txSpan, seenAt) =>
              txSpan.locally {
                process(tx, txSpan, seenAt) { tx =>
                  ZIO.foreach(tx.events.to(Chunk))(convertEvent(_, tx.offset, rights)(using codecs, identifiers))
                }
              }
            }
      )
    )

  private def consumerSpan(name: String) =
    traces.attributes(
      "messaging.system"           -> "canton",
      "messaging.destination.name" -> name,
      "messaging.operation.name"   -> "consume",
      "messaging.operation.type"   -> "process"
    ) @@ traces.root(s"consume $name", SpanKind.CONSUMER)

  private def process[T, E](tx: DataAdapter[T], txSpan: DetachedSpan, seenAt: Long)(
      eventsConverter: T => Task[Chunk[E]]
  ) =
    val remoteSpan = tx.traceContext.remoteSpanContext
    logs.tag("correlation_id" -> remoteSpan.getOrElse(SpanContext.getInvalid).getTraceId) {
      for
        _ <- logTrace(s"Ledger ${tx.sourceType}: ${pprint(tx, height = Int.MaxValue)}")
        _ <- txSpan.addAttributes(
          "daml.command_id"     -> tx.commandId,
          "daml.effective_at"   -> TimestampConverters.asJavaInstant(tx.effectiveAt).toString,
          "daml.events_count"   -> tx.eventsSize.toLong,
          "daml.offset"         -> tx.offset,
          "daml.transaction_id" -> tx.transactionId,
          "daml.workflow_id"    -> tx.workflowId
        )
        logAttrs = (Seq("offset" -> tx.offset, "events" -> tx.eventsSize)
          ++ remoteSpan.map("remote trace" -> _.getTraceId))
          .map(_.productIterator.mkString(": "))
          .mkString("(", ", ", ")")
        _ <- logInfo(s"Converting ${tx.sourceType} ${tx.transactionId} $logAttrs")
        _ <- ZIO.whenCase(remoteSpan) { case Some(rs) => txSpan.addLink(rs, "target" -> "↥ ledger submission") }
        _ <- txSpan.addEvent(s"canonicalizing ${tx.sourceType}")
        convertedEvents <- eventsConverter(tx.source)
        canonicalTx <- ZIO.attempt {
          Transaction(
            transactionId = TransactionId(tx.transactionId),
            commandId = CommandId(tx.commandId),
            workflowId = WorkflowId(tx.workflowId),
            effectiveAt = TimestampConverters.asJavaInstant(tx.effectiveAt),
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
