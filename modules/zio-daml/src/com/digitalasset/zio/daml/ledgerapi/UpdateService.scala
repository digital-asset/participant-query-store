// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.trace_context.TraceContext
import com.daml.ledger.api.v2.transaction_filter.{TransactionFormat, TransactionShape, UpdateFormat}
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.daml.ledger.api.v2.update_service.{GetUpdatesRequest, GetUpdatesResponse}
import com.digitalasset.canonical.*
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import com.digitalasset.pqs.o11y.{logs, traces}
import com.digitalasset.transcode.schema.Dictionary
import com.digitalasset.zio.daml.*
import com.digitalasset.zio.daml.ledgerapi.*
import com.digitalasset.zio.daml.ledgerapi.DataAdapter.{ReassignmentAdapter, TransactionAdapter}
import com.digitalasset.zio.daml.ledgerapi.eventConverters.{convertEvent, convertReassignmentEvent}
import io.opentelemetry.api.trace.*
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding
import zio.ZIO.{logInfo, logTrace}
import zio.metrics.Metric
import zio.stream.ZStream
import zio.{Chunk, Task, UIO, ZIO, ZLayer, stream}

import java.time.{Duration, Instant}
import scala.language.implicitConversions
import scala.reflect.Selectable.reflectiveSelectable

object UpdateService:
  val live: ZLayer[ZManagedChannel & ProtobufCodecs & DamlSchema, Throwable, UpdateService] =
    UpdateServiceClient.live
      >>> ZLayer.fromFunction(UpdateService.apply)

case class UpdateService(
    updateServiceClient: UpdateServiceClient,
    codecs: ProtobufCodecs,
    schema: DamlSchema
):
  private given ProtobufCodecs = codecs
  private given DamlSchema     = schema

  private val txLagGauge = Metric
    .gauge(
      "tx_lag_from_ledger_wallclock",
      "Lag from ledger (wall-clock delta (in ms) from command completion to receipt by pipeline)"
    )
    .contramap[Duration](_.toMillis.toDouble / 1_000)

  // TODO #74: lag is measured from the ledger effective time, which only a transaction
  // has, so a reassignment never contributes to it. `record_time` is present on every update and
  // would let this gauge cover them too — revisit once it is ingested.
  inline private def lag(chunk: Iterable[{ def effectiveAt: Option[Instant] }]): UIO[Option[Duration]] =
    zio.Clock.instant.map(now =>
      // The first update that has an effective time, not simply the first update: a chunk headed by
      // a reassignment can still hold transactions, and their lag is worth reporting. A chunk with
      // no times at all yields None, and nothing is published.
      chunk.iterator
        .flatMap(_.effectiveAt)
        .nextOption()
        .map(ts => Duration.between(ts, now))
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
  ): stream.Stream[Throwable, Transaction[TransactionEvent | ReassignmentEvent]] =
    ZStream.unwrap(
      for _ <- logFilterContents(schema)
      yield getTransactionStream(
        beginExclusive,
        offset =>
          GetUpdatesRequest(
            beginExclusive = offset.toLong,
            endInclusive = endInclusive.toEndLedgerOffset,
            updateFormat = Some(
              UpdateFormat.defaultInstance
                .withIncludeTransactions(
                  TransactionFormat(
                    eventFormat = Some(mkEventFormat(rights, schema)),
                    transactionShape = transactionShape
                  )
                )
                .withIncludeReassignments(mkEventFormat(rights, schema))
            ),
            descendingOrder = false
          ),
        req =>
          updateServiceClient
            .getUpdates(req)
            .map(_.update)
            .collect {
              case GetUpdatesResponse.Update.Transaction(value)  => TransactionAdapter(value, transactionShape)
              case GetUpdatesResponse.Update.Reassignment(value) => ReassignmentAdapter(value)
            }
            .mapChunksZIO { chunk =>
              logInfo(s"Received update responses at offsets: ${offsets(chunk)}") *>
                lag(chunk).flatMap(latest => ZIO.foreach(latest)(txLagGauge.update(_))) *>
                zio.Clock.nanoTime.map(now => chunk.map(_ -> now))
            }
            .mapZIO { (update, seenAt) =>
              consumerSpan("com.daml.ledger.api.v2.UpdateService/GetUpdates") {
                traces.makeDetachedSpan(s"export ${update.sourceType}").map(span => (update, span, seenAt))
              }
            }
            .mapZIOPar(16) { (update, updateSpan, seenAt) =>
              updateSpan.locally {
                update match
                  case adapter: TransactionAdapter =>
                    process(adapter, updateSpan, seenAt) { tx =>
                      val syncId = SynchronizerId(tx.synchronizerId)
                      ZIO.foreach(tx.events.to(Chunk))(convertEvent(_, syncId))
                    }
                  case adapter: ReassignmentAdapter =>
                    process(adapter, updateSpan, seenAt) { rs =>
                      val syncId = SynchronizerId(rs.synchronizerId)
                      ZIO.foreach(rs.events.to(Chunk))(convertReassignmentEvent(_, syncId))
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
          "daml.command_id"      -> tx.commandId,
          "daml.events_count"    -> tx.eventsSize.toLong,
          "daml.offset"          -> tx.offset,
          "daml.synchronizer_id" -> tx.synchronizerId,
          "daml.transaction_id"  -> tx.transactionId,
          "daml.workflow_id"     -> tx.workflowId
        )
        // TODO #74: a reassignment has no ledger effective time, so its span carries no
        // time at all. Both update kinds do carry `record_time` and `synchronizer_id` on the wire;
        // add them here as `daml.record_time` and `daml.synchronizer_id` once those are ingested,
        // so every update kind is traceable against the database.
        _ <- ZIO.foreach(tx.effectiveAt) { ts =>
          txSpan.addAttributes("daml.effective_at" -> ts.toString)
        }
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
            effectiveAt = tx.effectiveAt,
            offset = tx.offset.toOffset,
            events = convertedEvents,
            synchronizerId = tx.synchronizerId,
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
