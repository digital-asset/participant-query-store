// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical
import com.digitalasset.canonical.specific.{EventId, NodeId, Offset}
import com.digitalasset.canonical.{ContractId, DomainId, Party}
import com.digitalasset.pqs.postgres.document.model.given
import com.digitalasset.pqs.postgres.document.{IdPlaceholder, model}
import com.digitalasset.transcode.schema.ChoiceName
import ujson.Value
import zio.config.magnolia.Descriptor
import zio.jdbc.JdbcDecoder

import java.time.{Instant, ZonedDateTime}
import scala.util.Try

object specific:
  trait ValueConverter[A] { def convert(value: A): String }

  given eventIdConverter: ValueConverter[EventId] = value => value.toString

  implicit val offsetEncoder: JdbcDecoder[Offset] = (ix, rs) => (ix, Offset.Absolute(rs.getLong(ix)))

  extension (offset: Offset) def toSqlValue: Long = offset.toLongOffset

  sealed trait PruningBoundary
  object PruningBoundary:
    final case class OffsetBoundary(offset: Offset.Absolute) extends PruningBoundary:
      override def toString: String = offset.toString
    final case class TimeBoundary(time: ZonedDateTime) extends PruningBoundary:
      override def toString: String = time.toString
    final case class DurationBoundary(duration: java.time.Duration) extends PruningBoundary:
      override def toString: String = duration.toString

    given pruningBoundaryDesc: Descriptor[PruningBoundary] = Descriptor.from(
      Descriptor[String].transform[PruningBoundary](
        s =>
          // try parsing the pruning boundary as an offset, timestamp or duration
          def tryDuration = Try(java.time.Duration.parse(s)).map(DurationBoundary.apply)
          def tryTime     = Try(ZonedDateTime.parse(s)).map(TimeBoundary.apply)
          def offset      = OffsetBoundary(Offset.Absolute(s.toLong))
          tryDuration orElse tryTime getOrElse offset
        ,
        {
          case OffsetBoundary(offset)     => offset.toString
          case TimeBoundary(time)         => time.toString
          case DurationBoundary(duration) => duration.toString
        }
      )
    )

  final class Transaction(
      val ix: Long,
      val offset: Offset,
      transactionId: Option[String] = None,
      effectiveAt: Option[Instant] = None,
      domainId: Option[DomainId] = None,
      workflowId: Option[String] = None,
      remoteSpan: Option[(String, String)] = None,
      externalTransactionHash: Option[Array[Byte]] = None,
      paidTrafficCost: Option[Long] = None
  ):
    val columns = Seq(
      "ix",
      "\"offset\"",
      "transaction_id",
      "effective_at",
      "domain_id",
      "workflow_id",
      "trace_context",
      "external_transaction_hash",
      "paid_traffic_cost"
    )
    val rowValues = model.values(ix)(offset.toSqlValue)(transactionId)(effectiveAt)(domainId)(workflowId)(remoteSpan)(
      externalTransactionHash
    )(paidTrafficCost)

  final case class Event(
      pk: IdPlaceholder,
      txIx: Long,
      eventId: EventId,
      eventType: model.EventType
  ):
    val columns   = Seq("pk", "tx_ix", "event_id", "type")
    val rowValues = model.values(pk)(txIx)(eventId)(eventType)

  final case class Contract(
      qualifiedName: String,
      entityType: model.EntityTypePk,
      createEventPk: IdPlaceholder,
      createdAtIx: Long,
      contractId: ContractId,
      signatories: Seq[Party],
      observers: Seq[Party],
      witnesses: Seq[Party],
      payload: Value,
      contractKey: Option[Value],
      contractKeyHash: Option[Array[Byte]],
      metadata: Option[Array[Byte]],
      acsDelta: Boolean,
      packagePk: model.PackagePk,
      creationPackageId: Option[String]
  ):
    val columns = Seq(
      "tpe_pk",
      "create_event_pk",
      "created_at_ix",
      "contract_id",
      "payload",
      "contract_key",
      "contract_key_hash",
      "metadata",
      "package_pk",
      "creation_package_id",
      "signatories",
      "observers",
      "witnesses",
      "divulged_only"
    )
    val rowValues =
      model.values(entityType)(createEventPk)(createdAtIx)(contractId)(payload)(contractKey)(contractKeyHash)(metadata)(
        packagePk
      )(creationPackageId)(signatories)(observers)(witnesses)(!acsDelta)

  final case class Exercise(
      qualifiedName: String,
      entityType: model.EntityTypePk,
      contractEntityType: model.EntityTypePk,
      exerciseEventPk: IdPlaceholder,
      exercisedAt: Long,
      contractId: ContractId,
      choiceName: ChoiceName,
      argument: Value,
      result: Value,
      controllers: Seq[Party],
      witnesses: Seq[Party],
      lastDescendant: NodeId,
      packagePk: model.PackagePk
  ):
    val columns = Seq(
      "tpe_pk",
      "contract_tpe_pk",
      "exercise_event_pk",
      "exercised_at_ix",
      "contract_id",
      "argument",
      "result",
      "controllers",
      "witnesses",
      "last_descendant_node_id",
      "package_pk"
    )
    val rowValues = model.values(entityType)(contractEntityType)(exerciseEventPk)(exercisedAt)(contractId)(argument)(
      result
    )(controllers)(witnesses)(lastDescendant)(packagePk)
