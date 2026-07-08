// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canonical

import com.digitalasset.scribe.o11y.traces.DetachedSpan
import com.digitalasset.scribe.utils.safeequals.===
import com.digitalasset.transcode.schema
import zio.Chunk

import java.time.Instant

object specific:

  type NodeId                                 = Int
  opaque type EventId <: Tuple2[Long, NodeId] = (Long, NodeId)
  inline def EventId(value: (Long, NodeId)): EventId = value

  sealed trait Offset:
    override def toString: String = this match
      case Offset.Genesis          => "GENESIS"
      case Offset.Infinity         => "INFINITY"
      case Offset.Absolute(offset) => offset.toString

    def toActiveAtLedgerOffset: Long    = toLongOffset
    def toBeginLedgerOffset: Long       = toLongOffset
    def toEndLedgerOffset: Option[Long] = Option(toLongOffset).filter(_ != Long.MaxValue)

    def toLongOffset: Long = this match
      case Offset.Genesis          => 0L
      case Offset.Absolute(offset) => offset
      case Offset.Infinity         => Long.MaxValue

  object Offset:
    case object Genesis                     extends Offset
    final case class Absolute(offset: Long) extends Offset
    case object Infinity                    extends Offset

    implicit val order: Ordering[Offset] = (x, y) =>
      (x, y) match
        case (a, b) if a === b                        => 0
        case (Offset.Genesis, _)                      => -1
        case (_, Offset.Genesis)                      => 1
        case (Offset.Absolute(a), Offset.Absolute(b)) => a.compareTo(b)
        case (Offset.Infinity, _)                     => 1
        case (_, Offset.Infinity)                     => -1

  case class Transaction[+E](
      transactionId: TransactionId,
      commandId: CommandId,
      workflowId: WorkflowId,
      effectiveAt: Instant,
      offset: Offset,
      events: Chunk[E],
      domainId: Option[DomainId] = None,
      externalTransactionHash: Option[Array[Byte]] = None,
      paidTrafficCost: Option[Long] = None,
      seenAt: Long, // nano time this transaction was first observed in scribe
      span: DetachedSpan,
      remoteSpan: Option[(String, String)] = None
  )

  sealed trait Event
  sealed trait TransactionEvent extends Event
  sealed trait TreeEvent        extends Event
  object Event:
    final case class Created(
        eventId: EventId,
        representativePackageId: schema.PackageId,
        templateQualifiedName: String,
        contractId: ContractId,
        contractKey: Option[schema.DynamicValue],
        contractKeyHash: Option[Array[Byte]],
        payloads: Chunk[(schema.Identifier, schema.DynamicValue)],
        signatories: Chunk[Party],
        observers: Chunk[Party],
        witnesses: Chunk[Party],
        created_at: Option[Instant],
        metadata: Option[Array[Byte]],
        acsDelta: Boolean,
        creationPackageId: Option[String]
    ) extends TransactionEvent
        with TreeEvent

    final case class Archived(
        eventId: EventId,
        templateId: schema.Identifier,
        contractId: ContractId
    ) extends TransactionEvent

    final case class Exercised(
        eventId: EventId,
        // Template of the contract on which the choice is exercised
        templateId: schema.Identifier,
        // Where the choice is defined: Either a template or an interface
        entityId: schema.Identifier,
        choice: schema.ChoiceName,
        consuming: Boolean,
        contractId: ContractId,
        arg: schema.DynamicValue,
        result: schema.DynamicValue,
        controllers: Chunk[Party],
        witnesses: Chunk[Party],
        lastDescendant: NodeId
    ) extends TransactionEvent
        with TreeEvent
  end Event
