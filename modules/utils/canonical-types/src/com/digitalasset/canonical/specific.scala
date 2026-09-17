// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canonical

import com.digitalasset.pqs.o11y.traces.DetachedSpan
import com.digitalasset.transcode.schema
import zio.Chunk

import java.time.Instant

object specific:
  type NodeId                                 = Int
  opaque type EventId <: Tuple2[Long, NodeId] = (Long, NodeId)
  inline def EventId(value: (Long, NodeId)): EventId = value

  case class Transaction[+E](
      transactionId: TransactionId,
      commandId: CommandId,
      workflowId: WorkflowId,
      effectiveAt: Option[Instant],
      offset: Offset,
      events: Chunk[E],
      synchronizerId: SynchronizerId,
      externalTransactionHash: Option[Array[Byte]] = None,
      paidTrafficCost: Option[Long] = None,
      seenAt: Long, // nano time this transaction was first observed in PQS
      span: DetachedSpan,
      remoteSpan: Option[(String, String)] = None
  )

  sealed trait Event
  sealed trait TransactionEvent  extends Event
  sealed trait TreeEvent         extends Event
  sealed trait ReassignmentEvent extends Event

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

    final case class Unassigned(
        eventId: EventId,
        reassignmentId: String,
        source: SynchronizerId,
        target: SynchronizerId,
        submitter: Option[Party],
        reassignmentCounter: Long,
        contractId: ContractId,
        templateId: schema.Identifier,
        witnesses: Chunk[Party],
        // Before this time only the submitter of the unassignment can initiate the assignment
        assignmentExclusivity: Option[Instant]
    ) extends ReassignmentEvent

    final case class Assigned(
        eventId: EventId,
        reassignmentId: String,
        source: SynchronizerId,
        target: SynchronizerId,
        submitter: Option[Party],
        reassignmentCounter: Long,
        contractId: ContractId,
        templateId: schema.Identifier,
        witnesses: Chunk[Party]
    ) extends ReassignmentEvent

  end Event
