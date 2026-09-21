// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canonical

import com.digitalasset.transcode.schema
import zio.Chunk

import java.time.Instant

type NodeId                                 = Int
opaque type EventId <: Tuple2[Long, NodeId] = (Long, NodeId)
inline def EventId(value: (Long, NodeId)): EventId = value

sealed trait Event
sealed trait TransactionEvent  extends Event
sealed trait TreeEvent         extends Event
sealed trait ReassignmentEvent extends Event

object Event:
  final case class Created(
      eventId: EventId,
      synchronizerId: SynchronizerId,
      contract: Contract
  ) extends TransactionEvent
      with TreeEvent

  final case class Archived(
      eventId: EventId,
      synchronizerId: SynchronizerId,
      templateId: schema.Identifier,
      contractId: ContractId
  ) extends TransactionEvent

  final case class Exercised(
      eventId: EventId,
      synchronizerId: SynchronizerId,
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
      synchronizerId: SynchronizerId,
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
      synchronizerId: SynchronizerId,
      reassignmentCounter: Long,
      contract: Contract
  ) extends ReassignmentEvent

end Event
