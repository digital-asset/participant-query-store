// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import com.digitalasset.transcode.schema
import com.digitalasset.transcode.schema.IdentifierFilter
import com.digitalasset.canonical.specific.EventId
import zio.Chunk
import zio.config.magnolia.Descriptor

import java.time.Instant

package object canonical:
  opaque type Party <: String = String
  inline def Party(value: String): Party = value

  opaque type ContractId <: String = String
  inline def ContractId(value: String): ContractId = value

  opaque type DomainId <: String = String
  inline def DomainId(value: String): DomainId = value

  opaque type WorkflowId <: String = String
  inline def WorkflowId(value: String): WorkflowId = value

  opaque type TransactionId <: String = String
  inline def TransactionId(value: String): TransactionId = value

  opaque type CommandId <: String = String
  inline def CommandId(value: String): CommandId = value

  // TODO change to opaque type when https://github.com/zio/zio/issues/8882 is fixed
  case class ContractFilter(filter: IdentifierFilter) { override def toString: String = filter.toString() }
  case class MetadataFilter(filter: IdentifierFilter) { override def toString: String = filter.toString() }
  private val identifierFilterDescriptor =
    Descriptor[String].transformOrFailLeft(IdentifierFilter.fromString)(x => x.toString)
  given contractFilterDescriptor: Descriptor[ContractFilter] =
    Descriptor.from(identifierFilterDescriptor.transform(ContractFilter(_), _.filter))
  given metadataFilterDescriptor: Descriptor[MetadataFilter] =
    Descriptor.from(identifierFilterDescriptor.transform(MetadataFilter(_), _.filter))

  /** An event of a `Reassignment` update.
    *
    * Deliberately outside the `Event` hierarchy in `specific.scala`: the Ledger API models transaction events and
    * reassignment events as siblings under "update", with no common supertype, and so does PQS.
    */
  sealed trait ReassignmentEvent
  object ReassignmentEvent:
    final case class Unassigned(
        eventId: EventId,
        reassignmentId: String,
        source: DomainId,
        target: DomainId,
        // Empty if the unassignment happened offline via the repair service
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
        source: DomainId,
        target: DomainId,
        // Empty if the assignment happened offline via the repair service
        submitter: Option[Party],
        reassignmentCounter: Long,
        contractId: ContractId,
        templateId: schema.Identifier,
        witnesses: Chunk[Party]
    ) extends ReassignmentEvent

  enum UserRight:
    case AsParties(parties: Set[Party])
    case AsAnyParty
