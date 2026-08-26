// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import com.digitalasset.transcode.schema
import com.digitalasset.transcode.schema.IdentifierFilter
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

  case class ReassignmentEvent(
      unassignId: String,
      source: DomainId,
      target: DomainId,
      submitter: Party,
      reassignmentCounter: Long,
      contractId: ContractId,
      templateId: schema.Identifier,
      witnessParties: Chunk[Party],
      assignmentExclusivity: Option[Instant]
  )

  enum UserRight:
    case AsParties(parties: Set[Party])
    case AsAnyParty
