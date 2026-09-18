// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canonical

import com.digitalasset.pqs.o11y.traces.DetachedSpan
import com.digitalasset.transcode.schema.IdentifierFilter
import zio.Chunk
import zio.config.magnolia.Descriptor

import java.time.Instant

opaque type Party <: String = String
inline def Party(value: String): Party = value

opaque type ContractId <: String = String
inline def ContractId(value: String): ContractId = value

opaque type SynchronizerId <: String = String
inline def SynchronizerId(value: String): SynchronizerId = value

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

enum UserRight:
  case AsParties(parties: Set[Party])
  case AsAnyParty

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
