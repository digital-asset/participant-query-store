// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.auth.Auth
import com.digitalasset.canonical.specific.{Event, Offset, Transaction, TransactionEvent}
import com.digitalasset.canonical.{ContractFilter, MetadataFilter, UserRight}
import com.digitalasset.scribe.configuration.filter.PartyFilterParser.PartyFilter
import com.digitalasset.scribe.grpc.ZManagedChannel
import com.digitalasset.transcode.codec.proto.ProtobufCodec
import com.digitalasset.transcode.schema.{Dictionary, Schema}
import com.digitalasset.zio.daml.ledgerapi.{PartiesService, StateService, UpdateService}
import zio.{Tag, Task, ZLayer, stream}

object Ledger:
  val live: ZLayer[ZManagedChannel & Schema & ContractFilter & MetadataFilter & Auth, Throwable, Ledger] =
    (KnownEntityIdentifiers.live ++ DamlSchema.produce(ProtobufCodec).update(_.matchByPackageId))
      >+> (PartiesService.live ++ StateService.live ++ UpdateService.live)
      >>> ZLayer.fromFunction(Ledger.apply)

case class Ledger(
    knownIdentifiers: KnownEntityIdentifiers,
    partiesService: PartiesService,
    stateService: StateService,
    updateService: UpdateService
):

  def getUserRights(partyFilter: PartyFilter): Task[UserRight] =
    partiesService.getRights(partyFilter)

  def getLedgerStart(rights: UserRight): Task[Offset] =
    stateService.getLedgerStart(rights)

  def getLedgerEnd: Task[Offset] =
    stateService.getLedgerEnd

  def getActiveContracts(
      rights: UserRight,
      activeAtOffset: Offset.Absolute
  ): stream.Stream[Throwable, Event.Created | Offset.Absolute] =
    stateService.getActiveContracts(rights, activeAtOffset)

  def getTransactions(
      rights: UserRight,
      beginExclusive: Offset,
      endInclusive: Offset
  ): stream.Stream[Throwable, Transaction[TransactionEvent]] =
    updateService.getTransactions(rights, beginExclusive, endInclusive)

  def getTransactionTrees(
      rights: UserRight,
      beginExclusive: Offset,
      endInclusive: Offset
  ): stream.Stream[Throwable, Transaction[Event]] =
    updateService.getTransactionTrees(rights, beginExclusive, endInclusive)
