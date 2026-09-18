// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.state_service.ZioStateService.StateServiceClient
import com.digitalasset.canonical.*
import com.digitalasset.canonical.Event
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.transcode.schema.Dictionary
import com.digitalasset.zio.daml.*
import com.digitalasset.zio.daml.ledgerapi.*
import com.digitalasset.zio.daml.ledgerapi.eventConverters.*
import zio.ZIO.*
import zio.stream.{Stream, ZStream}
import zio.{IO, ZLayer}

object StateService:
  val live: ZLayer[ZManagedChannel & ProtobufCodecs & DamlSchema, Throwable, StateService] =
    StateServiceClient.live >>> ZLayer.fromFunction(StateService.apply)

case class StateService(
    stateServiceClient: StateServiceClient,
    codecs: ProtobufCodecs,
    damlSchema: DamlSchema
):

  def getActiveContracts(
      rights: UserRight,
      activeAtOffset: Offset.Absolute
  ): Stream[Throwable, Event.Created] =
    ZStream.unwrap(
      for _ <- logFilterContents(damlSchema)
      yield stateServiceClient
        .getActiveContracts(
          GetActiveContractsRequest(
            activeAtOffset = activeAtOffset.toLong,
            eventFormat = Some(mkEventFormat(rights, damlSchema)),
            streamContinuationToken = None
          )
        )
        .map(_.contractEntry.activeContract)
        .collectSome
        .mapZIO(contract =>
          val evt    = contract.getCreatedEvent
          val syncId = SynchronizerId(contract.synchronizerId)
          logDebug(s"Converting active contract") *>
            convertCreatedEvent(evt, syncId)(using codecs)(using damlSchema)
              .tap { conv =>
                logTrace(s"Ledger event: ${pprint(evt, height = Int.MaxValue)}") *>
                  logTrace(s"Canonical event: ${pprint(conv, height = Int.MaxValue)}")
              }
        )
    )

  def getLedgerStart(rights: UserRight): IO[Throwable, Offset] =
    stateServiceClient
      .getLatestPrunedOffsets(GetLatestPrunedOffsetsRequest())
      .map(_.participantPrunedUpToInclusive.toOffset)
      .tap(offset => logInfo(s"Retrieved ledger start offset: $offset"))

  def getLedgerEnd: IO[Throwable, Offset] =
    stateServiceClient
      .getLedgerEnd(GetLedgerEndRequest(Seq.empty))
      .map(_.offset.toOffset)
      .tap(offset => logInfo(s"Retrieved ledger end offset: $offset"))
