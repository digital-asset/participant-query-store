// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.state_service.ZioStateService.StateServiceClient
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.digitalasset.canonical.*
import com.digitalasset.canonical.specific.{Event, Offset}
import com.digitalasset.scribe.grpc.ZManagedChannel
import com.digitalasset.transcode.schema.Dictionary
import com.digitalasset.zio.daml.*
import com.digitalasset.zio.daml.ledgerapi.*
import com.digitalasset.zio.daml.ledgerapi.specific.*
import zio.ZIO.*
import zio.stream.{Stream, ZStream}
import zio.{IO, ZLayer}

object StateService:
  val live: ZLayer[ZManagedChannel & Codecs & KnownEntityIdentifiers, Throwable, StateService] =
    (StateServiceClient.live ++ UpdateServiceClient.live)
      >>> ZLayer.fromFunction(StateService.apply)

case class StateService(
    stateServiceClient: StateServiceClient,
    updateServiceClient: UpdateServiceClient,
    codecs: Codecs,
    identifiers: KnownEntityIdentifiers
):

  def getActiveContracts(
      rights: UserRight,
      activeAtOffset: Offset.Absolute
  ): Stream[Throwable, Event.Created] =
    ZStream.unwrap(
      for _ <- logFilterContents(identifiers)
      yield stateServiceClient
        .getActiveContracts(
          GetActiveContractsRequest(
            activeAtOffset = activeAtOffset.toActiveAtLedgerOffset,
            eventFormat = Some(mkEventFormat(rights, identifiers)),
            streamContinuationToken = None
          )
        )
        .map(_.getActiveContract.createdEvent)
        .collectSome
        .mapZIO(evt =>
          logDebug(s"Converting active contract") *>
            convertCreatedEvent(evt)(using codecs, identifiers)
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
