// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.digitalasset.auth.Auth
import com.digitalasset.canonical.{Party, UserRight}
import com.digitalasset.scribe.configuration.filter.PartyFilterParser.PartyFilter
import com.digitalasset.scribe.grpc.ZManagedChannel
import io.grpc.Status
import zio.ZIO.*
import zio.stream.ZStream
import zio.{Chunk, Task, ZIO, ZLayer}

import scala.collection.immutable.ListSet

object PartiesService:
  val live: ZLayer[ZManagedChannel & Auth, Throwable, PartiesService] =
    (UserManagementServiceClient.live ++ PartyManagementServiceClient.live)
      >>> ZLayer.fromFunction(PartiesService.apply)

case class PartiesService(
    userManagementServiceClient: UserManagementServiceClient,
    partyManagementServiceClient: PartyManagementServiceClient,
    auth: Auth
):

  def getRights(partyFilter: PartyFilter): Task[UserRight] = for
    userRights <- auth match
      case _: Auth.OAuth | _: Auth.AccessToken => getUserRights(partyFilter)
      case Auth.NoAuth                         => handleNoAuthPartyFilter(partyFilter)(getAllRights)
    _ <- unless {
      userRights match
        case UserRight.AsParties(parties) => parties.nonEmpty
        case UserRight.AsAnyParty         => true
    } {
      fail(Status.UNAVAILABLE.withDescription(s"No parties found matching `${partyFilter.toString}`.").asException())
    }
  yield userRights

  private def getAllRights(partyFilter: PartyFilter) = for
    cantonInternalParty <- partyManagementServiceClient.getParticipantId(GetParticipantIdRequest()).map(_.participantId)
    _                   <- logDebug(s"Internal party will be filtered out: $cantonInternalParty")
    fetchAllParties = ZStream.unfoldChunkZIO(Option.empty[String])(token =>
      partyManagementServiceClient
        .listKnownParties(ListKnownPartiesRequest.defaultInstance.withPageToken(token.getOrElse("")))
        .tap(resp =>
          logDebug(s"Listed known parties: ${resp.partyDetails.map(_.party)}. Next page token: ${resp.nextPageToken}")
        )
        .map(resp => Chunk.from(resp.partyDetails) -> Some(resp.nextPageToken))
        .unless(token.contains(""))
    )
    parties <- fetchAllParties
      .filter(_.isLocal)                                // filter out parties from other ledgers
      .filterNot(_.party.contains(cantonInternalParty)) // filter out the canton ledgerId internal party
      .filter(partyDetails =>
        partyFilter.contains(partyDetails.party)
      ) // filter parties as given in the initial configuration
      .mapZIO(partyDetails => ZIO.attempt(Party(Ref.Party.assertFromString(partyDetails.party))))
      .runCollect
      .tap(parties => logInfo(s"${parties.length} known parties retrieved") *> logDebug(s"Parties: $parties"))
  yield UserRight.AsParties(parties.distinct.to(ListSet))

  private def getUserRights(partyFilter: PartyFilter) =
    userManagementServiceClient
      .listUserRights(ListUserRightsRequest.defaultInstance)
      .tap(resp => logInfo(s"Retrieved ${resp.rights.length} user rights"))
      .mapAttempt(resp => convertRights(resp.rights.map(_.kind), partyFilter))
      .tap {
        case UserRight.AsParties(parties) =>
          logInfo(s"${parties.size} parties can actAs/readAs") *> logDebug(s"Parties: $parties")
        case UserRight.AsAnyParty =>
          logInfo(s"participant can access any party") *> logDebug(s"Acting/Reading asAnyParty")
      }
