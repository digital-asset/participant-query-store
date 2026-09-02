// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import com.daml.ledger.api.v2.admin.package_management_service.ZioPackageManagementService.PackageManagementServiceClient
import com.daml.ledger.api.v2.admin.package_management_service.{ListKnownPackagesRequest, UploadDarFileRequest}
import com.daml.ledger.api.v2.admin.participant_pruning_service.PruneRequest
import com.daml.ledger.api.v2.admin.participant_pruning_service.ZioParticipantPruningService.ParticipantPruningServiceClient
import com.daml.ledger.api.v2.admin.party_management_service.ZioPartyManagementService.PartyManagementServiceClient
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest,
  ListKnownPartiesRequest
}
import com.daml.ledger.api.v2.admin.user_management_service
import com.daml.ledger.api.v2.admin.user_management_service.Right.Kind
import com.daml.ledger.api.v2.admin.user_management_service.ZioUserManagementService.UserManagementServiceClient
import com.daml.ledger.api.v2.admin.user_management_service.{CreateUserRequest, GrantUserRightsRequest}
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  Filters,
  InterfaceFilter,
  WildcardFilter,
  UpdateFormat,
  TransactionFormat,
  TransactionShape,
  EventFormat
}
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.pqs.utils.safeequals.{===, =/=}
import com.google.protobuf.ByteString
import zio.stream.ZStream
import zio.{Chunk, Schedule, ZLayer, durationInt}

case class Api(
    channel: ZLayer[Docker & Service[Ledger], Throwable, ZManagedChannel]
) {
  private val svc = channel >>> (
    PartyManagementServiceClient.live
      ++ UserManagementServiceClient.live
      ++ PackageManagementServiceClient.live
      ++ ParticipantPruningServiceClient.live
      ++ UpdateServiceClient.live
  )

  def participantId = svc(
    PartyManagementServiceClient.getParticipantId(GetParticipantIdRequest()).map(_.participantId)
  )

  /** ACS-delta transaction updates from Genesis, as the given identifier filter sees them. */
  private def updatesFromGenesis(parties: Seq[String], filter: CumulativeFilter.IdentifierFilter) =
    val updateFormat = UpdateFormat.defaultInstance
      .withIncludeTransactions(
        TransactionFormat(
          eventFormat = Some(
            EventFormat.defaultInstance.withFiltersByParty(
              parties.map(p => p -> Filters.of(Seq(CumulativeFilter.of(filter)))).toMap
            )
          ),
          transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
        )
      )
    UpdateServiceClient
      .getUpdates(GetUpdatesRequest.defaultInstance.withBeginExclusive(Genesis.value).withUpdateFormat(updateFormat))

  def getSingleCreatedBlob(parties: Seq[String], transactionId: String) = svc {
    updatesFromGenesis(
      parties,
      CumulativeFilter.IdentifierFilter.WildcardFilter(WildcardFilter(includeCreatedEventBlob = true))
    )
      .dropWhile(_.getTransaction.updateId =/= transactionId)
      .runHead
      .someOrFail(Throwable("Transaction id not found"))
      .map(_.getTransaction.events.to(Chunk))
      .collect(Throwable("Single event expected")) { case Chunk(one) => one }
      .map(_.getCreated.createdEventBlob.toByteArray)
      .filterOrFail(_.nonEmpty)(Throwable("Metadata is empty"))
  }

  /** The created event of a contract, as an interface-only subscription sees it: an interface filter alone, with no
    * template filter attached.
    *
    * @param interfaceQname
    *   fully qualified interface name, `"package-name:Module:Entity"`
    */
  def getCreatedEventViaInterface(parties: Seq[String], interfaceQname: String, contractId: String) = svc {
    val parts = interfaceQname.split(':')
    updatesFromGenesis(
      parties,
      CumulativeFilter.IdentifierFilter.InterfaceFilter(
        InterfaceFilter.of(
          interfaceId = Some(Identifier(s"#${parts(0)}", parts(1), parts(2))),
          includeInterfaceView = true,
          includeCreatedEventBlob = false
        )
      )
    )
      .flatMap(response => ZStream.fromIterable(response.getTransaction.events))
      .filter(_.getCreated.contractId === contractId)
      .runHead
      .someOrFail(Throwable(s"Created event of contract $contractId not found"))
      .map(_.getCreated)
  }

  def listPackageIds = svc(
    PackageManagementServiceClient.listKnownPackages(ListKnownPackagesRequest()).map(_.packageDetails.map(_.packageId))
  )

  def uploadDar(dar: DarFile) = svc(
    PackageManagementServiceClient.uploadDarFile(
      UploadDarFileRequest.defaultInstance.withDarFile(ByteString.copyFrom(dar.darBytes))
    )
  )

  def allocateParty(hint: String) = svc(
    PartyManagementServiceClient
      .allocateParty(AllocatePartyRequest.defaultInstance.withPartyIdHint(hint))
      .map(_.partyDetails.map(_.party))
      .someOrFail(Throwable(s"No party details available after allocating party $hint"))
  )

  def listKnownParties = svc(
    PartyManagementServiceClient
      .listKnownParties(ListKnownPartiesRequest.defaultInstance)
      .map(_.partyDetails)
  )

  def createUser(
      userId: String,
      primaryParty: String,
      canActAs: Seq[String],
      canReadAs: Seq[String],
      canReadAsAnyParty: Boolean
  ) = svc(
    UserManagementServiceClient.createUser(
      CreateUserRequest(
        user = Some(
          com.daml.ledger.api.v2.admin.user_management_service.User.defaultInstance
            .withId(userId)
            .withPrimaryParty(primaryParty)
        ),
        rights = (
          canActAs.map(x => Kind.CanActAs(user_management_service.Right.CanActAs(x)))
            ++ canReadAs.map(x => Kind.CanReadAs(user_management_service.Right.CanReadAs(x)))
            ++ (if canReadAsAnyParty then Seq(Kind.CanReadAsAnyParty(user_management_service.Right.CanReadAsAnyParty()))
                else Seq.empty)
        ).map(user_management_service.Right(_))
      )
    )
  )

  def grantRights(partyId: String) = svc(
    UserManagementServiceClient.grantUserRights(
      GrantUserRightsRequest.defaultInstance
        .withUserId(Ledger.participantAdmin)
        .addRights(user_management_service.Right(Kind.CanActAs(user_management_service.Right.CanActAs(partyId))))
    )
  )

  def pruneLedger(upToOffset: Offset.Absolute) = svc(
    ParticipantPruningServiceClient
      .prune(PruneRequest.defaultInstance.withPruneUpTo(upToOffset.toActiveAtLedgerOffset))
      .logError
      .retry(Schedule.spaced(1.second))
  )
}
