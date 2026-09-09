// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import com.daml.ledger.api.v2.admin.package_management_service.ZioPackageManagementService.PackageManagementServiceClient
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.ledger.api.v2.admin.participant_pruning_service.PruneRequest
import com.daml.ledger.api.v2.admin.participant_pruning_service.ZioParticipantPruningService.ParticipantPruningServiceClient
import com.daml.ledger.api.v2.admin.party_management_service.ZioPartyManagementService.PartyManagementServiceClient
import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.admin.user_management_service
import com.daml.ledger.api.v2.admin.user_management_service.Right.Kind
import com.daml.ledger.api.v2.admin.user_management_service.ZioUserManagementService.UserManagementServiceClient
import com.daml.ledger.api.v2.admin.user_management_service.{CreateUserRequest, GrantUserRightsRequest}
import com.daml.ledger.api.v2.command_service.ZioCommandService.CommandServiceClient
import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.commands.*
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.reassignment_commands.*
import com.daml.ledger.api.v2.state_service.GetConnectedSynchronizersRequest
import com.daml.ledger.api.v2.state_service.ZioStateService.StateServiceClient
import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.daml.ledger.api.v2.value.{Identifier, Value}
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.grpc.{ZClientInterceptor, ZManagedChannel}
import com.digitalasset.pqs.utils.safeequals.*
import com.google.protobuf.ByteString
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.Metadata
import zio.stream.ZStream
import zio.*

sealed trait Ledger

object Ledger:
  val container  = ZIO.service[Service[Ledger]]
  val authHeader = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
  private val channel: RLayer[Docker & Service[Ledger], ZManagedChannel] = ZLayer.scoped(
    for
      svc               <- Ledger.container
      adminTokenService <- inspectMaybe[TokenService]
      ca                <- Docker.certificateAuthority
      cert              <- ca.generate("participant")
      mkBuilder = () =>
        NettyChannelBuilder
          .forAddress(svc.exposedAddress, svc.exposedPorts(CantonConf.participantPort))
          .useTransportSecurity()
          .sslContext(
            GrpcSslContexts.forClient
              .keyManager(cert.certificate.privateKey, cert.certificate.certificate)
              .trustManager(ca.certificate.certificate)
              .build()
          )
      interceptor = ZClientInterceptor.intercept { md =>
        ZIO
          .whenCase(adminTokenService) {
            case Some(ts) => ts.getParticipantAdminToken.flatMap { token => md.put(authHeader, token) }
          }
          .orDie
      }
      channel <- ZManagedChannel(mkBuilder(), 128, interceptor).build
    yield channel.get
  )

  private val svc = channel >>> (
    PartyManagementServiceClient.live
      ++ UserManagementServiceClient.live
      ++ PackageManagementServiceClient.live
      ++ ParticipantPruningServiceClient.live
      ++ UpdateServiceClient.live
      ++ CommandServiceClient.live
      ++ StateServiceClient.live
  )

  def participantId = svc(
    PartyManagementServiceClient.getParticipantId(GetParticipantIdRequest()).map(_.participantId)
  )

  def getAllSynchronizers = svc(
    StateServiceClient
      .getConnectedSynchronizers(GetConnectedSynchronizersRequest.defaultInstance)
      .map(_.connectedSynchronizers)
  )

  def listPackageIds = svc(
    PackageManagementServiceClient.listKnownPackages(ListKnownPackagesRequest()).map(_.packageDetails.map(_.packageId))
  )

  def uploadDar(dar: DarFile, withVetting: Boolean) = svc {
    val request = UploadDarFileRequest.defaultInstance
      .withDarFile(ByteString.copyFrom(dar.darBytes))
      .withVettingChange(
        if withVetting then UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES
        else UploadDarFileRequest.VettingChange.VETTING_CHANGE_DONT_VET_ANY_PACKAGES
      )
    PackageManagementServiceClient.uploadDarFile(request)
  }

  def vetDar(dar: DarFile, synchronizer: Synchronizer) = svc {
    val packageRefs = dar.packageInfo.map { case (name, version, id) => VettedPackagesRef(id, name, version) }
    val vet = VettedPackagesChange.Operation.Vet(VettedPackagesChange.Vet.defaultInstance.withPackages(packageRefs))
    val request = UpdateVettedPackagesRequest.defaultInstance
      .withChanges(Seq(VettedPackagesChange(vet)))
      .withSynchronizerId(synchronizer.id)
    PackageManagementServiceClient.updateVettedPackages(request)
  }

  def allocateParty(synchronizerId: String, hint: String) = svc {
    val request = AllocatePartyRequest.defaultInstance
      .withPartyIdHint(hint)
      .withSynchronizerId(synchronizerId)
    PartyManagementServiceClient
      .allocateParty(request)
      .map(_.partyDetails.map(_.party))
      .someOrFail(Throwable(s"No party details available after allocating party $hint"))
  }

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
        .withUserId(CantonConf.participantAdmin)
        .addRights(user_management_service.Right(Kind.CanActAs(user_management_service.Right.CanActAs(partyId))))
    )
  )

  def create(
      templateQname: String,
      args: com.daml.ledger.api.v2.value.Record,
      actAs: Party,
      sync: Synchronizer
  ): ZIO[Docker & Service[Ledger] & DeployedDar, Throwable, SubmitAndWaitForTransactionResponse] = svc {
    for
      templateId <- toIdentifier(templateQname)
      command = CreateCommand.defaultInstance.withTemplateId(templateId).withCreateArguments(args)
      response <- submitAndWaitForTransaction(actAs, sync, Command(Command.Command.Create(command)))
    yield response
  }

  def archive(templateQname: String, contractId: String, actAs: Party, sync: Synchronizer) =
    for
      templateId <- toIdentifier(templateQname)
      command = ExerciseCommand.defaultInstance
        .withTemplateId(templateId)
        .withContractId(contractId)
        .withChoice("Archive")
        .withChoiceArgument(Value(Value.Sum.Record(com.daml.ledger.api.v2.value.Record())))
      response <- submitAndWaitForTransaction(actAs, sync, Command(Command.Command.Exercise(command)))
    yield response

  def reassign(contractId: String, submitter: Party, source: Synchronizer, target: Synchronizer) = svc {
    val unassignCommand = ReassignmentCommand.Command.UnassignCommand(
      UnassignCommand(contractId, source.id, target.id)
    )
    val eventFormat = buildEventFormat(Seq(submitter), wildcardFilter(false))
    for
      unassignResp <- submitAndWaitForReassignment(submitter, unassignCommand)
      reassignmentId = unassignResp.getReassignment.events(0).getUnassigned.reassignmentId
      assignCommand = ReassignmentCommand.Command.AssignCommand(
        AssignCommand(reassignmentId, source.id, target.id)
      )
      _ <- submitAndWaitForReassignment(submitter, assignCommand)
    yield ()
  }

  def getSingleCreatedEvent(
      parties: Seq[Party],
      transactionId: String
  ): ZIO[Docker & Service[Ledger], Throwable, CreatedEvent] = svc {
    updatesFromGenesis(parties, wildcardFilter(true))
      .dropWhile(_.getTransaction.updateId =/= transactionId)
      .runHead
      .someOrFail(Throwable("Transaction id not found"))
      .map(_.getTransaction.events.to(Chunk))
      .collect(Throwable("Single event expected")) { case Chunk(one) => one }
      .map(_.getCreated)
  }

  /** The created event of a contract, as an interface-only subscription sees it: an interface filter alone, with no
    * template filter attached.
    *
    * @param interfaceQname
    *   fully qualified interface name, `"package-name:Module:Entity"`
    */
  def getCreatedEventViaInterface(parties: Seq[Party], interfaceFqname: String, contractId: String) = svc {
    val parts = interfaceFqname.split(':')
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

  def pruneLedger(upToOffset: Offset.Absolute) = svc(
    ParticipantPruningServiceClient
      .prune(PruneRequest.defaultInstance.withPruneUpTo(upToOffset.toActiveAtLedgerOffset))
      .logError
      .retry(Schedule.spaced(1.second))
  )

  private def submitAndWaitForTransaction(actAs: Party, sync: Synchronizer, command: Command) = svc {
    for
      commandId <- nextCommandId
      resp <- CommandServiceClient.submitAndWaitForTransaction(
        SubmitAndWaitForTransactionRequest.defaultInstance.withCommands(
          Commands.defaultInstance
            .withCommandId(commandId)
            .withUserId(actAs.name)
            .withActAs(Seq(actAs.id))
            .withSynchronizerId(sync.id)
            .addCommands(command)
        )
      )
    yield resp
  }

  private def submitAndWaitForReassignment(submitter: Party, command: ReassignmentCommand.Command) =
    for
      commandId <- nextCommandId
      resp <- CommandServiceClient.submitAndWaitForReassignment(
        SubmitAndWaitForReassignmentRequest.defaultInstance.withReassignmentCommands(
          ReassignmentCommands.defaultInstance
            .withUserId(submitter.name)
            .withSubmitter(submitter.id)
            .withCommandId(commandId)
            .addCommands(ReassignmentCommand(command))
        )
      )
    yield resp

  private def toIdentifier(templateQname: String) = ZIO.service[DeployedDar].mapAttempt { dar =>
    val parts = templateQname.split(':')
    Identifier(dar.packageId, parts(0), parts(1))
  }

  /** ACS-delta transaction updates from Genesis, as the given identifier filter sees them. */
  private def updatesFromGenesis(parties: Seq[Party], filter: CumulativeFilter.IdentifierFilter) =
    val updateFormat = UpdateFormat.defaultInstance
      .withIncludeTransactions(
        TransactionFormat(
          eventFormat = Some(buildEventFormat(parties, filter)),
          transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
        )
      )
    UpdateServiceClient
      .getUpdates(GetUpdatesRequest.defaultInstance.withBeginExclusive(Genesis.value).withUpdateFormat(updateFormat))

  private def buildEventFormat(parties: Seq[Party], filter: CumulativeFilter.IdentifierFilter) =
    EventFormat.defaultInstance.withFiltersByParty(
      parties.map(p => p.id -> Filters.of(Seq(CumulativeFilter.of(filter)))).toMap
    )

  private def wildcardFilter(includeCreatedEventBlob: Boolean) =
    CumulativeFilter.IdentifierFilter.WildcardFilter(WildcardFilter(includeCreatedEventBlob))

  private def nextCommandId: UIO[String] = Random.nextUUID.map(_.toString)
