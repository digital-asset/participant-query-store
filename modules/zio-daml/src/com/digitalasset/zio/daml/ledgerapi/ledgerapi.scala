// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2
import com.digitalasset.canonical.*
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.Error as ArchiveError
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.pqs.configuration.filter.PartyFilterParser.PartyFilter
import com.digitalasset.pqs.utils.safeequals.===
import com.digitalasset.transcode.schema
import zio.{Task, ZIO}

import scala.collection.immutable.ListSet
import scala.reflect.Selectable.reflectiveSelectable

type PartyManagementServiceClient =
  v2.admin.party_management_service.ZioPartyManagementService.PartyManagementServiceClient
val PartyManagementServiceClient =
  v2.admin.party_management_service.ZioPartyManagementService.PartyManagementServiceClient
type UserManagementServiceClient =
  v2.admin.user_management_service.ZioUserManagementService.UserManagementServiceClient
val UserManagementServiceClient =
  v2.admin.user_management_service.ZioUserManagementService.UserManagementServiceClient
type PackageServiceClient =
  v2.package_service.ZioPackageService.PackageServiceClient
val PackageServiceClient =
  v2.package_service.ZioPackageService.PackageServiceClient
type VersionServiceClient =
  v2.version_service.ZioVersionService.VersionServiceClient
val VersionServiceClient =
  v2.version_service.ZioVersionService.VersionServiceClient

val GetParticipantIdRequest     = v2.admin.party_management_service.GetParticipantIdRequest
val ListKnownPartiesRequest     = v2.admin.party_management_service.ListKnownPartiesRequest
val ListUserRightsRequest       = v2.admin.user_management_service.ListUserRightsRequest
val GetPackageRequest           = v2.package_service.GetPackageRequest
val GetPackageResponse          = v2.package_service.GetPackageResponse
val ListPackagesRequest         = v2.package_service.ListPackagesRequest
val GetLedgerApiVersionRequest  = v2.version_service.GetLedgerApiVersionRequest
val GetLedgerApiVersionResponse = v2.version_service.GetLedgerApiVersionResponse

val CanActAsKind           = v2.admin.user_management_service.Right.Kind.CanActAs
val CanReadAsKind          = v2.admin.user_management_service.Right.Kind.CanReadAs
val CanReadAsAnyPartyKind  = v2.admin.user_management_service.Right.Kind.CanReadAsAnyParty
val CanActAsRight          = v2.admin.user_management_service.Right.CanActAs
val CanReadAsRight         = v2.admin.user_management_service.Right.CanReadAs
val CanReadAsAnyPartyRight = v2.admin.user_management_service.Right.CanReadAsAnyParty

val Ref = com.digitalasset.daml.lf.data.Ref

def decodePackageSignature(
    response: v2.package_service.GetPackageResponse
): Either[ArchiveError, Ast.PackageSignature] =
  val lfArchiveBuilder = com.digitalasset.daml.lf.archive.DamlLf.Archive.newBuilder()
  val archive          = lfArchiveBuilder.setHash(response.hash).setPayload(response.archivePayload).build()
  Decode.decodeArchiveSchema(archive).map(_._2)

type RightKind = v2.admin.user_management_service.Right.Kind
val RightKind = v2.admin.user_management_service.Right.Kind
private type PartyHandler = PartyFilter => Task[UserRight]

def handleNoAuthPartyFilter(partyFilter: PartyFilter)(resolveUserRight: PartyHandler): Task[UserRight] =
  if partyFilter.toString === PartyFilter.All.toString
  then ZIO.succeed(UserRight.AsAnyParty)
  else resolveUserRight(partyFilter)

def convertRights(rights: Seq[RightKind], partyFilter: PartyFilter): UserRight =
  if rights.exists {
      case CanReadAsAnyPartyKind(CanReadAsAnyPartyRight()) => true
      case _                                               => false
    }
  then UserRight.AsAnyParty
  else
    UserRight.AsParties(
      rights
        .collect {
          case CanActAsKind(CanActAsRight(party)) if partyFilter.contains(party) =>
            Party(Ref.Party.assertFromString(party))
          case CanReadAsKind(CanReadAsRight(party)) if partyFilter.contains(party) =>
            Party(Ref.Party.assertFromString(party))
        }
        .distinct
        .to(ListSet)
    )

extension (id: schema.Identifier)
  private[ledgerapi] def toRefId: com.daml.ledger.api.v2.value.Identifier =
    com.daml.ledger.api.v2.value.Identifier(s"#${id.packageName}", id.moduleName, id.entityName)

extension (x: Long)
  def toOffset: Offset = x match
    case 0L    => Offset.Genesis
    case other => Offset.Absolute(other)

def offsets(chunk: Iterable[{ def offset: Long }]) =
  chunk.map(_.offset).mkString(", ")
