// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.daml.ledger.api.v2.transaction_filter.*
import com.digitalasset.canonical.*
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.Error as ArchiveError
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.pqs.configuration.filter.PartyFilterParser.PartyFilter
import com.digitalasset.pqs.utils.safeequals.===
import com.digitalasset.transcode.schema
import com.digitalasset.transcode.schema.*
import zio.{IO, Task, ZIO}

import scala.collection.immutable.ListSet
import scala.reflect.Selectable.reflectiveSelectable

package object ledgerapi:
  import com.daml.ledger.api.v2

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

  private def templateCumulativeFilter(id: schema.Identifier, includeBlob: Boolean): CumulativeFilter =
    CumulativeFilter.of(
      CumulativeFilter.IdentifierFilter.TemplateFilter(
        TemplateFilter.of(
          templateId = Some(id.toRefId),
          includeCreatedEventBlob = includeBlob
        )
      )
    )

  private def interfaceCumulativeFilter(id: schema.Identifier, includeBlob: Boolean): CumulativeFilter =
    CumulativeFilter.of(
      CumulativeFilter.IdentifierFilter.InterfaceFilter(
        InterfaceFilter.of(
          interfaceId = Some(id.toRefId),
          includeInterfaceView = true,
          includeCreatedEventBlob = includeBlob
        )
      )
    )

  def mkEventFormat(userRights: UserRight, knownIds: KnownEntityIdentifiers): EventFormat =
    val entityFilter =
      if knownIds.includesAll then
        val metadataTemplates = knownIds.metadata.diff(knownIds.interfaces)
        Filters.of(
          Seq(
            CumulativeFilter.of(
              CumulativeFilter.IdentifierFilter.WildcardFilter(
                WildcardFilter(includeCreatedEventBlob = knownIds.includesAllMetadata)
              )
            )
          )
          // Selective metadata: add TemplateFilter for templates needing blobs
          // WildcardFilter(blob=false) handles delivery; these add blob via OR
            ++ (if !knownIds.includesAllMetadata then
                  metadataTemplates.map(id => templateCumulativeFilter(id, includeBlob = true)).toSeq
                else Seq.empty)
            ++ knownIds.interfaces
              .map(id => interfaceCumulativeFilter(id, includeBlob = knownIds.metadata.contains(id)))
              .toSeq
        )
      else
        Filters.of(
          (
            knownIds.filtered.templates
              .map(id => templateCumulativeFilter(id, includeBlob = knownIds.filtered.metadata.contains(id)))
              ++ knownIds.filtered.interfaces
                .map(id => interfaceCumulativeFilter(id, includeBlob = knownIds.filtered.metadata.contains(id)))
          ).toSeq
        )
    userRights match
      case UserRight.AsParties(parties) =>
        EventFormat.defaultInstance
          .withFiltersByParty(Map.from[String, Filters](parties.map(_ -> entityFilter)))
      case UserRight.AsAnyParty =>
        EventFormat.defaultInstance.withFiltersForAnyParty(entityFilter)

  extension (id: schema.Identifier)
    private[ledgerapi] def toRefId: com.daml.ledger.api.v2.value.Identifier =
      com.daml.ledger.api.v2.value.Identifier(s"#${id.packageName}", id.moduleName, id.entityName)

  extension (id: schema.Identifier)(using identifiers: KnownEntityIdentifiers)
    private def isIncluded: Boolean         = identifiers.filtered.entities.contains(id)
    private def isMetadataIncluded: Boolean = identifiers.filtered.metadata.contains(id)

  extension (id: com.daml.ledger.api.v2.value.Identifier)(using identifiers: KnownEntityIdentifiers)
    private def toIdentifier(
        representativePackageId: Option[String] = None
    ): IO[UnknownDamlPackageException, schema.Identifier] =
      val effectivePackageId = representativePackageId.getOrElse(id.packageId)
      ZIO
        .fromOption(
          identifiers.byPackageId.get(
            (PackageId(effectivePackageId), ModuleName(id.moduleName), EntityName(id.entityName))
          )
        )
        .orElseFail(
          new UnknownDamlPackageException(
            effectivePackageId,
            id.moduleName,
            id.entityName
          )
        )

  extension (x: Long)
    def toOffset: Offset = x match
      case 0L    => Offset.Genesis
      case other => Offset.Absolute(other)

  def offsets(chunk: Iterable[{ def offset: Long }]) =
    chunk.map(_.offset).mkString(", ")
