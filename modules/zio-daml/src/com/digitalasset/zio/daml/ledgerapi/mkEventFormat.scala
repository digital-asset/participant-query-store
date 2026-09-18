// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.transaction_filter.*
import com.digitalasset.canonical.UserRight
import com.digitalasset.transcode.schema
import com.digitalasset.zio.daml.DamlSchema

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

def mkEventFormat(userRights: UserRight, damlSchema: DamlSchema): EventFormat =
  val entityFilter =
    if damlSchema.includesAll then
      val metadataTemplates = damlSchema.metadata.diff(damlSchema.interfaces)
      Filters.of(
        Seq(
          CumulativeFilter.of(
            CumulativeFilter.IdentifierFilter.WildcardFilter(
              WildcardFilter(includeCreatedEventBlob = damlSchema.includesAllMetadata)
            )
          )
        )
        // Selective metadata: add TemplateFilter for templates needing blobs
        // WildcardFilter(blob=false) handles delivery; these add blob via OR
          ++ (if !damlSchema.includesAllMetadata then
                metadataTemplates.map(id => templateCumulativeFilter(id, includeBlob = true)).toSeq
              else Seq.empty)
          ++ damlSchema.interfaces
            .map(id => interfaceCumulativeFilter(id, includeBlob = damlSchema.metadata.contains(id)))
            .toSeq
      )
    else
      Filters.of(
        (
          damlSchema.filtered.templates
            .map(id => templateCumulativeFilter(id, includeBlob = damlSchema.filtered.metadata.contains(id)))
            ++ damlSchema.filtered.interfaces
              .map(id => interfaceCumulativeFilter(id, includeBlob = damlSchema.filtered.metadata.contains(id)))
        ).toSeq
      )
  userRights match
    case UserRight.AsParties(parties) =>
      EventFormat.defaultInstance
        .withFiltersByParty(Map.from[String, Filters](parties.map(_ -> entityFilter)))
    case UserRight.AsAnyParty =>
      EventFormat.defaultInstance.withFiltersForAnyParty(entityFilter)
