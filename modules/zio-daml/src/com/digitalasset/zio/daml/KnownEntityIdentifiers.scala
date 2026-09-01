// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.canonical.{ContractFilter, MetadataFilter}
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml
import zio.ZLayer
import com.digitalasset.pqs.utils.safeequals.===

object KnownEntityIdentifiers:
  val live: ZLayer[Schema & ContractFilter & MetadataFilter, Throwable, KnownEntityIdentifiers] =
    (ZLayer.service[ContractFilter] ++ ZLayer.service[MetadataFilter]).flatMap(env =>
      ZLayer.succeed(ContractFilter(IdentifierFilter.AcceptAll))
        >>> DamlSchema.produce(Visitor(env.get[ContractFilter], env.get[MetadataFilter]))
    )

  private class Visitor(
      contractFilter: ContractFilter,
      metadataFilter: MetadataFilter
  ) extends SchemaVisitor.Unit:
    type Result = KnownEntityIdentifiers
    def collect(entities: Seq[Template[Unit]]) =
      KnownEntityIdentifiers(entities, contractFilter, metadataFilter)
  end Visitor
end KnownEntityIdentifiers

class KnownEntityIdentifiers(
    schema: Seq[Template[?]],
    contractFilter: ContractFilter,
    metadataFilter: MetadataFilter
):
  val includesAll: Boolean =
    contractFilter.filter.toString() === IdentifierFilter.AcceptAll.toString()
  val includesAllMetadata: Boolean =
    metadataFilter.filter.toString() === IdentifierFilter.AcceptAll.toString()
  val excludesAllMetadata: Boolean =
    metadataFilter.filter.toString() === IdentifierFilter.RejectAll.toString()

  val packageIds: Set[PackageId] =
    schema.map(_.templateId.packageId).toSet
  val entities: Set[Identifier] =
    schema.map(_.templateId).toSet
  val templates: Set[Identifier] =
    schema.filterNot(_.isInterface).map(_.templateId).toSet
  val interfaces: Set[Identifier] =
    schema.filter(_.isInterface).map(_.templateId).toSet
  val metadata: Set[Identifier] =
    entities.filter(metadataFilter.filter)
  val byPackageId: Map[(PackageId, ModuleName, EntityName), Identifier] =
    entities.map(id => (id.packageId, id.moduleName, id.entityName) -> id).toMap
  val byPackageName: Map[(PackageName, ModuleName, EntityName), Identifier] =
    entities.map(id => (id.packageName, id.moduleName, id.entityName) -> id).toMap
  val implements: Map[Identifier, Set[Identifier]] =
    schema.filterNot(_.isInterface).groupMapReduce(x => x.templateId)(x => x.implements.toSet)(_ ++ _)

  lazy val withoutInterfaces: KnownEntityIdentifiers =
    new KnownEntityIdentifiers(
      schema,
      ContractFilter(id => !interfaces.contains(id) && contractFilter.filter(id)),
      metadataFilter
    )

  lazy val filtered: KnownEntityIdentifiers =
    if includesAll
    then this
    else
      new KnownEntityIdentifiers(
        schema.filter(x => contractFilter.filter(x.templateId)),
        contractFilter,
        metadataFilter
      )
