// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.canonical.{ContractFilter, MetadataFilter}
import com.digitalasset.scribe.grpc.ZManagedChannel
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml.ledgerapi.PackageService
import zio.ZIO.*
import zio.{Tag, ZIO, ZLayer}

import scala.collection.immutable

object DamlSchema:
  val schema: ZLayer[ZManagedChannel & FileCache, Throwable, Schema] =
    PackageService.live >>> ZLayer.fromZIO(DamlSchema.getSchema)

  def produce(sp: SchemaVisitor)(implicit tag: Tag[sp.Result]): ZLayer[Schema & ContractFilter, Throwable, sp.Result] =
    ZLayer.fromZIO(DamlSchema.processFromDescriptors(sp))

  //

  private def processFromDescriptors(sp: SchemaVisitor) = for
    schema         <- service[Schema]
    contractFilter <- service[ContractFilter]
    ids = KnownEntityIdentifiers(schema.entities, contractFilter, MetadataFilter(IdentifierFilter.AcceptAll))
    _ <- verifyFilterSelectionNotEmpty(ids)
    entitiesToAdd <- DamlSchemaDamlVersionSpecific.interfaceImplementationsIntegrityAction(
      ids
    ) // Canton version specific action,
    // on Canton 2, we do not support interface-only filters, so we fail
    // on Canton 3 we generate a set of all the required templates and interfaces so PQS can process events
    _ <- logDebug(s"Processing schema descriptors for ${sp.getClass.getName}")
    resultMaybe <- attemptBlocking {
      DescriptorSchemaProcessor.process(schema, sp, id => (contractFilter.filter(id) || entitiesToAdd.contains(id)))
    }
    _      <- logDebug(s"Processed schema descriptors for ${sp.getClass.getName}")
    result <- fromEither(resultMaybe).mapError(Throwable(_))
  yield result

  private def verifyFilterSelectionNotEmpty(knownEntityIdentifiers: KnownEntityIdentifiers): ZIO[Any, Throwable, Unit] =
    ZIO
      .attempt {
        require(
          knownEntityIdentifiers.filtered.entities.nonEmpty,
          "No user-supplied Daml models found on connected ledger. Please, deploy your application's DAR to the ledger before running Scribe."
        )
      }

  /** Finds all entity types that are not included in the filter but subset of them will be reported by update stream
    * from gRPC API. The expanded set of types will be used to bootstrap database schema, so all elements that may occur
    * in the stream can be stored.
    *
    * For templates include all interfaces implemented by the template T as the stream will contain choice exercises
    * from those interfaces on contracts of template T. For interface I, the stream will contain create arguments for
    * contracts of templates implementing I, choices on all contracts implementing I (that includes I's choices,
    * template's choices and another interfaces')
    *
    *   1. For each template loaded in participant, create a set containing the template and all interfaces implemented
    *      by it {templateId, iface1, iface2, iface3...}.
    *   1. For each set check if either all elements are selected by the filter or onne elements are selected by the
    *      filter.
    *   1. If the above is not true, split the set into included and excluded subsets.
    *
    * The method returns a pair of (included, excluded) sequences of sets. The first sequence is a sequence of included
    * sets generated in step 3., the other element is a sequence of excluded sets from the point 3.
    */
  def findMissingInterfaceImplementations(
      ids: KnownEntityIdentifiers
  ): (immutable.Iterable[Set[Identifier]], immutable.Iterable[Set[Identifier]]) =
    val (inconsistentIncluded, inconsistentExcluded) = ids.implements.toSeq.view
      .map((tId, iFaces) => (iFaces + tId).partition(ids.filtered.entities.contains))
      .filter((included, excluded) => included.nonEmpty && excluded.nonEmpty)
      .toSeq
      .unzip
    (inconsistentIncluded, inconsistentExcluded)

  private def getSchema = for
    packageService <- service[PackageService]
    fileCache      <- service[FileCache]
    packageIds     <- packageService.listPackages
    key = s"descriptors-${packageIds.distinct.sorted.hashCode().toHexString}"
    schema <- fileCache.cache(key)(Schema.deserialize, Schema.serialize)(getSchemaFromLedger)
    _      <- logDebug(Debug.showDescriptorsFlat(schema))
  yield schema

  private def getSchemaFromLedger = for
    packageService <- service[PackageService]
    _              <- logInfo("Fetching schema descriptors from ledger")
    result         <- packageService.processFromLf(DescriptorVisitor)
    _              <- logDebug("Fetched schema descriptors from ledger")
  yield result.useStrictPackageMatching(true)

  def pretty(ids: Iterable[Set[Identifier]]): String =
    ids.flatten.toSeq.distinct.sorted
      .map(id => s"${id.packageName}:${id.moduleName}:${id.entityName}")
      .mkString(",")
