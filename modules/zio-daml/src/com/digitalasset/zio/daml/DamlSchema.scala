// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.canonical.{ContractFilter, MetadataFilter}
import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.pqs.utils.safeequals.===
import com.digitalasset.zio.daml.ledgerapi.UnknownDamlPackageException
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml.ledgerapi.PackageService
import zio.ZIO.*
import zio.*

import scala.collection.immutable

final class DamlSchema(
    val schema: Dictionary[Descriptor],
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
    schema.entities.map(_.templateId.packageId).toSet
  val entities: Set[Identifier] =
    schema.entities.map(_.templateId).toSet
  val templates: Set[Identifier] =
    schema.entities.filterNot(_.isInterface).map(_.templateId).toSet
  val interfaces: Set[Identifier] =
    schema.entities.filter(_.isInterface).map(_.templateId).toSet
  val metadata: Set[Identifier] =
    entities.filter(metadataFilter.filter)
  val byPackageId: Map[(PackageId, ModuleName, EntityName), Identifier] =
    entities.map(id => (id.packageId, id.moduleName, id.entityName) -> id).toMap
  val implements: Map[Identifier, Set[Identifier]] =
    schema.entities.filterNot(_.isInterface).groupMapReduce(x => x.templateId)(x => x.implements.toSet)(_ ++ _)

  lazy val withoutInterfaces: DamlSchema =
    new DamlSchema(
      schema,
      ContractFilter(id => !interfaces.contains(id) && contractFilter.filter(id)),
      metadataFilter
    )

  lazy val filtered: DamlSchema =
    if includesAll
    then this
    else
      new DamlSchema(
        Dictionary(schema.entities.filter(x => contractFilter.filter(x.templateId))),
        contractFilter,
        metadataFilter
      )

  def process(sp: SchemaVisitor) =
    for
      _ <- ZIO.attempt {
        require(
          filtered.entities.nonEmpty,
          "No user-supplied Daml models found on connected ledger. Please, deploy your application's DAR to the ledger before running PQS."
        )
      }
      entitiesToAdd <- interfaceImplementationsIntegrityAction
    yield DescriptorSchemaProcessor.process(schema, sp, id => (contractFilter.filter(id) || entitiesToAdd.contains(id)))

  def toIdentifier(
      packageId: String,
      moduleName: String,
      entityName: String
  ): ZIO[Any, UnknownDamlPackageException, Identifier] =
    val typedTuple = (PackageId(packageId), ModuleName(moduleName), EntityName(entityName))
    ZIO
      .fromOption(byPackageId.get(typedTuple))
      .orElseFail(new UnknownDamlPackageException(packageId, moduleName, entityName))

  extension (id: com.daml.ledger.api.v2.value.Identifier)
    def toIdentifier(representativePackageId: Option[String] = None): IO[Throwable, Identifier] =
      val effectivePackageId = representativePackageId.getOrElse(id.packageId)
      this.toIdentifier(effectivePackageId, id.moduleName, id.entityName)

  extension (id: Identifier)
    def isIncluded: Boolean         = filtered.entities.contains(id)
    def isMetadataIncluded: Boolean = filtered.metadata.contains(id)

  private def interfaceImplementationsIntegrityAction: Task[Set[Identifier]] =
    val (inconsistentIncluded, inconsistentExcluded) = findMissingInterfaceImplementations
    logInfo(
      s"Extending filter to match missing entities. Filter selects [${DamlSchema.pretty(inconsistentExcluded)}]  which need to be included along with [${DamlSchema.pretty(inconsistentIncluded)}]"
    ).map(_ => inconsistentExcluded.flatten.toSet)

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
  private[daml] def findMissingInterfaceImplementations
      : (immutable.Iterable[Set[Identifier]], immutable.Iterable[Set[Identifier]]) =
    val (inconsistentIncluded, inconsistentExcluded) = implements.toSeq.view
      .map((tId, iFaces) => (iFaces + tId).partition(filtered.entities.contains))
      .filter((included, excluded) => included.nonEmpty && excluded.nonEmpty)
      .toSeq
      .unzip
    (inconsistentIncluded, inconsistentExcluded)

object DamlSchema:
  val layer: ZLayer[ZManagedChannel & FileCache & ContractFilter & MetadataFilter, Throwable, DamlSchema] =
    PackageService.live >>> ZLayer.fromZIO(DamlSchema.getSchema)

  def produce(sp: SchemaVisitor)(implicit tag: Tag[sp.Result]): ZLayer[DamlSchema, Throwable, sp.Result] =
    ZLayer.fromZIO(processFromDescriptors(sp))

  //

  private def processFromDescriptors(sp: SchemaVisitor) = for
    schema <- service[DamlSchema]
    // on Canton 3 we generate a set of all the required templates and interfaces so PQS can process events

    _           <- logDebug(s"Processing schema descriptors for ${sp.getClass.getName}")
    resultMaybe <- schema.process(sp)
    _           <- logDebug(s"Processed schema descriptors for ${sp.getClass.getName}")
    result      <- fromEither(resultMaybe).mapError(Throwable(_))
  yield result

  private def getSchema = for
    packageService <- service[PackageService]
    fileCache      <- service[FileCache]
    contractFilter <- service[ContractFilter]
    metadataFilter <- service[MetadataFilter]
    packageIds     <- packageService.listPackages
    key = s"descriptors-${packageIds.distinct.sorted.hashCode().toHexString}"
    schema <- fileCache.cache(key)(Schema.deserialize, Schema.serialize)(getSchemaFromLedger)
    _      <- logDebug(Debug.showDescriptorsFlat(schema))
  yield DamlSchema(schema, contractFilter, metadataFilter)

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
