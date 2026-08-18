// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.digitalasset.pqs.grpc.ZManagedChannel
import com.digitalasset.transcode.daml_lf.LfSchemaProcessor
import com.digitalasset.transcode.schema.{IdentifierFilter, SchemaVisitor}
import com.digitalasset.zio.daml.FileCache
import zio.ZIO.{attemptBlocking, fromEither, logDebug, logInfo}
import zio.{Cause, IO, ZIO, ZLayer}

object PackageService:
  val live: ZLayer[ZManagedChannel & FileCache, Throwable, PackageService] =
    PackageServiceClient.live
      >>> ZLayer.fromFunction(PackageService.apply)

case class PackageService(
    packageServiceClient: PackageServiceClient,
    fileCache: FileCache
):

  def listPackages: IO[Throwable, Seq[String]] =
    packageServiceClient
      .listPackages(ListPackagesRequest())
      .map(_.packageIds)
      .tap(pkgIds => logInfo(s"Listed ${pkgIds.length} packages"))

  def processFromLf(schemaVisitor: SchemaVisitor): IO[Throwable, schemaVisitor.Result] =
    for
      ids <- packageServiceClient
        .listPackages(ListPackagesRequest())
        .mapAttempt(_.packageIds.map(Ref.PackageId.assertFromString))
      decoded <- ZIO.withParallelism(5)(ZIO.foreachPar(ids)(fetchAndDecode))
      dictionary = decoded.flatten.toMap
      resultMaybe <- attemptBlocking {
        LfSchemaProcessor.process(dictionary, IdentifierFilter.AcceptAll)(schemaVisitor)
      }
      _      <- logDebug(s"Processed schema from LF for ${dictionary.size} packages")
      result <- fromEither(resultMaybe).mapError(IllegalArgumentException(_))
    yield result

  private def fetchAndDecode(packageId: Ref.PackageId) = for
    contents <- fileCache.cache(s"packageBytes-$packageId")(GetPackageResponse.parseFrom, _.toByteArray)(
      packageServiceClient.getPackage(GetPackageRequest(packageId = packageId))
    )
    _       <- logDebug(s"Fetched contents of package $packageId")
    decoded <- attemptBlocking(decodePackageSignature(contents))
    result <- decoded match
      case Right(signature) =>
        logDebug(s"Parsed signature of package $packageId") *> ZIO.succeed(Some(packageId -> signature))
      case Left(error) =>
        ZIO
          .logWarningCause(
            s"Skipping Daml package $packageId: it could not be decoded (${error.getMessage}). " +
              "This is expected when the participant holds a package built for a newer Daml-LF version " +
              "than this PQS release supports. The package is ignored; upgrade PQS to ingest it. " +
              "Transactions referencing this package cannot be processed and will cause retries, so no " +
              "subsequent transactions will be processed until PQS is upgraded.",
            Cause.fail(error)
          ) *> ZIO.succeed(None)
  yield result
