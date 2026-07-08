// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.package_service
import com.digitalasset.daml.lf.archive.DarParser
import com.digitalasset.transcode.schema.{SchemaVisitor, Template}
import com.digitalasset.zio.daml.FileCache
import com.google.protobuf.ByteString
import zio.*
import zio.test.*

import java.util.zip.ZipInputStream

object PackageServiceSpec extends ZIOSpecDefault:

  private val badPackageId   = "000000"
  private val goodPackageDar = "/com/digitalasset/zio/daml/ledgerapi/decodable-test-module.dar"

  private val goodPackagePayloads =
    ZIO
      .fromAutoCloseable(ZIO.attempt(ZipInputStream(getClass.getResourceAsStream(goodPackageDar))))
      .flatMap(dar => ZIO.fromEither(DarParser.readArchive(goodPackageDar, dar)))
      .map(_.all.map(archive => archive.getHash -> archive.getPayload).toMap)

  private def stubClient(payloads: Map[String, ByteString]): PackageServiceClient =
    new PackageServiceClient:
      def listPackages(request: package_service.ListPackagesRequest) =
        ZIO.succeed(package_service.ListPackagesResponse(packageIds = payloads.keys.toSeq))
      def getPackage(request: package_service.GetPackageRequest) =
        ZIO.succeed(
          package_service.GetPackageResponse.defaultInstance
            .withHash(request.packageId)
            .withArchivePayload(payloads(request.packageId))
        )
      def getPackageStatus(request: package_service.GetPackageStatusRequest) =
        ZIO.die(new NotImplementedError("getPackageStatus is not used by this test"))
      def listVettedPackages(request: package_service.ListVettedPackagesRequest) =
        ZIO.die(new NotImplementedError("listVettedPackages is not used by this test"))

  private val schemaVisitor = new SchemaVisitor.Unit:
    type Result = Seq[Template[Unit]]
    def collect(entities: Seq[Template[Unit]]): Seq[Template[Unit]] = entities

  private val fileCache: RIO[Scope, FileCache] =
    for
      dir        <- ZIO.acquireRelease(ZIO.attempt(os.temp.dir()))(d => ZIO.attempt(os.remove.all(d)).ignore)
      semaphores <- Ref.Synchronized.make(Map.empty[String, Semaphore])
    yield FileCache(dir, semaphores)

  def spec = suite("PackageService")(
    test("skips an undecodable package while still processing the decodable ones"):
      for
        goodPackage <- goodPackagePayloads
        cache       <- fileCache
        payloads = goodPackage + (badPackageId -> ByteString.copyFromUtf8("not a valid daml-lf archive"))
        service  = PackageService(stubClient(payloads), cache)
        result <- service.processFromLf(schemaVisitor)
        logs   <- ZTestLogger.logOutput
      yield assertTrue(
        // The decodable package's template is still processed.
        result.exists(t =>
          t.templateId.moduleName == "DecodableTestModule" && t.templateId.entityName == "DecodableTestTemplate"
        ),
        // The undecodable one is skipped with a warning rather than failing the whole thing.
        logs.exists(log =>
          log.logLevel == LogLevel.Warning &&
            log.message().contains(badPackageId) &&
            log.message().contains("could not be decoded")
        )
      )
  )
