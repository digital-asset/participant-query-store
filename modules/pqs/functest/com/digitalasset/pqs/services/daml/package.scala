// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services

import com.digitalasset.transcode.schema.*
import org.semver4j.Semver
import zio.*

package object daml:
  case class DamlSource(
      name: PackageName,
      version: PackageVersion,
      deps: List[DamlSource],
      contents: List[(ModuleName, String)]
  ):
    def withNameSuffix(suffix: String): DamlSource = if suffix.nonEmpty then withName(s"$name-$suffix") else this
    def withName(name: String): DamlSource         = copy(name = PackageName(name))
    def withVersion(version: String): DamlSource   = copy(version = PackageVersion(version))
    def dependsOn(deps: DamlSource*): DamlSource   = copy(deps = this.deps ++ deps)
    def upgrades(base: DamlSource): DamlSource =
      copy(name = base.name, version = PackageVersion(Semver.parse(base.version).withIncPatch().toString))
  object DamlSource:
    def apply(source: (String, String)*)(implicit name: sourcecode.FullName): DamlSource =
      DamlSource(
        PackageName(name.value.replaceAll("[^a-zA-Z0-9]+", "-")),
        PackageVersion("0.0.0"),
        List.empty,
        source.map((m, c) => ModuleName(m) -> c).toList
      )

  case class DarFile(
      source: DamlSource,
      mainPackageDir: os.Path,
      darPath: os.Path,
      darBytes: Array[Byte],
      packageInfo: List[(PackageName, PackageVersion, PackageId)]
  ):
    val packageId: PackageId     = packageInfo.map((name, version, id) => name -> id).toMap.apply(source.name)
    def packageName: PackageName = source.name

  case class DeployedDar(dar: DarFile)

  type ParticipantId = String

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def inspectMaybe[T: Tag]: UIO[Option[T]] =
    ZIO.environment[Any].mapAttempt(_.asInstanceOf[ZEnvironment[T]].get[T]).option
