// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services

import com.digitalasset.pqs.utils.safeequals.=/=
import com.digitalasset.transcode.schema.*
import org.semver4j.Semver
import zio.{FiberRef, Task, Unsafe}

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

  /** Allocated Daml Party. Party ID is populated after the party is allocated. */
  case class Party(prefix: String):
    private[daml] val _name: FiberRef[Option[String]] = Unsafe.unsafe { unsafe ?=>
      FiberRef.unsafe.make(Option.empty[String])
    }
    def name: Task[String] = _name.get.someOrFail(notInitialized)
    private[daml] val _id: FiberRef[Option[String]] = Unsafe.unsafe { unsafe ?=>
      FiberRef.unsafe.make(Option.empty[String])
    }
    def id: Task[String]       = _id.get.someOrFail(notInitialized)
    private val notInitialized = new RuntimeException(s"Party $prefix is not allocated")
  end Party

  /** Service representing allocated parties */
  final case class Parties(get: Seq[Party])
  type ParticipantId = String

  final case class User(
      primaryParty: Party,
      canActAs: Seq[Party] = Seq.empty,
      canReadAs: Seq[Party] = Seq.empty,
      canReadAsAnyParty: Boolean = false
  ) {
    val id = primaryParty.id.map(_.takeWhile(_ =/= ':'))
  }

  final case class Users(get: Seq[User])

  trait Ledger

  object Ledger:
    val participantPort: Int = 6865
    val adminApiPort: Int    = 6866
    val participantAdmin     = "participant_admin"
