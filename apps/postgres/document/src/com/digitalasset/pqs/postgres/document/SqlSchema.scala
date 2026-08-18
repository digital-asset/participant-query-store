// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.transcode.schema.*
import io.github.classgraph.{ClassGraph, Resource}

import java.nio.charset.StandardCharsets
import scala.util.Using

object SqlSchema extends SchemaVisitor.Unit:
  type Result = Service
  case class Service(schema: String, mappings: String)

  def collect(entities: Seq[Template[Unit]]) =
    val packages = entities
      .flatMap(t => t.templateId +: t.implements)
      .distinctBy(_.packageId)
      .map(id => (id.packageId, id.packageName, id.packageVersion))
    val getPackageName = packages.map((id, name, version) => id -> name).toMap.apply
    Service(
      schema = migrations,
      mappings = (packages.map(initPackage) ++ entities.flatMap(initEntity(getPackageName))).mkString(
        s"-- DAML<=>PG mappings${System.lineSeparator}do $$$$ begin${System.lineSeparator}",
        System.lineSeparator,
        s"${System.lineSeparator}end; $$$$"
      )
    )

  private def initPackage(id: PackageId, name: PackageName, version: PackageVersion) =
    s"call __initialize_package('$name', '$version', '$id');"

  private def initEntity(getPackageName: PackageId => PackageName)(entity: Template[Unit]) =
    Seq(
      s"""call __initialize_contract_tpe(
         |  '${getPackageName(entity.templateId.packageId)}',
         |  '${entity.templateId.moduleName}',
         |  '${entity.templateId.entityName}',
         |  '${if entity.isInterface then "interface" else "template"}'
         |);""".stripMargin
    ) ++ entity.implements.map { interfaceId =>
      s"""call __initialize_contract_tpe(
         |  '${getPackageName(interfaceId.packageId)}',
         |  '${interfaceId.moduleName}',
         |  '${interfaceId.entityName}',
         |  'interface'
         |);""".stripMargin
    } ++ entity.implements.map { interfaceId =>
      s"""call __initialize_contract_implements(
         |  '${getPackageName(entity.templateId.packageId)}:${entity.templateId.qualifiedName}',
         |  '${getPackageName(interfaceId.packageId)}:${interfaceId.qualifiedName}'
         |);""".stripMargin
    } ++ entity.choices.map { c =>
      s"""call __initialize_exercise_tpe(
         |  '${getPackageName(entity.templateId.packageId)}',
         |  '${entity.templateId.moduleName}',
         |  '${entity.templateId.entityName}',
         |  '${c.name}',
         |  ${c.consuming}
         |);""".stripMargin
    }

  private lazy val migrations =
    val sb  = StringBuilder()
    val sep = System.lineSeparator
    Using.Manager { use =>
      val scanResult = use(ClassGraph().acceptPaths("db/migration").scan())
      val migrations = scanResult.getResourcesWithExtension("sql")
      migrations.sort((a, b) => a.getPath.compareTo(b.getPath))
      migrations.forEachByteArrayThrowingIOException((res: Resource, content: Array[Byte]) => {
        val path       = res.getPath
        val frameStart = "-- " + ">".repeat(path.length + 8) + " --"
        val frameEnd   = "-- " + "<".repeat(path.length + 6) + " --"
        sb ++= s"$sep$frameStart$sep"
        sb ++= s"-- $path (start) --"
        sb ++= s"$sep$frameStart$sep$sep"
        sb ++= String(content, StandardCharsets.UTF_8)
        sb ++= sep
        sb ++= s"$sep$frameEnd$sep"
        sb ++= s"-- $path (end) --"
        sb ++= s"$sep$frameEnd$sep$sep"
      })
    }
    sb.result()

end SqlSchema
