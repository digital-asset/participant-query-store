// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.canonical.{ContractFilter, MetadataFilter}
import com.digitalasset.transcode.schema.{
  EntityName,
  Identifier,
  IdentifierFilter,
  ModuleName,
  PackageId,
  PackageName,
  PackageVersion,
  Template
}
import com.digitalasset.zio.daml.DamlSchemaSpec.test
import com.digitalasset.zio.daml.{DamlSchema, KnownEntityIdentifiers}
import zio.internal.stacktracer.SourceLocation
import zio.test.Assertion.{hasSameElements, isEmpty}
import zio.test.{Spec, TestConstructor, TestResult, ZIOSpecDefault, assert}

object DamlSchemaSpec extends ZIOSpecDefault:
  private def identifier(entity: String): Identifier =
    Identifier(PackageId(""), PackageName("PkgName"), PackageVersion("1.0.0"), ModuleName("Module"), EntityName(entity))
  private val i: Identifier = identifier("I")
  private val j: Identifier = identifier("J")
  private val t: Identifier = identifier("T")
  private val s: Identifier = identifier("S")
  private def template(identifier: Identifier, implements: Seq[Identifier] = Seq.empty): Template[Unit] =
    Template[Unit](
      templateId = identifier,
      payload = (),
      key = None,
      isInterface = false,
      implements = implements,
      choices = Seq.empty
    )
  private def interface(identifier: Identifier): Template[Unit] =
    Template[Unit](
      templateId = identifier,
      payload = (),
      key = None,
      isInterface = true,
      implements = Seq.empty,
      choices = Seq.empty
    )
  private val schema: Seq[Template[?]] = Seq(
    interface(i),
    interface(j),
    template(t, Seq(i, j)),
    template(s, Seq(i))
  )

  private def filterString(entities: Identifier*) =
    IdentifierFilter.assertFromString(entities.map(e => e.universalName.replace("#", "")).mkString("| "))

  private def testCase(filter: Seq[Identifier], expectedExcluded: Set[Identifier])(implicit
      testConstructor: TestConstructor[Nothing, TestResult],
      sourceLocation: SourceLocation,
      trace: zio.Trace
  ): testConstructor.Out =
    def names(entities: Iterable[Identifier]) = entities.map(_.entityName).mkString("[", " ", "]")
    test(s"filter for ${names(filter)} extends to ${names(expectedExcluded)}"):
      val knownIdentifiers = new KnownEntityIdentifiers(
        schema = schema,
        contractFilter = ContractFilter(filterString(filter*)),
        metadataFilter = MetadataFilter(IdentifierFilter.AcceptAll)
      )
      val (actualIncluded, actualExcluded) = DamlSchema.findMissingInterfaceImplementations(knownIdentifiers)
      assert(actualIncluded.flatten.toSet)(hasSameElements(filter.toSet))
      assert(actualExcluded.flatten.toSet)(hasSameElements(expectedExcluded))

  def spec: Spec[Any, Nothing] = suite("DamlSchema")(
    suite("query extension")(
      test("empty schema doesn't result in extended filters"):
        val knownIdentifiers = new KnownEntityIdentifiers(
          schema = Seq.empty,
          contractFilter = ContractFilter(IdentifierFilter.AcceptAll),
          metadataFilter = MetadataFilter(IdentifierFilter.AcceptAll)
        )
        val (actualIncluded, actualExcluded) = DamlSchema.findMissingInterfaceImplementations(knownIdentifiers)
        assert(actualIncluded)(isEmpty)
        assert(actualExcluded)(isEmpty)
      ,
      test("schema of [T] and filter for [T] doesn't result in extended filter"):
        val knownIdentifiers = new KnownEntityIdentifiers(
          schema = Seq(template(t)),
          contractFilter = ContractFilter(filterString(t)),
          metadataFilter = MetadataFilter(IdentifierFilter.AcceptAll)
        )
        val (actualIncluded, actualExcluded) = DamlSchema.findMissingInterfaceImplementations(knownIdentifiers)
        assert(actualIncluded)(isEmpty)
        assert(actualExcluded)(isEmpty)
      ,
      testCase(Seq(i, j), Set(t, s)), // Interfaces must include templates implementing them
      testCase(Seq(i, j, t), Set(s)), // Interfaces must include templates implementing them
      testCase(Seq(i, j, s), Set(t)), // Interfaces must include templates implementing them
      testCase(
        Seq(i),
        Set(j, t, s)
      ), // Interfaces must include templates implementing them and interfaces implemented by those templates
      testCase(
        Seq(j),
        Set(i, t)
      ), // Interfaces must include templates implementing them and interfaces implemented by those templates
      testCase(Seq(t), Set(i, j)),    // Template must also include implemented interfaces
      testCase(Seq(s), Set(i)),       // Template must also include implemented interfaces
      testCase(Seq(t, s), Set(i, j)), // Template must also include implemented interfaces
      testCase(Seq(i, t, s), Set(j)), // Template must also include implemented interfaces
      testCase(
        Seq(i, t),
        Set(j, s)
      ), // Crossover of template must also include implemented interfaces and interfaces must include templates implementing them
      testCase(Seq(j, t), Set(i)) // Template must also include implemented interfaces
    )
  )
