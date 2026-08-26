// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.configuration

import com.digitalasset.scribe.configuration.filter.{ContractFilterParser, DottedName, QualifiedName}
import com.digitalasset.scribe.configuration.filter.ContractFilterParser.*
import zio.test.{ZIOSpecDefault, assert, assertTrue}
import zio.test.Assertion.*

import scala.language.implicitConversions

object ContractFilterParserSpec extends ZIOSpecDefault:
  private def matchesIdentifier(filter: ContractFilter)(fqn: String) =
    val segments   = fqn.split(':')
    val pkgName    = segments(0)
    val moduleName = DottedName(segments(1).split('.').toSeq)
    val name       = DottedName(segments(2).split('.').toSeq)
    val qn         = QualifiedName(moduleName, name)
    filter.contains((pkgName, qn))

  def spec =
    suite("ContractFilterParser containsIdentifier")(
      test("matches simple FQN") {
        val filter = ContractFilterParser("foo.bar.MyClass").get
        assertTrue(
          // match
          matchesIdentifier(filter)("PackageName:foo.bar:MyClass"),
          // wrong constructor
          !matchesIdentifier(filter)("PackageName:foo.bar:OtherClass"),
          // wrong package
          !matchesIdentifier(filter)("PackageName:foo.other:MyClass")
        )
      },
      test("matches simple FQN with package ID") {
        val filter = ContractFilterParser("PackageName:foo.bar.MyClass").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:foo.bar:MyClass"),
          !matchesIdentifier(filter)("DoesNotMatch:foo.bar:MyClass")
        )
      },
      test("allows asterisk at root") {
        val filter = ContractFilterParser("*").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:any:Any")
        )
      },
      test("allows asterisk at root with package ID") {
        val filter = ContractFilterParser("PackageName:*").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:any:Any"),
          !matchesIdentifier(filter)("DoesNotMatch:any:Any")
        )
      },
      test("matches union of simple FQNs") {
        val filter = ContractFilterParser("foo.bar.MyClass | foo.bar.OtherClass").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:foo.bar:MyClass"),
          matchesIdentifier(filter)("PackageName:foo.bar:OtherClass")
        )
      },
      test("matches union of three simple FQNs") {
        val filter = ContractFilterParser("A.Foo | B.Foo | C.Foo").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:A:Foo"),
          matchesIdentifier(filter)("PackageName:B:Foo"),
          matchesIdentifier(filter)("PackageName:C:Foo")
        )
      },
      test("matches intersection of simple FQNs") {
        val filter = ContractFilterParser("foo.bar.* & foo.bar.OtherClass").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:foo.bar:OtherClass"),
          !matchesIdentifier(filter)("PackageName:foo.bar:MyClass")
        )
      },
      test("matches intersection of simple FQNs and negation") {
        val filter = ContractFilterParser("foo.bar.* & foo.bar.OtherClass & !foo.bar.ThirdClass").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:foo.bar:OtherClass"),
          !matchesIdentifier(filter)("PackageName:foo.bar:MyClass"),
          !matchesIdentifier(filter)("PackageName:foo.bar:ThirdClass")
        )
      },
      test("matches negation of simple FQN") {
        val filter = ContractFilterParser("!foo.bar.MyClass").get
        assertTrue(
          !matchesIdentifier(filter)("PackageName:foo.bar:MyClass"),
          matchesIdentifier(filter)("PackageName:any.other:FQN")
        )
      },
      test("matches intersection of everything with negation of FQN") {
        val filter = ContractFilterParser("* &!foo.bar.MyClass").get
        assertTrue(
          !matchesIdentifier(filter)("PackageName:foo.bar:MyClass"),
          matchesIdentifier(filter)("PackageName:any.other:FQN")
        )
      },
      test("intersection has precedence over union") {
        val filter = ContractFilterParser("a.b.Foo & a.b.Bar | a.b.Baz").get
        assertTrue(
          !matchesIdentifier(filter)("PackageName:a.b:Foo"),
          !matchesIdentifier(filter)("PackageName:a.b:Bar"),
          matchesIdentifier(filter)("PackageName:a.b:Baz")
        )
      },
      test("negation has precedence over union and intersection") {
        val filter = ContractFilterParser("a.b.Foo & !a.b.Bar | a.b.Baz").get
        assertTrue(
          !matchesIdentifier(filter)("PackageName:a.b:Bar"),
          matchesIdentifier(filter)("PackageName:a.b:Foo"),
          matchesIdentifier(filter)("PackageName:a.b:Baz")
        )
      },
      test("matches expression with top-level parentheses") {
        val filter = ContractFilterParser("(a.b.Foo & a.b.Bar | a.b.Baz)").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:a.b:Baz")
        )
      },
      test("parentheses override precedence") {
        val filter = ContractFilterParser("a.b.c.Foo & (a.b.c.Bar | a.b.c.*)").get
        assertTrue(
          matchesIdentifier(filter)("PackageName:a.b.c:Foo")
        )
      }
    ) + suite("ContractFilterParser parsing")(
      test("parses complex expression without whitespaces") {
        assertTrue(
          ContractFilterParser("(a.b.c.*&!(a.b.c.Foo|a.b.c.Bar))|(g.e.f.Baz&!(g.e.f.Qux|g.e.f.Quux))").isSuccess
        )
      },
      test("parses complex expression with many whitespaces") {
        assertTrue(
          ContractFilterParser(
            "  (  a.b.c.*  &  !  (  a.b.c.Foo  |  a.b.c.Bar  )  )  |  (  g.e.f.Baz  &  !  (  g.e.f.Qux  |  g.e.f.Quux  )  )  "
          ).isSuccess
        )
      },
      test("does not accept spaces between FQN dots") {
        assertTrue(ContractFilterParser("foo .bar.MyClass").isFailure)
        assertTrue(ContractFilterParser("foo. bar.MyClass").isFailure)
        assertTrue(ContractFilterParser("foo.bar. MyClass").isFailure)
      },
      test("does not accept spaces in FQN identifiers") {
        assertTrue(ContractFilterParser("foo.bar.M yClass").isFailure)
        assertTrue(ContractFilterParser("foo.bar.My Class").isFailure)
      },
      test("does not accept spaces in package name") {
        assertTrue(
          ContractFilterParser(
            s"Package Name:foo.bar.MyClass"
          ).isFailure
        )
      }
    ) + suite("ContractFilter toString")(
      // list of cleanly formatted filter expressions for which .toString should be the inverse to the original expression
      contractFilterExpressionToStringInverse("*"),
      contractFilterExpressionToStringInverse("a.b.c.Bar"),
      contractFilterExpressionToStringInverse("a.b.c.*"),
      contractFilterExpressionToStringInverse("PackageName:a.b.c.Foo"),
      contractFilterExpressionToStringInverse("!a.b.c.Bar"),
      contractFilterExpressionToStringInverse("a.b.c.Foo & a.b.c.Bar"),
      contractFilterExpressionToStringInverse("(a.b.c.Foo | a.b.c.Bar)"),
      contractFilterExpressionToStringInverse("(a.b.c.* & !(a.b.c.Foo | a.b.c.Bar) | g.e.f.Baz)"),
      contractFilterExpressionToStringInverse(
        "(PackageName:a.b.c.* & !(PackageName:a.b.c.Foo | PackageName:a.b.c.Bar) | PackageName:g.e.f.Baz)"
      )
    )

  private def contractFilterExpressionToStringInverse(expr: String) =
    test(s"""ContractFilterParser("$expr").toString === "$expr"""") {
      assert(ContractFilterParser(expr).fold(e => throw e, identity).toString)(equalTo(expr))
    }
