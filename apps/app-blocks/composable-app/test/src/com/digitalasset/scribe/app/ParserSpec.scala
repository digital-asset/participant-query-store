// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.app

import com.digitalasset.scribe.utils.safeequals.===
import zio.*
import zio.config.magnolia.describe
import zio.schema.internal.SourceLocation
import zio.test.*

import scala.language.implicitConversions

object ParserSpec extends ZIOSpecDefault {

  def spec: Spec[TestEnvironment & Scope, Any] = suite("CLI Parser")(
    suite("parsing")(
      verify("simple command")(
        "scribe",
        //
        "scribe" -> Success(()),
        ""       -> Error("scribe expected"),
        "foo"    -> Error("scribe expected but unexpected argument foo found")
      ),
      verify("simple mapping")(
        "scribe".as("result"),
        //
        "scribe" -> Success("result"),
        ""       -> Error("scribe expected"),
        "foo"    -> Error("scribe expected but unexpected argument foo found")
      ),
      verify("simple composition")(
        "scribe" - "param1".as("result1") - "param2".as(42),
        //
        "scribe param1 param2"                -> Success(("result1", 42)),
        "scribe"                              -> Error("param1 expected"),
        "scribe param1"                       -> Error("param2 expected"),
        "scribe paramX"                       -> Error("param1 expected but unexpected argument paramX found"),
        "scribe param1 paramX"                -> Error("param2 expected but unexpected argument paramX found"),
        "scribe param1 param2 param3"         -> Error("unexpected argument param3 found"),
        "scribe param1 param2 param3 param 4" -> Error("unexpected arguments found: param3 param 4")
      ),
      verify("compositional mapping")(
        ("scribe" - "param1".as("one-") - "param2".as(2)) `map` (_ * _),
        //
        "scribe param1 param2" -> Success("one-one-")
      ),
      verify("simple variance")(
        "scribe" - ("one".as(1) | "two".as(2)),
        //
        "scribe one"   -> Success(1),
        "scribe two"   -> Success(2),
        "scribe three" -> Error("one of one, two expected but unexpected argument three found"),
        "scribe"       -> Error("one of one, two expected")
      ),
      verify("complex composition")(
        "scribe" - (("one".as(1) | "two".as(2)) | ("three".as(3) - "four".as(4))),
        //
        "scribe one"        -> Success(1),
        "scribe two"        -> Success(2),
        "scribe three four" -> Success((3, 4)),
        "scribe three"      -> Error("four expected"),
        "scribe two four"   -> Error("unexpected argument four found"),
        "scribe three two"  -> Error("four expected but unexpected argument two found")
      ),
      verify("config")(
        "scribe" @@ Command("Main Command") - cliConfig[Config],
        //
        "scribe --foo-bar=value" -> Success(isLayerWith(Config(Foo("value")))),
        "scribe -h" -> Help("""
Usage: scribe [OPTIONS]

Main Command

Options:
  --config file       Path to configuration overrides via an external HOCON file (optional)
  --foo-bar string    bar help
"""),
        "scribe -H" -> Help("""
Usage: scribe [OPTIONS]

Main Command

Options:
  --config file       Path to configuration overrides via an external HOCON file (optional)
                       + Environment variable: SCRIBE_CONFIG
                       + System property:      config
  --foo-bar string    bar help
                       + Environment variable: SCRIBE_FOO_BAR
                       + System property:      foo.bar
""")
      )
    ),
    suite("help")(
      verify("no help provided by default")(
        "scribe",
        //
        "scribe --help" -> Error("unexpected argument --help found")
      ),
      verify("simple command help")(
        "scribe" @@ Command("Help message"),
        //
        "scribe" -> Success(()),
        Seq(
          "scribe --help",
          "scribe -h",
          "scribe --help-verbose",
          "scribe -H"
        ) -> Help(
          """
Usage: scribe

Help message
"""
        )
      ),
      verify("subcommands")(
        "scribe" @@ Command("Main Command")
          - "sub1" @@ Command("Sub Command 1")
          - "sub2" @@ Command("Sub Command 2"),
        //
        "scribe -h" -> Help("""
Usage: scribe COMMAND

Main Command

Commands:
  sub1    Sub Command 1

Run 'scribe COMMAND --help[-verbose]' for more information on a command.
"""),
        "scribe sub1 -h" -> Help("""
Usage: scribe sub1 COMMAND

Sub Command 1

Commands:
  sub2    Sub Command 2

Run 'scribe sub1 COMMAND --help[-verbose]' for more information on a command.
"""),
        "scribe sub1 sub2 -h" -> Help("""
Usage: scribe sub1 sub2

Sub Command 2
""")
      ),
      verify("alternative commands")(
        "scribe" @@ Command("Main Command") -
          (
            ("one" @@ Command("One") - ("foo" @@ Command("Foo") - "fooParam" | "bar" @@ Command("Bar") - "barParam"))
              | "two" @@ Command("Two")
          ),
        //
        "scribe -h" -> Help("""
Usage: scribe COMMAND

Main Command

Commands:
  one    One
  two    Two

Run 'scribe COMMAND --help[-verbose]' for more information on a command.
"""),
        "scribe one -h" -> Help("""
Usage: scribe one COMMAND

One

Commands:
  foo    Foo
  bar    Bar

Run 'scribe one COMMAND --help[-verbose]' for more information on a command.
"""),
        "scribe one bar -h" -> Help("""
Usage: scribe one bar barParam

Bar
"""),
        "scribe two -h" -> Help("""
Usage: scribe two

Two
""")
      ),
      verify("options")(
        "scribe" @@ Command("Main Command")
          - ("opt1" @@ Variant("Option 1") | "opt2" @@ Variant("Option 2")) @@ Param("PARAM", "Available params"),
        "scribe -h" -> Help("""
Usage: scribe PARAM

Main Command

Available params:
  opt1    Option 1
  opt2    Option 2
""".stripMargin)
      )
    )
  )

  inline private def verify[A](label: String)(
      app: CliTree[A],
      assertions: (String | Seq[String], Result[A | Assertion[A]])*
  )(implicit sl: SourceLocation) =
    test(label)(
      assertions
        .flatMap {
          case (inputs: Seq[String], expected) => inputs.map(input => input -> expected)
          case (input: String, expected)       => Seq(input -> expected)
        }
        .map((input, expected) =>
          app
            .parse(input.split("\\s+").filter(_.nonEmpty).toSeq)
            .fold(
              value =>
                expected match
                  case Success(assertion: Assertion[A] @unchecked) => assert(value)(assertion)
                  case exp                                         => assertTrue(Success(value) == exp)
              ,
              str => assertTrue(Error(fansi.Str(str).plainText) == expected),
              str => assertTrue(Help("\n" + fansi.Str(str).plainText + "\n") == expected)
            )
            .label(s"[$input] → $expected")
        )
        .reduce(_ && _)
    )

  case class Input(args: String) {
    override def toString: String = s"[$args]"
  }
  sealed trait Result[+A]
  final case class Success[A](value: A) extends Result[A]
  final case class Error(msg: String)   extends Result[Nothing]
  final case class Help(msg: String)    extends Result[Nothing]

  case class Config(
      @describe("foo help")
      foo: Foo
  )
  case class Foo(
      @describe("bar help")
      bar: String
  )

  private def isLayerWith[A: Tag](expected: A): Assertion[ZLayer[Any, Throwable, A]] =
    zio.test.Assertion.apply(
      TestArrow.make { (layer: ZLayer[Any, Throwable, A]) =>
        zio.Unsafe
          .unsafe { unsafe =>
            zio.Runtime.default.unsafe.run(ZIO.scoped(layer.build).map(_.get[A]))(using zio.Trace.empty, unsafe)
          }
          .foldExit(
            failed = f => TestTrace.fail(s"Reading failed: ${f.prettyPrint}"),
            completed = TestTrace.succeed
          )
      } >>> TestArrow.make { actual =>
        if actual === expected then TestTrace.succeed(true)
        else TestTrace.fail(s"$actual is not equal to $expected")
      }
    )
}
