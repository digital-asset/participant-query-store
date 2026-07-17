// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.app

import com.digitalasset.pqs.utils.safeequals.===
import zio.*
import zio.config.magnolia.describe
import zio.schema.internal.SourceLocation
import zio.test.*

import scala.language.implicitConversions

object ParserSpec extends ZIOSpecDefault {

  def spec: Spec[TestEnvironment & Scope, Any] = suite("CLI Parser")(
    suite("parsing")(
      verify("simple command")(
        "pqs",
        //
        "pqs" -> Success(()),
        ""    -> Error("pqs expected"),
        "foo" -> Error("pqs expected but unexpected argument foo found")
      ),
      verify("simple mapping")(
        "pqs".as("result"),
        //
        "pqs" -> Success("result"),
        ""    -> Error("pqs expected"),
        "foo" -> Error("pqs expected but unexpected argument foo found")
      ),
      verify("simple composition")(
        "pqs" - "param1".as("result1") - "param2".as(42),
        //
        "pqs param1 param2"                -> Success(("result1", 42)),
        "pqs"                              -> Error("param1 expected"),
        "pqs param1"                       -> Error("param2 expected"),
        "pqs paramX"                       -> Error("param1 expected but unexpected argument paramX found"),
        "pqs param1 paramX"                -> Error("param2 expected but unexpected argument paramX found"),
        "pqs param1 param2 param3"         -> Error("unexpected argument param3 found"),
        "pqs param1 param2 param3 param 4" -> Error("unexpected arguments found: param3 param 4")
      ),
      verify("compositional mapping")(
        ("pqs" - "param1".as("one-") - "param2".as(2)) `map` (_ * _),
        //
        "pqs param1 param2" -> Success("one-one-")
      ),
      verify("simple variance")(
        "pqs" - ("one".as(1) | "two".as(2)),
        //
        "pqs one"   -> Success(1),
        "pqs two"   -> Success(2),
        "pqs three" -> Error("one of one, two expected but unexpected argument three found"),
        "pqs"       -> Error("one of one, two expected")
      ),
      verify("complex composition")(
        "pqs" - (("one".as(1) | "two".as(2)) | ("three".as(3) - "four".as(4))),
        //
        "pqs one"        -> Success(1),
        "pqs two"        -> Success(2),
        "pqs three four" -> Success((3, 4)),
        "pqs three"      -> Error("four expected"),
        "pqs two four"   -> Error("unexpected argument four found"),
        "pqs three two"  -> Error("four expected but unexpected argument two found")
      ),
      verify("config")(
        "pqs" @@ Command("Main Command") - cliConfig[Config],
        //
        "pqs --foo-bar=value" -> Success(isLayerWith(Config(Foo("value")))),
        "pqs -h" -> Help("""
Usage: pqs [OPTIONS]

Main Command

Options:
  --config file       Path to configuration overrides via an external HOCON file (optional)
  --foo-bar string    bar help
"""),
        "pqs -H" -> Help("""
Usage: pqs [OPTIONS]

Main Command

Options:
  --config file       Path to configuration overrides via an external HOCON file (optional)
                       + Environment variable: PQS_CONFIG
                       + System property:      config
  --foo-bar string    bar help
                       + Environment variable: PQS_FOO_BAR
                       + System property:      foo.bar
""")
      )
    ),
    suite("help")(
      verify("no help provided by default")(
        "pqs",
        //
        "pqs --help" -> Error("unexpected argument --help found")
      ),
      verify("simple command help")(
        "pqs" @@ Command("Help message"),
        //
        "pqs" -> Success(()),
        Seq(
          "pqs --help",
          "pqs -h",
          "pqs --help-verbose",
          "pqs -H"
        ) -> Help(
          """
Usage: pqs

Help message
"""
        )
      ),
      verify("subcommands")(
        "pqs" @@ Command("Main Command")
          - "sub1" @@ Command("Sub Command 1")
          - "sub2" @@ Command("Sub Command 2"),
        //
        "pqs -h" -> Help("""
Usage: pqs COMMAND

Main Command

Commands:
  sub1    Sub Command 1

Run 'pqs COMMAND --help[-verbose]' for more information on a command.
"""),
        "pqs sub1 -h" -> Help("""
Usage: pqs sub1 COMMAND

Sub Command 1

Commands:
  sub2    Sub Command 2

Run 'pqs sub1 COMMAND --help[-verbose]' for more information on a command.
"""),
        "pqs sub1 sub2 -h" -> Help("""
Usage: pqs sub1 sub2

Sub Command 2
""")
      ),
      verify("alternative commands")(
        "pqs" @@ Command("Main Command") -
          (
            ("one" @@ Command("One") - ("foo" @@ Command("Foo") - "fooParam" | "bar" @@ Command("Bar") - "barParam"))
              | "two" @@ Command("Two")
          ),
        //
        "pqs -h" -> Help("""
Usage: pqs COMMAND

Main Command

Commands:
  one    One
  two    Two

Run 'pqs COMMAND --help[-verbose]' for more information on a command.
"""),
        "pqs one -h" -> Help("""
Usage: pqs one COMMAND

One

Commands:
  foo    Foo
  bar    Bar

Run 'pqs one COMMAND --help[-verbose]' for more information on a command.
"""),
        "pqs one bar -h" -> Help("""
Usage: pqs one bar barParam

Bar
"""),
        "pqs two -h" -> Help("""
Usage: pqs two

Two
""")
      ),
      verify("options")(
        "pqs" @@ Command("Main Command")
          - ("opt1" @@ Variant("Option 1") | "opt2" @@ Variant("Option 2")) @@ Param("PARAM", "Available params"),
        "pqs -h" -> Help("""
Usage: pqs PARAM

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
