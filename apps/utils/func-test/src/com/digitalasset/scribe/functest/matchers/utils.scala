// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest.matchers

import zio.internal.stacktracer.SourceLocation
import zio.test.diff.Diff
import zio.test.internal.{OptionalImplicit, SmartAssertions}
import zio.test.{Assertion, CompileVariants, TestResult}
import zio.{Trace, ZIO}

private[matchers] object utils {
  object Macros {
    import scala.quoted.*
    def assertZIO_eq_impl[R: Type, E: Type, A: Type](
        effect: Expr[ZIO[R, E, A]]
    )(expected: Expr[A])(diff: Expr[OptionalImplicit[Diff[A]]])(using Quotes): Expr[ZIO[R, E, TestResult]] = {
      val code         = Expr(zio.test.Macros.showExpr(effect))
      val expectedCode = Expr(zio.test.Macros.showExpr(expected))
      '{
        _root_.com.digitalasset.scribe.functest.matchers.utils.Proxy
          .assertZIOEq($effect, $code, $expectedCode, $expected, $diff)
      }
    }
  }

  object Proxy {
    def assertZIOEq[R, E, A](
        io: ZIO[R, E, A],
        code: String,
        expectedCode: String,
        expected: A,
        diff: OptionalImplicit[Diff[A]]
    )(implicit trace: Trace, sourceLocation: SourceLocation) = {
      val expectedCodeLines = expectedCode.linesIterator.toSeq
      val abridgedExpectedCode =
        if expectedCodeLines.size > 1
        then s"equal to ${expectedCodeLines.take(1).mkString}..."
        else s"equal to $expectedCode"
      CompileVariants.assertZIOProxy(io, code, abridgedExpectedCode)(
        Assertion(SmartAssertions.equalTo(expected)(using diff))
      )
    }
  }
}
