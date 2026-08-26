// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest

import com.digitalasset.scribe.functest.table.Table
import com.digitalasset.scribe.utils.safeequals.===
import zio.*
import zio.internal.stacktracer.SourceLocation
import zio.test.*
import zio.test.Assertion.{anything, contains}
import zio.test.diff.Diff
import zio.test.internal.OptionalImplicit

import java.util.concurrent.atomic.AtomicReference

package object matchers:

  extension [R, E, A](inline io: ZIO[R, E, A])
    inline def is(inline assertion: Assertion[A]): ZIO[R, E, TestResult] =
      ${ Macros.assertZIO_impl('io)('assertion) }
    inline def is(
        inline expected: A
    )(implicit diff: OptionalImplicit[Diff[A]], trace: Trace, sourceLocation: SourceLocation): ZIO[R, E, TestResult] =
      ${ utils.Macros.assertZIO_eq_impl('io)('expected)('diff) }
    inline def are(inline assertion: Assertion[A]): ZIO[R, E, TestResult] =
      ${ Macros.assertZIO_impl('io)('assertion) }
    inline def are(
        inline expected: A
    )(implicit diff: OptionalImplicit[Diff[A]], trace: Trace, sourceLocation: SourceLocation): ZIO[R, E, TestResult] =
      ${ utils.Macros.assertZIO_eq_impl('io)('expected)('diff) }
    inline def returns(inline assertion: Assertion[A]): ZIO[R, E, TestResult] =
      ${ Macros.assertZIO_impl('io)('assertion) }
    inline def returns(
        inline expected: A
    )(implicit diff: OptionalImplicit[Diff[A]], trace: Trace, sourceLocation: SourceLocation): ZIO[R, E, TestResult] =
      ${ utils.Macros.assertZIO_eq_impl('io)('expected)('diff) }

  val empty: Assertion[String]                               = Assertion.isEmptyString
  def stringContaining(substring: String): Assertion[String] = Assertion.containsString(substring)
  def stringMatching(regex: String): Assertion[String]       = Assertion.matchesRegex(regex)
  def iterableContaining[A](value: A): Assertion[Iterable[A]] = Assertion(
    TestArrow.make[Iterable[A], Boolean](seq =>
      TestTrace.boolean(seq.exists(_ === value)) {
        zio.test.ErrorMessage.pretty(seq) + zio.test.ErrorMessage.did + "contain" + zio.test.ErrorMessage.pretty(value)
      }
    )
  )

  class Capture[A: Tag] {
    private val _value = new AtomicReference[Option[A]](None)
    def capture: Assertion[A] = Assertion.assertion("capture") { a =>
      _value.updateAndGet(_.orElse(Some(a))) === Some(a)
    }
    def captureOptional: Assertion[Option[A]] = Assertion.assertion("captureOptional") { actual =>
      val updatedValue = _value.updateAndGet(_.orElse(actual))
      updatedValue.nonEmpty && updatedValue === actual
    }

    def captureFromService: ZIO[A, Nothing, TestResult] = zio.ZIO.service[A] `is` capture
    def get: A                    = _value.get.getOrElse(throw new RuntimeException("Value is not yet captured"))
    override def toString: String = get.toString
  }
  object Capture { def apply[A: Tag] = new Capture[A] }

  def matchesTableUnordered(expectedPayloadsTbl: Table): Assertion[Iterable[Any]] =
    expectedPayloadsTbl.rows.map(contains).reduceOption(_ && _).getOrElse(anything)

end matchers
