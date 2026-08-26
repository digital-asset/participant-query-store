// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.configuration

import fastparse.SingleLineWhitespace.*
import fastparse.*

import scala.annotation.nowarn
import scala.util.Try

package object filter:
  trait Filter[-T]:
    def contains(t: T): Boolean

  object Filter:
    val All = new Filter[Any]:
      def contains(t: Any): Boolean = true
      override def toString: String = "*"
    val None = new Filter[Any]:
      def contains(t: Any): Boolean = false
      override def toString: String = "!*"

  abstract class FilterParser[T]:
    def simpleFilter[$: P]: P[Filter[T]]

    @nowarn
    def apply(input: String): Try[Filter[T]] = parse(input.strip(), run => fullParser(using run)).fold(
      (failure, index, _) => scala.util.Failure(RuntimeException(s"Parsing failed at index $index: $failure")),
      (success, _) => scala.util.Success(success)
    )

    private def fullParser[$: P] = Start ~ union ~ End

    private def union[$: P]: P[Filter[T]] = intersection.rep(min = 1, sep = "|"./).map { filters =>
      filters.fold(Filter.None)((a, b) =>
        new Filter[T]:
          def contains(t: T): Boolean   = a.contains(t) || b.contains(t)
          override def toString: String = filters.map(_.toString).mkString(" | ")
      )
    }

    private def intersection[$: P]: P[Filter[T]] =
      (negation | value).rep(min = 1, sep = "&"./).map { filters =>
        filters.fold(Filter.All)((a, b) =>
          new Filter[T]:
            def contains(t: T): Boolean   = a.contains(t) && b.contains(t)
            override def toString: String = filters.map(_.toString).mkString(" & ")
        )
      }

    private def negation[$: P]: P[Filter[T]] = ("!" ~/ value).map(filter =>
      new Filter[T]:
        def contains(t: T): Boolean   = !filter.contains(t)
        override def toString: String = "!" + filter.toString
    )

    private def value[$: P]: P[Filter[T]] = parens | simpleFilter

    private def parens[$: P]: P[Filter[T]] = ("(" ~/ union ~ ")").map(f =>
      new Filter[T]:
        def contains(t: T): Boolean   = f.contains(t)
        override def toString: String = "(" + f.toString + ")"
    )
  end FilterParser
