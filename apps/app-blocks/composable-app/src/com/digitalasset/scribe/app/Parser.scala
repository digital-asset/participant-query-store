// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.app

import com.digitalasset.scribe.app.{CliTree, ParseResult}
import com.digitalasset.scribe.utils.safeequals.===
object Parser {
  def parse[A](ast: CliTree[A], args: Seq[String]): ParseResult[A] =
    Parser.parse(ast, args.toList) match
      case ParseResult.Success(value, remaining) if remaining.nonEmpty =>
        ParseResult.Fail(Seq.empty, remaining, false)
      case other => other

  private def parse[A](ast: CliTree[A], args: List[String]): ParseResult[A] = ast match
    case CliTree.Word(word, annotations) =>
      args match
        case head :: tail if head === word =>
          val helpAnnotated = annotations.collectFirst { case h: Command => true }.getOrElse(false)
          tail match
            case ("--help" | "-h") :: Nil if helpAnnotated         => ParseResult.Help(ast, List(word), false)
            case ("--help-verbose" | "-H") :: Nil if helpAnnotated => ParseResult.Help(ast, List(word), true)
            case _                                                 => ParseResult.Success((), tail)
        case _ => fail(ast, args)

    case CliTree.Remaining(convert, _) =>
      ParseResult.Success(convert(args), Nil)

    case CliTree.Map(a, f) =>
      parse(a, args).map(f)

    case CliTree.Compose(a, b) =>
      parse(a, args) match
        case ParseResult.Success(aVal, aTail) =>
          parse(b, aTail) match
            case ParseResult.Success(bVal, bTail) => ParseResult.Success((aVal, bVal), bTail)
            case f: ParseResult.Fail              => f.copy(cut = true)
            case h: ParseResult.Help => h.copy(ast = ast, path = args.take(args.size - aTail.size) ++ h.path)
        case f: ParseResult.Fail => f
        case h: ParseResult.Help => h.copy(ast = ast)

    case v: CliTree.Or[_] =>
      v.variants.map(parse(_, args)) collectFirst {
        case s: ParseResult.Success[_]    => s
        case c: ParseResult.Fail if c.cut => c
        case h: ParseResult.Help          => h.copy(ast = ast)
      } getOrElse {
        fail(ast, args)
      }

  private def fail(ast: CliTree[?], args: List[String]) =
    def getExpected(ast: CliTree[?]): Seq[String] = ast match
      case CliTree.Word(word, annotations) => Seq(word)
      case CliTree.Or(variants)            => variants.flatMap(getExpected)
      case CliTree.Compose(a, b)           => getExpected(a)
      case CliTree.Map(a, f)               => getExpected(a)
      case _                               => Seq.empty

    ParseResult.Fail(getExpected(ast), args.headOption.toList, false)
  end fail

}
