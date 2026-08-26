// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.app

import com.digitalasset.scribe.app.{Annotation, CliTree}
import com.digitalasset.scribe.configuration.OptionInfo
import com.digitalasset.scribe.utils.safeequals.===

object Helper {
  def prettyPrint(ast: CliTree[?], path: List[String], verbose: Boolean): String =
    val reduced = reduce(ast)
    val scope   = getScope(path, reduce(ast))

    val cmd = scope
      .collect {
        case Elem.Word(_, _, Some(param), _, _, _) => param
        case Elem.Word(w, _, _, _, _, _)           => w
        case Elem.ConfigOptions(_)                 => "[OPTIONS]"
        case Elem.Branch(variants) =>
          variants.values.collectFirst { case Elem.Word(_, _, Some(param), _, _, _) :: _ => param }.getOrElse("PARAM")
      }
      .mkString(" ")
    val usage = s"Usage: $cmd"
    val help  = scope.collectFirst { case Elem.Word(_, Some(help), _, _, _, _) => help }.getOrElse("")
    val optionParams = scope
      .collect {
        case Elem.Word(w, _, Some(_), Some(paramDescription), optionDescription, _) =>
          s"$paramDescription:\n  $w    ${optionDescription.getOrElse("")}"
        case Elem.Branch(variants) =>
          val description =
            variants.values
              .map(_.headOption)
              .collectFirst { case Some(Elem.Word(_, _, _, Some(paramDescription), _, _)) => paramDescription }
              .getOrElse("Options")
          val padSize = variants.keys.map(_.length).maxOption.getOrElse(0)
          val opts = variants.values
            .map(_.headOption)
            .collect {
              case Some(Elem.Word(w, _, _, _, Some(optionDescription), _)) =>
                s"  ${w.padTo(padSize, ' ')}    $optionDescription"
            }
            .mkString("\n")
          s"$description:\n$opts"
        case Elem.ConfigOptions(help) => help(verbose)
      }
      .mkString("\n\n")
    val helpHint = scope
      .collect {
        case Elem.Word(_, _, _, _, _, Some(hint)) => Seq(hint(cmd))
        case Elem.Branch(variants) =>
          variants.values.map(_.headOption).collect { case Some(Elem.Word(_, _, _, _, _, Some(hint))) => hint(cmd) }
      }
      .flatten
      .toSet
      .mkString("\n")
    def section(str: String) = if str.nonEmpty then s"\n\n$str" else str
    usage + section(help) + section(optionParams) + section(helpHint)

  private def reduce(ast: CliTree[?]): List[Elem] = ast match
    case CliTree.Word(word, annotations) =>
      def first[A](pfs: PartialFunction[Annotation, A]*): Option[A] =
        pfs.map(annotations.collectFirst).foldLeft(Option.empty[A])(_ orElse _)
      List(
        Elem.Word(
          word,
          help = first { case Command(description) => description },
          paramName = first(
            { case Param(name, description, hint) => name },
            { case Command(_) => "COMMAND" }
          ),
          paramDescription = first(
            { case Param(name, description, hint) => description },
            { case Command(_) => "Commands" }
          ),
          optionDescription = first(
            { case Variant(description) => description },
            { case Command(description) => description }
          ),
          paramHint = first(
            { case Param(_, _, Some(hint)) => hint },
            { case Command(_) => cmd => s"Run '$cmd --help[-verbose]' for more information on a command." }
          )
        )
      )
    case CliTree.Or(variants) =>
      val elems = variants.map(reduce)
      def flattenBranches: PartialFunction[List[Elem], List[(String, List[Elem])]] =
        case (w: Elem.Word) :: rest     => List(w.word -> (w :: rest))
        case Elem.Branch(vars2) :: rest => vars2.map { case (k, v) => k -> (v ++ rest) }.toList
      List(Elem.Branch(elems.collect(flattenBranches).flatten.toMap))
    case CliTree.Compose(a, b)      => reduce(a) ++ reduce(b)
    case CliTree.Map(a, f)          => reduce(a)
    case CliTree.Remaining(_, help) => List(Elem.ConfigOptions(help))
    case _                          => sys.error(s"Invalid CLI Tree: $ast")

  private def getScope(path: List[String], elems: List[Elem]): List[Elem] = (path, elems) match
    case (Nil, elems) => // stop
      val size = elems.takeWhile { // take until next sub-command is found
        case w: Elem.Word => w.help.isEmpty
        case _            => true
      }.size
      elems.take(size + 1)
    case (head :: tail, (w: Elem.Word) :: tElem) if head === w.word => // composition
      (if tail.isEmpty then Elem.Word(w.word, w.help) else Elem.Word(w.word)) :: getScope(tail, tElem)
    case (head :: _, Elem.Branch(variants) :: _) => // choose variant
      getScope(path, variants(head))
    case other =>
      throw Exception(s"Unexpected case $other")

  def renderOptions(options: List[OptionInfo], verbose: Boolean) =
    options.renderUnlessEmpty {
      def combined(long: String, typ: Option[String]) = typ.fold(long)(t => s"$long $t")

      def addtlLine(padLeft: Int, prefix: String)(value: String) =
        System.lineSeparator + " ".repeat(padLeft + 4) + s"$prefix$value"

      val width        = maxLength(options.map(info => combined(info.longKey, info.dataType)))
      val hasShortKeys = options.map(_.shortKey).exists(_.isDefined)
      options
        .map {
          case OptionInfo(desc, long, short, typ, defaultValue, envVar, sysProp, variants) =>
            val combo = combined(long, typ)
            val keysColumn =
              if hasShortKeys
              then s"  ${short.map(s => s"$s, ").getOrElse("    ")}${combo.padTo(width, ' ')}"
              else s"  ${combo.padTo(width, ' ')}"
            val defVal = defaultValue
              .map { s => if s === "None" then " (optional)" else s" (default: $s)" }
              .getOrElse("")
            val mainLine = s"$keysColumn    $desc$defVal"
            val addtlLines =
              if verbose
              then
                envVar.map(addtlLine(keysColumn.length, " + Environment variable: ")).getOrElse("") +
                  sysProp.map(addtlLine(keysColumn.length, " + System property:      ")).getOrElse("") +
                  variants.renderUnlessEmpty {
                    addtlLine(keysColumn.length, " + Enumeration values:   ")(variants.mkString(", "))
                  }
              else ""
            s"$mainLine$addtlLines"
        }
        .mkString(s"Options:${System.lineSeparator}", System.lineSeparator, "")
    }

  private def maxLength(tokens: List[String]): Int =
    tokens
      .map(_.length)
      .reduceOption { case (a, b) => scala.math.max(a, b) }
      .getOrElse(0)

  extension [A](it: Iterable[A])
    /** Returns supplied string for non-empty collections, or an empty string otherwise. */
    private def renderUnlessEmpty(s: => String) = if it.isEmpty then "" else s
  end extension

  private sealed trait Elem
  private object Elem {
    final case class Word(
        word: String,
        help: Option[String] = None,
        paramName: Option[String] = None,
        paramDescription: Option[String] = None,
        optionDescription: Option[String] = None,
        paramHint: Option[String => String] = None
    ) extends Elem
    final case class Branch(variants: Map[String, List[Elem]])     extends Elem
    final case class ConfigOptions(description: Boolean => String) extends Elem
  }
}
