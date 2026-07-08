// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import com.digitalasset.scribe.configuration.{EnvVarPrefix, OptionConfigFromFile}
import zio.config.magnolia.Descriptor
import zio.{Chunk, Tag, ZIOAppArgs, ZLayer}

import scala.annotation.targetName

package object app:
  implicit val stringToCliTree: Conversion[String, CliTree[Unit]]     = CliTree.Word(_)
  implicit def cliTreeIdentity[A]: Conversion[CliTree[A], CliTree[A]] = identity

  implicit class CliTreeOps[A, AA](a: A)(implicit aC: Conversion[A, CliTree[AA]]):
    @targetName("compose")
    final def -[B, BB, C](b: B)(implicit
        bC: Conversion[B, CliTree[BB]],
        sequencer: fastparse.Implicits.Sequencer[AA, BB, C]
    ): CliTree[C] =
      CliTree.Compose(aC(a), bC(b)).map(sequencer.apply)

    @targetName("or")
    final def |[B, BB >: AA](b: B)(implicit bC: Conversion[B, CliTree[BB]]): CliTree[BB] = aC(a) match
      case CliTree.Or(variants) => CliTree.Or(variants :+ bC(b))
      case other                => CliTree.Or(Seq(aC(a), bC(b)))

    @targetName("annotate")
    final def @@(annotation: Annotation): CliTree[AA] = @@(Seq(annotation))

    @targetName("annotate")
    final def @@(annotations: Seq[Annotation]): CliTree[AA] = aC(a) match
      case CliTree.Word(word, anno) => CliTree.Word(word, anno ++ annotations)
      case CliTree.Or(variants)     => CliTree.Or(variants.map(_ @@ annotations))
      case CliTree.Compose(a, b)    => CliTree.Compose(a @@ annotations, b)
      case CliTree.Map(a, f)        => CliTree.Map(a @@ annotations, f)
      case other                    => throw Exception(s"Can't apply annotation to ${other.getClass}")

    final def as[B](b: => B): CliTree[B] = map(_ => b)

    final def map[B](f: AA => B): CliTree[B] = aC(a) match
      case CliTree.Map(a, g) => CliTree.Map(a, f compose g)
      case other             => CliTree.Map(aC(a), f)
  end CliTreeOps

  sealed trait CliTree[+A]:
    final def parse(args: Seq[String]): ParseResult[A] = Parser.parse(this, args)
    final def nearestHelp(args: Seq[String]): String =
      args.toList.reverse
        .scanRight(List.empty[String])(_ :: _)
        .map("--help" :: _)
        .map(_.reverse)
        .map(parse)
        .collectFirst { case ParseResult.Help(ast, path, verbose) => "\n" + Helper.prettyPrint(ast, path, verbose) }
        .getOrElse("")
  object CliTree:
    final case class Remaining[T] private[app] (convert: Seq[String] => T, help: Boolean => String) extends CliTree[T]
    final case class Word private[app] (word: String, annotations: Seq[Annotation] = Seq.empty) extends CliTree[Unit]
    final case class Or[A] private[app] (variants: Seq[CliTree[? <: A]])                        extends CliTree[A]
    final case class Compose[A, B] private[app] (a: CliTree[A], b: CliTree[B])                  extends CliTree[(A, B)]
    final case class Map[A, B] private[app] (a: CliTree[A], f: A => B)                          extends CliTree[B]
  end CliTree

  def cliConfig[T: Tag: Descriptor]: CliTree[ZLayer[Any, Throwable, T]] = CliTree.Remaining(
    args => ZLayer.succeed(ZIOAppArgs(Chunk.fromIterable(args))) >>> com.digitalasset.scribe.configuration.apply[T],
    verbose => {
      import com.digitalasset.scribe.configuration.PrettyPrinter.ConfigDescriptor.*
      val opts = OptionConfigFromFile +: implicitly[Descriptor[T]].desc.prettyOptions(EnvVarPrefix)
      Helper.renderOptions(opts, verbose)
    }
  )

  sealed trait Annotation
  final case class Command(help: String) extends Annotation
  final case class Param(placeholder: String, description: String, hint: scala.Option[String => String] = None)
      extends Annotation
  final case class Variant(description: String) extends Annotation

  sealed trait ParseResult[+A]:
    final def map[B](f: A => B): ParseResult[B] = this match
      case ParseResult.Success(value, remaining)    => ParseResult.Success(f(value), remaining)
      case f: (ParseResult.Fail | ParseResult.Help) => f

    final def fold[Z](onSuccess: A => Z, onFailure: String => Z, onHelp: String => Z): Z = this match
      case ParseResult.Success(value, remaining) => onSuccess(value)
      case f: ParseResult.Fail                   => onFailure(f.prettyPrint)
      case h: ParseResult.Help                   => onHelp(h.prettyPrint)
  end ParseResult

  object ParseResult:
    final private[app] case class Fail(expected: Seq[String], found: Seq[String], cut: Boolean)
        extends ParseResult[Nothing]:
      def prettyPrint: String =
        import scala.io.AnsiColor.*
        val expectedPart = expected.toList match
          case Nil        => ""
          case one :: Nil => s"$GREEN$one$RESET expected"
          case many       => s"one of ${many.map(s => GREEN + s + RESET).mkString(", ")} expected"
        val foundPart = found.toList match
          case Nil        => ""
          case one :: Nil => s"unexpected argument $RED$one$RESET found"
          case many       => s"unexpected arguments found: $RED${many.mkString(" ")}$RESET"
        val butPart = if expectedPart.nonEmpty && foundPart.nonEmpty then " but " else ""

        expectedPart + butPart + foundPart
    end Fail

    final private[app] case class Help(ast: CliTree[?], path: List[String], verbose: Boolean)
        extends ParseResult[Nothing]:
      def prettyPrint: String = Helper.prettyPrint(ast, path, verbose)

    final private[app] case class Success[A](value: A, remaining: List[String]) extends ParseResult[A]
  end ParseResult
end app
