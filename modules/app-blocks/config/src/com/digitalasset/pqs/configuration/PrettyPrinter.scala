// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.configuration

import zio.config.PropertyTreePath.Step
import zio.config.Table.TableRow
import zio.config.{PropertyTreePath, ReadError, Table, generateDocs}
import com.digitalasset.pqs.utils.safeequals.===

object PrettyPrinter:

  object ConfigDescriptor:
    extension [A](descriptor: zio.config.ConfigDescriptor[A])
      def prettyOptions(envVarPrefix: String): List[OptionInfo] = {
        def go(helpText: HelpText): List[OptionInfo] = helpText match
          case Section(title, rows) =>
            rows.toList.flatMap(go)
          case Property(path, descriptions, possibleValues) =>
            val dottedPath = path.mkString(".")
            val key        = dottedPathToCliFlag(dottedPath)
            val typ = descriptions
              .collectFirst { case `valueTypeRegex`(typeName) => typeName }
              .orElse {
                if possibleValues.isEmpty
                then None
                else
                  possibleValues
                    .collectFirst {
                      case x if x.contains("value of type") =>
                        s"[enum | ${x match { case `valueTypeRegex`(typeName) => typeName }}]"
                    }
                    .orElse(Some("enum"))
              }
            val defval = descriptions
              .collectFirst { case `defaultValueRegex`(defaultValue) => defaultValue }
            val desc = descriptions.find(isCustomDescription).getOrElse("")
            List(
              OptionInfo(
                desc,
                key,
                None,
                typ,
                defval,
                Some(dottedPathToEnvVar(envVarPrefix, dottedPath)),
                Some(dottedPath),
                justEnumValues(possibleValues.filter(_.startsWith("constant string")))
              )
            )

        def justEnumValue(l: String)          = l.substring(l.indexOf('\'') + 1, l.lastIndexOf('\''))
        def justEnumValues(doc: List[String]) = doc.map(justEnumValue)

        tableRowsToHelpText(generateDocs(descriptor).toTable.rows).flatMap(go)
      }
  end ConfigDescriptor

  object ReadError:
    extension [A](error: zio.config.ReadError[A])
      def pretty: String = {
        final case class Failure(cause: String, path: String, details: Option[String])

        def renderPath(steps: List[PropertyTreePath.Step[A]]): String = dottedPathToCliFlag {
          steps
            .foldLeft(new StringBuilder()) {
              case (r, Step.Key(k))   => r.append('.').append(k.toString)
              case (r, Step.Index(i)) => r.append('[').append(i).append(']')
            }
            .delete(0, 1)
            .toString()
        }

        def flattenFailures(readError: zio.config.ReadError[A]): List[Failure] =
          readError match {
            case r: zio.config.ReadError.MissingValue[A @unchecked]    => List(renderMissingValue(r))
            case r: zio.config.ReadError.SourceError                   => List(renderSourceError(r))
            case r: zio.config.ReadError.FormatError[A @unchecked]     => List(renderFormatError(r))
            case r: zio.config.ReadError.ConversionError[A @unchecked] => List(renderConversionError(r))
            case zio.config.ReadError.ZipErrors(list, _)               => list.flatMap(flattenFailures)
            case zio.config.ReadError.ListErrors(list, _)              => list.flatMap(flattenFailures)
            case zio.config.ReadError.MapErrors(list, _)               => list.flatMap(flattenFailures)
            case zio.config.ReadError.Irrecoverable(list, _)           => list.flatMap(flattenFailures)
            case zio.config.ReadError.OrErrors(list, _)                => list.flatMap(flattenFailures)
          }

        def renderMissingValue(err: zio.config.ReadError.MissingValue[A]): Failure =
          Failure(
            cause = "Missing value",
            path = renderPath(err.path),
            details = Option.when(err.detail.nonEmpty)(err.detail.mkString(", "))
          )

        def renderFormatError(err: zio.config.ReadError.FormatError[A]): Failure =
          Failure(
            cause = "Format error",
            path = renderPath(err.path),
            details = Option.when(err.detail.nonEmpty)(err.detail.mkString(", "))
          )

        def renderConversionError(err: zio.config.ReadError.ConversionError[A]): Failure =
          Failure(
            cause = "Conversion error",
            path = renderPath(err.path),
            details = Some(err.message)
          )

        def renderSourceError(err: zio.config.ReadError.SourceError): Failure =
          Failure(cause = s"Source Error", path = "", details = Some(err.message))

        flattenFailures(error)
          .map(f => s"[ERROR] ${f.cause}: ${f.path}${f.details.map(s => s". Details: $s").getOrElse("")}")
          .sorted
          .mkString(System.lineSeparator)
      }
  end ReadError

  // these descriptions are auto-generated by ZIO Config
  private val defaultValueRegex    = "^default value: (.*)$".r
  private val isOptionalValueRegex = "^optional value$".r
  private val valueTypeRegex       = "^value of type (.*)$".r

  private def isCustomDescription(s: String) =
    !Seq(defaultValueRegex, isOptionalValueRegex, valueTypeRegex).exists(_.matches(s))

  private def dottedPathToCliFlag(path: String)                = s"--${dotsTo(path, '-').toLowerCase}"
  private def dottedPathToEnvVar(prefix: String, path: String) = s"${prefix}_${dotsTo(path, '_')}".toUpperCase
  private def dotsTo(path: String, ch: Char)                   = path.replace('.', ch)

  /** Internal AST of the help-text. Tree-structure of "Sections" with "Properties" as Leaf-nodes. */
  private sealed trait HelpText

  /** Internal representation of a grouping of properties. */
  private final case class Section(title: String, rows: Seq[HelpText]) extends HelpText

  /** Internal representation of a configurable property for help-text generation. */
  private final case class Property(path: Seq[String], descriptions: Seq[String], possibleValues: List[String] = Nil)
      extends HelpText

  private def tableRowsToHelpText(rows: Seq[TableRow], parentPaths: List[String] = Nil): List[HelpText] = {
    def fieldName(paths: List[Table.FieldName]) =
      paths.reverse.collectFirst { case Table.FieldName.Key(s) => s }

    def tableToPossibleValues(t: Table): List[String] =
      t.rows.collect { case TableRow(_, _, description :: _, _, _) => description.description }

    rows.flatMap {
      case TableRow(paths, format, description, nested, _) =>
        val pathElement = fieldName(paths).toList
        if format.contains(Table.Format.Map)
        then
          List(
            Property(
              parentPaths ::: pathElement,
              description.map(_.description).prepended("value of type map").filterNot(_ === "default value: Map()"),
              List.empty
            )
          )
        else
          nested.fold(
            Option
              .when(pathElement.nonEmpty)(Property(parentPaths ::: pathElement, description.map(_.description)))
              .toList
          ) { table =>
            val inner = tableRowsToHelpText(table.rows, parentPaths ::: fieldName(paths).toList)
            if pathElement.isEmpty then inner
            else if format.contains(Table.Format.AnyOneOf)
            then
              List(Property(parentPaths ::: pathElement, description.map(_.description), tableToPossibleValues(table)))
            else
              List(
                Section(
                  description
                    .map(_.description)
                    .find(isCustomDescription)
                    .getOrElse(pathElement.map(_.capitalize).mkString(" ")),
                  inner
                )
              )
          }
    }.toList
  }

end PrettyPrinter
