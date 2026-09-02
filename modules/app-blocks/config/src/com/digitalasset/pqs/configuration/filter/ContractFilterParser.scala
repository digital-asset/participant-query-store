// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.configuration.filter

import com.digitalasset.pqs.utils.safeequals.*
import fastparse.{P, *}
import zio.config.magnolia.Descriptor

type PackageName = String
case class DottedName(segments: Seq[String])
type ModuleName = DottedName
case class QualifiedName(module: ModuleName, name: DottedName)

type FullyQualifiedName = (PackageName, QualifiedName)

object ContractFilterParser extends FilterParser[FullyQualifiedName]:

  override def simpleFilter[$: P]: P[Filter[FullyQualifiedName]] = ((packageName ~~ ":"./).? ~~ qnPath).map {
    (packageName, qn) =>
      new Filter[FullyQualifiedName]:
        def contains(fqn: FullyQualifiedName): Boolean =
          packageName.fold(true)(_ === fqn._1)
            && qn
              .zip(fqn._2.module.segments ++ fqn._2.name.segments)
              .takeWhile(_._1 =/= "*")
              .foldLeft(true) { case (acc, (a, b)) => acc && a === b }

        override def toString: String = packageName.map(_ + ":").getOrElse("") + qn.mkString(".")
  }

  private def packageName[$: P]: P[String] = packageNameSegment.repX(1, sep = "-").!
  private def packageNameSegment[$: P]     = CharIn("A-Za-z") ~~ CharIn("A-Za-z0-9").repX

  // either "*" or a dot-separated path of valid identifiers optionally ending in ".*"
  private def qnPath[$: P]: P[Seq[String]] =
    ("*".! | identifier).repX(1, sep = ".").filter(x => !x.contains("*") || x.indexOf("*") == x.size - 1)

  private def identifier[$: P]: P[String] = (CharIn("a-zA-Z_") ~~ CharIn("a-zA-Z0-9_'").repX).!

  given toContractFilter: Conversion[Filter[FullyQualifiedName], ContractFilter] = (f: Filter[FullyQualifiedName]) =>
    new ContractFilter:
      def contains(identifier: FullyQualifiedName): Boolean = f.contains(identifier)
      override def toString: String                         = f.toString

  given contractFilterDescriptor: Descriptor[ContractFilter] =
    Descriptor.from(
      Descriptor[String]
        .transformOrFailLeft[ContractFilter](
          ContractFilterParser(_)
            .map(toContractFilter)
            .toEither
            .left
            .map(_.toString)
        )(_.toString)
    )

  sealed trait ContractFilter:
    /** Checks if an identifier is included in a filter expression */
    def contains(templateId: FullyQualifiedName): Boolean
  object ContractFilter:
    val All: ContractFilter  = toContractFilter(Filter.All)
    val None: ContractFilter = toContractFilter(Filter.None)
