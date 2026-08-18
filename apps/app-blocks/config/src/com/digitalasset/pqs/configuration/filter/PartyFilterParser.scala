// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.configuration.filter

import fastparse.*
import zio.config.magnolia.Descriptor

type Party = String

object PartyFilterParser extends FilterParser[Party]:
  override def simpleFilter[$: P]: P[Filter[Party]] = identifier.map { identifier =>
    new Filter[Party]:
      def contains(partyId: Party): Boolean = s"^$identifier$$".split('*').mkString(".*").r.matches(partyId)
      override def toString: String         = identifier
  }
  private def identifier[$: P]: P[String] = CharIn("A-Za-z0-9:\\-_*").repX(min = 1).!

  given toPartyFilter: Conversion[Filter[Party], PartyFilter] with
    def apply(f: Filter[Party]): PartyFilter =
      new PartyFilter:
        def contains(partyId: Party): Boolean = f.contains(partyId)
        override def toString: String         = f.toString

  given partyFilterDescriptor: Descriptor[PartyFilter] =
    Descriptor.from(
      Descriptor[String]
        .transformOrFailLeft[PartyFilter](
          PartyFilterParser(_)
            .map(toPartyFilter)
            .toEither
            .left
            .map(_.toString)
        )(_.toString)
    )

  sealed trait PartyFilter:
    def contains(partyId: Party): Boolean
  object PartyFilter:
    val All: PartyFilter  = toPartyFilter(Filter.All)
    val None: PartyFilter = toPartyFilter(Filter.None)
