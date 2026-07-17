// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.configuration

import com.digitalasset.pqs.configuration.filter.PartyFilterParser
import com.digitalasset.pqs.configuration.filter.PartyFilterParser.PartyFilter
import zio.test.{ZIOSpecDefault, assertTrue}

import scala.language.implicitConversions

object PartyFilterParserSpec extends ZIOSpecDefault:
  private val alice1 = "Alice_1::122055fc4b190e3ff438587b699495a4b6388e911e2305f7e013af160f49a76080ab"
  private val alice2 = "Alice_2::122053933e4803c2995e41faa8a29981ca0d1faf6b4ffbf917ba1edd0db133acb634"
  private val peter1 = "Peter-1::358400000000000000000000000"
  private val peter2 = "Peter-2::358400100010050000000000000"

  private def matchesIdentifier(filter: PartyFilter)(party: String) =
    filter.contains(party)

  def spec =
    suite("PartyFilterParser containsIdentifier")(
      test("matches simple prefix") {
        val filter = PartyFilterParser("Alice_*::*").get
        assertTrue(
          matchesIdentifier(filter)(alice1),
          matchesIdentifier(filter)(alice2),
          !matchesIdentifier(filter)(peter1),
          !matchesIdentifier(filter)(peter2)
        )
      },
      test("matches simple suffix") {
        val filter = PartyFilterParser("*0000000").get
        assertTrue(
          !matchesIdentifier(filter)(alice1),
          !matchesIdentifier(filter)(alice2),
          matchesIdentifier(filter)(peter1),
          matchesIdentifier(filter)(peter2)
        )
      },
      test("matches union") {
        val filter = PartyFilterParser("Alice* | *-2::*").get
        assertTrue(
          matchesIdentifier(filter)(alice1),
          matchesIdentifier(filter)(alice2),
          !matchesIdentifier(filter)(peter1),
          matchesIdentifier(filter)(peter2)
        )
      }
    )
