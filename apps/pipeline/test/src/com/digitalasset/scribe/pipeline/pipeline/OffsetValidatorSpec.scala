// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline.pipeline

import com.digitalasset.canonical.specific.Offset
import com.digitalasset.canonical.specific.Offset.*
import zio.ZIO
import zio.test.Assertion.*
import zio.test.*

import scala.language.postfixOps

object OffsetValidatorSpec extends ZIOSpecDefault:
  override def spec =
    suite("offset validation")(
      test("requestedStart > requestedEnd") {
        s"""   CLI:     ]----[
           |    DB:
           |Ledger:  [--------------]
           |""".stripMargin `failsWith` s"Requested end '${formatExpectedValue(5)}' must be greater than or equal to start '${formatExpectedValue(10)}'."
      },
      test("requestedStart > requestedEnd IG") {
        s"""   CLI:     I----G
           |    DB:
           |Ledger:  [--------------]
           |""".stripMargin `failsWith` "Requested end 'GENESIS' must be greater than or equal to start 'INFINITY'."
      },
      test("requestedStart > requestedEnd IA") {
        s"""   CLI:     I----]
           |    DB:
           |Ledger:  [--------------]
           |""".stripMargin `failsWith` s"Requested end '${formatExpectedValue(10)}' must be greater than or equal to start 'INFINITY'."
      },
      test("requestedStart > requestedEnd AG") {
        s"""   CLI:     [----G
           |    DB:
           |Ledger:  [--------------]
           |""".stripMargin `failsWith` s"Requested end 'GENESIS' must be greater than or equal to start '${formatExpectedValue(5)}'."
      },
      test("requestedStart < ledgerStart") {
        s"""   CLI:[----------------I
           |    DB:
           |Ledger:  [--------------]
           |""".stripMargin `failsWith` s"Requested start '${formatExpectedValue(0)}' is outside of ledger history '${formatExpectedValue(2)}...${formatExpectedValue(17)}'."
      },
      test("requestedStart > ledgerEnd") {
        s"""   CLI:      [----I
           |    DB:
           |Ledger: [--]
           |""".stripMargin `failsWith` s"Requested start '${formatExpectedValue(6)}' is outside of ledger history '${formatExpectedValue(1)}...${formatExpectedValue(4)}'."
      },
      test("requestedStart == ledgerEnd")(
        s"""   CLI:    [----I
           |    DB:
           |Ledger: [--]
           |""".stripMargin passes
      ),
      test("requestedEnd > ledgerEnd") {
        s"""   CLI:[-------]
           |    DB:
           |Ledger:[--]
           |""".stripMargin `failsWith` s"Requested end '${formatExpectedValue(8)}' is outside of ledger history '${formatExpectedValue(0)}...${formatExpectedValue(3)}'."
      },
      test("Infinity as requestedEnd > ledgerEnd") {
        s"""   CLI:[-------I
           |    DB:
           |Ledger:[--]
           |""".stripMargin passes
      },
      test("requestedStart < dbStart") {
        s"""   CLI:[------]
           |    DB:  [--]
           |Ledger:[------]
           |""".stripMargin `failsWith` s"Cannot prepend to existing datastore. Requested start '${formatExpectedValue(0)}', datastore start '${formatExpectedValue(2)}'."
      },
      test("requestedStart > dbEnd") {
        s"""   CLI:    [----]
           |    DB: [-]
           |Ledger:[--------]
           |""".stripMargin `failsWith` s"Requested offsets '${formatExpectedValue(4)}...${formatExpectedValue(9)}' will produce gap in datastore history '${formatExpectedValue(1)}...${formatExpectedValue(3)}'."
      },
      test("requestedStart > dbEnd but on empty datastore")(
        s"""   CLI:  [-------I
           |    DB:
           |Ledger:[---------]
           |""".stripMargin passes
      ),
      test("Genesis as requestedStart on pruned ledger") {
        s"""   CLI:G--------]
           |    DB:
           |Ledger:  [------]
           |""".stripMargin `failsWith` s"Requested start 'GENESIS' is outside of ledger history '${formatExpectedValue(2)}...${formatExpectedValue(9)}'."
      },
      test("requested offsets already exist in datastore history") {
        s"""   CLI:  [---]
           |    DB: [------]
           |Ledger:[---------]
           |""".stripMargin passes
      }
    )

  extension (state: String)
    def failsWith(msg: String) =
      verify(state).map(result => assert(result)(fails(hasMessage(equalTo(msg)))))

    def passes =
      verify(state).map(result => assert(result)(succeeds(isUnit)))

  def verify(state: String) =
    for
      offsets <- ZIO.attempt(translateIntoOffsets(state))
      ((requestedStart, requestedEnd), (dbStart, dbEnd), (ledgerStart, ledgerEnd)) = offsets
      result <- OffsetValidator
        .validate(
          requestedStart,
          requestedEnd,
          dbStart,
          dbEnd,
          ledgerStart,
          ledgerEnd
        )
        .exit
    yield result

  def translateIntoOffsets(state: String) =

    def parseOffsets(line: String): (Offset, Offset) = {
      if line.isEmpty then (Genesis, Genesis)
      else
        def findFirstOr(c: String, mkOffset: Int => Offset, offset: Offset): Offset = {
          val ix = line.indexOf(c)
          if ix == -1 then offset else mkOffset(ix)
        }

        def findLastOr(c: String, mkOffset: Int => Offset, offset: Offset): Offset = {
          val ix = line.lastIndexOf(c)
          if ix == -1 then offset else mkOffset(ix)
        }

        val start = findFirstOr("[", absolute, findFirstOr("I", _ => Infinity, Genesis))
        val end   = findLastOr("]", absolute, findLastOr("G", _ => Genesis, Infinity))
        start -> end
    }

    val lines = state
      .split(System.lineSeparator)
      .map { line =>
        val (key, offsetDef) = line.trim.span(_ != ':')
        key -> offsetDef.drop(1)
      }
      .toMap
    (parseOffsets(lines("CLI")), parseOffsets(lines("DB")), parseOffsets(lines("Ledger")))
  end translateIntoOffsets
end OffsetValidatorSpec
