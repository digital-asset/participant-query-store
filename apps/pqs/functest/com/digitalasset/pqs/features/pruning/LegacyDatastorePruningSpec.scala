// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.pruning

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.specific.{OffsetType, biggestOffset, smallestOffset}
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ZLayer
import zio.jdbc.sqlInterpolator
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object LegacyDatastorePruningSpec extends SharedLedgerAndPostgresTest:
  lazy val alice = Party("Alice")
  lazy val pingDaml = DamlSource(
    "Pings" -> """module Pings where
                 |
                 |import Daml.Script
                 |
                 |template Ping
                 |  with
                 |    owner : Party
                 |    label : Text
                 |  where
                 |    signatory owner
                 |    choice ChangeLabel : ContractId Ping
                 |      with
                 |        newLabel : Text
                 |      controller owner
                 |      do
                 |        create Ping with label = newLabel, ..
                 |
                 |setup: Party -> Script ()
                 |setup alice = do
                 |  one <- submit alice $ createCmd (Ping with owner = alice, label = "one")
                 |  submit alice $ exerciseCmd one ChangeLabel with newLabel = "one updated"
                 |  two <- submit alice $ createCmd (Ping with owner = alice, label = "two")
                 |  submit alice $ exerciseCmd two Archive
                 |  -- ^ pruning cut-off ^ --
                 |  three <- submit alice $ createCmd (Ping with owner = alice, label = "three")
                 |  submit alice $ exerciseCmd three ChangeLabel with newLabel = "three updated"
                 |  pure ()
                 |""".stripMargin
  )
  private val packageName = pingDaml.name
  private val templateRef = s"$packageName:Pings:Ping"
  private val choiceRef   = s"$templateRef:ChangeLabel"

  val context = DamlSdk.dar(pingDaml) ++ DamlSdk.parties(alice) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("Pings:setup", alice.id)
    >+> Pqs.runPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      "--pipeline-ledger-start=Genesis",
      "--pipeline-ledger-stop=Latest"
    )

  extension (offset: Capture[OffsetType])
    def max(other: Capture[OffsetType]) = if offset.get > other.get then offset else other

  def spec =
    suite("Legacy offset based pruning")(
      funcTest("pruning to offset") {
        val oneCreated   = Capture[OffsetType]
        val oneUpdated   = Capture[OffsetType]
        val twoCreated   = Capture[OffsetType]
        val twoArchived  = Capture[OffsetType]
        val threeCreated = Capture[OffsetType]
        val threeUpdated = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            oneCreated.capture |
              oneUpdated.capture |
              twoCreated.capture |
              twoArchived.capture |
              threeCreated.capture |
              threeUpdated.capture
          }.transpose
        Expect:
          // dry run
          Postgres
            .query(
              sql"select affected_transactions, squash_inclusive, new_oldest from validate_pruning_offset(${twoArchived.get})"
            )
            .returns(table(4 | twoArchived | threeCreated))
        And:
          // actual pruning
          Postgres
            .query(
              sql"select affected_transactions, squash_inclusive, new_oldest from prune_to_offset(${twoArchived.get})"
            )
            .returns(table(4 | twoArchived | threeCreated))
        And:
          // another dry run on already-pruned offset is a no-op (returns empty set)
          Postgres
            .query(
              sql"select count(*) from validate_pruning_offset(${twoArchived.get})"
            )
            .returns(table(0))
        And:
          Postgres query {
            sql"""select payload->>'label', created_at_offset from active($templateRef) order by created_at_offset"""
          } `returns` table {
            "one updated"   | threeCreated
            "three updated" | threeUpdated
          }
        And:
          // archived contracts created before the pruning offset should be deleted
          Postgres query {
            sql"""select payload->>'label', created_at_offset, archived_at_offset from archives($templateRef)"""
          } `returns` table { "three" | threeCreated | threeUpdated }
        And:
          Postgres query {
            sql"""select argument->>'newLabel' from exercises($choiceRef)"""
          } `returns` table { "three updated" }
      },
      funcTest("prevent pruning at illegal offsets") {
        val firstOffset  = Capture[OffsetType]
        val secondOffset = Capture[OffsetType]
        val latestOffset = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` table { firstOffset.capture }
        And:
          Postgres query {
            // the second offset is the exercise transaction, which archives contract "one" and creates contract "one updated"
            sql"""select "offset" from __transactions where ix=2"""
          } `returns` table { secondOffset.capture }
        And:
          Postgres query {
            sql"""select "offset" from latest_checkpoint()"""
          } `returns` table { latestOffset.capture }
        Expect:
          // latest offset is illegal
          Postgres
            .query(sql"select affected_transactions from prune_to_offset(${latestOffset.get})")
            .exit
            .map {
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Illegal pruning offset ${latestOffset.get} coincides with latest consistent checkpoint of contiguous history"
                    )
                  )
                )
              )
            }
        And:
          // out of upper bounds
          Postgres
            .query(sql"select affected_transactions from prune_to_offset($biggestOffset)")
            .exit
            .map {
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Illegal pruning offset $biggestOffset is beyond upper bounds of contiguous history"
                    )
                  )
                )
              )
            }
        And:
          // out of lower bounds
          Postgres
            .query(sql"select affected_transactions from prune_to_offset($smallestOffset)")
            .exit
            .map {
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Illegal pruning offset $smallestOffset is outside lower bounds of contiguous history"
                    )
                  )
                )
              )
            }
        When:
          // pruning to second offset:
          // - deletes the first transaction, because contract "one" is archived and pruned
          // - keeps the second transaction, because contract "one updated" is still active
          Postgres.query(sql"select affected_transactions from prune_to_offset(${secondOffset.get})")
        Expect:
          // pruning below already-pruned offset is a no-op (zeroed stats)
          Postgres
            .query(sql"select affected_transactions from prune_to_offset(${firstOffset.get})")
            .returns(table(0))
      },
      funcTest("pruning can continue from the next surviving offset") {
        val twoArchived  = Capture[OffsetType]
        val threeCreated = Capture[OffsetType]
        val threeUpdated = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            anything | anything | anything | twoArchived.capture | threeCreated.capture | threeUpdated.capture
          }.transpose
        When:
          Postgres
            .query(
              sql"select affected_transactions, squash_inclusive, new_oldest from prune_to_offset(${twoArchived.get})"
            )
            .returns(table(4 | twoArchived | threeCreated))
        Then:
          // After the first prune, the next surviving offset should still be a legal prune target.
          Postgres
            .query(
              sql"select affected_transactions, squash_inclusive, new_oldest from prune_to_offset(${threeCreated.get})"
            )
            .returns(table(1 | threeCreated | threeUpdated))
      },
      funcTest("deny pruning at non-contiguous offsets") {
        val thirdOffset = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix limit 1 offset 2"""
          } `returns` table {
            thirdOffset.capture
          }
        And:
          // delete second transaction offset
          Postgres `makeGap` (2 to 2) `returns` 1
        Expect:
          // out of contiguous range
          Postgres
            .query(sql"select affected_transactions from prune_to_offset(${thirdOffset.get})")
            .exit
            .map {
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Illegal pruning offset ${thirdOffset.get} is beyond upper bounds of contiguous history"
                    )
                  )
                )
              )
            }
      },
      funcTest("pruning with NULL argument offset does nothing") {
        val firstOffset = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` table { firstOffset.capture }
        And:
          Postgres.query(sql"select affected_transactions from prune_to_offset(null)").returns(anything)
        Expect:
          // nothing should be pruned
          Postgres query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` table { firstOffset }
      }
    ) + suite("time based pruning")(
      (for (testLabel, offsetFunction) <- List(
          ("timestamp", sql"nearest_offset(${"1970-01-01 08:01:00+08"} :: timestamp with time zone)"),
          ("interval", sql"nearest_offset(${"PT2H"} :: interval)")
        )
      yield funcTest(s"$testLabel-based pruning") { // reuses offset based pruning function, serves as a smoke test
        val oneCreated   = Capture[OffsetType]
        val oneUpdated   = Capture[OffsetType]
        val twoCreated   = Capture[OffsetType]
        val twoArchived  = Capture[OffsetType]
        val threeCreated = Capture[OffsetType]
        val threeUpdated = Capture[OffsetType]
        val resetTime    = "1970-01-01 08:00:00+08"

        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            oneCreated.capture |
              oneUpdated.capture |
              twoCreated.capture |
              twoArchived.capture |
              threeCreated.capture |
              threeUpdated.capture
          }.transpose
        And:
          Postgres `query`
            sql"""
                 update __transactions
                 set effective_at = $resetTime :: timestamp with time zone
                 where "offset" <= ${twoArchived.get}
              """.update `returns` anything
        Expect:
          // expected deletion of two transactions
          Postgres
            .query(sql"select affected_transactions from prune_to_offset($offsetFunction)")
            .returns(table(4))
        And:
          Postgres query {
            sql"""select payload->>'label', created_at_offset from active($templateRef) order by created_at_offset"""
          } `returns` table {
            "one updated"   | threeCreated
            "three updated" | threeUpdated
          }
        And:
          Postgres query {
            sql"""select payload->>'label', created_at_offset, archived_at_offset from archives($templateRef)"""
          } `returns` table { "three" | threeCreated | threeUpdated }
      })*
    )
