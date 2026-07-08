// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.pruning

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.specific.{OffsetType, biggestOffset, smallestOffset}
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.{Promise, RIO, ZLayer}
import zio.jdbc.sqlInterpolator
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object DatastorePruningSpec extends SharedLedgerAndPostgresTest:
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
    >+> Scribe.runPipeline(
      "--pipeline-datasource=TransactionTreeStream",
      "--pipeline-ledger-start=Genesis",
      "--pipeline-ledger-stop=Latest"
    )

  extension (offset: Capture[OffsetType])
    def max(other: Capture[OffsetType]) = if offset.get > other.get then offset else other

  extension [R, A](io: RIO[R, A])
    def failsWithMessage(message: String): RIO[R, TestResult] =
      io.exit.map(result => assert(result)(fails(hasMessage(startsWithString(message)))))

  def spec =
    suite("offset based pruning")(
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
            .query(sql"select * from prune_archived_to_offset_dry_run(${twoArchived.get})")
            .returns(table(threeCreated | 2 | 2 | 4 | 3))
        And:
          // dry run should NOT persist pruned_offset
          Postgres.query(sql"select pruned_offset() is null").returns(table(true))
        And:
          // actual pruning
          Postgres
            .query(sql"select * from prune_archived_to_offset(${twoArchived.get})")
            .returns(table(threeCreated | 2 | 2 | 4 | 3))
        And:
          // force run should persist pruned_offset
          Postgres.query(sql"select pruned_offset()").returns(table(twoArchived))
        And:
          // another dry run with the same offset is a no-op (returns zeroed stats)
          Postgres
            .query(
              sql"select deleted_contracts from prune_archived_to_offset_dry_run(${twoArchived.get})"
            )
            .returns(table(0))
        And:
          Postgres query {
            sql"""select payload->>'label', created_at_offset from active($templateRef) order by created_at_offset"""
          } `returns` table {
            "one updated"   | oneUpdated
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
            .query(sql"select deleted_transactions from prune_archived_to_offset(${latestOffset.get})")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset ${latestOffset.get} coincides with latest consistent checkpoint of contiguous history"
            )
        And:
          // out of upper bounds
          Postgres
            .query(sql"select deleted_transactions from prune_archived_to_offset($biggestOffset)")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset $biggestOffset is beyond upper bounds of contiguous history"
            )
        And:
          // out of lower bounds
          Postgres
            .query(sql"select deleted_transactions from prune_archived_to_offset($smallestOffset)")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset $smallestOffset is outside lower bounds of contiguous history"
            )
        When:
          // pruning to second offset:
          // - deletes the first transaction, because contract "one" is archived and pruned
          // - keeps the second transaction, because contract "one updated" is still active
          Postgres.query(sql"select deleted_transactions from prune_archived_to_offset(${secondOffset.get})")
        Expect:
          Postgres.query(sql"select pruned_offset()").returns(table(secondOffset))
        And:
          // pruning below already-pruned offset is a no-op (zeroed stats)
          Postgres
            .query(sql"select deleted_transactions from prune_archived_to_offset(${firstOffset.get})")
            .returns(table(0))
        And:
          // pruned_offset unchanged after no-op
          Postgres.query(sql"select pruned_offset()").returns(table(secondOffset))
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
            .query(sql"select deleted_transactions from prune_archived_to_offset(${thirdOffset.get})")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset ${thirdOffset.get} is beyond upper bounds of contiguous history"
            )
      },
      funcTest("reset below pruned offset retreats pruned_offset") {
        val secondOffset = Capture[OffsetType]
        val fourthOffset = Capture[OffsetType]
        val fifthOffset  = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            anything | secondOffset.capture | anything | fourthOffset.capture | fifthOffset.capture | anything
          }.transpose
        When:
          // prune at fourth offset
          Postgres.query(sql"select * from prune_archived_to_offset(${fourthOffset.get})").returns(anything)
        Then:
          Postgres.query(sql"select pruned_offset()").returns(table(fourthOffset))
        And:
          // reset to fifth offset (above pruned) - pruned_offset should remain
          Postgres.query(sql"select * from reset_to_offset(${fifthOffset.get})").returns(anything)
        And:
          Postgres.query(sql"select pruned_offset()").returns(table(fourthOffset))
        And:
          // reset to second offset (below pruned) - pruned_offset should retreat to reset offset
          Postgres.query(sql"select * from reset_to_offset(${secondOffset.get})").returns(anything)
        And:
          Postgres.query(sql"select pruned_offset()").returns(table(secondOffset))
      },
      funcTest("reset exactly to pruned offset preserves pruned_offset") {
        val secondOffset = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions where ix=2"""
          } `returns` table { secondOffset.capture }
        When:
          // prune at second offset - its transaction survives (has active "one updated")
          Postgres.query(sql"select * from prune_archived_to_offset(${secondOffset.get})").returns(anything)
        Then:
          Postgres.query(sql"select pruned_offset()").returns(table(secondOffset))
        And:
          // reset exactly to the pruned offset - pruned_offset should remain unchanged
          Postgres.query(sql"select * from reset_to_offset(${secondOffset.get})").returns(anything)
        And:
          Postgres.query(sql"select pruned_offset()").returns(table(secondOffset))
      },
      funcTest("reset to pruned-away offset resolves to nearest surviving transaction") {
        val secondOffset = Capture[OffsetType]
        val fourthOffset = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            anything | secondOffset.capture | anything | fourthOffset.capture | anything | anything
          }.transpose
        When:
          // prune at fourth offset - deletes orphaned transactions at ix 1, 3, 4
          Postgres.query(sql"select * from prune_archived_to_offset(${fourthOffset.get})").returns(anything)
        Then:
          Postgres.query(sql"select pruned_offset()").returns(table(fourthOffset))
        And:
          // reset to fourth offset whose transaction was pruned away;
          // should resolve to nearest surviving tx (secondOffset) and retreat pruned_offset
          Postgres.query(sql"select * from reset_to_offset(${fourthOffset.get})").returns(anything)
        And:
          Postgres.query(sql"select pruned_offset()").returns(table(secondOffset))
        And:
          // watermark should point to the nearest surviving transaction
          Postgres.query(sql"""select "offset" from latest_checkpoint()""").returns(table(secondOffset))
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
          Postgres.query(sql"select deleted_transactions from prune_archived_to_offset(null)").returns(anything)
        Expect:
          // nothing should be pruned
          Postgres query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` table { firstOffset }
      },
      funcTest("reset to pruned-away offset resolves to nearest surviving transaction") {
        val secondOffset = Capture[OffsetType]
        val fourthOffset = Capture[OffsetType]
        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            anything | secondOffset.capture | anything | fourthOffset.capture | anything | anything
          }.transpose
        When:
          // prune at fourth offset - deletes orphaned transactions at ix 1, 3, 4
          Postgres.query(sql"select * from prune_archived_to_offset(${fourthOffset.get})").returns(anything)
        Then:
          // reset to fourth offset whose transaction was pruned away;
          // should resolve to nearest surviving tx (secondOffset)
          Postgres.query(sql"select * from reset_to_offset(${fourthOffset.get})").returns(anything)
        And:
          // watermark should point to the nearest surviving transaction
          Postgres.query(sql"""select "offset" from latest_checkpoint()""").returns(table(secondOffset))
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
            .query(sql"select deleted_transactions from prune_archived_to_offset($offsetFunction)")
            .returns(table(3))
        And:
          Postgres query {
            sql"""select payload->>'label', created_at_offset from active($templateRef) order by created_at_offset"""
          } `returns` table {
            "one updated"   | oneUpdated
            "three updated" | threeUpdated
          }
        And:
          Postgres query {
            sql"""select payload->>'label', created_at_offset, archived_at_offset from archives($templateRef)"""
          } `returns` table { "three" | threeCreated | threeUpdated }
      })*
    ) + suite("pruning lock contention")(
      funcTest("concurrent prunes block each other via SHARE UPDATE EXCLUSIVE") {
        @volatile var pruneStarted: Promise[Nothing, Unit] = null
        @volatile var releasePrune: Promise[Nothing, Unit] = null
        val pruneTarget                                    = Capture[OffsetType]

        Given:
          context
        And:
          Postgres query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` table {
            anything | anything | anything | pruneTarget.capture | anything | anything
          }.transpose
        When:
          // Fork a transaction that calls the actual prune function. prune_archived_to_offset
          // acquires SHARE UPDATE EXCLUSIVE on __pruning_metadata (see R__functions.sql), which
          // self-conflicts per the PostgreSQL lock compatibility matrix. By holding the
          // transaction open (blocking on releasePrune), the lock persists until we release it.
          //
          // Why sql"begin" is needed: Postgres.query wraps the effect in zio-jdbc's `transact`,
          // which manages the connection's autocommit mode. An explicit BEGIN ensures the lock
          // acquired inside prune_archived_to_offset is held across our await boundary. Without
          // it, zio-jdbc may commit the implicit transaction, releasing the lock prematurely.
          ZLayer.fromZIO {
            for {
              _ <- Promise.make[Nothing, Unit].map(pruneStarted = _)
              _ <- Promise.make[Nothing, Unit].map(releasePrune = _)
              _ <- Postgres.query {
                for {
                  _ <- sql"begin".execute
                  _ <- sql"select * from prune_archived_to_offset(${pruneTarget.get})".query[String].selectOne
                  _ <- pruneStarted.succeed(())
                  _ <- releasePrune.await
                } yield ()
              }.fork
            } yield ()
          }
        When:
          pruneStarted.await
        Expect:
          // A second concurrent prune should block on the same SHARE UPDATE EXCLUSIVE lock
          // and time out, proving that two prune operations cannot run simultaneously.
          //
          // The second call also needs sql"begin" because SET LOCAL and the lock acquired by
          // prune_archived_to_offset require an active transaction block. Without BEGIN,
          // PostgreSQL rejects LOCK TABLE with "can only be used in transaction blocks".
          Postgres
            .query {
              for {
                _ <- sql"begin".execute
                _ <- sql"SET LOCAL lock_timeout = '3s'".execute
                r <- sql"select * from prune_archived_to_offset(${pruneTarget.get})".query[String].selectOne
              } yield r
            }
            .failsWithMessage("ERROR: canceling statement due to lock timeout")
        When:
          // Release the prune transaction. The forked fiber may be interrupted by the test
          // framework's layer teardown, rolling back the prune. This is fine: the lock timeout
          // above already proved SHARE UPDATE EXCLUSIVE self-conflicts.
          releasePrune.succeed(())
      }
    )
