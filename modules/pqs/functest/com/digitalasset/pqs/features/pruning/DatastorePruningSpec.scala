// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.pruning

import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.{OffsetType, biggestOffset, smallestOffset}
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.{Promise, RIO, ZLayer}
import zio.jdbc.sqlInterpolator
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object DatastorePruningSpec extends SharedLedgerAndPostgresTest:
  private val packageName = Dars.label.name
  private val templateRef = s"$packageName:Label:Label"
  private val choiceRef   = s"$templateRef:ChangeLabel"

  def context =
    val alice = Party("Alice")
    DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.deploy(Dars.label)
      >+> DamlSdk.runScript("Label:setup", alice.id)
      >+> Pqs.runPipeline(
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
          Database query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` Table {
            oneCreated.capture |
              oneUpdated.capture |
              twoCreated.capture |
              twoArchived.capture |
              threeCreated.capture |
              threeUpdated.capture
          }.transpose
        Expect:
          // dry run
          Database
            .query(sql"select * from prune_archived_to_offset_dry_run(${twoArchived.get})")
            .returns(Table(threeCreated | 2 | 2 | 4 | 3 | 0))
        And:
          // dry run should NOT persist pruned_offset
          Database.query(sql"select pruned_offset() is null").returns(Table(true))
        And:
          // actual pruning
          Database
            .query(sql"select * from prune_archived_to_offset(${twoArchived.get})")
            .returns(Table(threeCreated | 2 | 2 | 4 | 3 | 0))
        And:
          // force run should persist pruned_offset
          Database.query(sql"select pruned_offset()").returns(Table(twoArchived))
        And:
          // another dry run with the same offset is a no-op (returns zeroed stats)
          Database
            .query(
              sql"select deleted_contracts from prune_archived_to_offset_dry_run(${twoArchived.get})"
            )
            .returns(Table(0))
        And:
          Database query {
            sql"""select payload->>'label', created_at_offset from active($templateRef) order by created_at_offset"""
          } `returns` Table {
            "one updated"   | oneUpdated
            "three updated" | threeUpdated
          }
        And:
          // archived contracts created before the pruning offset should be deleted
          Database query {
            sql"""select payload->>'label', created_at_offset, archived_at_offset from archives($templateRef)"""
          } `returns` Table { "three" | threeCreated | threeUpdated }
        And:
          Database query {
            sql"""select argument->>'newLabel' from exercises($choiceRef)"""
          } `returns` Table { "three updated" }
      },
      funcTest("prevent pruning at illegal offsets") {
        val firstOffset  = Capture[OffsetType]
        val secondOffset = Capture[OffsetType]
        val latestOffset = Capture[OffsetType]
        Given:
          context
        And:
          Database query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` Table { firstOffset.capture }
        And:
          Database query {
            // the second offset is the exercise transaction, which archives contract "one" and creates contract "one updated"
            sql"""select "offset" from __transactions where ix=2"""
          } `returns` Table { secondOffset.capture }
        And:
          Database query {
            sql"""select "offset" from latest_checkpoint()"""
          } `returns` Table { latestOffset.capture }
        Expect:
          // latest offset is illegal
          Database
            .query(sql"select deleted_transactions from prune_archived_to_offset(${latestOffset.get})")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset ${latestOffset.get} coincides with latest consistent checkpoint of contiguous history"
            )
        And:
          // out of upper bounds
          Database
            .query(sql"select deleted_transactions from prune_archived_to_offset($biggestOffset)")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset $biggestOffset is beyond upper bounds of contiguous history"
            )
        And:
          // out of lower bounds
          Database
            .query(sql"select deleted_transactions from prune_archived_to_offset($smallestOffset)")
            .failsWithMessage(
              s"ERROR: Illegal pruning offset $smallestOffset is outside lower bounds of contiguous history"
            )
        When:
          // pruning to second offset:
          // - deletes the first transaction, because contract "one" is archived and pruned
          // - keeps the second transaction, because contract "one updated" is still active
          Database.query(sql"select deleted_transactions from prune_archived_to_offset(${secondOffset.get})")
        Expect:
          Database.query(sql"select pruned_offset()").returns(Table(secondOffset))
        And:
          // pruning below already-pruned offset is a no-op (zeroed stats)
          Database
            .query(sql"select deleted_transactions from prune_archived_to_offset(${firstOffset.get})")
            .returns(Table(0))
        And:
          // pruned_offset unchanged after no-op
          Database.query(sql"select pruned_offset()").returns(Table(secondOffset))
      },
      funcTest("deny pruning at non-contiguous offsets") {
        val thirdOffset = Capture[OffsetType]
        Given:
          context
        And:
          Database query {
            sql"""select "offset" from __transactions order by ix limit 1 offset 2"""
          } `returns` Table {
            thirdOffset.capture
          }
        And:
          // delete second transaction offset
          Database `makeGap` (2 to 2) `returns` 1
        Expect:
          // out of contiguous range
          Database
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
          Database query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` Table {
            anything | secondOffset.capture | anything | fourthOffset.capture | fifthOffset.capture | anything
          }.transpose
        When:
          // prune at fourth offset
          Database.query(sql"select * from prune_archived_to_offset(${fourthOffset.get})").returns(anything)
        Then:
          Database.query(sql"select pruned_offset()").returns(Table(fourthOffset))
        And:
          // reset to fifth offset (above pruned) - pruned_offset should remain
          Database.query(sql"select * from reset_to_offset(${fifthOffset.get})").returns(anything)
        And:
          Database.query(sql"select pruned_offset()").returns(Table(fourthOffset))
        And:
          // reset to second offset (below pruned) - pruned_offset should retreat to reset offset
          Database.query(sql"select * from reset_to_offset(${secondOffset.get})").returns(anything)
        And:
          Database.query(sql"select pruned_offset()").returns(Table(secondOffset))
      },
      funcTest("reset exactly to pruned offset preserves pruned_offset") {
        val secondOffset = Capture[OffsetType]
        Given:
          context
        And:
          Database query {
            sql"""select "offset" from __transactions where ix=2"""
          } `returns` Table { secondOffset.capture }
        When:
          // prune at second offset - its transaction survives (has active "one updated")
          Database.query(sql"select * from prune_archived_to_offset(${secondOffset.get})").returns(anything)
        Then:
          Database.query(sql"select pruned_offset()").returns(Table(secondOffset))
        And:
          // reset exactly to the pruned offset - pruned_offset should remain unchanged
          Database.query(sql"select * from reset_to_offset(${secondOffset.get})").returns(anything)
        And:
          Database.query(sql"select pruned_offset()").returns(Table(secondOffset))
      },
      funcTest("reset to pruned-away offset resolves to nearest surviving transaction") {
        val secondOffset = Capture[OffsetType]
        val fourthOffset = Capture[OffsetType]
        Given:
          context
        And:
          Database query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` Table {
            anything | secondOffset.capture | anything | fourthOffset.capture | anything | anything
          }.transpose
        When:
          // prune at fourth offset - deletes orphaned transactions at ix 1, 3, 4
          Database.query(sql"select * from prune_archived_to_offset(${fourthOffset.get})").returns(anything)
        Then:
          Database.query(sql"select pruned_offset()").returns(Table(fourthOffset))
        And:
          // reset to fourth offset whose transaction was pruned away;
          // should resolve to nearest surviving tx (secondOffset) and retreat pruned_offset
          Database.query(sql"select * from reset_to_offset(${fourthOffset.get})").returns(anything)
        And:
          Database.query(sql"select pruned_offset()").returns(Table(secondOffset))
        And:
          // watermark should point to the nearest surviving transaction
          Database.query(sql"""select "offset" from latest_checkpoint()""").returns(Table(secondOffset))
      },
      funcTest("pruning with NULL argument offset does nothing") {
        val firstOffset = Capture[OffsetType]
        Given:
          context
        And:
          Database query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` Table { firstOffset.capture }
        And:
          Database.query(sql"select deleted_transactions from prune_archived_to_offset(null)").returns(anything)
        Expect:
          // nothing should be pruned
          Database query {
            sql"""select "offset" from oldest_checkpoint()"""
          } `returns` Table { firstOffset }
      },
      funcTest("reset to pruned-away offset resolves to nearest surviving transaction") {
        val secondOffset = Capture[OffsetType]
        val fourthOffset = Capture[OffsetType]
        Given:
          context
        And:
          Database query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` Table {
            anything | secondOffset.capture | anything | fourthOffset.capture | anything | anything
          }.transpose
        When:
          // prune at fourth offset - deletes orphaned transactions at ix 1, 3, 4
          Database.query(sql"select * from prune_archived_to_offset(${fourthOffset.get})").returns(anything)
        Then:
          // reset to fourth offset whose transaction was pruned away;
          // should resolve to nearest surviving tx (secondOffset)
          Database.query(sql"select * from reset_to_offset(${fourthOffset.get})").returns(anything)
        And:
          // watermark should point to the nearest surviving transaction
          Database.query(sql"""select "offset" from latest_checkpoint()""").returns(Table(secondOffset))
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
          Database query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` Table {
            oneCreated.capture |
              oneUpdated.capture |
              twoCreated.capture |
              twoArchived.capture |
              threeCreated.capture |
              threeUpdated.capture
          }.transpose
        And:
          Database `query`
            sql"""
                 update __transactions
                 set effective_at = $resetTime :: timestamp with time zone
                 where "offset" <= ${twoArchived.get}
              """.update `returns` anything
        Expect:
          // expected deletion of two transactions
          Database
            .query(sql"select deleted_transactions from prune_archived_to_offset($offsetFunction)")
            .returns(Table(3))
        And:
          Database query {
            sql"""select payload->>'label', created_at_offset from active($templateRef) order by created_at_offset"""
          } `returns` Table {
            "one updated"   | oneUpdated
            "three updated" | threeUpdated
          }
        And:
          Database query {
            sql"""select payload->>'label', created_at_offset, archived_at_offset from archives($templateRef)"""
          } `returns` Table { "three" | threeCreated | threeUpdated }
      })*
    ) + suite("pruning lock contention")(
      funcTest("concurrent prunes block each other via SHARE UPDATE EXCLUSIVE") {
        @volatile var pruneStarted: Promise[Nothing, Unit] = null
        @volatile var releasePrune: Promise[Nothing, Unit] = null
        val pruneTarget                                    = Capture[OffsetType]

        Given:
          context
        And:
          Database query {
            sql"""select "offset" from __transactions order by ix"""
          } `returns` Table {
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
              _ <- Database.query {
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
          Database
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
