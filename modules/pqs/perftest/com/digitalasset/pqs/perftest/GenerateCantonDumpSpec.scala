// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.daml.ledger.api.v2.value.{Record, RecordField, Value, List => ValueList}
import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.docker.Docker
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import zio.test.*
import zio.*

import scala.language.implicitConversions

/** Regenerates the checked-in performance-test Postgres dump.
  */
object GenerateCantonDumpSpec extends FuncTestStandalone:

  def spec = suite("GenerateCantonDumpSpec")(
    funcTest(s"generate dump files"):
      val alice = Party("alice")
      Given:
        Postgres.instance(CantonDumps.all.map(_.databaseName))

      // Starting up Canton + running the ingestion script + the graceful shutdown on scope close
      When(timeout = 20.minutes):
        // Run in its own scope so the Ledger is stopped before we generate the dumps
        // TODO Closing the scope is slow because Canton participant node fails to close gracefully
        ZIO.scoped {
          val layer = Ledger.instanceWithPgStorage
            >+> DamlSdk.deploy(Dars.seedAndBlobs)
            >+> DamlSdk.parties(alice)
            // create 20K tx and 1M contracts, including ~250K active blobs
            >+> ZLayer.fromZIO(
              hydrateLedger(
                alice,
                totalTx = 20_000,
                totalContracts = 1_000_000,
                activeRatio = 0.25
              )
            )
          layer.build
        }

      When(timeout = 4.minutes):
        ZIO.foreachDiscard(CantonDumps.all)(Postgres.dumpDatabase(_))
  ) @@ TestAspect.ifEnv("REGENERATE_CANTON_DUMP")(_.toBooleanOption.getOrElse(false))

  private def hydrateLedger(
      alice: Party,
      totalTx: Int,
      totalContracts: Int,
      activeRatio: Float,
      workers: Int = 8
  ) =
    for
      txAndContractCounts <- Ref.make((0, 0))
      stop      = txAndContractCounts.get.map((tx, contracts) => tx > totalTx && contracts > totalContracts)
      increment = trackProgress(totalTx, totalContracts, txAndContractCounts)(_, _)
      _ <- ZIO.collectAllParDiscard(
        Chunk.fill(workers)(startWorker(alice, totalTx, totalContracts, activeRatio, stop, increment))
      )
    yield ()

  private def startWorker(
      alice: Party,
      totalTx: Int,
      totalContracts: Int,
      activeRatio: Float,
      stop: UIO[Boolean],
      trackProgress: (Int, Int) => UIO[Unit]
  ) =
    val maxValue         = Math.round(totalContracts * activeRatio / 10)
    val contractPerTxAvg = totalContracts / totalTx
    def loop(seed: String, activeBlobs: Chunk[String]): ZIO[Docker & Ledger & DarFile, Throwable, Unit] =
      ZIO.unlessZIODiscard(stop) {
        for
          newBlobs <- ZIO
            .collectAllPar(Chunk.fill(4)(createBlobs(alice, seed, contractPerTxAvg, maxValue)))
            .map(_.flatten)
          allBlobs <- Random.RandomLive.shuffle(activeBlobs ++ newBlobs)
          (blobsToArchive, remainingBlobs) = allBlobs.splitAt(Math.round((1 - activeRatio) * newBlobs.size))
          _ <- archiveBlobs(alice, seed, blobsToArchive)
          _ <- trackProgress(5, newBlobs.size)
          _ <- loop(seed, remainingBlobs)
        yield ()
      }
    for
      seed <- createSeed(alice)
      _    <- loop(seed, Chunk.empty)
    yield ()

  private def trackProgress(totalTx: Int, totalContracts: Int, txAndContractCounts: Ref[(Int, Int)])(
      txInc: Int,
      contractInc: Int
  ): UIO[Unit] =
    for
      (_, previousContracts) <- txAndContractCounts.get
      previousPercentage = previousContracts * 100 / totalContracts
      (nextTx, nextContracts) <- txAndContractCounts.updateAndGet((tx, contracts) =>
        (tx + txInc, contracts + contractInc)
      )
      nextPercentage = nextContracts * 100 / totalContracts
      _ <- ZIO.unlessDiscard(previousPercentage == nextPercentage)(
        ZIO.logInfo(s"Hydrating ledger: $nextPercentage% ($nextTx transactions and $nextContracts contracts)")
      )
    yield ()

  private def createSeed(alice: Party) =
    val args = Record.defaultInstance.addFields(RecordField("owner", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("Blob:Seed", args, alice)
      .map(_.getTransaction.events(0).getCreated.contractId)

  private def createBlobs(
      alice: Party,
      seed: String,
      averageSize: Int,
      maxValue: Int
  ): ZIO[Docker & Ledger & DarFile, Throwable, Chunk[String]] =
    def randomBlobValues(maxSize: Int) =
      for
        size   <- Random.nextIntBounded(maxSize)
        values <- ZIO.collectAll(Chunk.fill(size)(Random.nextIntBounded(maxValue)))
      yield Value(Value.Sum.List(ValueList.of(values.map(v => Value(Value.Sum.Int64(v))))))
    for {
      as <- randomBlobValues(averageSize)
      bs <- randomBlobValues(averageSize / 2)
      cs <- randomBlobValues(averageSize / 4)
      arg = Value(
        Value.Sum.Record(
          Record().addFields(
            RecordField("as", Some(as)),
            RecordField("bs", Some(bs)),
            RecordField("cs", Some(cs))
          )
        )
      )
      response <- Ledger.exercise("Blob:Seed", seed, "CreateMany", arg, alice)
    } yield Chunk.fromIterable(response.getTransaction.events.map(_.getCreated.contractId))

  private def archiveBlobs(
      alice: Party,
      seed: String,
      blobs: Chunk[String]
  ): ZIO[Docker & Ledger & DarFile, Throwable, Unit] =
    val arg = Value(
      Value.Sum.Record(
        Record().addFields(
          RecordField(
            "blobs",
            Some(Value(Value.Sum.List(ValueList.of(blobs.map(blob => Value(Value.Sum.ContractId(blob)))))))
          )
        )
      )
    )
    Ledger.exercise("Blob:Seed", seed, "ArchiveMany", arg, alice).unit
