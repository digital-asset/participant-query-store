// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.pipeline.pipeline.ledger.CliStartOffset
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import com.digitalasset.pqs.utils.safeequals.=/=
import zio.*
import zio.jdbc.*
import zio.test.*
import zio.test.Assertion.*

object SingleWriterSpec extends SharedLedgerAndPostgresTest:

  def spec = suite("single writer")(
    funcTest("concurrent PQS instances keep a single writer and a consistent database") {
      val alice            = Party("Alice")
      val ledger           = Capture[LedgerState]
      val a                = Capture[Instance]
      val b                = Capture[Instance]
      val c                = Capture[Instance]
      val d                = Capture[Instance]
      val e                = Capture[Instance]
      val loser            = Capture[Instance]
      val unblockWatermark = Capture[UIO[Unit]]
      def survivor         = if loser.get == d.get then e.get else d.get

      Given:
        DamlSdk.deploy(Dars.pingSender) ++ DamlSdk.parties(alice) ++ Postgres.database
      When:
        startLedgerTraffic(alice)
      And:
        startPqs("a", CliStartOffset.Genesis).is(a.capture)
      When:
        waitUntilStarted(a.get)
      When:
        startRecordingWriters
      Then:
        latestWriter.is(Some("pqs-a")).retryUntilTimeout(asyncProcessingTimeout)
      And:
        startPqs("b", CliStartOffset.Oldest).is(b.capture)
      Then:
        waitForExit(a.get).is(replacedByNewerWriter)
      And:
        latestWriter.is(Some("pqs-b")).retryUntilTimeout(asyncProcessingTimeout)
      And:
        startPqs("c", CliStartOffset.Oldest, "--target-schema-autoapply=false").is(c.capture)
      Then:
        waitForExit(b.get).is(replacedByNewerWriter)
      And:
        latestWriter.is(Some("pqs-c")).retryUntilTimeout(asyncProcessingTimeout)
      And:
        blockWatermarkUpdates.is(unblockWatermark.capture)
      And:
        startPqs("d", CliStartOffset.Oldest).is(d.capture)
      And:
        startPqs("e", CliStartOffset.Oldest).is(e.capture)
      Then:
        appsWaitingOnWatermark.is(hasSubset(Set("pqs-d", "pqs-e"))).retryUntilTimeout(asyncProcessingTimeout)
      When:
        unblockWatermark.get
      Then:
        waitForExit(c.get).is(replacedByNewerWriter)
      And:
        firstToExit(d.get, e.get).is(loser.capture)
      Then:
        waitForExit(loser.get).is(replacedByNewerWriter)
      And:
        stopLedgerTraffic.is(ledger.capture)
      Then:
        lastProcessedOffset.is(Some(ledger.get.lastOffset)).retryUntilTimeout(asyncProcessingTimeout)
      And:
        statusAfter(survivor, 2.seconds).is(stillRunning)
      When:
        fillReferenceDatabase(ledger.get.lastOffset)
      Then:
        sameAsReference(createdContracts)
      And:
        sameAsReference(archivedContracts)
      And:
        activeContracts.is(ledger.get.activeContractIds)
      And:
        // We know the order of the first 3 writers and the last writer (the survivor),
        // but we do not know whether the loser committed anything.
        // This can happen when:
        // 1. d inserts a transaction and updates the watermark.
        // 2. e fails because of a unique constraint on that transaction.
        // 3. e restarts and registers itself as the writer.
        // 4. d fails because it is no longer the main writer.
        // 5. e updates the watermark.
        writersInOrder.is(startsWith(Chunk("pqs-a", "pqs-b", "pqs-c")) && hasLast(equalTo(survivor.app)))
      And:
        // we're checking here that the last two writers we started concurrently did not compete
        writersInOrder.is(isDistinct)
      And:
        advancesByDeposedWriters.is(isEmpty)
      And:
        watermarkPositions.is(strictlyIncreasing)
    }
  )

  private type LedgerEnv = Docker & Ledger & DarFile
  private type PqsEnv    = LedgerEnv & Parties & Postgres & Database & Scope

  private val pingFqn = s"${Dars.pingSender.name}:PingPong:Ping"

  private val asyncProcessingTimeout: Duration = 50.seconds

  // Business operations

  private final case class Traffic(
      alice: Party,
      activeContractIds: Ref[Set[String]],
      stop: Promise[Nothing, Unit],
      workers: Fiber[Throwable, Unit]
  )

  private final case class LedgerState(lastOffset: Long, activeContractIds: Set[String])

  // 4 workers create contracts and archive all but their 3 newest, so deactivations are pending at every takeover
  private def startLedgerTraffic(alice: Party): URLayer[LedgerEnv, Traffic] =
    ZLayer.scoped(
      for
        activeContractIds <- Ref.make(Set.empty[String])
        stop              <- Promise.make[Nothing, Unit]
        workers <- ZIO
          .collectAllParDiscard(List.fill(4)(trafficWorker(alice, activeContractIds, stop, Vector.empty)))
          .forkScoped
      yield Traffic(alice, activeContractIds, stop, workers)
    )

  // Fail fast if a traffic worker died unexpectedly
  private def whileTrafficRuns[R, A](io: ZIO[R, Throwable, A]): ZIO[R & Traffic, Throwable, A] =
    ZIO.serviceWithZIO[Traffic](traffic => io.raceFirst(traffic.workers.join *> ZIO.never))

  private def trafficWorker(
      alice: Party,
      activeContractIds: Ref[Set[String]],
      stop: Promise[Nothing, Unit],
      own: Vector[String]
  ): ZIO[LedgerEnv, Throwable, Unit] =
    ZIO.unlessZIODiscard(stop.isDone)(
      for
        cid <- createContract(alice, activeContractIds)
        _   <- ZIO.foreachDiscard(own.dropRight(2))(archive(alice, activeContractIds))
        _   <- trafficWorker(alice, activeContractIds, stop, own.takeRight(2) :+ cid)
      yield ()
    )

  private def createContract(alice: Party, activeContractIds: Ref[Set[String]]): ZIO[LedgerEnv, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger
      .create("PingPong:Ping", args, alice)
      .map(_.getTransaction.events(0).getCreated.contractId)
      .tap(cid => activeContractIds.update(_ + cid))

  private def archive(alice: Party, activeContractIds: Ref[Set[String]])(cid: String): ZIO[LedgerEnv, Throwable, Long] =
    Ledger.archive("PingPong:Ping", cid, alice).map(_.getTransaction.offset) <* activeContractIds.update(_ - cid)

  // Workers interleave and the shared ledger's end includes other specs' transactions, so the last offset this PQS
  // sees is taken from one more archive once the workers have stopped
  private val stopLedgerTraffic: ZIO[LedgerEnv & Traffic, Throwable, LedgerState] =
    for
      traffic <- ZIO.service[Traffic]
      _       <- traffic.stop.succeed(()) *> traffic.workers.join
      cid     <- traffic.activeContractIds.get.map(_.headOption).someOrFail(Throwable("No active contract to archive"))
      lastOffset        <- archive(traffic.alice, traffic.activeContractIds)(cid)
      activeContractIds <- traffic.activeContractIds.get
    yield LedgerState(lastOffset, activeContractIds)

  // A PQS instance and the application name its sessions and audit rows carry
  private final case class Instance(app: String, svc: Service[Pipeline])

  private def startPqs(label: String, ledgerStart: CliStartOffset, args: String*): ZIO[PqsEnv, Throwable, Instance] =
    val app = s"pqs-$label"
    val common = Seq(
      s"--pipeline-ledger-start=$ledgerStart",
      s"--target-postgres-properties-ApplicationName=$app",
      "--target-postgres-maxconnections=4",
      // Instances writing at the same time during a takeover collide on each other's rows and restart
      "--retry-counter-attempts=3"
    )
    Pqs.attemptPipeline((common ++ args)*).build.map(env => Instance(app, env.get[Service[Pipeline]]))

  private def waitUntilStarted(pqs: Instance): Task[Unit] =
    pqs.svc
      .blockUntilStdOut(_.contains("Continuing from offset"))
      .timeoutFail(RuntimeException(s"${pqs.app} did not start"))(asyncProcessingTimeout)

  private def firstToExit(x: Instance, y: Instance): RIO[Traffic, Instance] =
    whileTrafficRuns(x.svc.exitCode.as(x).raceFirst(y.svc.exitCode.as(y)))
      .timeoutFail(RuntimeException(s"neither ${x.app} nor ${y.app} exited"))(asyncProcessingTimeout + 5.seconds)

  private val blockWatermarkUpdates = holdLock(sql"select 1 from __watermark for update".query[Int].selectAll)

  // Takes the lock in a transaction left open until the returned release action closes it
  private def holdLock(lock: ZIO[ZConnection, Throwable, Any]): ZIO[Database & Scope, Throwable, UIO[Unit]] =
    for
      tx   <- ZIO.scopeWith(_.fork)
      conn <- tx.extend[Database](Database.transaction.build)
      _    <- lock.provideEnvironment(conn)
    yield tx.close(Exit.unit)

  // Records each claim of the writer slot and each watermark advance with the app that made it
  private val startRecordingWriters: ZIO[Database, Throwable, Unit] =
    Postgres.call(
      sql"""create table ft_watermark_audit (
              seq bigserial primary key,
              app text not null default current_setting('application_name'),
              kind text not null,
              ix bigint not null);
            create function ft_watermark_audit_fn() returns trigger as $$$$
            begin
              insert into ft_watermark_audit(kind, ix) values (tg_argv[0], new.ix);
              return new;
            end $$$$ language plpgsql;
            create trigger ft_watermark_claim_trg after update of instance_id on __watermark
              for each row execute function ft_watermark_audit_fn('claim');
            create trigger ft_watermark_advance_trg after update of ix on __watermark
              for each row execute function ft_watermark_audit_fn('advance');"""
    )

  private final case class Reference(db: Database)

  // A fresh database filled by a single PQS from Genesis up to `stopAt`
  private def fillReferenceDatabase(stopAt: Long) =
    Postgres.database >+> Pqs.runPipeline("--pipeline-ledger-start=Genesis", s"--pipeline-ledger-stop=$stopAt")
      >>> ZLayer.fromFunction((db: Database) => Reference(db))

  // Observations

  private final case class Exited(code: ExitCode, stderr: String)

  private def waitForExit(pqs: Instance): RIO[Traffic, Exited] =
    for
      code <- whileTrafficRuns(pqs.svc.exitCode)
        .timeoutFail(RuntimeException(s"${pqs.app} is still running"))(asyncProcessingTimeout)
      stderr <- Pqs.stderr.provideEnvironment(ZEnvironment(pqs.svc))
    yield Exited(code, stderr)

  // None while the instance is still running
  private def statusAfter(pqs: Instance, hold: Duration): Task[Option[ExitCode]] =
    pqs.svc.exitCode.timeout(hold)

  // Waiters for a locked row queue on its tuple lock: the first holds it while waiting, the rest wait for it.
  // pg_stat_activity is server-wide, so queries on it filter by database
  private val appsWaitingOnWatermark: ZIO[Database, Throwable, Set[String]] =
    Postgres
      .query(
        sql"""select distinct a.application_name from pg_stat_activity a join pg_locks l on l.pid = a.pid
              where a.datname = current_database() and a.wait_event_type = 'Lock'
                and l.locktype = 'tuple' and l.relation = '__watermark'::regclass""".query[String].selectAll
      )
      .map(_.toSet)

  // Claims and advances in commit order: the app, whether it claimed the writer slot, and the ix
  private val watermarkUpdates: ZIO[Database, Throwable, Chunk[(String, Boolean, Long)]] =
    Postgres.query(
      sql"select app, kind = 'claim', ix from ft_watermark_audit order by seq".query[(String, Boolean, Long)].selectAll
    )

  private val advances: ZIO[Database, Throwable, Chunk[(String, Long)]] =
    watermarkUpdates.map(_.collect { case (app, false, ix) => (app, ix) })

  // Advances made after another app's claim; pqs-a claimed before recording started
  private val advancesByDeposedWriters: ZIO[Database, Throwable, Chunk[String]] =
    watermarkUpdates.map(
      _.foldLeft(("pqs-a", Chunk.empty[String])) {
        case ((_, deposed), (app, true, _))                             => (app, deposed)
        case ((claimant, deposed), (app, false, _)) if app =/= claimant => (claimant, deposed :+ app)
        case (state, _)                                                 => state
      }._2
    )

  // Apps in the order they advanced the watermark, consecutive repeats collapsed
  private val writersInOrder: ZIO[Database, Throwable, Chunk[String]] =
    advances.map(_.map(_._1).dedupe)

  private val latestWriter: ZIO[Database & Traffic, Throwable, Option[String]] =
    whileTrafficRuns(writersInOrder.map(_.lastOption))

  private val watermarkPositions: ZIO[Database, Throwable, Chunk[Long]] =
    advances.map(_.map(_._2))

  private val lastProcessedOffset: ZIO[Database, Throwable, Option[Long]] =
    Postgres.query(sql"""select "offset" from latest_checkpoint()""".query[Long].selectOne)

  private val activeContracts: ZIO[Database, Throwable, Set[String]] =
    Postgres.query(sql"select contract_id from active($pingFqn)".query[String].selectAll).map(_.toSet)

  // Compared through the read API, so changes to PQS tables need no changes here
  private val createdContracts: ZIO[Database, Throwable, Table] =
    Database.creates(extraColumns = Seq("created_at_offset"))

  private val archivedContracts: ZIO[Database, Throwable, Table] =
    Database.archives(extraColumns = Seq("archived_at_offset"))

  private def sameAsReference(query: RIO[Database, Table]): RIO[Database & Reference, TestResult] =
    for
      expected <- ZIO.serviceWithZIO[Reference](ref => query.provideEnvironment(ZEnvironment(ref.db)))
      result   <- query.is(expected)
    yield result

  // Expected results

  private val replacedByNewerWriter: Assertion[Exited] =
    hasField("exit code", (_: Exited).code, equalTo(ExitCode.failure)) &&
      hasField("stderr", (_: Exited).stderr, containsString("PQS writer instance has changed"))

  private val stillRunning: Assertion[Option[ExitCode]] = isNone

  private val strictlyIncreasing: Assertion[Iterable[Long]] = isSorted[Long] && isDistinct
