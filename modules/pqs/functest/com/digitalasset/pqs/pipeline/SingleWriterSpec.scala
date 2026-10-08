// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.*
import zio.jdbc.*
import zio.test.*
import zio.test.Assertion.*

object SingleWriterSpec extends SharedLedgerAndPostgresTest:
  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |template Ping
                    |  with
                    |    sender: Party
                    |  where
                    |    signatory sender
                    |""".stripMargin
  )

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
        DamlSdk.deploy(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database
      When:
        startLedgerTraffic(alice)
      And:
        startPqs("a", genesis).is(a.capture)
      When:
        waitUntilStarted(a.get)
      When:
        startRecordingWriters
      Then:
        latestWriter.is(Some("pqs-a")).retryUntilTimeout(asyncProcessingTimeout)
      And:
        startPqs("b", resume).is(b.capture)
      Then:
        waitForExit(a.get).is(replacedByNewerWriter)
      And:
        latestWriter.is(Some("pqs-b")).retryUntilTimeout(asyncProcessingTimeout)
      And:
        startPqs("c", resume, "--target-schema-autoapply=false").is(c.capture)
      Then:
        waitForExit(b.get).is(replacedByNewerWriter)
      And:
        latestWriter.is(Some("pqs-c")).retryUntilTimeout(asyncProcessingTimeout)
      And:
        blockWatermarkUpdates.is(unblockWatermark.capture)
      And:
        startPqs("d", resume).is(d.capture)
      And:
        startPqs("e", resume).is(e.capture)
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
        rowsOnlyInOneDatabase.is(isEmpty)
      And:
        activeContracts.is(ledger.get.unarchived)
      And:
        // we know the order of first 3 writers and we know the last writer (survivor), but don't know if the loser committed anything
        writersInOrder.is(startsWith(Chunk("pqs-a", "pqs-b", "pqs-c")) && hasLast(equalTo(survivor.app)))
      And:
        // we're checking here that the last two writers we started concurrently did not compete
        writersInOrder.is(isDistinct)
      And:
        watermarkPositions.is(strictlyIncreasing)
    }
  )

  private type LedgerEnv = Docker & Service[Ledger] & DarFile
  private type PqsEnv    = LedgerEnv & Parties & Postgres & Database & Scope

  private val genesis = "--pipeline-ledger-start=Genesis"
  private val resume  = "--pipeline-ledger-start=Oldest"
  private val pingFqn = s"${pingPong.name}:PingPong:Ping"

  private val asyncProcessingTimeout: Duration = 50.seconds

  // Business operations

  private final case class Traffic(
      alice: Party,
      live: Ref[Set[String]],
      stop: Promise[Nothing, Unit],
      workers: Fiber[Throwable, Unit]
  )

  private final case class LedgerState(lastOffset: Long, unarchived: Set[String])

  // 4 workers create contracts and archive all but their 3 newest, so deactivations are pending at every takeover
  private def startLedgerTraffic(alice: Party): URLayer[LedgerEnv, Traffic] =
    ZLayer.scoped(
      for
        live    <- Ref.make(Set.empty[String])
        stop    <- Promise.make[Nothing, Unit]
        workers <- ZIO.collectAllParDiscard(List.fill(4)(trafficWorker(alice, live, stop, Vector.empty))).forkScoped
      yield Traffic(alice, live, stop, workers)
    )

  // Takeovers only show up with new transactions, so a wait on one fails with the traffic error once the workers die
  private def whileTrafficRuns[R, A](io: ZIO[R, Throwable, A]): ZIO[R & Traffic, Throwable, A] =
    ZIO.serviceWithZIO[Traffic](traffic => io.raceFirst(traffic.workers.join *> ZIO.never))

  private def trafficWorker(
      alice: Party,
      live: Ref[Set[String]],
      stop: Promise[Nothing, Unit],
      own: Vector[String]
  ): ZIO[LedgerEnv, Throwable, Unit] =
    ZIO.unlessZIODiscard(stop.isDone)(
      for
        cid <- createContract(alice)
        _   <- live.update(_ + cid)
        _   <- ZIO.foreachDiscard(own.dropRight(2))(archive(alice, live))
        _   <- trafficWorker(alice, live, stop, own.takeRight(2) :+ cid)
      yield ()
    )

  private def createContract(alice: Party): ZIO[LedgerEnv, Throwable, String] =
    val args = Record.defaultInstance.addFields(RecordField("sender", Some(Value(Value.Sum.Party(alice.id)))))
    Ledger.create("PingPong:Ping", args, alice).map(_.getTransaction.events(0).getCreated.contractId)

  private def archive(alice: Party, live: Ref[Set[String]])(cid: String): ZIO[LedgerEnv, Throwable, Long] =
    Ledger.archive("PingPong:Ping", cid, alice).map(_.getTransaction.offset) <* live.update(_ - cid)

  // The last offset is that of one more archive once the workers have stopped
  private val stopLedgerTraffic: ZIO[LedgerEnv & Traffic, Throwable, LedgerState] =
    for
      traffic    <- ZIO.service[Traffic]
      _          <- traffic.stop.succeed(()) *> traffic.workers.join
      cid        <- traffic.live.get.map(_.headOption).someOrFail(Throwable("No live contract to archive"))
      lastOffset <- archive(traffic.alice, traffic.live)(cid)
      unarchived <- traffic.live.get
    yield LedgerState(lastOffset, unarchived)

  // A PQS instance and the application name its sessions and audit rows carry
  private final case class Instance(app: String, svc: Service[Pipeline])

  // The random gap varies the traffic phase a takeover lands in
  private def startPqs(label: String, args: String*): ZIO[PqsEnv, Throwable, Instance] =
    val app = s"pqs-$label"
    val common = Seq(
      s"--target-postgres-properties-ApplicationName=$app",
      "--target-postgres-maxconnections=4",
      "--retry-backoff-cap=PT2S"
    )
    Random.nextIntBounded(3000).flatMap(ms => ZIO.sleep(ms.millis)) *>
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

  // Records each watermark advance with the advancing app; claims that leave ix unchanged are skipped
  private val startRecordingWriters: ZIO[Database, Throwable, Unit] =
    Postgres.call(
      sql"""create table ft_watermark_audit (
              seq bigserial primary key,
              app text not null default current_setting('application_name'),
              ix bigint not null);
            create function ft_watermark_audit_fn() returns trigger as $$$$
            begin
              insert into ft_watermark_audit(ix) values (new.ix);
              return new;
            end $$$$ language plpgsql;
            create trigger ft_watermark_audit_trg after update on __watermark
              for each row when (old.ix is distinct from new.ix) execute function ft_watermark_audit_fn();"""
    )

  private final case class Reference(db: Database)

  // A fresh database filled by a single PQS from Genesis up to `stopAt`
  private def fillReferenceDatabase(stopAt: Long) =
    Postgres.database >+> Pqs.runPipeline(genesis, s"--pipeline-ledger-stop=$stopAt")
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

  // Watermark advances in commit order: the advancing app and the new ix
  private val advances: ZIO[Database, Throwable, Chunk[(String, Long)]] =
    Postgres.query(sql"select app, ix from ft_watermark_audit order by seq".query[(String, Long)].selectAll)

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

  // Every compared row, tagged with its table; surrogate keys left out
  private val snapshot: ZIO[Database, Throwable, Chunk[(String, String)]] =
    Postgres.query(
      sql"""select '__transactions', row(ix, "offset", transaction_id, synchronizer_id, effective_at, workflow_id)::text
              from __transactions
            union all select '__events', row(tx_ix, event_id, type)::text from __events
            union all select '__contracts()', row(template_fqn, contract_id, created_at_ix, archived_at_ix,
                create_event_id, archive_event_id, payload, signatories, observers, witnesses)::text from __contracts()
            union all select '__contract_tpe', template_fqn from __contract_tpe
            union all select '__exercise_tpe', choice_fqn from __exercise_tpe
            union all select '__packages', row(name, version, id)::text from __packages
              where pk in (select package_pk from __contracts)
            union all select '__tmp_deactivated_contracts', row(c.template_fqn, d.contract_id, a.event_id,
                d.archived_at_ix, u.event_id, d.unassigned_at_ix, d.synchronizer_id)::text
              from __tmp_deactivated_contracts d
              join __contract_tpe c on c.pk = d.tpe_pk
              left join __events a on a.pk = d.archive_event_pk
              left join __events u on u.pk = d.unassign_event_pk
            union all select '__exercises', row(t.choice_fqn, c.template_fqn, ev.event_id, x.exercised_at_ix,
                x.contract_id, x.argument, x.result, x.redaction_id, p.id, x.controllers, x.last_descendant_node_id,
                x.witnesses)::text
              from __exercises x
              join __exercise_tpe t on t.pk = x.tpe_pk
              join __contract_tpe c on c.pk = x.contract_tpe_pk
              left join __events ev on ev.pk = x.exercise_event_pk
              left join __packages p on p.pk = x.package_pk
            union all select '__reassignments', row(c.template_fqn, ev.event_id, r.reassigned_at_ix, r.type,
                r.contract_id, r.reassignment_id, r.source_synchronizer_id, r.target_synchronizer_id, r.submitter,
                r.reassignment_counter, r.witnesses, r.assignment_exclusivity)::text
              from __reassignments r
              join __contract_tpe c on c.pk = r.contract_tpe_pk
              join __events ev on ev.pk = r.reassign_event_pk
            union all select '__watermark', row(ix, "offset")::text from __watermark"""
        .query[(String, String)]
        .selectAll
    )

  // Rows missing on either side, not a whole-table diff
  private val rowsOnlyInOneDatabase: ZIO[Database & Reference, Throwable, Chunk[(String, String, String)]] =
    for
      stressed  <- snapshot
      reference <- ZIO.serviceWithZIO[Reference](ref => snapshot.provideEnvironment(ZEnvironment(ref.db)))
    yield stressed.diff(reference).map(("only in the stressed database", _, _)) ++
      reference.diff(stressed).map(("only in the reference", _, _))

  // Expected results

  private val replacedByNewerWriter: Assertion[Exited] =
    hasField("exit code", (_: Exited).code, equalTo(ExitCode.failure)) &&
      hasField("stderr", (_: Exited).stderr, containsString("PQS writer instance has changed"))

  private val stillRunning: Assertion[Option[ExitCode]] = isNone

  private val strictlyIncreasing: Assertion[Iterable[Long]] = isSorted[Long] && isDistinct
