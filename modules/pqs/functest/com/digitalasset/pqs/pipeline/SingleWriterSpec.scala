// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.pqs.SharedMultiSyncLedgerSpec
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.*
import zio.jdbc.*
import zio.test.*
import zio.test.Assertion.*

// TODO: add a long-running, manually triggered variant: lockstep retries of simultaneous starts and takeovers during
// deferred-archive application only surface over dozens of takeovers, beyond a CI budget.
object SingleWriterSpec extends SharedMultiSyncLedgerSpec:
  def spec = suite("single writer")(
    funcTest("concurrent PQS instances keep a single writer and a consistent database") {
      val alice       = Party("Alice")
      val load        = Capture[Load]
      val a           = Capture[Service[Pipeline]]
      val b           = Capture[Service[Pipeline]]
      val c           = Capture[Service[Pipeline]]
      val d           = Capture[Service[Pipeline]]
      val e           = Capture[Service[Pipeline]]
      val loser       = Capture[Service[Pipeline]]
      val release     = Capture[Promise[Nothing, Unit]]
      val lastOffset  = Capture[Long]
      def dLost       = loser.get.container == d.get.container
      def survivor    = if dLost then e.get else d.get
      def survivorApp = if dLost then "pqs-e" else "pqs-d"

      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1)) ++ Postgres.database
      Then:
        startLoad(alice).is(load.capture)
      And:
        startPqs("a", genesis).is(a.capture)
      And:
        streaming(a.get)
      When:
        installAudit
      Then:
        startPqs("b", resume).is(b.capture)
      And:
        deposed(a.get)
      And:
        streaming(b.get)
      Then:
        startPqs("c", resume, "--target-schema-autoapply=false").is(c.capture)
      And:
        deposed(b.get)
      And:
        streaming(c.get)
      Then:
        holdLock(sql"select 1 from __watermark for update".query[Int].selectAll).is(release.capture)
      And:
        startPqs("d", resume).is(d.capture)
      And:
        startPqs("e", resume).is(e.capture)
      And:
        lockWaiters("pqs-d", "pqs-e").is(2L).retryUntilTimeout(50.seconds)
      When:
        release.get.succeed(())
      Then:
        deposed(c.get)
      And:
        firstToExit(d.get, e.get).is(loser.capture)
      And:
        deposed(loser.get)
      Then:
        stopLoad(alice, load.get).is(lastOffset.capture)
      And:
        caughtUp(lastOffset.get)
      And:
        running(survivor)
      When:
        reference(lastOffset.get)
      Expect:
        ZIO.foreach(compared)((table, query) => sameAsReference(table, query)).map(_.reduce(_ && _))
      And:
        activeIds.zip(load.get.live.get).map((ids, live) => assertTrue(ids == live))
      And:
        advances.map { rows =>
          val writers    = collapse(rows.map(_._1))
          val ixs        = rows.map(_._2)
          val increasing = ixs.zip(ixs.drop(1)).forall((x, y) => x < y)
          assertTrue(
            writers.distinct == writers,
            writers.take(3) == Chunk("pqs-a", "pqs-b", "pqs-c"),
            writers.lastOption.contains(survivorApp),
            increasing
          )
        }
    },
    // Fails today: a deposed instance re-claims the writer slot when it restarts after a recoverable error (#124)
    funcTest("a deposed instance does not take the writer slot back") {
      val alice  = Party("Alice")
      val load   = Capture[Load]
      val idleAt = Capture[Long]
      val a      = Capture[Service[Pipeline]]
      val b      = Capture[Service[Pipeline]]

      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1)) ++ Postgres.database
      Then:
        startLoad(alice).is(load.capture)
      And:
        startPqs("a", genesis, "--target-postgres-probeinterval=PT0S").is(a.capture)
      And:
        streaming(a.get)
      When:
        installAudit
      Then:
        stopLoad(alice, load.get).is(idleAt.capture)
      And:
        caughtUp(idleAt.get)
      And:
        startPqs("b", resume).is(b.capture)
      And:
        continuing(b.get).timeout(50.seconds).is(isSome(anything))
      And:
        running(a.get)
      When:
        terminateSessions("pqs-a")
      When:
        startLoad(alice)
      Then:
        b.get.exitCode.timeout(30.seconds).is(isNone)
      And:
        advances.map(rows => assertTrue(collapse(rows.map(_._1)) == Chunk("pqs-a", "pqs-b")))
    } @@ TestAspect.ignore
  )

  private final case class Load(live: Ref[Set[String]], stop: Promise[Nothing, Unit], fiber: Fiber[Throwable, Unit])
  private final case class Reference(db: Database)

  private type LedgerEnv = Docker & Service[Ledger] & DeployedDar
  private type PqsEnv    = LedgerEnv & Parties & Postgres & Database & Scope

  private val genesis = "--pipeline-ledger-start=Genesis"
  private val resume  = "--pipeline-ledger-start=Oldest"
  private val pingFqn = s"${pingPong.name}:PingPong:Ping"

  private def startLoad(alice: Party): ZIO[LedgerEnv & Scope, Nothing, Load] =
    for
      live  <- Ref.make(Set.empty[String])
      stop  <- Promise.make[Nothing, Unit]
      fiber <- ZIO.collectAllParDiscard(List.fill(4)(churn(alice, live, stop, Vector.empty))).forkScoped
    yield Load(live, stop, fiber)

  private def churn(
      alice: Party,
      live: Ref[Set[String]],
      stop: Promise[Nothing, Unit],
      own: Vector[String]
  ): ZIO[LedgerEnv, Throwable, Unit] =
    stop.isDone.flatMap {
      case true => ZIO.unit
      case false =>
        createContract(alice).tap(cid => live.update(_ + cid)).flatMap { cid =>
          val kept = own match
            case oldest +: rest if rest.size >= 2 =>
              Ledger.archive("PingPong:Ping", oldest, alice, sync1) *> live.update(_ - oldest).as(rest)
            case _ => ZIO.succeed(own)
          kept.flatMap(rest => churn(alice, live, stop, rest :+ cid))
        }
    }

  private def stopLoad(alice: Party, load: Load): ZIO[LedgerEnv, Throwable, Long] =
    for
      _        <- load.stop.succeed(()) *> load.fiber.join
      cid      <- load.live.get.map(_.headOption).someOrFail(Throwable("No live contract to archive"))
      archived <- Ledger.archive("PingPong:Ping", cid, alice, sync1)
      _        <- load.live.update(_ - cid)
    yield archived.getTransaction.offset

  private def startPqs(label: String, extraArgs: String*): ZIO[PqsEnv, Throwable, Service[Pipeline]] =
    Random.nextIntBounded(3000).flatMap(ms => ZIO.sleep(ms.millis)) *>
      Pqs
        .attemptPipeline(
          (Seq(
            s"--target-postgres-properties-ApplicationName=pqs-$label",
            "--target-postgres-maxconnections=4",
            "--retry-backoff-cap=PT2S"
          ) ++ extraArgs)*
        )
        .build
        .map(_.get[Service[Pipeline]])

  private def continuing(pqs: Service[Pipeline]): Task[Unit] =
    pqs.blockUntilStdOut(_.contains("Continuing from offset"))

  private def streaming(pqs: Service[Pipeline]): Task[TestResult] =
    (continuing(pqs) *> pqs.blockUntilStdOut(_.contains("Advanced watermark")))
      .timeout(50.seconds)
      .is(isSome(anything))

  private def deposed(pqs: Service[Pipeline]): Task[TestResult] =
    for
      code   <- pqs.exitCode.timeoutFail(RuntimeException(s"${pqs.container.hostName} is still running"))(50.seconds)
      stderr <- Pqs.stderr.provideEnvironment(ZEnvironment(pqs))
    yield assertTrue(code == ExitCode.failure, stderr.contains("PQS writer instance has changed"))

  private def running(pqs: Service[Pipeline]): Task[TestResult] =
    pqs.exitCode.timeout(2.seconds).is(isNone)

  private def firstToExit(x: Service[Pipeline], y: Service[Pipeline]): Task[Service[Pipeline]] =
    x.exitCode
      .as(x)
      .raceFirst(y.exitCode.as(y))
      .timeoutFail(RuntimeException("neither instance of the pair exited"))(55.seconds)

  private def holdLock(
      lock: ZIO[ZConnection, Throwable, Any]
  ): ZIO[Database & Scope, Throwable, Promise[Nothing, Unit]] =
    for
      held    <- Promise.make[Throwable, Unit]
      release <- Promise.make[Nothing, Unit]
      _       <- Postgres.query(lock *> held.succeed(()) *> release.await).catchAll(held.fail(_).unit).forkScoped
      _       <- held.await
    yield release

  private def lockWaiters(apps: String*): ZIO[Database, Throwable, Long] =
    Postgres
      .query(
        sql"""select count(distinct application_name) from pg_stat_activity
              where datname = current_database() and wait_event_type = 'Lock'
                and application_name in (${apps.toList})""".query[Long].selectOne
      )
      .someOrElse(0L)

  private val installAudit: ZIO[Database, Throwable, Unit] =
    Postgres.call(
      sql"""create table ft_watermark_audit (
              seq bigserial primary key,
              app text not null default current_setting('application_name'),
              old_instance text, new_instance text, old_ix bigint, new_ix bigint);
            create function ft_watermark_audit_fn() returns trigger as $$$$
            begin
              insert into ft_watermark_audit(old_instance, new_instance, old_ix, new_ix)
              values (old.instance_id, new.instance_id, old.ix, new.ix);
              return new;
            end $$$$ language plpgsql;
            create trigger ft_watermark_audit_trg after update on __watermark
              for each row execute function ft_watermark_audit_fn();"""
    )

  private val advances: ZIO[Database, Throwable, Chunk[(String, Long)]] =
    Postgres.query(
      sql"select app, new_ix from ft_watermark_audit where old_ix is distinct from new_ix order by seq"
        .query[(String, Long)]
        .selectAll
    )

  private def collapse(apps: Chunk[String]): Chunk[String] =
    apps.foldLeft(Chunk.empty[String])((acc, app) => if acc.lastOption.contains(app) then acc else acc :+ app)

  private def caughtUp(offset: Long): ZIO[Database, Throwable, TestResult] =
    Postgres
      .query(sql"""select "offset" from latest_checkpoint()""".query[Long].selectOne)
      .is(isSome(equalTo(offset)))
      .retryUntilTimeout(50.seconds)

  private def reference(stopAt: Long) =
    Postgres.database >+> Pqs.runPipeline(genesis, s"--pipeline-ledger-stop=$stopAt")
      >>> ZLayer.fromFunction((db: Database) => Reference(db))

  private val compared: List[(String, SqlFragment)] = List(
    "__transactions" ->
      sql"""select row(ix, "offset", transaction_id, synchronizer_id, effective_at, workflow_id)::text
            from __transactions order by 1""",
    "__events" -> sql"select row(tx_ix, event_id, type)::text from __events order by 1",
    "__contracts()" ->
      sql"""select row(template_fqn, contract_id, created_at_ix, archived_at_ix, create_event_id, archive_event_id,
                       payload, signatories, observers, witnesses)::text
            from __contracts() order by 1""",
    "__contract_tpe" -> sql"select template_fqn from __contract_tpe order by 1",
    "__exercise_tpe" -> sql"select choice_fqn from __exercise_tpe order by 1",
    "__packages" ->
      sql"""select row(name, version, id)::text from __packages
            where pk in (select package_pk from __contracts) order by 1""",
    "__tmp_deactivated_contracts" -> sql"select count(*)::text from __tmp_deactivated_contracts",
    "__exercises"                 -> sql"select count(*)::text from __exercises",
    "__reassignments"             -> sql"select count(*)::text from __reassignments",
    "__watermark"                 -> sql"""select row(ix, "offset")::text from __watermark"""
  )

  private def sameAsReference(table: String, query: SqlFragment): ZIO[Database & Reference, Throwable, TestResult] =
    val rows = Postgres.query(query.query[String].selectAll)
    for
      expected <- ZIO.serviceWithZIO[Reference](ref => rows.provideEnvironment(ZEnvironment(ref.db)))
      actual   <- rows
    yield assert(actual.diff(expected))(isEmpty.label(s"$table rows missing from the reference")) &&
      assert(expected.diff(actual))(isEmpty.label(s"$table rows missing from the stressed database"))

  private val activeIds: ZIO[Database, Throwable, Set[String]] =
    Postgres.query(sql"select contract_id from active($pingFqn)".query[String].selectAll).map(_.toSet)

  private def terminateSessions(app: String): ZIO[Database, Throwable, Unit] =
    Postgres
      .query(
        sql"""select pg_terminate_backend(pid) from pg_stat_activity
              where datname = current_database() and application_name = $app""".query[Boolean].selectAll
      )
      .unit
