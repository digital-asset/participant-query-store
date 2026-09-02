// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.sqlInterpolator
import zio.{durationInt, ExitCode, Promise, ZIO, ZLayer}

import scala.language.implicitConversions

object PipelineUpdateSpec extends SharedLedgerAndPostgresTest:
  val pingPong = DamlSource(
    "PingPong" ->
      """module PingPong where
        |
        |import Daml.Script
        |import DA.Functor (void)
        |
        |template Ping
        |  with
        |    sender: Party
        |    receiver: Party
        |    text: Text
        |  where
        |    signatory sender
        |    observer receiver
        |
        |transact1 : (Party, Party, Text) -> Script ()
        |transact1 (alice, bob, text) = void do
        |  submitMulti [alice, bob] [] $ createCmd Ping with sender = alice, receiver = bob, text = text
        |
        |-- test Archive edge case, create another contract with implicit Archive in the same module
        |template ArchiveTest
        |  with nobody: Party
        |  where signatory nobody
        |
        |transact2 : (Party, Party, Text) -> Script ()
        |transact2 (alice, bob, text) = void do
        |  cid <- submitMulti [alice, bob] [] $ createCmd Ping with sender = alice, receiver = bob, text = text
        |  submit alice $ archiveCmd cid
        |""".stripMargin
  )

  private val packageName = pingPong.name
  private val templateRef = s"$packageName:PingPong:Ping"
  private val archiveRef  = s"$templateRef:Archive"

  def spec = suite("pipeline")(
    funcTest("multiple instances of pipeline/pqs are correctly handled") {
      val alice = Party("Alice")
      val bob   = Party("Bob")
      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      And:
        Postgres.query(sql"""select count(*) from active($templateRef)""").returns(table(1))
      And:
        Postgres.query {
          sql"""update __watermark set instance_id='another-instance' """.update.returns(1)
        }
      And:
        // We want to deliberately spoil the instance_id in the watermark to trigger an error in the pipeline
        // Seems that such trigger is the most stable way (I tried running a thread with concurrent updates,
        // but that was a flaky solution)
        Postgres
          .query {
            sql"""
              create function override_instance_id()
              returns trigger
              language plpgsql
              as $$$$
              begin
                  new.instance_id := 'another-instance';
                  return new;
              end;
              $$$$;

              create trigger trg_override_instance_id
              before update on  __watermark
              for each row
              execute function override_instance_id();
             """.update
          }
          .returns(0)
      And:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      And:
        Pqs.attemptPipeline(
          "--pipeline-ledger-start=Latest",
          "--pipeline-ledger-stop=Latest"
        )
      And:
        Pqs.exitCode.is(ExitCode.failure)
      And:
        Pqs.stderr.is(stringContaining("PQS writer instance has changed") && stringContaining("another-instance"))
      And:
        Pqs.stdout.is(!stringContaining("inserting watermark"))
      Expect:
        Postgres.query(sql"""select count(*) from active($templateRef)""").returns(table(1))
    },
    funcTest("pipeline cannot start while __watermark is updated") {
      val alice = Party("Alice")
      val bob   = Party("Bob")

      @volatile var startTransaction: Promise[Nothing, Unit]   = null
      @volatile var releaseTransaction: Promise[Nothing, Unit] = null

      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      When:
        // Run pipeline once to initialize the schema
        Pqs.runPipeline("--pipeline-ledger-stop=Latest").unit
      Expect:
        Postgres.query(sql"select count(*) from __contracts").returns(table(0))
      When:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      When:
        ZLayer.fromZIO {
          for {
            _ <- Promise.make[Nothing, Unit].map(startTransaction = _)
            _ <- Promise.make[Nothing, Unit].map(releaseTransaction = _)
            // Start a SQL transaction that updates __watermark
            _ <- Postgres.query {
              for {
                _ <- sql"begin".execute
                _ <- sql"update __watermark set instance_id='another-instance'".execute
                _ <- startTransaction.succeed(())
                _ <- releaseTransaction.await
                // the transaction is not committed automatically
                _ <- sql"commit".execute
              } yield ()
            }.forkScoped
          } yield ()
        }
      When:
        // Wait for the SQL transaction to start
        ZIO.attempt(startTransaction.await)
      And:
        Pqs.attemptPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.stdoutContainsWithin("Executing SQL callback: beforeMigrate", duration = 1.minute)
      And:
        // Assert that the pipeline is stuck and has no exit code within 5 seconds
        Pqs.exitCode.timeout(5.seconds).is(None)
      When:
        // Release the SQL transaction so the pipeline can proceed
        releaseTransaction.succeed(())
      Expect:
        Pqs.exitCode.is(ExitCode.success).retryUntilTimeout
      Expect:
        Postgres.query(sql"""select count(*) from active($templateRef)""").returns(table(1))
    },
    funcTest("pipeline can start while __watermark is being read") {
      val alice = Party("Alice")
      val bob   = Party("Bob")

      @volatile var startTransaction: Promise[Nothing, Unit]   = null
      @volatile var releaseTransaction: Promise[Nothing, Unit] = null

      Given:
        DamlSdk.dar(pingPong)
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice, bob) ++ Postgres.database
      When:
        // Run pipeline once to initialize the schema
        Pqs.runPipeline("--pipeline-ledger-stop=Latest").unit
      Expect:
        Postgres.query(sql"select count(*) from __contracts").returns(table(0))
      When:
        DamlSdk.runScript("PingPong:transact1", alice.id <&> bob.id <&> ZIO.succeed("test"))
      When:
        ZLayer.fromZIO {
          for {
            _ <- Promise.make[Nothing, Unit].map(startTransaction = _)
            _ <- Promise.make[Nothing, Unit].map(releaseTransaction = _)
            // Start a SQL transaction that reads __watermark
            _ <- Postgres.query {
              for {
                _ <- sql"begin".execute
                _ <- sql"select ix from __watermark".query[Long].selectOne
                _ <- startTransaction.succeed(())
                _ <- releaseTransaction.await
                // the transaction is not committed automatically
                _ <- sql"commit".execute
              } yield ()
            }.forkScoped
          } yield ()
        }
      When:
        // Wait for the SQL transaction to start
        ZIO.attempt(startTransaction.await)
      And:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      Expect:
        Pqs.exitCode.is(ExitCode.success)
      Expect:
        Postgres.query(sql"""select count(*) from active($templateRef)""").returns(table(1))
    }
  )
