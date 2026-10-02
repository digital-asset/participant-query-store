// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.jdbc.*

import scala.language.{implicitConversions, postfixOps}

/** Test highlighting a bug where gaps in transaction history cna appear due to COMMIT failure . */
object CommitFailureSpec extends FuncTestStandalone:
  private val foo = DamlSource(
    "Foo" -> """module Foo where
               |
               |import Daml.Script
               |import DA.Functor (void)
               |
               |template Foo
               |  with
               |    owner: Party
               |    label: Text
               |  where
               |    signatory owner
               |
               |createFoo: (Party, Text) -> Script ()
               |createFoo (alice, label) = void $ submit alice $ createCmd Foo with owner = alice, label = label
               |""".stripMargin
  )

  def spec = suite("Commit failure")(
    funcTest("a create whose commit is lost together with its connection is permanently missing from PQS"):
      val alice = Party("Alice")
      Given:
        DamlSdk.dar(foo) ++ DamlSdk.ledger ++ Postgres.instance
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        Pqs.pipeline("--pipeline-ledger-start=Oldest", "--pipeline-ledger-stop=Never")
      And:
        DamlSdk.runScript("Foo:createFoo", (alice.id, "before"))
      Then:
        activeLabels.is(Some("before")).retryUntilTimeout
      When:
        failCommitsWritingTransactions
      And:
        DamlSdk.runScript("Foo:createFoo", (alice.id, "failure"))
      And:
        Pqs.stdoutContainsWithin("Failed to commit transaction org.postgresql.util.PSQLException: ERROR: boom")
      When:
        restoreCommitsWritingTransactions
      When:
        DamlSdk.runScript("Foo:createFoo", (alice.id, "after"))
      Then:
        // PQS retries ingesting "failure" after the trigger is dropped
        activeLabels.is(Some("after,before,failure")).retryUntilTimeout
  )

  private val failCommitsWritingTransactions =
    // Fail every transaction at commit time, using an PQS-recoverable error code
    Postgres.call(
      sql"""
        create function fail_commit() returns trigger language plpgsql as $$$$
        begin
          raise exception 'boom' using errcode = '40001';
        end $$$$"""
    ) *> Postgres.call(
      sql"""
        create constraint trigger fail_commit_trg after insert on __transactions
        deferrable initially deferred for each row execute function fail_commit()
      """
    )

  private val restoreCommitsWritingTransactions =
    Postgres.call(sql"drop trigger fail_commit_trg on __transactions")
      *> Postgres.call(sql"drop function fail_commit()")

  private val activeLabels = Postgres.get(
    sql"select string_agg(payload->>'label', ',' order by payload->>'label') from active('Foo:Foo')"
  )
