// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.schema.postgres.document

import com.digitalasset.scribe.{SharedLedgerAndPostgresTest, Utils}
import com.digitalasset.scribe.services.daml.{DamlSdk, Party}
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.ZIO
import zio.test.*

import scala.language.implicitConversions

/** The purpose of this test is to generate and keep up-to-date a SQL dump mirroring how would a fresh PQS database look
  * like after all Flyway migrations are applied to it. The SQL dump can then be inspected for eased development or
  * debugging purposes.
  *
  * To ensure the SQL dump is kept up to date, this test compares the current SQL dump against the checked-in snapshot
  * at `postgres/document/resources/db/schema-dump.sql`. The test fails if the snapshot has drifted out of sync with the
  * migrations.
  *
  * Whenever changes are effected to the SQL migrations in `postgres/document/resources/db/migration` , the reference
  * SQL dump must be regenerated, by setting the `REGENERATE_SCHEMA_DUMP` env var when running this spec:
  * {{{
  * REGENERATE_SCHEMA_DUMP=true mill scribe.functest.testOnly com.digitalasset.scribe.schema.postgres.document.SchemaDumpSpec
  * }}}
  */
object SchemaDumpSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")

  // Mill forks test JVMs with a sandboxed working directory, so `os.pwd` does *not* point at the repo checkout.
  // Mill sets `MILL_WORKSPACE_ROOT` on forked processes precisely to work around this; fall back to `os.pwd` when
  // running outside of Mill (e.g. directly from an IDE).
  private val workspaceRoot = sys.env.get("MILL_WORKSPACE_ROOT").fold(os.pwd)(os.Path(_))
  private val target        = workspaceRoot / "postgres" / "document" / "resources" / "db" / "schema-dump.sql"
  private val regenerate    = sys.env.contains("REGENERATE_SCHEMA_DUMP")

  private val staleMessage =
    s"The reference SQL dump ($target) is stale: regenerate it with `REGENERATE_SCHEMA_DUMP=true mill scribe.functest.testOnly " +
      s"${getClass.getName}` and commit it"

  def spec = suite("SchemaDumpSpec")(
    funcTest(s"$target matches the schema produced by the current Flyway migrations"):
      Given:
        DamlSdk.dar(Utils.pingPongTransact) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy
      When:
        // Running the pipeline once triggers Scribe's normal schema auto-apply (Flyway migrate + mappings)
        // against a brand-new, empty database.
        Scribe.runPipeline("--pipeline-ledger-stop=Latest")
      Then:
        if regenerate then Postgres.dumpSchemaTo(target).as(assertCompletes)
        else
          for
            current   <- Postgres.dumpSchema
            exists    <- ZIO.attemptBlocking(os.exists(target))
            checkedIn <- ZIO.attemptBlocking(if exists then os.read(target) else "")
          yield assertTrue(exists) && (assertTrue(current == checkedIn) ?? staleMessage)
  )
