// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.services.daml.{DamlSdk, Ledger, Party}
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

      When:
        // Run in its own scope so that the Ledger is stopped before we generate the dumps
        // TODO Closing the scope is slow because Canton participant node fails to close properly:
        // "Task closing node-participant1 did not complete within 10 seconds"
        ZIO.scoped {
          val layer = Ledger.instanceWithPgStorage
            >+> DamlSdk.deploy(Dars.pingPongTransact)
            >+> DamlSdk.parties(alice)
            >+> DamlSdk.runScript("PingPong:transact", alice.id)
          layer.build
        }

      When:
        ZIO.foreachDiscard(CantonDumps.all)(Postgres.dumpDatabase(_))
  ) @@ TestAspect.ifEnv("REGENERATE_CANTON_DUMP")(_.toBooleanOption.getOrElse(false))

  // Standing up Canton + running the ingestion script + the graceful shutdown on scope close
  override protected def instructionTimeout = 2.minutes
