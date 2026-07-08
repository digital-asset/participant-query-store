// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.{Database, Postgres}
import com.digitalasset.scribe.services.scribe.Scribe
import zio.test.Assertion.*
import scala.language.implicitConversions

object DamlDecodingSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")

  private val void = DamlSource(
    "Void" -> """|module Void where
                 |
                 |import Daml.Script
                 |
                 |-- non-serializable type
                 |data Void
                 |
                 |template T1
                 |  with
                 |    owner: Party
                 |  where
                 |    signatory owner
                 |
                 |template T2
                 |  with
                 |    owner: Party
                 |    cid: ContractId Void
                 |  where
                 |    signatory owner
                 |
                 |setup: Party -> Script ()
                 |setup alice = do
                 |  c1 <- submit alice $ createCmd T1 with owner = alice
                 |  submit alice $ createCmd T2 with owner = alice, cid = coerceContractId c1
                 |  return ()
                 |""".stripMargin
  )
  private val packageName = void.name

  def spec = suite("DamlDecodingSpec")(
    funcTest("Decode payload of ContractId Void") {
      val cid = Capture[String]
      Given:
        DamlSdk.dar(void) >+> DamlSdk.deploy
          ++ DamlSdk.parties(alice)
          ++ Postgres.database
      And:
        DamlSdk.runScript("Void:setup", alice.id)
      And:
        Scribe.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Database
          .active(Some(s"$packageName:Void:T1"))
          .returns(
            table {
              anything | s"$packageName:Void:T1" | "template" | cid.capture
            }
          )
      Expect:
        Database
          .active(Some(s"$packageName:Void:T2"), Seq("payload->>'cid'"))
          .returns(
            table {
              anything | s"$packageName:Void:T2" | "template" | anything | cid.get
            }
          )
    } @@ DamlSdk.onlyCantonVersion(">=3.5.2")
  )
