// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.nuck

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.daml.DamlSdk.onlyDamlLfVersion
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.{Pipeline, Scribe}
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.*
import scala.language.implicitConversions

object NuckSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")

  private val withKey = DamlSource(
    "WithKey" -> """module WithKey where
                   |
                   |import Daml.Script
                   |import DA.Functor (void)
                   |
                   |template WithKey
                   |  with
                   |    owner: Party
                   |    k: Int
                   |    label: Text
                   |  where
                   |    signatory owner
                   |    key (owner, k): (Party, Int)
                   |    maintainer key._1
                   |
                   |setup: Party -> Script ()
                   |setup alice = void do
                   |  submit alice $ createCmd WithKey with owner = alice, k = 42, label = "A"
                   |  submit alice $ createCmd WithKey with owner = alice, k = 43, label = "C"
                   |  submit alice $ createCmd WithKey with owner = alice, k = 42, label = "B"
                   |""".stripMargin
  )

  def spec = suite("Non-Unique Contract Keys")(
    funcTest("Two contracts with the same key are both stored and queryable") {
      val contractKeyHash = Capture[String]
      Given:
        (DamlSdk.dar(withKey) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        DamlSdk.runScript("WithKey:setup", alice.id)
      And:
        Scribe.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )

      // Looking up contracts by key "(alice, 42)" returns A and B, but not C.
      Expect:
        alice.id.flatMap { aliceId =>
          lookupByKey(aliceId, 42).returns(
            table {
              anything | contractKeyHash.capture | "A"
              anything | contractKeyHash.capture | "B"
            }
          )
        }
      Expect:
        lookupByKeyHash(contractKeyHash.get).returns(
          table {
            anything | "A"
            anything | "B"
          }
        )
    }
  ) @@ onlyDamlLfVersion(">=2.3")

  private def lookupByKey(partyId: String, k: Int) = Postgres.query(
    sql"""select contract_id, contract_key_hash, payload ->> 'label'
          from __contracts
          where contract_key = jsonb_build_object('_1', $partyId, '_2', ${k.toString})
          order by created_at_ix"""
  )

  private def lookupByKeyHash(contractKeyHash: String) = Postgres.query(
    sql"""select contract_id, payload ->> 'label'
          from __contracts
          where contract_key_hash = $contractKeyHash::bytea
          order by created_at_ix"""
  )
