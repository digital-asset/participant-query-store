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

  private val iKeyed = DamlSource(
    "IKeyed" -> """module IKeyed where
                  |
                  |interface IKeyed
                  |  where
                  |    viewtype VKeyed
                  |
                  |data VKeyed = VKeyed with label: Text
                  |  deriving (Eq, Show)
                  |""".stripMargin
  )

  private val withKey = DamlSource(
    "WithKey" -> """module WithKey where
                   |
                   |import Daml.Script
                   |import DA.Functor (void)
                   |import IKeyed
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
                   |    interface instance IKeyed for WithKey where
                   |      view = VKeyed with label = label
                   |
                   |setup: Party -> Script ()
                   |setup alice = void do
                   |  submit alice $ createCmd WithKey with owner = alice, k = 42, label = "A"
                   |  submit alice $ createCmd WithKey with owner = alice, k = 43, label = "C"
                   |  submit alice $ createCmd WithKey with owner = alice, k = 42, label = "B"
                   |""".stripMargin
  ).dependsOn(iKeyed)

  private val context =
    DamlSdk.dar(withKey) ++ DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.deploy >+> DamlSdk.runScript("WithKey:setup", alice.id)

  private val templateFqn  = s"${withKey.name}:WithKey:WithKey"
  private val interfaceFqn = s"${iKeyed.name}:IKeyed:IKeyed"

  def spec = suite("Non-Unique Contract Keys")(
    funcTest("Two contracts with the same key are both stored and queryable") {
      val contractKeyHash = Capture[String]
      Given:
        context
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
      // Only template rows carry the key hash, so this lookup - which does not pin the entity -
      // returns one row per contract.
      Expect:
        lookupByKeyHash(contractKeyHash.get).returns(
          table {
            anything | "A"
            anything | "B"
          }
        )
    },
    funcTest("Interface rows store neither the contract key nor its hash") {
      val keyHash42 = Capture[String]
      val keyHash43 = Capture[String]
      Given:
        context
      And:
        Scribe.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )

      // The Ledger API does deliver both key and hash on an interface-only subscription; PQS drops
      // both, because they describe the underlying template, not the interface view.
      Expect:
        for
          aliceId <- alice.id
          stored <- Postgres
            .query(
              sql"""select contract_id, contract_key is null, contract_key_hash is null
                    from active($interfaceFqn)
                    where payload ->> 'label' = 'A'""".query[(String, Boolean, Boolean)].selectOne
            )
            .someOrFail(Throwable("interface row of contract A not found"))
          (contractId, storedKeyIsNull, storedKeyHashIsNull) = stored
          fromApi <- DamlSdk.api.getCreatedEventViaInterface(Seq(aliceId), interfaceFqn, contractId)
        yield zio.test.assertTrue(
          fromApi.interfaceViews.map(_.getInterfaceId.entityName) == Seq("IKeyed"),
          fromApi.contractKey.isDefined,
          !fromApi.contractKeyHash.isEmpty,
          storedKeyIsNull,
          storedKeyHashIsNull
        )

      Expect:
        alice.id.flatMap { aliceId =>
          keyColumnsOf(templateFqn).returns(
            table {
              "key owner" | "key k" | "key hash"        | "label"
              ---         | ---     | ---               | ---
              aliceId     | "42"    | keyHash42.capture | "A"
              aliceId     | "43"    | keyHash43.capture | "C"
              aliceId     | "42"    | keyHash42.capture | "B"
            }
          )
        }

      Expect:
        keyColumnsOf(interfaceFqn).returns(
          table {
            "key owner" | "key k" | "key hash" | "label"
            ---         | ---     | ---        | ---
            isNull      | isNull  | isNull     | "A"
            isNull      | isNull  | isNull     | "C"
            isNull      | isNull  | isNull     | "B"
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

  private def keyColumnsOf(qname: String) = Postgres.query(
    sql"""select contract_key ->> '_1', contract_key ->> '_2', contract_key_hash, payload ->> 'label'
          from active($qname)
          order by created_at_ix"""
  )
