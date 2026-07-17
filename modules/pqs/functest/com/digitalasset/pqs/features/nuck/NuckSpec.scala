// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.nuck

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyDamlLfVersion
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
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
        Pqs.runPipeline(
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
      // The same hash is stored on the interface row of each contract, so this lookup - which does
      // not pin the entity - returns both rows of A and both rows of B.
      Expect:
        lookupByKeyHash(contractKeyHash.get).returns(
          table {
            anything | "A" // template row
            anything | "A" // interface row
            anything | "B"
            anything | "B"
          }
        )
    },
    funcTest("Interface rows store the key hash the Ledger API sent, but not the contract key") {
      val keyHash42 = Capture[String]
      val keyHash43 = Capture[String]
      Given:
        context
      And:
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )

      // What the Ledger API delivers for the interface, against what PQS kept on the interface row.
      Expect:
        for
          aliceId <- alice.id
          stored <- Postgres
            .query(
              sql"""select contract_id, contract_key_hash, contract_key is null
                    from active($interfaceFqn)
                    where payload ->> 'label' = 'A'""".query[(String, Array[Byte], Boolean)].selectOne
            )
            .someOrFail(Throwable("interface row of contract A not found"))
          (contractId, storedKeyHash, storedKeyIsNull) = stored
          fromApi <- DamlSdk.api.getCreatedEventViaInterface(Seq(aliceId), interfaceFqn, contractId)
        yield zio.test.assertTrue(
          fromApi.interfaceViews.map(_.getInterfaceId.entityName) == Seq("IKeyed"),
          /** From SDK contractKey: _root_.scala.Option[com.daml.ledger.api.v2.value.Value], contractKeyHash:
            * _root_.com.google.protobuf.ByteString,
            *
            * Why is the contractKeyHas required when contractKey is optional?
            */
          fromApi.contractKey.isDefined,
          !fromApi.contractKeyHash.isEmpty,
          // Of the two, PQS keeps the hash on the interface row - byte for byte - and drops the key.
          storedKeyIsNull,
          storedKeyHash.toSeq == fromApi.contractKeyHash.toByteArray.toSeq
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
            "key owner" | "key k" | "key hash"        | "label"
            ---         | ---     | ---               | ---
            isNull      | isNull  | keyHash42.capture | "A"
            isNull      | isNull  | keyHash43.capture | "C"
            isNull      | isNull  | keyHash42.capture | "B"
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
