// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.daml.ledger.api.v2.value.*
import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.*
import scala.language.implicitConversions
import zio.jdbc.*
import zio.test.Assertion.*

/** This test is standalone because PQS 3.4 does not support the Void package uploaded by DamlDecodingSpec to the shared
  * Canton instance.
  */
object FlywayMigrationSpec extends FuncTestStandalone:

  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    owner: Party
                    |  where
                    |    signatory owner
                    |
                    |transact : Party -> Script ()
                    |transact party = void do
                    |  submit party $ createCmd Ping with owner = party
                    |""".stripMargin
  )

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

  private val pingPongWithKey = DamlSource(
    "PingPongWithKey" -> """module PingPongWithKey where
                           |
                           |import Daml.Script
                           |import DA.Functor (void)
                           |import IKeyed
                           |
                           |template PingWithKey
                           |  with
                           |    owner: Party
                           |    k: Int
                           |    label: Text
                           |  where
                           |    signatory owner
                           |    key (owner, k): (Party, Int)
                           |    maintainer key._1
                           |
                           |    interface instance IKeyed for PingWithKey where
                           |      view = VKeyed with label = label
                           |
                           |setup: Party -> Script ()
                           |setup alice = void do
                           |  submit alice $ createCmd PingWithKey with owner = alice, k = 42, label = "A"
                           |  submit alice $ createCmd PingWithKey with owner = alice, k = 43, label = "C"
                           |  submit alice $ createCmd PingWithKey with owner = alice, k = 42, label = "B"
                           |""".stripMargin
  ).dependsOn(iKeyed)

  private val templateFqn  = s"${pingPongWithKey.name}:PingPongWithKey:PingWithKey"
  private val interfaceFqn = s"${iKeyed.name}:IKeyed:IKeyed"

  def spec = suite("FlyMigrationSpec")(
    funcTest("Migrate from 3.4.6 to main") {
      val alice      = Party("Alice")
      val instanceId = Capture[String]
      Given:
        DamlSdk.ledger ++ Postgres.instance
          >+> DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database
          >+> DamlSdk.deploy

      And:
        DamlSdk.runScript("PingPong:transact", alice.id)
      And:
        Pqs34.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Database
          .active(Some(s"${pingPong.name}:PingPong:Ping"))
          .returns(
            table {
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | anything
            }
          )
      Expect:
        // There was a bug in PQS 3.4 where the instance_id was only inserted on the second run.
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(isNull))
      And:
        // Run it a second time to insert the instance_id
        Pqs34.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(not(isNull) && instanceId.capture))
      And:
        DamlSdk.runScript("PingPong:transact", alice.id)
      And:
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest",
          "--retry-counter-attempts=0"
        )
      Expect:
        Database
          .active(Some(s"${pingPong.name}:PingPong:Ping"))
          .returns(
            table {
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | anything
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | anything
            }
          )
      Expect:
        Postgres
          .query(sql"select instance_id from __watermark")
          .returns(table(not(isNull) && not(equalTo(instanceId.get))))
    } @@ DamlSdk.onlyDamlLfVersion("<=2.2"),
    funcTest("Migrate from 3.5.7 to main: V042 clears the interface key hashes") {
      val alice = Party("Alice")
      // Rows come back in creation order: A and B share the key (alice, 42) and so share a hash, C has
      // (alice, 43). Each capture is paired with a not-null check, because a null would otherwise be
      // captured happily and then satisfy the post-migration check too. Capture asserts equality on
      // every later use, which is what pins the template hashes as unchanged.
      val keyHash42 = Capture[String]
      val keyHash43 = Capture[String]
      Given:
        DamlSdk.ledger ++ Postgres.instance
          >+> DamlSdk.dar(pingPongWithKey) ++ DamlSdk.parties(alice) ++ Postgres.database
          >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("PingPongWithKey:setup", alice.id)
      And:
        // 3.5.7 applies V041, which stored the key hash on the interface rows as well as the template ones.
        Pqs35.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--retry-counter-attempts=0"
        )
      Expect:
        Database
          .active(Some(interfaceFqn), extraColumns = Seq("contract_key", "contract_key_hash"))
          .returns(
            table {
              anything | interfaceFqn | "interface" | anything | isNull | not(isNull)
              anything | interfaceFqn | "interface" | anything | isNull | not(isNull)
              anything | interfaceFqn | "interface" | anything | isNull | not(isNull)
            }
          )
      Expect:
        Database
          .active(Some(templateFqn), extraColumns = Seq("contract_key_hash"))
          .returns(
            table {
              anything | templateFqn | "template" | anything | (not(isNull) && keyHash42.capture)
              anything | templateFqn | "template" | anything | (not(isNull) && keyHash43.capture)
              anything | templateFqn | "template" | anything | (not(isNull) && keyHash42.capture)
            }
          )
      And:
        // Starting main applies V042.
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest",
          "--retry-counter-attempts=0"
        )
      Expect:
        Database
          .active(Some(interfaceFqn), extraColumns = Seq("contract_key", "contract_key_hash"))
          .returns(
            table {
              anything | interfaceFqn | "interface" | anything | isNull | isNull
              anything | interfaceFqn | "interface" | anything | isNull | isNull
              anything | interfaceFqn | "interface" | anything | isNull | isNull
            }
          )
      Expect:
        Database
          .active(Some(templateFqn), extraColumns = Seq("contract_key_hash"))
          .returns(
            table {
              anything | templateFqn | "template" | anything | keyHash42.capture
              anything | templateFqn | "template" | anything | keyHash43.capture
              anything | templateFqn | "template" | anything | keyHash42.capture
            }
          )
    } @@ DamlSdk.onlyDamlLfVersion("=2.3"),
    funcTest("Migrate from 3.6 to main: add support for reassignment") {
      val alice      = Party("Alice")
      val instanceId = Capture[String]
      val sync1      = Synchronizer("sync1")
      val sync2      = Synchronizer("sync2")

      def createContract(sync: Synchronizer) =
        val args = Record.defaultInstance.addFields(RecordField("owner", Some(Value(Value.Sum.Party(alice.id)))))
        Ledger
          .create("PingPong:Ping", args, alice, sync1)
          .map(_.getTransaction.events(0).getCreated.contractId)

      Given:
        DamlSdk.multiSyncLedger(sync1, sync2) ++ DamlSdk.dar(pingPong) ++ Postgres.instance
          >+> DamlSdk
            .allocateParties(alice -> Seq(sync1, sync2)) ++ DamlSdk.uploadAndVetDar(sync1, sync2) ++ Postgres.database

      val contractId1 = Capture[String]
      val contractId2 = Capture[String]
      Then:
        createContract(sync1).is(contractId1.capture)
      And:
        Ledger.reassign(contractId1.get, alice, sync1, sync2)
          *> createContract(sync2).is(contractId2.capture) // create another contract to force watermark advancement
      And:
        Pqs36.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )

      Expect:
        // PQS 36 does not ingest reassignment events
        Database
          .active(Some(s"${pingPong.name}:PingPong:Ping"))
          .returns(
            table {
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | contractId1
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | contractId2
            }
          )

      When:
        Ledger.reassign(contractId1.get, alice, sync2, sync1)
      And:
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest",
          "--retry-counter-attempts=0"
        )
      Expect:
        // The second reassignment deactivates the initial row and creates a new one
        Database
          .__contracts(extraColumns = Seq("unassigned_at_ix", "reassignment_counter", "synchronizer_id"))
          .returns(
            table {
              anything | anything | contractId1 | anything | not(equalTo(0)) | 0 | null
              anything | anything | contractId2 | anything | 0               | 0 | null
              anything | anything | contractId1 | anything | 0               | 2 | sync1.id
            }
          )
      Expect:
        Database
          .active(
            Some(s"${pingPong.name}:PingPong:Ping"),
            extraColumns = Seq("reassignment_counter", "synchronizer_id")
          )
          .returns(
            table {
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | contractId2 | 0 | null
              anything | s"${pingPong.name}:PingPong:Ping" | "template" | contractId1 | 2 | sync1.id
            }
          )

      val reassignmentId = Capture[String]
      Expect:
        Database
          .__reassignments()
          .returns(
            table {
              s"${pingPong.name}:PingPong:Ping" | "unassign" | contractId1 | reassignmentId.capture | sync2.id | sync1.id | alice.id | 2
              s"${pingPong.name}:PingPong:Ping" | "assign" | contractId1 | reassignmentId.capture | sync2.id | sync1.id | alice.id | 2
            }
          )
    }
  )
