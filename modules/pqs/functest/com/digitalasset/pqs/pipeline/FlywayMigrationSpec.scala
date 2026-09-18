// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.{Database, Postgres}
import com.digitalasset.pqs.services.pqs.{Pqs34, Pqs35, Pqs}
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
          "--pipeline-ledger-stop=Latest"
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
    funcTest("V042 clears the interface key hashes written by 3.5.7") {
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
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Database
          .active(Some(interfaceFqn), Seq("contract_key", "contract_key_hash"))
          .returns(
            table {
              anything | interfaceFqn | "interface" | anything | isNull | not(isNull)
              anything | interfaceFqn | "interface" | anything | isNull | not(isNull)
              anything | interfaceFqn | "interface" | anything | isNull | not(isNull)
            }
          )
      Expect:
        Database
          .active(Some(templateFqn), Seq("contract_key_hash"))
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
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        Database
          .active(Some(interfaceFqn), Seq("contract_key", "contract_key_hash"))
          .returns(
            table {
              anything | interfaceFqn | "interface" | anything | isNull | isNull
              anything | interfaceFqn | "interface" | anything | isNull | isNull
              anything | interfaceFqn | "interface" | anything | isNull | isNull
            }
          )
      Expect:
        Database
          .active(Some(templateFqn), Seq("contract_key_hash"))
          .returns(
            table {
              anything | templateFqn | "template" | anything | keyHash42.capture
              anything | templateFqn | "template" | anything | keyHash43.capture
              anything | templateFqn | "template" | anything | keyHash42.capture
            }
          )
    } @@ DamlSdk.onlyDamlLfVersion("=2.3"),
    funcTest("V045 backfills __reassignments partitions for contract types registered by 3.5.7") {
      val alice = Party("Alice")
      Given:
        DamlSdk.ledger ++ Postgres.instance
          >+> DamlSdk.dar(pingPongWithKey) ++ DamlSdk.parties(alice) ++ Postgres.database
          >+> DamlSdk.deploy
      And:
        DamlSdk.runScript("PingPongWithKey:setup", alice.id)
      And:
        // 3.5.7 registers both the template and the interface in __contract_tpe. It knows nothing
        // about __reassignments, which is only introduced by V045 on main.
        Pqs35.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        // Pins the "before" half of the upgrade: __reassignments doesn't exist yet on the 3.5.7
        // schema, so the assertion after the next run is verifying something the upgrade creates.
        Postgres.query(sql"select to_regclass('__reassignments') is null").returns(table(true))
      And:
        // Starting main applies V045, which creates __reassignments. __initialize_contract_tpe only
        // creates a partition for a contract type the first time it registers it, so the template and
        // interface registered above — already present in __contract_tpe before V045 ran — can only get
        // a partition from V045's own backfill loop.
        Pqs.runPipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest"
        )
      Expect:
        // Every __contract_tpe row was registered by the 3.5.7 run, so it predates V045 — its
        // __reassignments partition can only come from V045's backfill loop, because
        // __initialize_contract_tpe never runs again for a type it has already registered.
        // Partition creation is deliberately unguarded by contract-type kind, so the interface
        // (IKeyed) is expected to have a partition too, even though interface contracts never
        // produce __reassignments rows and its partition therefore stays empty forever — do not
        // "fix" this assertion by excluding interfaces.
        Postgres
          .query(sql"""select tpe.template_fqn
                       from __contract_tpe tpe
                       where to_regclass('__reassignments_' || tpe.pk) is null""")
          .returns(Table.empty)
    } @@ DamlSdk.onlyDamlLfVersion("=2.3")
  )
