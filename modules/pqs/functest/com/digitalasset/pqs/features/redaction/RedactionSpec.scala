// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.redaction

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.pipeline.pipeline.Config.TransactionApi
import com.digitalasset.pqs.pipeline.pipeline.Config.TransactionApi.{TransactionStream, TransactionTreeStream}
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.daml.DamlSdk.onlyDamlLfVersion
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.ZLayer
import zio.jdbc.{SqlFragment, sqlInterpolator}
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions
import com.digitalasset.pqs.services.postgres.Database

object RedactionSpec extends SharedLedgerAndPostgresTest {
  private val nonExistenceEventId = s"(${Long.MaxValue},${Int.MaxValue})"
  val redactionId                 = "some_reason"
  lazy val interfaces = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface IPingable
                      |  where
                      |    viewtype VPingable
                      |
                      |data VPingable = VPingable with sender: Party deriving (Eq,  Show)
                      |""".stripMargin
  )
  lazy val pingDaml = DamlSource(
    "Pings" -> """module Pings where
                 |
                 |import Daml.Script
                 |import Interfaces
                 |
                 |template Ping
                 |  with
                 |    owner : Party
                 |    label : Text
                 |  where
                 |    signatory owner
                 |
                 |    interface instance IPingable for Ping where
                 |      view = VPingable with sender = owner
                 |
                 |    choice ChangeLabel : ContractId Ping
                 |      with
                 |        newLabel : Text
                 |      controller owner
                 |      do
                 |        create Ping with label = newLabel, ..
                 |
                 |setup: Party -> Script ()
                 |setup alice = do
                 |  one <- submit alice $ createCmd (Ping with owner = alice, label = "one")
                 |  two <- submit alice $ createCmd (Ping with owner = alice, label = "two")
                 |  submit alice $ exerciseCmd one ChangeLabel with newLabel = "one updated"
                 |  submit alice $ exerciseCmd two ChangeLabel with newLabel = "two updated"
                 |  pure ()
                 |""".stripMargin
  ).dependsOn(interfaces)
  private val packageName = pingDaml.name
  private val templateRef = s"$packageName:Pings:Ping"
  private val choiceRef   = s"$templateRef:ChangeLabel"

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
                   |  cid <- submit alice $ createCmd WithKey with owner = alice, k = 42, label = "A"
                   |  submit alice $ archiveCmd cid
                   |""".stripMargin
  )

  def context(dataSource: TransactionApi) =
    val alice = Party("Alice")
    DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.deploy(pingDaml)
      >+> DamlSdk.runScript("Pings:setup", alice.id)
      >+> Pqs.runPipeline(
        s"--pipeline-datasource=$dataSource",
        "--pipeline-ledger-start=Genesis",
        "--pipeline-ledger-stop=Latest"
      )

  def spec =
    suite("redaction")(
      funcTest("can redact archived contract including interfaces") {
        // underscore duplicates due to contract vs. interface. Expected behavior is same for both.
        val oneArchived = Capture[String]
        Given:
          context(TransactionStream)
        And:
          Database
            .query(sql"select contract_id from archives($templateRef) limit 1")
            .returns(Table(oneArchived.capture))
        When:
          // 2 => affected contracts/interface views
          Database.query(sql"select redact_contract(${oneArchived.get}, $redactionId)").returns(Table(2))
        And:
          Database
            .query(
              sql"select payload, redaction_id from lookup_contract(${oneArchived.get})"
            )
            .returns(
              Table {
                isNull | redactionId
                isNull | redactionId
              }
            )
      },
      funcTest("raises exception on redaction of active, non-existent, and already redacted contracts") {
        val active   = Capture[String]
        val redacted = Capture[String]
        Given:
          context(TransactionTreeStream)
        And:
          Database.query(sql"select contract_id from active($templateRef) limit 1").returns(Table(active.capture))
        And:
          Database.query(sql"select contract_id from archives($templateRef) limit 1").returns(Table(redacted.capture))
        And:
          // redact valid contract to make sure it cannot be redacted again
          // 2 affected (template + interface)
          Database.query(sql"select redact_contract(${redacted.get}, ${"reason"})").returns(Table(2))
        Expect:
          // active contracts cannot be redacted
          Database
            .query(sql"select redact_contract(${active.get}, ${"reason"})")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot redact contract ${active.get} because it is active"
                    )
                  )
                )
              )
            )
        And:
          // redacted contracts cannot be redacted again
          Database
            .query(sql"select redact_contract(${redacted.get}, ${"reason"})")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot redact contract ${redacted.get} because it is already redacted"
                    )
                  )
                )
              )
            )
        And:
          val nonexistent = active.get.reverse
          Database
            .query(sql"select redact_contract($nonexistent, ${"reason"})")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot find contract $nonexistent"
                    )
                  )
                )
              )
            )
      },
      funcTest("can redact exercise") {
        val targetEventId  = Capture[String]
        val controlEventId = Capture[String] // ensure other events are not affected
        Given:
          context(TransactionTreeStream)
        And:
          Database
            .query(sql"select exercise_event_id from exercises($choiceRef) limit 2")
            .returns(Table(targetEventId.capture | controlEventId.capture).transpose)
        When:
          // 2 => affected contracts/interface views
          Database
            .query(sql"select redact_exercise(${targetEventId.get}::event_id, $redactionId)")
            .returns(Table.empty)
        And:
          Database
            .query(
              sql"select argument, result, redaction_id from exercises($choiceRef) where exercise_event_id = ${targetEventId.get}::event_id"
            )
            .returns(Table(isNull | isNull | redactionId))
        And:
          Database
            .query(
              sql"select redaction_id from  exercises($choiceRef) where exercise_event_id = ${controlEventId.get}::event_id"
            )
            .returns(Table(isNull))
      },
      funcTest("raises exception on redaction of non-existent and already redacted exercise") {
        val redacted = Capture[String]
        Given:
          context(TransactionTreeStream)
        And:
          Database
            .query(sql"select exercise_event_id from exercises($choiceRef) limit 1")
            .returns(Table(redacted.capture))
        And:
          // redact exercise to make sure it cannot be redacted again
          Database.query(sql"select redact_exercise(${redacted.get}::event_id, ${"reason"})").returns(anything)
        Expect:
          // redacted exercises cannot be redacted again
          Database
            .query(sql"select redact_exercise(${redacted.get}::event_id, ${"reason"})")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot redact exercise with event ID ${redacted.get} because it is already redacted"
                    )
                  )
                )
              )
            )
        And:
          Database
            .query(sql"select redact_exercise($nonExistenceEventId::event_id, 'reason')")
            .exit
            .map(
              assert(_)(
                fails(
                  hasMessage(
                    startsWithString(
                      s"ERROR: Cannot find exercise with event ID $nonExistenceEventId"
                    )
                  )
                )
              )
            )
      },
      funcTest("can redact keys and key hashes") {
        val alice       = Party("Alice")
        val archivedCid = Capture[String]
        Given:
          DamlSdk.parties(alice) ++ Postgres.database
            >+> DamlSdk.deploy(withKey)
            >+> DamlSdk.runScript("WithKey:setup", alice.id)
        And:
          Pqs.runPipeline(
            "--pipeline-datasource=TransactionStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        And:
          Database.query(sql"select contract_id from archives()").returns(Table(archivedCid.capture))
        And:
          Database.query(sql"select redact_contract(${archivedCid.get}, $redactionId)").returns(Table(1))
        Expect:
          Database
            .query(
              sql"select payload, contract_key, contract_key_hash, redaction_id from lookup_contract(${archivedCid.get})"
            )
            .returns(Table(isNull | isNull | isNull | redactionId))
      } @@ onlyDamlLfVersion(">=2.3")
    )
}
