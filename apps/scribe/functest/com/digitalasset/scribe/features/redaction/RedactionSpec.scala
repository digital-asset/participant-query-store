// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.redaction

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.specific.nonExistenceEventId
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.pipeline.pipeline.Config.TransactionApi
import com.digitalasset.scribe.pipeline.pipeline.Config.TransactionApi.{TransactionStream, TransactionTreeStream}
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.daml.DamlSdk.onlyDamlLfVersion
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.{Pipeline, Scribe}
import zio.ZLayer
import zio.jdbc.{SqlFragment, sqlInterpolator}
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object RedactionSpec extends SharedLedgerAndPostgresTest {
  val redactionId = "some_reason"
  lazy val alice  = Party("Alice")
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

  def context(dataSource: TransactionApi) = DamlSdk.dar(pingDaml) ++ DamlSdk.parties(alice) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("Pings:setup", alice.id)
    >+> Scribe.runPipeline(
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
          Postgres
            .query(sql"select contract_id from archives($templateRef) limit 1")
            .returns(table(oneArchived.capture))
        When:
          // 2 => affected contracts/interface views
          Postgres.query(sql"select redact_contract(${oneArchived.get}, $redactionId)").returns(table(2))
        And:
          Postgres
            .query(
              sql"select payload, redaction_id from lookup_contract(${oneArchived.get})"
            )
            .returns(
              table {
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
          Postgres.query(sql"select contract_id from active($templateRef) limit 1").returns(table(active.capture))
        And:
          Postgres.query(sql"select contract_id from archives($templateRef) limit 1").returns(table(redacted.capture))
        And:
          // redact valid contract to make sure it cannot be redacted again
          // 2 affected (template + interface)
          Postgres.query(sql"select redact_contract(${redacted.get}, ${"reason"})").returns(table(2))
        Expect:
          // active contracts cannot be redacted
          Postgres
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
          Postgres
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
          Postgres
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
          Postgres
            .query(sql"select exercise_event_id from exercises($choiceRef) limit 2")
            .returns(table(targetEventId.capture | controlEventId.capture).transpose)
        When:
          // 2 => affected contracts/interface views
          Postgres
            .query(sql"select redact_exercise(${targetEventId.get}::event_id, $redactionId)")
            .returns(Table.empty)
        And:
          Postgres
            .query(
              sql"select argument, result, redaction_id from exercises($choiceRef) where exercise_event_id = ${targetEventId.get}::event_id"
            )
            .returns(table(isNull | isNull | redactionId))
        And:
          Postgres
            .query(
              sql"select redaction_id from  exercises($choiceRef) where exercise_event_id = ${controlEventId.get}::event_id"
            )
            .returns(table(isNull))
      },
      funcTest("raises exception on redaction of non-existent and already redacted exercise") {
        val redacted = Capture[String]
        Given:
          context(TransactionTreeStream)
        And:
          Postgres
            .query(sql"select exercise_event_id from exercises($choiceRef) limit 1")
            .returns(table(redacted.capture))
        And:
          // redact exercise to make sure it cannot be redacted again
          Postgres.query(sql"select redact_exercise(${redacted.get}::event_id, ${"reason"})").returns(anything)
        Expect:
          // redacted exercises cannot be redacted again
          Postgres
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
          Postgres
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
        val archivedCid = Capture[String]
        Given:
          DamlSdk.dar(withKey) ++ DamlSdk.parties(alice) ++ Postgres.database
            >+> DamlSdk.deploy
            >+> DamlSdk.runScript("WithKey:setup", alice.id)
        And:
          Scribe.runPipeline(
            "--pipeline-datasource=TransactionStream",
            "--pipeline-ledger-start=Genesis",
            "--pipeline-ledger-stop=Latest",
            s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
          )

        And:
          Postgres.query(sql"select contract_id from archives()").returns(table(archivedCid.capture))
        And:
          Postgres.query(sql"select redact_contract(${archivedCid.get}, $redactionId)").returns(table(1))
        Expect:
          Postgres
            .query(
              sql"select payload, contract_key, contract_key_hash, redaction_id from lookup_contract(${archivedCid.get})"
            )
            .returns(table(isNull | isNull | isNull | redactionId))
      } @@ onlyDamlLfVersion(">=2.3")
    )
}
