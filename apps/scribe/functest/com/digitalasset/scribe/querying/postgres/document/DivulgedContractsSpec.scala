// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.querying.postgres.document

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.ZLayer
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.anything

import scala.language.implicitConversions

object DivulgedContractsSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")
  private val bob   = Party("Bob")
  private val bank1 = Party("Bank1")
  private val bank2 = Party("Bank2")
  private val damlSource = DamlSource(
    "AssetApp" ->
      """module AssetApp where
        |
        |-- https://docs.digitalasset.com/overview/3.4/explanations/ledger-model/index.html
        |
        |import Daml.Script
        |import DA.Action
        |import DA.Optional
        |
        |template SimpleAsset with
        |    issuer : Party
        |    owner : Party
        |    asset : Text
        |  where
        |    signatory issuer
        |    observer owner
        |
        |    choice Transfer : ContractId SimpleAsset
        |      with
        |        newOwner : Party
        |      controller owner
        |      do
        |        create this with owner = newOwner
        |
        |template SimpleDvP with
        |    party1 : Party
        |    party2 : Party
        |    asset1 : ContractId SimpleAsset
        |    asset2 : ContractId SimpleAsset
        |  where
        |    signatory party1
        |    signatory party2
        |
        |    choice Settle : (ContractId SimpleAsset, ContractId SimpleAsset)
        |      with
        |        actor : Party
        |      controller actor
        |      do
        |        assert $ actor == party1 || actor == party2
        |        new1 <- exercise asset1 Transfer with newOwner = party2
        |        new2 <- exercise asset2 Transfer with newOwner = party1
        |        pure (new1, new2)
        |
        |template ProposeSimpleDvP with
        |    proposer : Party
        |    counterparty : Party
        |    allocated : ContractId SimpleAsset
        |    expected : SimpleAsset
        |  where
        |    signatory proposer
        |    observer counterparty
        |
        |    choice Accept : ContractId SimpleDvP
        |      with
        |        toBeAllocated : ContractId SimpleAsset
        |      controller counterparty
        |      do
        |        fetchedAsset <- fetch toBeAllocated
        |        assert $ fetchedAsset == expected
        |        create $ SimpleDvP with
        |          party1 = proposer
        |          party2 = counterparty
        |          asset1 = allocated
        |          asset2 = toBeAllocated
        |
        |    nonconsuming choice AcceptAndSettle : (ContractId SimpleAsset, ContractId SimpleAsset)
        |      with
        |        toBeAllocated: ContractId SimpleAsset
        |      controller counterparty
        |      do
        |        dvp <- exercise self $ Accept with ..
        |        exercise dvp $ Settle with actor = counterparty
        |
        |transact1 : (Party, Party, Party, Party) -> Script ()
        |transact1 parties = script do
        |  let (alice, bob, bank1, bank2) = parties
        |  replicateA_ 2 do
        |      let eurAsset = SimpleAsset with issuer = bank1, owner = alice, asset = "1 EUR"
        |      eur <- submit bank1 do createCmd eurAsset
        |      let usdAsset = SimpleAsset with issuer = bank2, owner = bob, asset = "1 USD"
        |      usd <- submit bank2 do createCmd usdAsset
        |
        |      proposeDvP <- submit alice $ do
        |          createCmd ProposeSimpleDvP with proposer = alice, counterparty = bob, allocated = eur, expected = usdAsset
        |      disclosedEur <- fromSome <$> queryDisclosure alice eur
        |
        |      dvp <- submit bob $ do exerciseCmd proposeDvP $ Accept with toBeAllocated = usd
        |      (newUsd, newEur) <- submitWithDisclosures bob [disclosedEur] $ do exerciseCmd dvp $ Settle with actor = bob
        |
        |      return ()
        |""".stripMargin
  )

  private val context = DamlSdk.dar(damlSource) ++ DamlSdk.parties(alice, bob, bank1, bank2) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("AssetApp:transact1", alice.id <&> bob.id <&> bank1.id <&> bank2.id)

  private def run(source: String, partyHint: String) =
    Scribe.runPipeline(
      "--pipeline-ledger-start=Oldest",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-datasource=$source",
      s"--pipeline-filter-parties=$partyHint::*"
    )

  def spec = suite("Divulged contracts")(
    funcTest("TransactionStream: divulged contracts are NOT present") {
      val bobHint = Capture[String]
      Given:
        context
      And:
        bob.name `is` bobHint.capture
      When:
        run(source = "TransactionStream", partyHint = bobHint.get)
      Expect:
        Postgres query {
          sql"select count(*) from creates('AssetApp:SimpleAsset') where divulged_only"
        } `returns` table { 0 }
      And:
        Postgres query {
          sql"select count(*) from archives('AssetApp:SimpleAsset') where divulged_only"
        } `returns` table { 0 }
      And:
        Postgres query {
          sql"select count(*) from active('AssetApp:SimpleAsset') where divulged_only"
        } `returns` table { 0 }
    },
    funcTest("TransactionTreeStream: divulged contracts are present") {
      // IMPORTANT: divulgences are only shipped in TransactionTreeStream from SDK 3.4+
      val bobHint = Capture[String]
      val bobId   = Capture[String]
      val aliceId = Capture[String]
      Given:
        context
      And:
        bob.name `is` bobHint.capture
      And:
        bob.id `is` bobId.capture
      And:
        alice.id `is` aliceId.capture
      When:
        run(source = "TransactionTreeStream", partyHint = bobHint.get)
      Expect:
        Postgres query { // Alice's assets get divulged to Bob
          sql"""select count(c.*)
                from creates('AssetApp:SimpleAsset') c
                where c.divulged_only
                  and c.stakeholders @> array[${aliceId.toString}]::text[]
                  and c.witnesses @> array[${bobId.toString}]::text[]"""
        } `returns` table { 2 }
      And:
        Postgres query {
          sql"select count(*) from creates()"
        } `returns` table { 10 }
      And:
        Postgres query {
          sql"select * from summary_creates() order by template_fqn"
        } `returns` table {
          s"${damlSource.name}:AssetApp:ProposeSimpleDvP" | "template" | 2
          s"${damlSource.name}:AssetApp:SimpleAsset"      | "template" | 6
          s"${damlSource.name}:AssetApp:SimpleDvP"        | "template" | 2
        }
      And:
        Postgres query { // archives are not broadcast for divulged contracts, only creates
          sql"select count(*) from archives('AssetApp:SimpleAsset') where divulged_only"
        } `returns` table { 0 }
      And:
        Postgres query {
          sql"select count(*) from archives()"
        } `returns` table { 6 }
      And:
        Postgres query {
          sql"select * from summary_archives() order by template_fqn"
        } `returns` table {
          s"${damlSource.name}:AssetApp:ProposeSimpleDvP" | "template" | 2
          s"${damlSource.name}:AssetApp:SimpleAsset"      | "template" | 2
          s"${damlSource.name}:AssetApp:SimpleDvP"        | "template" | 2
        }
      And:
        Postgres query { // active never includes divulged contracts
          sql"select count(*) from active('AssetApp:SimpleAsset') where divulged_only"
        } `returns` table { 0 }
      And:
        Postgres query {
          sql"select count(*) from active()"
        } `returns` table { 2 }
      And:
        Postgres query {
          sql"select * from summary_active() order by template_fqn"
        } `returns` table {
          s"${damlSource.name}:AssetApp:SimpleAsset" | "template" | 2
        }
      And:
        Postgres query {
          sql"select * from summary_transients() order by template_fqn"
        } `returns` table {
          s"${damlSource.name}:AssetApp:ProposeSimpleDvP" | "template" | 2
          s"${damlSource.name}:AssetApp:SimpleAsset"      | "template" | 2
          s"${damlSource.name}:AssetApp:SimpleDvP"        | "template" | 2
        }
    },
    funcTest("TransactionTreeStream: pruning of divulged contracts") {
      val bobHint        = Capture[String]
      val earlier_offset = Capture[Long]
      val later_offset   = Capture[Long]
      Given:
        context
      And:
        bob.name `is` bobHint.capture
      When:
        run(source = "TransactionTreeStream", partyHint = bobHint.get)
      Expect:
        Postgres query {
          sql"select template_fqn, created_at_offset from creates() where divulged_only order by create_event_id"
        } `returns` table {
          s"${damlSource.name}:AssetApp:SimpleAsset" | earlier_offset.capture
          s"${damlSource.name}:AssetApp:SimpleAsset" | later_offset.capture
        }
      And:
        Postgres
          .query(sql"select * from prune_archived_to_offset_dry_run(${earlier_offset.get})")
          .returns(table(anything | 4 | 4 | 8 | 3))
      And:
        Postgres
          .query(sql"select * from prune_archived_to_offset(${earlier_offset.get})")
          .returns(table(anything | 4 | 4 | 8 | 3))
      And:
        Postgres query { // divulgences are pruned akin to exercises (i.e. if created_at_offset <= cutoff_offset)
          sql"select template_fqn, created_at_offset from creates() where divulged_only order by create_event_id"
        } `returns` table {
          s"${damlSource.name}:AssetApp:SimpleAsset" | later_offset.get
        }
    },
    contractsPartiesPropagationSpec("TransactionStream"),
    contractsPartiesPropagationSpec("TransactionTreeStream"),
    exercisesPartiesPropagationSpec
  )

  private def contractsPartiesPropagationSpec(source: String) =
    funcTest(s"$source: should record stakeholders and witnesses on contracts") {
      val bobHint = Capture[String]
      val bobId   = Capture[String]
      val bank1Id = Capture[String]
      val bank2Id = Capture[String]
      Given:
        context
      And:
        bob.name `is` bobHint.capture
      And:
        bob.id `is` bobId.capture
      And:
        bank1.id `is` bank1Id.capture
      And:
        bank2.id `is` bank2Id.capture
      When:
        run(source = source, partyHint = bobHint.get)
      Expect:
        Postgres query {
          sql"""select c.payload->>'asset', c.signatories, c.observers, c.stakeholders, c.witnesses
                from creates('AssetApp:SimpleAsset') c
                order by c.create_event_id limit 1"""
        } `returns` table {
          "1 USD" | s"{$bank2Id}" | s"{$bobId}" | s"{$bank2Id,$bobId}" | s"{$bobId}"
        }
      And:
        Postgres query {
          sql"""select a.payload->>'asset', a.signatories, a.observers, a.stakeholders, a.witnesses
                from archives('AssetApp:SimpleAsset') a
                order by a.create_event_id limit 1;"""
        } `returns` table { // archives explicitly blinded wrt witnesses
          "1 USD" | s"{$bank2Id}" | s"{$bobId}" | s"{$bank2Id,$bobId}" | s"{}"
        }
      And:
        Postgres query {
          sql"""select a.payload->>'asset', a.signatories, a.observers, a.stakeholders, a.witnesses
                from active('AssetApp:SimpleAsset') a
                order by a.create_event_id limit 1;"""
        } `returns` table {
          "1 EUR" | s"{$bank1Id}" | s"{$bobId}" | s"{$bank1Id,$bobId}" | s"{$bobId}"
        }
    }

  private val exercisesPartiesPropagationSpec =
    funcTest("TransactionTreeStream: should record stakeholders and witnesses on exercises") {
      val bobHint = Capture[String]
      val bobId   = Capture[String]
      val aliceId = Capture[String]
      Given:
        context
      And:
        bob.name `is` bobHint.capture
      And:
        bob.id `is` bobId.capture
      And:
        alice.id `is` aliceId.capture
      When:
        run(source = "TransactionTreeStream", partyHint = bobHint.get)
      Expect:
        Postgres query {
          sql"""select e.argument->>'toBeAllocated', e.signatories, e.observers, e.stakeholders,
                       e.controllers, e.witnesses
                from exercises('AssetApp:ProposeSimpleDvP:Accept') e
                order by e.exercise_event_id limit 1"""
        } `returns` table {
          anything | s"{$aliceId}" | s"{$bobId}" | s"{$aliceId,$bobId}" | s"{$bobId}" | s"{$bobId}"
        }
    }
