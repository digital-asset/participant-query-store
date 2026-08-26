// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.errors

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.health.HealthEndpoint.responseStatus
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.http.Status
import zio.internal.stacktracer.SourceLocation

import scala.language.{implicitConversions, postfixOps}

object PackageReloadSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")

  // Use SourceLocation to create a unique name for the package
  private def createInterfaces(using location: SourceLocation) = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface IPingable
                      |  where
                      |    viewtype VPingable
                      |
                      |data VPingable = VPingable with sender: Party
                      |  deriving (Eq, Ord, Show)
                      |""".stripMargin
  ).withNameSuffix(s"v${location.line}")

  private def createPing(interfaces: DamlSource)(using location: SourceLocation) = DamlSource(
    "Ping" -> """module Ping where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |import Interfaces
                |
                |template Ping
                |  with
                |    sender: Party
                |    receiver: Party
                |  where
                |    signatory sender
                |    observer receiver
                |
                |    interface instance IPingable for Ping where
                |      view = VPingable with sender = sender
                |
                |transact: Party -> Script ()
                |transact alice = void do
                |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                |""".stripMargin
  ).withNameSuffix(s"v${location.line}").dependsOn(interfaces)

  private def createPong(using location: SourceLocation) = DamlSource(
    "Pong" -> """module Pong where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |
                |template Pong
                |  with
                |    sender: Party
                |    receiver: Party
                |  where
                |    signatory sender
                |    observer receiver
                |
                |transact: Party -> Script ()
                |transact alice = void do
                |  submit alice $ createCmd Pong with sender = alice, receiver = alice
                |""".stripMargin
  ).withNameSuffix(s"v${location.line}")

  // An upgrade of Pong (same package-name, different package-id) used to verify reload
  // stability after the template→template+interface subscription transition in
  // "Discover new packages after interfaces are introduced mid-stream".
  private def createPongUpgrade(basePong: DamlSource) = DamlSource(
    "Pong" -> """module Pong where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |
                |template Pong
                |  with
                |    sender: Party
                |    receiver: Party
                |    label: Optional Text
                |  where
                |    signatory sender
                |    observer receiver
                |
                |transact: Party -> Script ()
                |transact alice = void do
                |  submit alice $ createCmd Pong with sender = alice, receiver = alice, label = Some "upgraded"
                |""".stripMargin
  ).upgrades(basePong)

  def spec = suite("Package reload")(
    // Earlier, deploying a new package while the pipeline was running caused Scribe
    // to shut down. A fix introduced dynamic package reload: the open subscription
    // (WildcardFilter) delivers unknown-package events, triggering recovery.
    // This variant also tests selective metadata (includesAllMetadata=false), where
    // mkEventFormat appends TemplateFilter(blob=true) entries alongside WildcardFilter
    // (blob=false). Previously, selective metadata forced the else branch (closed
    // subscription), silently preventing new package discovery.
    // Also verifies /readyz stays Ok through the reload.
    funcTest("Recover when a new dar is deployed with selective metadata filter") {
      val interfaces = createInterfaces
      val ping       = createPing(interfaces)
      val pong       = createPong
      Given:
        (DamlSdk.dar(ping) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        DamlSdk.runScript("Ping:transact", alice.id)
      And:
        Scribe.pipeline(
          "--health-address=0.0.0.0",
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-filter-contracts=*",
          "--pipeline-filter-metadata=Ping.Ping"
        )
      And:
        Scribe `hasProcessedAtLeastTransactions` 1
      And:
        responseStatus("/readyz") `is` Some(Status.Ok) retryUntilTimeout

      lazy val pongDar = Capture[DeployedDar]
      When:
        DamlSdk.dar(pong) >+> DamlSdk.deploy >+> DamlSdk.runScript("Pong:transact", alice.id)
      And:
        pongDar.captureFromService
      Then:
        Scribe.stdoutContainsPackageReload(
          s"${pongDar.get.dar.packageId}:Pong:Pong"
        )
      And:
        Scribe `hasProcessedAtLeastTransactions` 2
      And:
        responseStatus("/readyz") `is` Some(Status.Ok) retryUntilTimeout
    },
    // Regression: when interfaces were present on the ledger, mkEventFormat fell through
    // to the else branch (closed subscription with explicit TemplateFilter/InterfaceFilter
    // entries only). A closed subscription never delivers events for unknown packages, so
    // Scribe would silently miss new packages entirely — no error, no reload, just lost updates.
    // This verifies that WildcardFilter is emitted alongside InterfaceFilters when
    // includesAll=true, keeping the subscription open for new package discovery.
    funcTest("Discover new packages when interfaces are present at startup") {
      val interfaces = createInterfaces
      val ping       = createPing(interfaces)
      val pong       = createPong

      val pingDar = Capture[DeployedDar]
      val pongDar = Capture[DeployedDar]

      Given:
        (DamlSdk.dar(ping) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        pingDar.captureFromService
      And:
        DamlSdk.runScript("Ping:transact", alice.id)
      And:
        Scribe.pipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-filter-contracts=*"
        )
      And:
        Scribe `hasProcessedAtLeastTransactions` 1

      When:
        DamlSdk.dar(pong) >+> DamlSdk.deploy >+> DamlSdk.runScript("Pong:transact", alice.id)
      And:
        pongDar.captureFromService
      Then:
        Scribe.stdoutContainsPackageReload(
          s"${pongDar.get.dar.packageId}:Pong:Pong"
        )
      And:
        Scribe.hasProcessedAtLeastTransactions(2)
    },
    // Unlike the test above, no interfaces exist at startup — the pipeline begins with
    // a pure WildcardFilter subscription. A package introducing interfaces triggers a
    // reload, and the rebuilt subscription must switch to WildcardFilter+InterfaceFilter
    // mode. A second reload (pongUpgrade — same package-name, different package-id)
    // verifies that new package discovery remains stable after the transition from
    // template-only to template+interface subscriptions.
    funcTest("Discover new packages after interfaces are introduced mid-stream") {
      val interfaces  = createInterfaces
      val ping        = createPing(interfaces)
      val pong        = createPong
      val pongUpgrade = createPongUpgrade(pong)

      val pongDar        = Capture[DeployedDar]
      val pingDar        = Capture[DeployedDar]
      val pongUpgradeDar = Capture[DeployedDar]

      Given:
        (DamlSdk.dar(pong) >+> DamlSdk.deploy) ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        pongDar.captureFromService
      And:
        DamlSdk.runScript("Pong:transact", alice.id)
      And:
        Scribe.pipeline(
          "--pipeline-datasource=TransactionTreeStream",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-filter-contracts=*"
        )
      And:
        Scribe.hasProcessedAtLeastTransactions(1)

      When:
        DamlSdk.dar(ping) >+> DamlSdk.deploy >+> DamlSdk.runScript("Ping:transact", alice.id)
      And:
        pingDar.captureFromService
      Then:
        Scribe.stdoutContainsPackageReload(
          s"${pingDar.get.dar.packageId}:Ping:Ping"
        )
      And:
        Scribe.hasProcessedAtLeastTransactions(2)

      When:
        DamlSdk.dar(pongUpgrade) >+> DamlSdk.deploy >+> DamlSdk.runScript("Pong:transact", alice.id)
      And:
        pongUpgradeDar.captureFromService
      Then:
        Scribe.stdoutContainsPackageReload(
          s"${pongUpgradeDar.get.dar.packageId}:Pong:Pong"
        )
      And:
        Scribe.hasProcessedAtLeastTransactions(3)
    }
  )
