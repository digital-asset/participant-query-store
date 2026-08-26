// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.schema.postgres.document

import com.digitalasset.scribe.docker.Service
import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.test.*
import zio.test.Assertion.not

import scala.language.{implicitConversions, postfixOps}

/** This must remain standalone to ensure log capture is not polluted by other concurrent tests.
  */
object SchemaCachingSpec extends FuncTestStandalone:
  val alice = Party("Alice")
  val interfaces = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface IPingable
                      |  where
                      |    viewtype VPingable
                      |
                      |data VPingable = VPingable with sender: Party
                      |  deriving (Eq, Ord, Show)
                      |""".stripMargin
  )
  val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
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
                    |-- filter out this
                    |template Pong
                    |  with
                    |    owner: Party
                    |  where
                    |    signatory owner
                    |
                    |    choice PongChoice : ()
                    |      controller owner
                    |      do return ()
                    |""".stripMargin
  ).dependsOn(interfaces)

  def spec = suite("schema caching spec")(
    funcTest("schema is cached"):
      Given:
        DamlSdk.dar(pingPong) ++ DamlSdk.ledger ++ Postgres.instance
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
      When:
        Scribe.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--logger-level=None"
        )
      And:
        Scribe.runPipeline(
          "--pipeline-ledger-stop=Latest",
          "--logger-level=Debug",
          "--logger-mappings-io=None",
          "--logger-mappings-org=None"
        )
      Then:
        Scribe.stdout `is`
          stringContaining("Successfully restored from cache at /ft/scribe-cache/descriptors-")
          && not(stringContaining("Fetched contents of package"))
          && not(stringContaining("Falling back to computing the value"))
      And:
        Postgres `hasTable` "__transactions"
  )
