// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.configuration

import zio.ZIO
import zio.config.magnolia.describe
import zio.test.{ZIOSpecDefault, assertTrue}

object PrettyPrinterSpec extends ZIOSpecDefault:
  case class DummyConfig(
      @describe("Pipeline config")
      pipeline: PipelineConfig,
      @describe("Postgres config")
      postgres: PostgresConfig
  )

  case class PostgresConfig(
      pool: ZConnectionPoolConfig,
      host: String = "localhost",
      port: Int = 5432,
      mode: PostgresConfig.Mode = PostgresConfig.Mode.ApplySchema
  )

  case class PipelineConfig(
      @describe("Ledger party identifier")
      party: String,
      number: Int
  )

  final case class ZConnectionPoolConfig(minConnections: Int)

  object PostgresConfig:
    sealed trait Mode
    object Mode:
      case object ApplySchema     extends Mode
      case object DontApplySchema extends Mode
    end Mode
  end PostgresConfig

  val spec = suite("Configuration-related pretty printers specification")(
    suite("Pretty options from ConfigDescriptor") {
      import PrettyPrinter.ConfigDescriptor.prettyOptions
      import zio.test.Assertion.*
      val actual = zio.config.magnolia.descriptor[DummyConfig].prettyOptions("PREFIX")
      test("finds every individual option") {
        zio.test.assert(actual)(
          hasSize(equalTo(6)) &&
            contains(
              OptionInfo(
                "Ledger party identifier",
                "--pipeline-party",
                None,
                Some("string"),
                None,
                Some("PREFIX_PIPELINE_PARTY"),
                Some("pipeline.party"),
                Nil
              )
            ) &&
            contains(
              OptionInfo(
                "",
                "--pipeline-number",
                None,
                Some("int"),
                None,
                Some("PREFIX_PIPELINE_NUMBER"),
                Some("pipeline.number"),
                Nil
              )
            ) &&
            contains(
              OptionInfo(
                "",
                "--postgres-pool-minconnections",
                None,
                Some("int"),
                None,
                Some("PREFIX_POSTGRES_POOL_MINCONNECTIONS"),
                Some("postgres.pool.minConnections"),
                Nil
              )
            ) &&
            contains(
              OptionInfo(
                "",
                "--postgres-mode",
                None,
                Some("enum"),
                Some("ApplySchema"),
                Some("PREFIX_POSTGRES_MODE"),
                Some("postgres.mode"),
                List("ApplySchema", "DontApplySchema")
              )
            ) &&
            contains(
              OptionInfo(
                "",
                "--postgres-port",
                None,
                Some("int"),
                Some("5432"),
                Some("PREFIX_POSTGRES_PORT"),
                Some("postgres.port"),
                Nil
              )
            ) &&
            contains(
              OptionInfo(
                "",
                "--postgres-host",
                None,
                Some("string"),
                Some("localhost"),
                Some("PREFIX_POSTGRES_HOST"),
                Some("postgres.host"),
                Nil
              )
            )
        )
      }
    },
    suite("ReadError")(
      test("User-friendly errors display") {
        import PrettyPrinter.ReadError.pretty
        val expected =
          """
            |[ERROR] Missing value: --number. Details: value of type int
            |[ERROR] Missing value: --party. Details: Ledger party identifier, value of type string
            |""".stripMargin
        zio.config
          .read(zio.config.magnolia.descriptor[PipelineConfig])
          .as("Config validation should fail, but did not")
          .catchAll(error => ZIO.attempt(error.pretty))
          .map { actual => assertTrue(actual.trim == expected.trim) }
      }
    )
  )
end PrettyPrinterSpec
