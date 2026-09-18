// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.document

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.transcode.schema.packageName
import zio.ZLayer
import zio.jdbc.{SqlFragment, sqlInterpolator}
import zio.test.*
import zio.test.Assertion.equalTo

import scala.language.{implicitConversions, postfixOps}

object SchemaSpec extends SharedLedgerAndPostgresTest:
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
                    |""".stripMargin
  ).dependsOn(interfaces)

  private def context =
    DamlSdk.dar(pingPong) ++ DamlSdk.parties(Party("Alice")) ++ Postgres.database >+> DamlSdk.deploy

  def spec = suite("schema spec")(
    suite("create_index_for_contract")(
      funcTest("creates index on partition"):
        val contractName = s"${pingPong.name.packageName}:PingPong:Ping"
        val indexName    = "test-index-name"
        val indexType    = "hash"
        Given:
          context
        When:
          Pqs.runPipeline("--pipeline-ledger-stop=Latest")
        Then:
          Postgres call {
            sql"call create_index_for_contract($indexName, $contractName, '(payload->>''receiver'')', $indexType);"
          } `returns` ()
        And:
          for
            tpe_pk <-
              Postgres
                .get {
                  sql"select __contract_tpe4name($contractName);"
                }
                .map(_.get)
            result <- Postgres query {
              sql"""select
                          c.relname as table_name,
                          i.relname as index_name,
                          am.amname as index_type
                      from pg_class c
                               join pg_index ix on c.oid = ix.indrelid
                               join pg_class i on i.oid = ix.indexrelid
                               join pg_am am on i.relam = am.oid
                      where c.relname like '__contracts_%' and i.relname like ${"%" + indexName + "_idx"}
                      ;"""
            } `returns` table {
              s"__contracts_$tpe_pk" | s"__contracts_${tpe_pk}_${indexName}_idx" | indexType
            }
          yield result
    ),
    suite("print_create_index_for_contract")(
      funcTest("creates index on partition"):
        val contractName    = s"${pingPong.name.packageName}:PingPong:Ping"
        val indexName       = "test-index-name-2"
        val indexExpression = "(payload->>'receiver')"
        val indexType       = "hash"
        Given:
          context
        When:
          Pqs.runPipeline("--pipeline-ledger-stop=Latest")
        Then:
          for
            tpe_pk <- Postgres
              .get {
                sql"select __contract_tpe4name($contractName);"
              }
              .map(_.get)
            queryConcurrently <- Postgres
              .get {
                sql"select print_create_index_for_contract($indexName, $contractName, $indexExpression, $indexType);"
              }
              .map(_.get)
            query <- Postgres
              .get {
                sql"select print_create_index_for_contract($indexName, $contractName, $indexExpression, $indexType, use_concurrently => false);"
              }
              .map(_.get)
          yield zio.test.assert(queryConcurrently)(
            equalTo(
              s"""create index concurrently if not exists "__contracts_${tpe_pk}_${indexName}_idx" on __contracts_$tpe_pk using $indexType($indexExpression )"""
            )
          ) &&
            zio.test.assert(query)(
              equalTo(
                s"""create index if not exists "__contracts_${tpe_pk}_${indexName}_idx" on __contracts_$tpe_pk using $indexType($indexExpression )"""
              )
            )
    ),
    suite("migrations")(
      funcTest("manage schema evolution with Flyway"):
        Given:
          context
        When:
          Pqs.runPipeline("--pipeline-ledger-stop=Latest")
        Then:
          Postgres `hasTable` "flyway_schema_history"
        And:
          Postgres `query`
            sql"""select version, description, script
                      from flyway_schema_history
                      order by installed_rank
                      limit 2""" `returns` table {
              "001" | "Create initial schema" | "V001__Create_initial_schema.sql"
              "002" | "Make initializecontractimplements function idempotent" | "V002__Make_initializecontractimplements_function_idempotent.sql"
            }
    )
  )
