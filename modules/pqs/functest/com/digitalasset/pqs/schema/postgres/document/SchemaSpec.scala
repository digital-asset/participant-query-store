// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema.postgres.document

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import com.digitalasset.pqs.specific.{eventIdSqlType, offsetSqlType}
import zio.ZLayer
import zio.jdbc.sqlInterpolator
import zio.test.*

import scala.language.{implicitConversions, postfixOps}

object SchemaSpec extends SharedLedgerAndPostgresTest:
  private val templateMatcher = s"%-$specName-%"
  val alice                   = Party("Alice")
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

  private val context = DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice) ++ Postgres.database >+> DamlSdk.deploy

  def spec = suite("schema spec")(
    funcTest("postgres document schema is created"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-stop=Latest",
          s"--pipeline-filter-contracts=(${pingPong.name}:PingPong:Ping | ${interfaces.name}:Interfaces:IPingable)"
        )
      Then:
        Postgres `hasTable` "__watermark"
      And:
        Postgres `columnsIn` "__watermark" `are` table {
          "column"      | "nullable" | "type"
          ---           | ---        | ---
          "singleton"   | "NO"       | "boolean"
          "ix"          | "YES"      | "bigint"
          "offset"      | "YES"      | offsetSqlType
          "instance_id" | "YES"      | "text"
        }
      And:
        Postgres `hasTable` "__pruning_metadata"
      And:
        Postgres `columnsIn` "__pruning_metadata" `are` table {
          "column"        | "nullable" | "type"
          ---             | ---        | ---
          "singleton"     | "NO"       | "boolean"
          "pruned_offset" | "YES"      | "bigint"
        }
      And:
        Postgres `columnsIn` "__transactions" `are` table {
          "column"                    | "nullable" | "type"
          ---                         | ---        | ---
          "ix"                        | "NO"       | "bigint"
          "offset"                    | "NO"       | offsetSqlType
          "transaction_id"            | "YES"      | "text"
          "effective_at"              | "YES"      | "timestamp with time zone"
          "workflow_id"               | "YES"      | "text"
          "domain_id"                 | "YES"      | "text"
          "trace_context"             | "YES"      | "USER-DEFINED"
          "external_transaction_hash" | "YES"      | "bytea"
          "paid_traffic_cost"         | "YES"      | "bigint"
        }
      And:
        Postgres `columnsStorageIn` "__transactions" `are` table {
          "column"                    | "storage"
          ---                         | ---
          "ix"                        | "plain"
          "offset"                    | "plain"
          "transaction_id"            | "extended"
          "effective_at"              | "plain"
          "workflow_id"               | "extended"
          "domain_id"                 | "extended"
          "trace_context"             | "extended"
          "external_transaction_hash" | "external"
          "paid_traffic_cost"         | "plain"
        }
      And:
        Postgres `columnsIn` "__packages" `are` table {
          "column"  | "nullable" | "type"
          ---       | ---        | ---
          "pk"      | "NO"       | "bigint"
          "name"    | "NO"       | "text"
          "version" | "NO"       | "text"
          "id"      | "NO"       | "text"
        }
      And:
        Postgres `columnsIn` "__contract_tpe" `are` table {
          "column"       | "nullable" | "type"
          ---            | ---        | ---
          "pk"           | "NO"       | "bigint"
          "payload_type" | "NO"       | "USER-DEFINED"
          "aliases"      | "NO"       | "ARRAY"
          "package_name" | "NO"       | "text"
          "module_name"  | "NO"       | "text"
          "entity_name"  | "NO"       | "text"
          "template_fqn" | "NO"       | "text"
        }
      And:
        Postgres `columnsIn` "__exercise_tpe" `are` table {
          "column"       | "nullable" | "type"
          ---            | ---        | ---
          "pk"           | "NO"       | "bigint"
          "choice"       | "NO"       | "text"
          "consuming"    | "NO"       | "boolean"
          "aliases"      | "NO"       | "ARRAY"
          "package_name" | "NO"       | "text"
          "module_name"  | "NO"       | "text"
          "entity_name"  | "NO"       | "text"
          "template_fqn" | "NO"       | "text"
          "choice_fqn"   | "NO"       | "text"
        }
      And:
        Postgres `columnsIn` "__events" `are` table {
          "column"   | "nullable" | "type"
          ---        | ---        | ---
          "pk"       | "NO"       | "bigint"
          "tx_ix"    | "NO"       | "bigint"
          "event_id" | "NO"       | eventIdSqlType
          "type"     | "NO"       | "USER-DEFINED"
        }
      And:
        Postgres `columnsIn` "__contracts" `are` table {
          "column"              | "nullable" | "type"
          ---                   | ---        | ---
          "tpe_pk"              | "NO"       | "bigint"
          "create_event_pk"     | "YES"      | "bigint"
          "created_at_ix"       | "YES"      | "bigint"
          "archive_event_pk"    | "YES"      | "bigint"
          "archived_at_ix"      | "YES"      | "bigint"
          "life_ix"             | "NO"       | "int8range"
          "contract_id"         | "NO"       | "text"
          "payload"             | "YES"      | "jsonb"
          "contract_key"        | "YES"      | "jsonb"
          "metadata"            | "YES"      | "bytea"
          "redaction_id"        | "YES"      | "text"
          "package_pk"          | "NO"       | "bigint"
          "signatories"         | "NO"       | "ARRAY"
          "observers"           | "NO"       | "ARRAY"
          "witnesses"           | "NO"       | "ARRAY"
          "divulged_only"       | "NO"       | "boolean"
          "creation_package_id" | "YES"      | "text"
          "contract_key_hash"   | "YES"      | "bytea"
        }
      And:
        Postgres `columnsIn` "__contracts_1" `are` table {
          "column"              | "nullable" | "type"
          ---                   | ---        | ---
          "tpe_pk"              | "NO"       | "bigint"
          "create_event_pk"     | "YES"      | "bigint"
          "created_at_ix"       | "YES"      | "bigint"
          "archive_event_pk"    | "YES"      | "bigint"
          "archived_at_ix"      | "YES"      | "bigint"
          "life_ix"             | "NO"       | "int8range"
          "contract_id"         | "NO"       | "text"
          "payload"             | "YES"      | "jsonb"
          "contract_key"        | "YES"      | "jsonb"
          "metadata"            | "YES"      | "bytea"
          "redaction_id"        | "YES"      | "text"
          "package_pk"          | "NO"       | "bigint"
          "signatories"         | "NO"       | "ARRAY"
          "observers"           | "NO"       | "ARRAY"
          "witnesses"           | "NO"       | "ARRAY"
          "divulged_only"       | "NO"       | "boolean"
          "creation_package_id" | "YES"      | "text"
          "contract_key_hash"   | "YES"      | "bytea"
        }
      And:
        Postgres `columnsStorageIn` "__contracts_1" `are` table { // partition for Ping
          "column"              | "storage"
          ---                   | ---
          "tpe_pk"              | "plain"
          "create_event_pk"     | "plain"
          "created_at_ix"       | "plain"
          "archive_event_pk"    | "plain"
          "archived_at_ix"      | "plain"
          "life_ix"             | "extended"
          "contract_id"         | "extended"
          "payload"             | "extended"
          "contract_key"        | "extended"
          "metadata"            | "external"
          "redaction_id"        | "extended"
          "package_pk"          | "plain"
          "signatories"         | "extended"
          "observers"           | "extended"
          "witnesses"           | "extended"
          "divulged_only"       | "plain"
          "creation_package_id" | "extended"
          "contract_key_hash"   | "extended"
        }
      And:
        Postgres `columnsIn` "__exercises" `are` table {
          "column"                  | "nullable" | "type"
          ---                       | ---        | ---
          "tpe_pk"                  | "NO"       | "bigint"
          "contract_tpe_pk"         | "NO"       | "bigint"
          "exercise_event_pk"       | "YES"      | "bigint"
          "exercised_at_ix"         | "YES"      | "bigint"
          "contract_id"             | "NO"       | "text"
          "argument"                | "YES"      | "jsonb"
          "result"                  | "YES"      | "jsonb"
          "redaction_id"            | "YES"      | "text"
          "package_pk"              | "NO"       | "bigint"
          "controllers"             | "NO"       | "ARRAY"
          "last_descendant_node_id" | "NO"       | "integer"
          "witnesses"               | "NO"       | "ARRAY"
        }
      And:
        Postgres `columnsIn` "__exercises_1" `are` table { // partition for Ping:Archive
          "column"                  | "nullable" | "type"
          ---                       | ---        | ---
          "tpe_pk"                  | "NO"       | "bigint"
          "contract_tpe_pk"         | "NO"       | "bigint"
          "exercise_event_pk"       | "YES"      | "bigint"
          "exercised_at_ix"         | "YES"      | "bigint"
          "contract_id"             | "NO"       | "text"
          "argument"                | "YES"      | "jsonb"
          "result"                  | "YES"      | "jsonb"
          "redaction_id"            | "YES"      | "text"
          "package_pk"              | "NO"       | "bigint"
          "controllers"             | "NO"       | "ARRAY"
          "last_descendant_node_id" | "NO"       | "integer"
          "witnesses"               | "NO"       | "ARRAY"
        }
      And:
        Postgres query {
          sql"""select pk, template_fqn, payload_type, aliases
                from __contract_tpe
                where template_fqn like $templateMatcher
                order by pk"""
        } `returns` table {
          1 | s"${interfaces.name}:Interfaces:IPingable" | "interface" | s"{${interfaces.name}:Interfaces:IPingable,Interfaces:IPingable,IPingable}"
          2 | s"${pingPong.name}:PingPong:Ping" | "template" | s"{${pingPong.name}:PingPong:Ping,PingPong:Ping,Ping}"
        }
      And:
        Postgres query {
          sql"""select pk, template_fqn, choice_fqn, choice, consuming, aliases
                from __exercise_tpe
                where template_fqn like $templateMatcher
                order by pk"""
        } `returns` table {
          1 | s"${interfaces.name}:Interfaces:IPingable" | s"${interfaces.name}:Interfaces:IPingable:Archive" | "Archive" | true | s"{${interfaces.name}:Interfaces:IPingable:Archive,Interfaces:IPingable:Archive,IPingable:Archive,Archive}"
          2 | s"${pingPong.name}:PingPong:Ping" | s"${pingPong.name}:PingPong:Ping:Archive" | "Archive" | true | s"{${pingPong.name}:PingPong:Ping:Archive,PingPong:Ping:Archive,Ping:Archive,Archive}"
        }
    ,
    funcTest("partitioned indexes are created from parent table"):
      Given:
        context
      When:
        Pqs.runPipeline("--pipeline-ledger-stop=Latest")
      Then:
        Postgres query {
          sql"""select tablename, indexname from pg_indexes where tablename = '__contracts' order by indexname;"""
        } `returns` table {
          "__contracts" | "__contracts_archive_event_pk_idx"
          "__contracts" | "__contracts_archived_at_ix_idx"
          "__contracts" | "__contracts_contract_id_idx"
          "__contracts" | "__contracts_create_event_pk_idx"
          "__contracts" | "__contracts_created_at_ix_idx"
          "__contracts" | "__contracts_life_ix_idx"
          "__contracts" | "__contracts_package_pk_idx"
        }
      And:
        Postgres query {
          sql"""select
                      c.relname as table_name,
                      i.relname as index_name,
                      am.amname as index_type
                  from pg_class c
                           join pg_index ix on c.oid = ix.indrelid
                           join pg_class i on i.oid = ix.indexrelid
                           join pg_am am on i.relam = am.oid
                  where c.relname like '__contracts_%'
                  order by c.relname, i.relname
                  limit 7;"""
        } `returns` table {
          "__contracts_1" | "__contracts_1_archive_event_pk_idx"      | "btree"
          "__contracts_1" | "__contracts_1_archived_at_ix_tpe_pk_idx" | "btree"
          "__contracts_1" | "__contracts_1_contract_id_idx"           | "hash"
          "__contracts_1" | "__contracts_1_create_event_pk_idx"       | "btree"
          "__contracts_1" | "__contracts_1_created_at_ix_tpe_pk_idx"  | "btree"
          "__contracts_1" | "__contracts_1_life_ix_tpe_pk_idx"        | "gist"
          "__contracts_1" | "__contracts_1_package_pk_idx"            | "btree"
        }
      And:
        Postgres query {
          sql"""select tablename, indexname from pg_indexes where tablename = '__exercises' order by indexname;"""
        } `returns` table {
          "__exercises" | "__exercises_contract_id_idx"
          "__exercises" | "__exercises_exercise_event_pk_idx"
          "__exercises" | "__exercises_exercised_at_ix_idx"
          "__exercises" | "__exercises_package_pk_idx"
        }
      And:
        Postgres query {
          sql"""select
                      c.relname as table_name,
                      i.relname as index_name,
                      am.amname as index_type
                  from pg_class c
                           join pg_index ix on c.oid = ix.indrelid
                           join pg_class i on i.oid = ix.indexrelid
                           join pg_am am on i.relam = am.oid
                  where c.relname like '__exercises_%'
                  order by c.relname, i.relname
                  limit 4;"""
        } `returns` table {
          "__exercises_1" | "__exercises_1_contract_id_idx"            | "hash"
          "__exercises_1" | "__exercises_1_exercise_event_pk_idx"      | "btree"
          "__exercises_1" | "__exercises_1_exercised_at_ix_tpe_pk_idx" | "btree"
          "__exercises_1" | "__exercises_1_package_pk_idx"             | "btree"
        }
    ,
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
