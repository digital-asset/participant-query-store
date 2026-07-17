// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, Party}
import com.digitalasset.pqs.services.o11y.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import com.digitalasset.pqs.utils.safeequals.===
import zio.jdbc.sqlInterpolator
import zio.test.Assertion.{equalTo, exists, hasSameElements, hasSubset}
import zio.{ZIO, ZLayer}

import scala.language.{implicitConversions, postfixOps}

private enum TxSource:
  case TransactionStream, TransactionTreeStream

object GrafanaSetupSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")
  private val pingPong = DamlSource(
    "PingPong" ->
      """module PingPong where
        |
        |import Daml.Script
        |import DA.Functor (void)
        |
        |template Ping
        |  with
        |    sender: Party
        |    receiver: Party
        |  where
        |    signatory sender
        |    observer receiver
        |
        |transact1: Party -> Script ()
        |transact1 alice = void do
        |  submit alice $ createCmd Ping with sender = alice, receiver = alice
        |""".stripMargin
  )
  private lazy val upAndRunning =
    (Loki.instance ++ Prometheus.instance ++ Tempo.instance >>> (Collector.instance ++ Grafana.instance))
      ++ (Postgres.database ++ DamlSdk.dar(pingPong) ++ DamlSdk.parties(alice))
      >+> DamlSdk.deploy
      >+> DamlSdk.runScript("PingPong:transact1", alice.id)

  def spec = suite("observability signals can be queried with Grafana")(
    initRoutineSpec,
    pipelineSpec(TxSource.TransactionStream, "com.daml.ledger.api.v2.UpdateService/GetUpdates", false),
    pipelineSpec(TxSource.TransactionTreeStream, "com.daml.ledger.api.v2.UpdateService/GetUpdates", false)
  )

  val initRoutineSpec =
    funcTest("[logs, metrics, traces] initialization routine (incl. ACS seed)") {
      val initTraceId = Capture[String]
      val initTrace   = Capture[Grafana.Trace]
      val database    = Capture[Database]
      Given:
        upAndRunning
      When:
        Pqs.runPipeline(
          "--logger-mappings-org.flywaydb=None",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}",
          "--pipeline-ledger-stop=Latest"
        )
      And:
        database.captureFromService
      Then:
        Grafana.findTraceByName("initialization routine") `is` initTraceId.captureOptional retryUntilTimeout
      And:
        Grafana.findTraceById(initTraceId.get) `is` initTrace.capture retryUntilTimeout
      And:
        // Check for most important spans to be present
        ZIO.succeed(initTrace.get.spanNames) `is` hasSubset(
          Seq(
            "initialization routine",
            "seed from ACS",
            "execute datastore transaction",
            "execute batch",
            "execute SQL",
            "advance datastore watermark",
            s"UPDATE ${database.get.name}.__transactions",
            s"UPDATE ${database.get.name}.__watermark"
          )
        )
      // Check for span attributes to be present
      And:
        ZIO.succeed(initTrace.get.attributes("initialization routine").keys) `is` hasSubset(
          Seq(
            "pqs.init.actual.start",
            "pqs.init.actual.end",
            "pqs.init.datastore.start",
            "pqs.init.datastore.end",
            "pqs.init.ledger.start",
            "pqs.init.ledger.end",
            "pqs.init.normalized.start",
            "pqs.init.normalized.end"
          )
        )
      And:
        ZIO.succeed(initTrace.get.attributes("seed from ACS").keys) `is` hasSubset(
          Seq("pqs.init.acs.offset")
        )
      And:
        // Check for indicative logs in the trace
        Grafana
          .getLogsForTraceId(initTraceId.get)
          .is(
            exists(stringContaining("Last checkpoint is absent. Seeding from ACS before processing transactions"))
              && exists(stringMatching("Contract filter inclusive of \\d+ templates and \\d+ interfaces"))
              && exists(stringContaining("Advanced watermark: ix = 0"))
          )
          .retryUntilTimeout
      And:
        // 2 events: 1 contract + 1 watermark
        Grafana.getMetrics("pipeline_wp_acs_statements_total") `is` exists(equalTo(2)) retryUntilTimeout
    }

  def pipelineSpec(source: TxSource, service: String, batchedConsume: Boolean = true) =
    funcTest(s"[traces] $source processing ($service)") {
      val label = source match
        case TxSource.TransactionStream if service.contains(".v1.")     => "transaction"
        case TxSource.TransactionTreeStream if service.contains(".v1.") => "transaction tree"
        case TxSource.TransactionStream                                 => "transaction (ACS delta)"
        case TxSource.TransactionTreeStream                             => "transaction (ledger effects)"
      val consumeTraceId = Capture[String]
      val consumeTrace   = Capture[Grafana.Trace]
      val flushTraceId   = Capture[String]
      val flushTrace     = Capture[Grafana.Trace]
      val advanceTraceId = Capture[String]
      val advanceTrace   = Capture[Grafana.Trace]
      val database       = Capture[Database]
      Given:
        upAndRunning
      When:
        Pqs.runPipeline(
          "--logger-mappings-org.flywaydb=None",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}",
          s"--pipeline-datasource=$source",
          "--pipeline-ledger-start=Oldest",
          "--pipeline-ledger-stop=Latest"
        )
      And:
        database.captureFromService

      // 1. `consume` root-span checks
      Then:
        Grafana.findTraceByName(s"consume $service") `is` consumeTraceId.captureOptional retryUntilTimeout
      And:
        Grafana.findTraceById(consumeTraceId.get) `is` consumeTrace.capture retryUntilTimeout
      And:
        ZIO.succeed(consumeTrace.get.spanNames) `is` hasSameElements(
          Seq(s"consume $service", s"export $label")
        )
      And:
        ZIO.succeed(consumeTrace.get.attributes(s"consume $service").keys) `is` hasSubset(
          Seq(
            "messaging.destination.name",
            "messaging.operation.name",
            "messaging.operation.type",
            "messaging.system"
          ) ++ (if batchedConsume then Seq("messaging.batch.message_count") else Seq.empty)
        )
      And:
        ZIO.succeed(consumeTrace.get.attributes(s"export $label").keys) `is` hasSubset(
          Seq(
            "daml.command_id",
            "daml.effective_at",
            "daml.events_count",
            "daml.offset",
            "daml.transaction_id",
            "daml.workflow_id"
          )
        )
      And:
        ZIO.succeed(consumeTrace.get.links(s"export $label").values.map(_("target").str)) `is` hasSameElements(
          Seq("↥ ledger submission", "↧ persist to datastore", "↧ advance watermark")
        )
      And:
        ZIO.succeed(consumeTrace.get.events(s"export $label").keys.map(_._1)) `is` hasSameElements(
          Seq(
            s"canonicalizing $label",
            s"canonicalized $label",
            "converting canonical transaction to domain model",
            "converted canonical transaction to domain model",
            "released transaction model into batch",
            "prepared SQL statements for transaction model",
            "flushed transaction model SQL to datastore",
            "advanced datastore watermark"
          )
        )
      And:
        ZIO.succeed(
          consumeTrace.get.events(s"export $label").map(x => x._1._1 -> x._2)("advanced datastore watermark").keys
        ) `is` hasSameElements(
          Seq("index", "offset")
        )

      // 2. `flush` root-span checks
      Then:
        Grafana.findTraceByName(s"execute datastore transaction") `is` flushTraceId.captureOptional retryUntilTimeout
      And:
        Grafana.findTraceById(flushTraceId.get) `is` flushTrace.capture retryUntilTimeout
      And:
        ZIO.succeed(flushTrace.get.spanNames) `is` hasSameElements(
          Seq(
            "execute datastore transaction",
            "acquire connection",
            "execute batch",
            "execute SQL",
            "commit transaction"
          )
        )
      And:
        ZIO.succeed(flushTrace.get.attributes(s"execute batch").keys) `is` hasSubset(
          Seq("pqs.batch.models_count")
        )
      And:
        ZIO.succeed(flushTrace.get.links(s"execute batch").values.flatMap(_.keys)) `is` hasSameElements(
          Seq("target", "offset")
        )
      And:
        ZIO.succeed(flushTrace.get.links(s"execute batch").values.map(_("target").str)) `is` hasSameElements(
          Seq("↥ incoming transaction")
        )
      And:
        ZIO.succeed(flushTrace.get.attributes(s"execute SQL").keys) `is` hasSubset(
          Seq(
            "pqs.__archives.rows_count",
            "pqs.__contracts.rows_count",
            "pqs.__events.rows_count",
            "pqs.__exercises.rows_count",
            "pqs.__transactions.rows_count"
          )
        )

      // 3. `advance` root-span checks
      Then:
        Grafana.findTraceByName(s"advance datastore watermark") `is` advanceTraceId.captureOptional retryUntilTimeout
      And:
        Grafana.findTraceById(advanceTraceId.get) `is` advanceTrace.capture retryUntilTimeout
      And:
        ZIO.succeed(advanceTrace.get.spanNames) `is` hasSubset(
          Seq(
            "advance datastore watermark",
            "acquire connection",
            s"UPDATE ${database.get.name}.__watermark",
            "commit transaction"
          )
        )
      And:
        ZIO.succeed(advanceTrace.get.attributes(s"advance datastore watermark").keys) `is` hasSubset(
          Seq("pqs.watermark.offset", "pqs.watermark.ix")
        )
      And:
        ZIO.succeed(
          advanceTrace.get.links(s"advance datastore watermark").values.map(_("target").str)
        ) `is` hasSameElements(
          Seq("↥ persist to datastore")
        )

      // 4. verify Pqs propagated original ledger transaction's trace context
      And:
        ZIO
          .attempt {
            consumeTrace.get
              .links(s"export $label")
              .find((_, attrs) => attrs("target").str === "↥ ledger submission")
              .map((ids, _) => s"00-${ids._1}-${ids._2}-0[13]")
              .fold(throw new Exception("Should've found link to original Daml transaction"))(identity)
          }
          .flatMap { traceCtxPattern =>
            Postgres query { sql"""select (trace_context).trace_parent from __transactions;""" } `returns` table {
              stringMatching(traceCtxPattern)
            }
          }
    }
