// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features

import com.digitalasset.canonical.{Event, SynchronizerId, Transaction}
import com.digitalasset.pqs.SharedMultiSyncLedgerSpec
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.pipeline.InProcessPipeline
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.transcode.codec.json.JsonCodec
import com.digitalasset.zio.daml.DamlSchema
import zio.metrics.{Metric, MetricLabel}
import zio.test.Assertion.*
import zio.{Chunk, ZIO}

object ReassignmentMetricsSpec extends SharedMultiSyncLedgerSpec:
  // A MetricKey includes its description, so this must match the pipeline's counter exactly.
  private val pipelineEvents = Metric.counter("pipeline_events", "Processed ledger events")

  private def labels(kv: (String, String)*): Set[MetricLabel] = kv.map(MetricLabel(_, _)).toSet

  private def counts(keys: Iterable[Set[MetricLabel]]) =
    ZIO.foreach(keys.toSeq)(key => pipelineEvents.tagged(key).value.map(key -> _.count)).map(_.toMap)

  // The metric registry is JVM-wide and other multi-sync specs replay PingPong:Ping concurrently.
  private def rename(id: SynchronizerId) = SynchronizerId(s"metrics-$id")

  private def onRenamedSynchronizers(tx: Transaction[Event]): Transaction[Event] =
    tx.copy(
      synchronizerId = rename(tx.synchronizerId),
      events = tx.events.map[Event] {
        case e: Event.Created   => e.copy(synchronizerId = rename(e.synchronizerId))
        case e: Event.Archived  => e.copy(synchronizerId = rename(e.synchronizerId))
        case e: Event.Exercised => e.copy(synchronizerId = rename(e.synchronizerId))
        case e: Event.Unassigned =>
          e.copy(synchronizerId = rename(e.synchronizerId), source = rename(e.source), target = rename(e.target))
        case e: Event.Assigned =>
          e.copy(synchronizerId = rename(e.synchronizerId), source = rename(e.source), target = rename(e.target))
      }
    )

  def spec = suite("Multi-Sync")(
    funcTest("pipeline_events counts reassignments once and labels them with synchronizer ids") {
      val alice        = Party("Alice")
      val contractId   = Capture[String]
      val transactions = Capture[Chunk[Transaction[Event]]]
      val before       = Capture[Map[Set[MetricLabel], Double]]

      def s1 = rename(SynchronizerId(sync1.id))
      def s2 = rename(SynchronizerId(sync2.id))
      def reassigned(tpe: String, on: SynchronizerId) = labels(
        "type"                   -> tpe,
        "template"               -> "PingPong:Ping",
        "synchronizer_id"        -> on,
        "source_synchronizer_id" -> s1,
        "target_synchronizer_id" -> s2
      )
      def expected = Map(
        labels("type" -> "create", "template" -> "PingPong:Ping", "synchronizer_id" -> s1)  -> 1.0,
        reassigned("unassign", on = s1)                                                     -> 1.0,
        reassigned("assign", on = s2)                                                       -> 1.0,
        labels("type" -> "archive", "template" -> "PingPong:Ping", "synchronizer_id" -> s2) -> 1.0,
        labels("type" -> "transaction", "synchronizer_id" -> s1)                            -> 2.0,
        labels("type" -> "transaction", "synchronizer_id" -> s2)                            -> 2.0,
        labels("type" -> "assign")                                                          -> 0.0,
        labels("type" -> "unassign")                                                        -> 0.0
      )

      Given:
        DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
      Then:
        createContract(alice).is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("PingPong:Ping", contractId.get, alice, sync2)
      And:
        Ledger.damlSchema()
          >+> DamlSchema.protobufCodecs
          >+> Ledger.updateService ++ Ledger.stateService
      Then:
        Ledger.recordTransactionStream.is(hasSize(equalTo(4)) && transactions.capture)
      When:
        Postgres.database
          >+> DamlSchema.produce(JsonCodec())
          >+> DamlSchema.produce(SqlSchema)
          >+> InProcessPipeline.destinationLayer()
      Then:
        counts(expected.keys).is(before.capture)
      When:
        InProcessPipeline.processTransactions(transactions.get.map(onRenamedSynchronizers))
      Expect:
        counts(expected.keys)
          .map(_.map((key, count) => key -> (count - before.get(key))))
          .is(equalTo(expected))
    }
  )
