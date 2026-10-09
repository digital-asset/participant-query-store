// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y

import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.pqs.SharedMultiSyncLedgerSpec
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.o11y.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.{Pipeline, Pqs}
import zio.test.Assertion.{equalTo, exists}

object GrafanaReassignmentSpec extends SharedMultiSyncLedgerSpec:
  private val assetInterface = DamlSource(
    "AssetInterface" -> """module AssetInterface where
                          |
                          |interface IAsset
                          |  where
                          |    viewtype VAsset
                          |
                          |data VAsset = VAsset with owner: Party
                          |  deriving (Eq, Ord, Show)
                          |""".stripMargin
  )

  // PQS stores every Asset event as two rows: the template's and IAsset's.
  private val asset = DamlSource(
    "Asset" -> """module Asset where
                 |
                 |import AssetInterface
                 |
                 |template Asset
                 |  with
                 |    owner: Party
                 |  where
                 |    signatory owner
                 |
                 |    interface instance IAsset for Asset where
                 |      view = VAsset with owner = owner
                 |""".stripMargin
  ).dependsOn(assetInterface)

  def spec = suite("observability signals can be queried with Grafana")(
    funcTest("[metrics] contract rows and contract events of a reassigned contract") {
      val alice      = Party("Alice")
      val contractId = Capture[String]
      def owner      = Record.defaultInstance.addFields(RecordField("owner", Some(Value(Value.Sum.Party(alice.id)))))
      def reassigned = s"source_synchronizer_id='${sync1.id}', target_synchronizer_id='${sync2.id}'"

      Given:
        Loki.instance ++ Prometheus.instance ++ Tempo.instance >>> (Collector.instance ++ Grafana.instance)
      And:
        Postgres.database ++ DamlSdk.allocateParties(alice -> Seq(sync1, sync2))
          ++ DamlSdk.uploadAndVetDar(asset)(sync1, sync2)
      Then:
        Ledger
          .create("Asset:Asset", owner, alice, sync1)
          .map(_.getTransaction.events(0).getCreated.contractId)
          .is(contractId.capture)
      When:
        Ledger.reassign(contractId.get, alice, sync1, sync2)
          *> Ledger.archive("Asset:Asset", contractId.get, alice, sync2)
      When:
        Pqs.runPipeline(
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )

      Then:
        // A row for Asset and one for IAsset
        Grafana
          .getMetrics(s"sum(pipeline_events_total{type='unassign', synchronizer_id='${sync1.id}'})")
          .is(exists(equalTo(2)))
          .retryUntilTimeout
      And:
        Grafana
          .getMetrics(s"sum(pipeline_events_total{type='assign', synchronizer_id='${sync2.id}'})")
          .is(exists(equalTo(2)))
          .retryUntilTimeout
      And:
        Grafana
          .getMetrics("sum(pipeline_contract_events_total{type='create'})")
          .is(exists(equalTo(1)))
          .retryUntilTimeout
      And:
        Grafana
          .getMetrics(
            s"sum(pipeline_contract_events_total{type='unassign', synchronizer_id='${sync1.id}', $reassigned})"
          )
          .is(exists(equalTo(1)))
          .retryUntilTimeout
      And:
        Grafana
          .getMetrics(s"sum(pipeline_contract_events_total{type='assign', synchronizer_id='${sync2.id}', $reassigned})")
          .is(exists(equalTo(1)))
          .retryUntilTimeout
      And:
        Grafana
          .getMetrics("sum(pipeline_contract_events_total{type='archive'})")
          .is(exists(equalTo(1)))
          .retryUntilTimeout
    }
  )
