// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.filtering

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.FuncTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ZLayer
import zio.jdbc.sqlInterpolator

import scala.language.implicitConversions

object MetadataFilteringSpec extends SharedLedgerAndPostgresTest:
  private val alice = Party("Alice")
  private val interfaces = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface IInterface1
                      |  where
                      |    viewtype IView
                      |
                      |interface IInterface2
                      |  where
                      |    viewtype IView
                      |
                      |data IView = IView with
                      |    owner: Party
                      |    bar: Text
                      |  deriving (Eq, Ord, Show)
                      |""".stripMargin
  )
  private val sample = DamlSource(
    "Sample" -> """module Sample where
                  |
                  |import Daml.Script
                  |import DA.Functor (void)
                  |
                  |import Interfaces
                  |
                  |template TTemplate1
                  |  with
                  |    owner: Party
                  |    foo: Text
                  |  where
                  |    signatory owner
                  |    interface instance IInterface1 for TTemplate1 where
                  |      view = IView with bar = foo, ..
                  |    interface instance IInterface2 for TTemplate1 where
                  |      view = IView with bar = foo, ..
                  |
                  |template TTemplate2
                  |  with
                  |    owner: Party
                  |    foo: Text
                  |  where
                  |    signatory owner
                  |
                  |setup : Party -> Script ()
                  |setup party = void do
                  |  submit party $ createCmd TTemplate1 with owner = party, foo = "foo1"
                  |  submit party $ createCmd TTemplate2 with owner = party, foo = "foo2"
                  |""".stripMargin
  ).dependsOn(interfaces)

  private val context =
    DamlSdk.dar(sample) ++ DamlSdk.parties(alice) ++ Postgres.database
      >+> DamlSdk.deploy >+> DamlSdk.runScript("Sample:setup", alice.id)

  private val metadataQuery = Postgres `query`
    sql"""select tp.template_fqn, count(c.*)
          from __contracts c join __contract_tpe tp on c.tpe_pk = tp.pk
          where c.metadata is not null
          group by tp.template_fqn
          order by tp.template_fqn"""

  def spec = suite("Metadata filtering")(
    funcTest("by default, no metadata is captured"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest"
        )
      Then:
        metadataQuery `returns` Table.empty
    ,
    funcTest("metadata captured only for configured filter"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=Sample.TTemplate2"
        )
      Then:
        metadataQuery `returns` table {
          s"${sample.name}:Sample:TTemplate2" | 1
        }
    ,
    funcTest("filtering on interface implicitly captures metadata for template"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=Interfaces.IInterface2"
        )
      Then:
        metadataQuery `returns` table {
          s"${interfaces.name}:Interfaces:IInterface1" | 1
          s"${interfaces.name}:Interfaces:IInterface2" | 1
          s"${sample.name}:Sample:TTemplate1"          | 1
        }
    ,
    funcTest("as long as at least one interface matches then metadata will get captured for entire projection set"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=Interfaces.IInterface1 & !Interfaces.IInterface2"
        )
      Then:
        metadataQuery `returns` table {
          s"${interfaces.name}:Interfaces:IInterface1" | 1
          s"${interfaces.name}:Interfaces:IInterface2" | 1
          s"${sample.name}:Sample:TTemplate1"          | 1
        }
    ,
    funcTest("can capture metadata for everything"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=*"
        )
      Then:
        metadataQuery `returns` table {
          s"${interfaces.name}:Interfaces:IInterface1" | 1
          s"${interfaces.name}:Interfaces:IInterface2" | 1
          s"${sample.name}:Sample:TTemplate1"          | 1
          s"${sample.name}:Sample:TTemplate2"          | 1
        }
    ,
    funcTest("seeding from ACS also applies metadata filter"):
      Given:
        context
      When:
        Pqs.runPipeline(
          "--pipeline-ledger-start=Latest",
          "--pipeline-ledger-stop=Latest",
          "--pipeline-filter-metadata=Sample.TTemplate2"
        )
      Then:
        metadataQuery `returns` table {
          s"${sample.name}:Sample:TTemplate2" | 1
        }
  )
