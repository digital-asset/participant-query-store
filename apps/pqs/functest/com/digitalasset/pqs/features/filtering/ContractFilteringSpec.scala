// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.filtering

import com.digitalasset.pqs.SharedLedgerAndPostgresTest
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.functest.table.*
import com.digitalasset.pqs.services.daml.*
import com.digitalasset.pqs.services.postgres.Postgres
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ZLayer
import zio.jdbc.sqlInterpolator

import scala.language.implicitConversions

object ContractFilteringSpec extends SharedLedgerAndPostgresTest:
  private val templateMatcher = s"%-$specName-%"
  private val alice           = Party("Alice")
  private val interfaces = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface U
                      |  where
                      |    viewtype UView
                      |
                      |    doU : Text -> U
                      |
                      |    choice UChoice : ContractId U
                      |      with newName : Text
                      |      controller (view this).owner
                      |      do create (doU this newName)
                      |
                      |data UView = UView with owner: Party, name: Text
                      |  deriving (Eq, Ord, Show)
                      |
                      |interface V
                      |  where
                      |    viewtype VView
                      |
                      |    doV : Text -> V
                      |
                      |    choice VChoice : ContractId V
                      |      with newName : Text
                      |      controller (view this).owner
                      |      do create (doV this newName)
                      |
                      |data VView = VView with owner: Party, name: Text
                      |  deriving (Eq, Ord, Show)
                      |
                      |interface W
                      |  where
                      |    viewtype WView
                      |
                      |    doW : Text -> W
                      |
                      |    choice WChoice : ContractId W
                      |      with newName : Text
                      |      controller (view this).owner
                      |      do create (doW this newName)
                      |
                      |data WView = WView with owner: Party, name: Text
                      |  deriving (Eq, Ord, Show)
                      |
                      |""".stripMargin
  )
  private val templates = DamlSource(
    "Templates" -> """module Templates where
                     |
                     |import DA.Text
                     |import DA.List
                     |import Daml.Script
                     |import DA.Functor (void)
                     |
                     |import Interfaces
                     |
                     |{-
                     |Hierarchy of templates and interfaces:
                     |   T    S   P   <-- templates
                     |  ┌┴┐   │   │
                     |U ┘ └ V ┘   W   <-- interfaces
                     |-}
                     |
                     |template T
                     |  with
                     |    owner : Party
                     |    firstName : Text
                     |    lastName : Text
                     |  where
                     |    signatory owner
                     |
                     |    nonconsuming choice DoNothingT : ()
                     |      controller owner
                     |      do pure ()
                     |
                     |    interface instance U for T where
                     |        view = UView with name = unwords [firstName, lastName], ..
                     |        doU newName = toInterface (this with firstName = head $ words newName, lastName = last $ words newName)
                     |    interface instance V for T where
                     |        view = VView with name = unwords [firstName, lastName], ..
                     |        doV newName = toInterface (this with firstName = head $ words newName, lastName = last $ words newName)
                     |
                     |template S
                     |  with
                     |    owner : Party
                     |    firstName : Text
                     |    lastName : Text
                     |  where
                     |    signatory owner
                     |
                     |    nonconsuming choice DoNothingS : ()
                     |      controller owner
                     |      do pure ()
                     |
                     |    interface instance V for S where
                     |        view = VView with name = unwords [firstName, lastName], ..
                     |        doV newName = toInterface (this with firstName = head $ words newName, lastName = last $ words newName)
                     |
                     |template P
                     |  with
                     |    owner : Party
                     |    firstName : Text
                     |    lastName : Text
                     |  where
                     |    signatory owner
                     |
                     |    nonconsuming choice DoNothingP : ()
                     |      controller owner
                     |      do pure ()
                     |
                     |    interface instance W for P where
                     |        view = WView with name = unwords [firstName, lastName], ..
                     |        doW newName = toInterface (this with firstName = head $ words newName, lastName = last $ words newName)
                     |
                     |setup: Party -> Script ()
                     |setup alice = void do
                     |  cid_t <- submit alice $ createCmd (T with owner = alice, firstName = "t", lastName = "t")
                     |  submit alice $ exerciseCmd cid_t DoNothingT
                     |  cid_ut <- submit alice $ exerciseCmd (toInterfaceContractId @U cid_t) UChoice with newName = "U T"
                     |  cid_vt <- submit alice $ exerciseCmd (toInterfaceContractId @V $ fromInterfaceContractId @T cid_ut) VChoice with newName = "V T"
                     |
                     |  cid_s <- submit alice $ createCmd (S with owner = alice, firstName = "s", lastName = "s")
                     |  submit alice $ exerciseCmd cid_s DoNothingS
                     |  cid_vs <- submit alice $ exerciseCmd (toInterfaceContractId @V cid_s) VChoice with newName = "V S"
                     |
                     |  cid_p <- submit alice $ createCmd (P with owner = alice, firstName = "p", lastName = "p")
                     |  submit alice $ exerciseCmd cid_p DoNothingP
                     |  cid_wp <- submit alice $ exerciseCmd (toInterfaceContractId @W cid_p) WChoice with newName = "W P"
                     |  pure ()
                     |
                     |""".stripMargin
  ).dependsOn(interfaces)

  private val context =
    (DamlSdk.dar(templates) ++ DamlSdk.parties(alice) ++ Postgres.database)
      >+> DamlSdk.deploy >+> DamlSdk.runScript("Templates:setup", alice.id)

  private def run(start: String, source: String, filter: String) =
    Pqs.runPipeline(
      s"--pipeline-ledger-start=$start",
      "--pipeline-ledger-stop=Latest",
      s"--pipeline-datasource=$source",
      s"--pipeline-filter-contracts=$filter"
    )

  private def queryActive() = Postgres `query`
    sql"""select substring(template_fqn from position(':' in template_fqn) + 1), payload->>'firstName', payload->>'lastName', payload->>'name'
          from active()
          where template_fqn like $templateMatcher
          order by created_at_ix, template_fqn"""

  private def querySummaryExercises() = Postgres `query`
    sql"""select substring(choice_fqn from position(':' in choice_fqn) + 1) as ch_fqn, count
          from summary_exercises()
          where choice_fqn like $templateMatcher
          order by choice_fqn"""

  def spec = suite("Contract filtering")(
    suite("transactions")(
      funcTest("`filter = *` captures all templates and interface views") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionStream", filter = "*")
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
            "Interfaces:W" | null | null | "W P"
            "Templates:P"  | "W"  | "P"  | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      },
      funcTest("`filter = Templates.T` captures: only template") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionStream", filter = "Templates.T")
        Expect:
          queryActive() `returns` table {
            "Templates:T" | "V" | "T" | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      },
      funcTest("`filter = Interfaces.V` captures: only interface view and implementing templates") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionStream", filter = "Interfaces.V")
        Expect:
          queryActive() `returns` table {
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      },
      funcTest("`filter = Interfaces.U` captures: only interface view and implementing template") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionStream", filter = "Interfaces.U")
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      },
      funcTest("`filter = Templates.T | Templates.S` captures: matching templates") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionStream", filter = "Templates.T | Templates.S")
        Expect:
          queryActive() `returns` table {
            "Templates:T" | "V" | "T" | null
            "Templates:S" | "V" | "S" | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      },
      funcTest("`filter = Interfaces.U | Interfaces.V` captures: matching interface views and implementing templates") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionStream", filter = "Interfaces.U | Interfaces.V")
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      },
      funcTest(
        "`filter = * & !(Interfaces.W | Templates.P)` captures: templates and interface views without specified hierarchy cluster"
      ) {
        Given:
          context
        When:
          run(
            start = "Oldest",
            source = "TransactionStream",
            filter = "* & !(Interfaces.W | Templates.P)"
          )
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
          }
        And:
          querySummaryExercises() `returns` Table.empty
      }
    ),
    suite("transaction trees")(
      funcTest("`filter = *` captures all templates, interface views and templates/interfaces choices") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionTreeStream", filter = "*")
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
            "Interfaces:W" | null | null | "W P"
            "Templates:P"  | "W"  | "P"  | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice"   | 1
            "Interfaces:V:VChoice"   | 2
            "Interfaces:W:WChoice"   | 1
            "Templates:P:DoNothingP" | 1
            "Templates:S:DoNothingS" | 1
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest("`filter = Templates.T` captures: only template but also its templates/interfaces choices") {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionTreeStream", filter = "Templates.T")
        Expect:
          queryActive() `returns` table {
            "Templates:T" | "V" | "T" | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice" | 1 // we did not ask for interfaces' exercises but ledger sent them so we store them
            "Interfaces:V:VChoice" | 1
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest(
        "`filter = Interfaces.V` captures: only interface view, implementing templates but also their templates/interfaces choices"
      ) {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionTreeStream", filter = "Interfaces.V")
        Expect:
          queryActive() `returns` table {
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice" | 1 // UChoice is included because T implements U, and T is transitively included through V
            "Interfaces:V:VChoice"   | 2 // T and S both had VChoice exercised
            "Templates:S:DoNothingS" | 1
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest(
        "`filter = Interfaces.U` captures: only interface view, implementing template but also their templates/interfaces choices"
      ) {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionTreeStream", filter = "Interfaces.U")
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice" | 1
            "Interfaces:V:VChoice" | 1 // VChoice is included because T implements U & V, but only T's exercise is recorded (not S's)
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest(
        "`filter = Templates.T | Templates.S` captures: matching templates but also their templates/interfaces choices"
      ) {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionTreeStream", filter = "Templates.T | Templates.S")
        Expect:
          queryActive() `returns` table {
            "Templates:T" | "V" | "T" | null
            "Templates:S" | "V" | "S" | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice"   | 1
            "Interfaces:V:VChoice"   | 2
            "Templates:S:DoNothingS" | 1
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest(
        "`filter = Interfaces.U | Interfaces.V` captures: matching interface views, implementing templates " +
          "but also their templates/interfaces choices"
      ) {
        Given:
          context
        When:
          run(start = "Oldest", source = "TransactionTreeStream", filter = "Interfaces.U | Interfaces.V")
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice"   | 1
            "Interfaces:V:VChoice"   | 2
            "Templates:S:DoNothingS" | 1
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest(
        "`filter = * & !(Interfaces.W | Templates.P)` captures: templates, interfaces views and templates/interfaces choices without specified hierarchy cluster"
      ) {
        Given:
          context
        When:
          run(
            start = "Oldest",
            source = "TransactionTreeStream",
            filter = "* & !(Interfaces.W | Templates.P)"
          )
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice"   | 1
            "Interfaces:V:VChoice"   | 2
            "Templates:S:DoNothingS" | 1
            "Templates:T:DoNothingT" | 1
          }
      },
      funcTest("filter by package ids: captures only templates and templates/interface choices") {
        lazy val dar = Capture[DeployedDar]
        Given:
          context
        And:
          dar.captureFromService

        When:
          run(
            start = "Oldest",
            source = "TransactionTreeStream",
            filter = s"${templates.name}@${dar.get.dar.packageId} | ${interfaces.name}"
          )
        Expect:
          queryActive() `returns` table {
            "Interfaces:U" | null | null | "V T"
            "Interfaces:V" | null | null | "V T"
            "Templates:T"  | "V"  | "T"  | null
            "Interfaces:V" | null | null | "V S"
            "Templates:S"  | "V"  | "S"  | null
            "Interfaces:W" | null | null | "W P"
            "Templates:P"  | "W"  | "P"  | null
          }
        And:
          querySummaryExercises() `returns` table {
            "Interfaces:U:UChoice"   | 1
            "Interfaces:V:VChoice"   | 2
            "Interfaces:W:WChoice"   | 1
            "Templates:P:DoNothingP" | 1
            "Templates:S:DoNothingS" | 1
            "Templates:T:DoNothingT" | 1
          }
      }
    )
  )
