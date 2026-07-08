// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.querying.postgres.document

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.querying.postgres.document.QueryingSpec.{funcTest}
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import com.digitalasset.scribe.specific.OffsetType
import zio.ZLayer
import zio.jdbc.sqlInterpolator
import zio.test.*
import zio.test.Assertion.*

import scala.language.implicitConversions

object QueryingSpec extends SharedLedgerAndPostgresTest:

  private val alice = Party("Alice")
  private val interfaces = DamlSource(
    "Interfaces" -> """module Interfaces where
                      |
                      |interface INameDocument
                      |  where
                      |    viewtype VNameDocument
                      |
                      |    setName : Text -> INameDocument
                      |
                      |    choice INameDocumentNameChange : ContractId INameDocument
                      |      with newName : Text
                      |      controller (view this).owner
                      |      do
                      |        create (setName this newName)
                      |
                      |data VNameDocument = VNameDocument with
                      |    owner : Party
                      |    name : Text
                      |  deriving (Eq, Ord, Show)
                      |""".stripMargin
  )
  private val nameRegistry = DamlSource(
    "NameRegistry" -> """module NameRegistry where
                        |
                        |import Daml.Script
                        |import DA.Functor (void)
                        |import DA.List
                        |import DA.Text
                        |
                        |import Interfaces
                        |
                        |template BirthCertificate
                        |  with
                        |    owner : Party
                        |    user_id : Text
                        |    firstName : Text
                        |    lastName : Text
                        |  where
                        |    signatory owner
                        |
                        |    choice BirthCertificateNameChange : ContractId BirthCertificate
                        |      with
                        |        newFirstName : Text
                        |        newLastName : Text
                        |      controller owner
                        |      do
                        |        cid <- create BirthCertificate with
                        |          owner = owner
                        |          user_id = user_id
                        |          firstName = newFirstName
                        |          lastName = newLastName
                        |        exercise cid DoNothing
                        |        return cid
                        |
                        |    nonconsuming choice DoNothing : ()
                        |      controller owner
                        |      do pure ()
                        |
                        |    interface instance INameDocument for BirthCertificate where
                        |        view = VNameDocument with name = unwords [firstName, lastName], ..
                        |        setName newName = toInterface (this with firstName = head $ words newName, lastName = last $ words newName)
                        |
                        |{--
                        |   watermark
                        |       v
                        |N 123456|78|9a
                        |A:[=====|==|==  alice citizen (exists forever)
                        |B: []   |  |    joe bloggs    (rename joe to fred @ 03)
                        |C:  []  |  |    fred bloggs
                        |D:    []|  |    bill myers    (rename myers to taylor @ at watermark)
                        |E:     [|] |    bill taylor   (rename taylor to doe @ in gap)
                        |F:      |[=|]   bill doe      (rename doe to kirk @ in already-flushed tx)
                        |G:      |  |[=  bill kirk
                        |H:      | [|==  jane smith    (created in gap, never archived)
                        |I:      |  | [  jill brown    (created post-gap)
                        |         ^^
                        |         gap
                        |--}
                        |setup: Party -> Script ()
                        |setup alice = void do
                        |  cid_alice  <- submit alice $ createCmd (BirthCertificate with owner = alice, user_id = "id-alice", firstName = "Alice", lastName = "Citizen")
                        |  cid_joe    <- submit alice $ createCmd (BirthCertificate with owner = alice, user_id = "id-joe", firstName = "Joe", lastName = "Bloggs")
                        |  cid_fred   <- submit alice $ exerciseCmd cid_joe BirthCertificateNameChange with newFirstName = "Fred", newLastName = "Bloggs"
                        |  _          <- submit alice $ archiveCmd cid_fred
                        |  cid_myers  <- submit alice $ createCmd (BirthCertificate with owner = alice, user_id = "id-bill", firstName = "Bill", lastName = "Myers")
                        |  cid_taylor <- submit alice $ exerciseCmd cid_myers BirthCertificateNameChange with newFirstName = "Bill", newLastName = "Taylor"
                        |  cid_doe    <- submit alice $ exerciseCmd (toInterfaceContractId @INameDocument cid_taylor) INameDocumentNameChange with newName = "Bill Doe"
                        |  cid_jane   <- submit alice $ createCmd (BirthCertificate with owner = alice, user_id = "id-jane", firstName = "Jane", lastName = "Smith")
                        |  _          <- submit alice $ exerciseCmd (toInterfaceContractId @INameDocument cid_doe) INameDocumentNameChange with newName = "Bill Kirk"
                        |  submit alice $ createCmd (BirthCertificate with owner = alice, user_id = "id-jill", firstName = "Jill", lastName = "Brown")
                        |""".stripMargin
  ).dependsOn(interfaces)

  private val historyBoundaries = sql"""select min(ix), max(ix)
                                        from __transactions
                                        where "offset" between oldest_offset() and latest_offset();"""

  private val upAndRunning = DamlSdk.dar(nameRegistry) ++ DamlSdk.parties(alice) ++ Postgres.database
    >+> DamlSdk.deploy
    >+> DamlSdk.runScript("NameRegistry:setup", alice.id)
    >+> Scribe.runPipeline(
      "--pipeline-ledger-start=Genesis",
      "--pipeline-ledger-stop=Latest",
      "--pipeline-datasource=TransactionTreeStream"
    )

  def spec = suite("Querying")(
    suite("time management")(
      funcTest("oldest/latest functions constrain to consistent history"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        Then:
          Postgres query { sql"""select min(ix), max(ix) from __transactions;""" } `returns` table { 1 <|> 10 }
        And:
          Postgres query { historyBoundaries } `returns` table { 1 <|> 6 }
      ,
      funcTest("can set/reset a session's oldest/latest offset"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        And:
          Postgres.Session.init
        And:
          Postgres.Session `setOldest` 2
        Then:
          Postgres.Session query { historyBoundaries } `returns` table { 2 <|> 6 }
        When:
          Postgres.Session `setLatest` 5
        Then:
          Postgres.Session query { historyBoundaries } `returns` table { 2 <|> 5 }
        When:
          Postgres.Session `setOldest` null
        And:
          Postgres.Session `setLatest` null
        Then:
          Postgres.Session query { historyBoundaries } `returns` table { 1 <|> 6 }
      ,
      funcTest("can pin a session's latest offset even on an advancing ledger"):
        Given:
          upAndRunning

        val offsetAt7 = Capture[OffsetType]
        Then:
          Postgres query {
            sql"""select "offset" from __transactions where ix = 7;"""
          } `returns` table { offsetAt7.capture }
        And:
          Postgres `makeGap` (7 to 8) `returns` 2

        When:
          Postgres.Session.init
        And:
          Postgres.Session `setLatest` 6
        And:
          Postgres query {
            sql"""insert into __transactions (ix, "offset") values (7, ${offsetAt7.get});""".insert
          } `returns` anything
        And:
          Postgres query {
            sql"""update __watermark set ix = 7, "offset" = ${offsetAt7.get}, instance_id = 'advance-watermark';""".update
          } `returns` anything
        Then:
          Postgres query { historyBoundaries } `returns` table { 1 <|> 7 }
        And:
          Postgres.Session query { historyBoundaries } `returns` table { 1 <|> 6 }
      ,
      funcTest("can pin a session's latest offset ensuring it is no less than supplied offset"):
        Given:
          upAndRunning

        val offsetAt4 = Capture[OffsetType]
        val offsetAt6 = Capture[OffsetType]
        val offsetAt7 = Capture[OffsetType]
        Then:
          Postgres query {
            sql"""select (select "offset" from __transactions where ix = 4),
                             (select "offset" from __transactions where ix = 6),
                             (select "offset" from __transactions where ix = 7);"""
          } `returns` table { offsetAt4.capture | offsetAt6.capture | offsetAt7.capture }
        And:
          Postgres `makeGap` (7 to 8) `returns` 2

        When:
          Postgres.Session.init
        And:
          Postgres.Session `setLatest` offsetAt4.get
        Then:
          Postgres.Session queryVerbose { historyBoundaries } `returns` List(
            table { offsetAt4.get },
            table { 1 <|> 4 }
          )
        When:
          Postgres.Session `setLatest` offsetAt7.get
        Then:
          (Postgres.Session query { sql"select 42;" }).exit.map(result =>
            assert(result)(
              fails(
                hasMessage(
                  containsString(s"Illegal offset ${offsetAt7.get} is beyond upper bounds of contiguous history")
                )
              )
            )
          )
      ,
      funcTest("can resolve offset based on temporal values (timestamp/interval)"):
        Given:
          upAndRunning

        val offsetSlightlyPriorTo3 = Capture[OffsetType]
        val offsetSlightlyPriorTo5 = Capture[OffsetType]
        val offsetAt3              = Capture[OffsetType]
        val offsetAt5              = Capture[OffsetType]
        Then:
          Postgres query {
            sql"""select
                        -- by timestamp (closest match prior)
                        (select nearest_offset((select (effective_at - interval '00:00:00.000001') from __transactions where ix = 3))),
                        -- by interval (closest match prior)
                        (select nearest_offset(now() - (select (effective_at - interval '00:00:00.000001') from __transactions where ix = 5))),
                        -- by timestamp (exact match)
                        (select nearest_offset((select effective_at from __transactions where ix = 3))),
                        -- by interval (exact match)
                        (select nearest_offset(now() - (select effective_at from __transactions where ix = 5)));"""
          } `returns` table {
            offsetSlightlyPriorTo3.capture | offsetSlightlyPriorTo5.capture | offsetAt3.capture | offsetAt5.capture
          }

        When:
          Postgres.Session.init
        And:
          Postgres.Session `setOldest` offsetSlightlyPriorTo3.get
        And:
          Postgres.Session `setLatest` offsetSlightlyPriorTo5.get
        Then:
          Postgres.Session query { historyBoundaries } `returns` table { 2 <|> 4 }
        And:
          Postgres.Session `setOldest` offsetAt3.get
        And:
          Postgres.Session `setLatest` offsetAt5.get
        Then:
          Postgres.Session query { historyBoundaries } `returns` table { 3 <|> 5 }
      ,
      funcTest("resolves valid offset when __watermark is behind latest transaction"):
        Given:
          upAndRunning

        val txOffset   = Capture[OffsetType]
        val txInFlight = Capture[OffsetType]
        And:
          Postgres query {
            sql"""select
                              (select nearest_offset((select effective_at from __transactions where ix = 5))),
                              (select nearest_offset((select effective_at from __transactions where ix = 10)));
                              """
          } `returns` table {
            txOffset.capture | txInFlight.capture
          }
        And:
          Postgres.reverseWatermark(5) `returns` 1
        When:
          Postgres.Session.init
        Then:
          Postgres query {
            sql"select nearest_offset((select effective_at from __transactions where ix = 10))"
          } `returns` table { txOffset.get }
    ),
    suite("read API")(
      funcTest("transactions queries"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select count(*) from transactions;"""
          } `returns` table { 6 }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 2
        And:
          Postgres.Session `setLatest` 5
        Then:
          Postgres.Session query {
            sql"""select count(*) from transactions;"""
          } `returns` table { 4 }
      ,
      funcTest("state-based queries: ACS @ any offset"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        Then:
          // Binds to `latest_offset()` value by default
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                      from active('NameRegistry:BirthCertificate')
                      order by created_at_offset;"""
          } `returns` table {
            "id-alice" | "Alice" | "Citizen"
            "id-bill"  | "Bill"  | "Taylor"
          }
        And:
          // ... or, can supply an offset explicitly
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                      from active('NameRegistry:BirthCertificate', (select "offset" from __transactions where ix = 3))
                      order by created_at_offset;"""
          } `returns` table {
            "id-alice" | "Alice" | "Citizen"
            "id-joe"   | "Fred"  | "Bloggs"
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setLatest` 3
        Then:
          Postgres.Session query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                      from active('NameRegistry:BirthCertificate')
                      order by created_at_offset;"""
          } `returns` table {
            "id-alice" | "Alice" | "Citizen"
            "id-joe"   | "Fred"  | "Bloggs"
          }
      ,
      funcTest("event-based queries: create events @ any [oldest..latest] range"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from creates('NameRegistry:BirthCertificate')
                    order by created_at_offset;"""
          } `returns` table {
            "id-alice" | "Alice" | "Citizen"
            "id-joe"   | "Joe"   | "Bloggs"
            "id-joe"   | "Fred"  | "Bloggs"
            "id-bill"  | "Bill"  | "Myers"
            "id-bill"  | "Bill"  | "Taylor"
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from creates('NameRegistry:BirthCertificate',
                                 (select "offset" from __transactions where ix = 3),
                                 (select "offset" from __transactions where ix = 5))
                    order by created_at_offset;"""
          } `returns` table {
            "id-joe"  | "Fred" | "Bloggs"
            "id-bill" | "Bill" | "Myers"
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 5
        Then:
          Postgres.Session query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from creates('NameRegistry:BirthCertificate')
                    order by created_at_offset;"""
          } `returns` table {
            "id-joe"  | "Fred" | "Bloggs"
            "id-bill" | "Bill" | "Myers"
          }
      ,
      funcTest("event-based queries: archive events @ any [oldest..latest] range"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from archives('NameRegistry:BirthCertificate')
                    order by created_at_offset;"""
          } `returns` table {
            "id-joe"  | "Joe"  | "Bloggs"
            "id-joe"  | "Fred" | "Bloggs"
            "id-bill" | "Bill" | "Myers"
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from archives('NameRegistry:BirthCertificate',
                                  (select "offset" from __transactions where ix = 3),
                                  (select "offset" from __transactions where ix = 5))
                    order by created_at_offset;"""
          } `returns` table {
            "id-joe" | "Joe"  | "Bloggs"
            "id-joe" | "Fred" | "Bloggs"
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 5
        Then:
          Postgres.Session query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from archives('NameRegistry:BirthCertificate')
                    order by created_at_offset;"""
          } `returns` table {
            "id-joe" | "Joe"  | "Bloggs"
            "id-joe" | "Fred" | "Bloggs"
          }
      ,
      funcTest("event-based queries: exercise events @ any [oldest..latest] range"):
        Given:
          upAndRunning
        When:
          Postgres `makeGap` (7 to 8) `returns` 2
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select argument->>'newFirstName', argument->>'newLastName'
                    from exercises('BirthCertificate:BirthCertificateNameChange')
                    order by exercised_at_offset;"""
          } `returns` table {
            "Fred" | "Bloggs"
            "Bill" | "Taylor"
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select argument->>'newFirstName', argument->>'newLastName'
                    from exercises('BirthCertificate:BirthCertificateNameChange',
                                   (select "offset" from __transactions where ix = 3),
                                   (select "offset" from __transactions where ix = 5))
                    order by exercised_at_offset;"""
          } `returns` table {
            "Fred" | "Bloggs"
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 5
        Then:
          Postgres.Session query {
            sql"""select argument->>'newFirstName', argument->>'newLastName'
                    from exercises('BirthCertificate:BirthCertificateNameChange')
                    order by exercised_at_offset;"""
          } `returns` table {
            "Fred" | "Bloggs"
          }
      ,
      funcTest("event-based queries: exercise events expose last descendant node") {
        Given:
          upAndRunning
        Then:
          Postgres query {
            sql"""select exercise_event_id, choice_fqn, last_descendant_node_id from exercises() order by exercise_event_id;"""
          } `returns` table {
            stringContaining(
              ",0)"
            ) <|> s"${nameRegistry.name}:NameRegistry:BirthCertificate:BirthCertificateNameChange" <|> 2
            stringContaining(",2)") <|> s"${nameRegistry.name}:NameRegistry:BirthCertificate:DoNothing" <|> 2
            stringContaining(",0)") <|> s"${nameRegistry.name}:NameRegistry:BirthCertificate:Archive" <|> 0
            stringContaining(
              ",0)"
            ) <|> s"${nameRegistry.name}:NameRegistry:BirthCertificate:BirthCertificateNameChange" <|> 2
            stringContaining(",2)") <|> s"${nameRegistry.name}:NameRegistry:BirthCertificate:DoNothing" <|> 2
            stringContaining(",0)") <|> s"${interfaces.name}:Interfaces:INameDocument:INameDocumentNameChange" <|> 1
            stringContaining(",0)") <|> s"${interfaces.name}:Interfaces:INameDocument:INameDocumentNameChange" <|> 1
          }
      },
      funcTest("hybrid queries: analytics over current data and history"):
        Given:
          upAndRunning
        Then:
          Postgres query {
            sql"""-- are there any active users who had been known under different names in the past?
                    select user_id, current_name, string_agg(other_name, ', ' order by other_name) also_known_as
                    from (
                      select a.payload->>'user_id' user_id,
                             (a.payload->>'firstName')::text || ' ' || (a.payload->>'lastName')::text current_name,
                             (c.payload->>'firstName')::text || ' ' || (c.payload->>'lastName')::text other_name
                      from active('NameRegistry:BirthCertificate') a
                      inner join archives('NameRegistry:BirthCertificate') c on a.payload->>'user_id' = c.payload->>'user_id'
                    ) t
                    group by user_id, current_name;"""
          } `returns` table {
            "id-bill" | "Bill Kirk" | "Bill Doe, Bill Myers, Bill Taylor"
          }
        And:
          Postgres query {
            sql"""-- show chronological changes of names
                    select t.user_id,
                           array_to_string(array_prepend((array_agg(t.from_name))[1], array_agg(t.to_name)), ' ~> ') names_evolution
                    from (
                        select c.payload->>'user_id' user_id,
                               (c.payload->>'firstName')::text || ' ' || (c.payload->>'lastName'::text) from_name,
                               e.new_name to_name
                        from creates('NameRegistry:BirthCertificate') c
                        inner join (
                            select exercised_at_offset, contract_id, argument->>'newName' new_name
                            from exercises('INameDocument:INameDocumentNameChange')
                              union all
                            select exercised_at_offset, contract_id, (argument->>'newFirstName')::text || ' ' || (argument->>'newLastName')::text new_name
                            from exercises('BirthCertificate:BirthCertificateNameChange')
                        ) e on c.contract_id = e.contract_id
                        inner join __transactions tx on e.exercised_at_offset = tx."offset"
                        order by tx.ix
                    ) t
                    group by t.user_id
                    order by t.user_id;"""
          } `returns` table {
            "id-bill" | "Bill Myers ~> Bill Taylor ~> Bill Doe ~> Bill Kirk"
            "id-joe"  | "Joe Bloggs ~> Fred Bloggs"
          }
      ,
      funcTest("lookup queries: get contract data by contract ID"):
        Given:
          upAndRunning
        Then:
          Postgres query {
            sql"""select template_fqn, created_at_offset, created_effective_at, archived_effective_at, contract_id, create_event_id, payload->>'user_id'
                  from lookup_contract((select distinct contract_id from __contracts where created_at_ix = 1))
                  order by template_fqn"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument" | anything | anything | anything | anything | anything | anything
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | anything | anything | anything | anything | anything | "id-alice"
          }
      ,
      funcTest("lookup queries: get exercises data by contract ID"):
        Given:
          upAndRunning
        Then:
          Postgres query {
            sql"""select template_fqn, choice_fqn, exercised_at_offset, exercised_effective_at, contract_id, exercise_event_id, argument->>'newLastName', result
                from lookup_exercises((select distinct contract_id from __contracts where created_at_ix = 2));"""
          } `returns` table {
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" |
              s"${nameRegistry.name}:NameRegistry:BirthCertificate:BirthCertificateNameChange" |
              anything |
              anything |
              anything |
              anything |
              "Bloggs" |
              anything
          }
      ,
      funcTest("summary queries: active Daml contracts counts at offset"):
        Given:
          upAndRunning
        Then:
          // Binds to `latest_offset()` value by default
          Postgres query {
            sql"""select * from summary_active();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 4
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 4
          }
        And:
          // ... or, can supply an offset explicitly
          Postgres query {
            sql"""select * from summary_active((select "offset" from __transactions where ix = 3));"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 2
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 2
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setLatest` 3
        And:
          Postgres.Session query {
            sql"""select * from summary_active();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 2
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 2
          }
      ,
      funcTest("summary queries: created Daml contracts counts in offset range"):
        Given:
          upAndRunning
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select * from summary_creates();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 9
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 9
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select * from summary_creates(
                        (select "offset" from __transactions where ix = 3),
                        (select "offset" from __transactions where ix = 7));"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 4
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 4
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 7
        And:
          Postgres.Session query {
            sql"""select * from summary_creates();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 4
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 4
          }
      ,
      funcTest("summary queries: archived Daml contracts counts in offset range"):
        Given:
          upAndRunning
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select * from summary_archives();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 5
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 5
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select * from summary_archives(
                        (select "offset" from __transactions where ix = 3),
                        (select "offset" from __transactions where ix = 7));"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 4
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 4
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 7
        And:
          Postgres.Session query {
            sql"""select * from summary_archives();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 4
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 4
          }
      ,
      funcTest("summary queries: transient Daml contracts counts in offset range"):
        Given:
          upAndRunning
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select * from summary_transients();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 5
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 5
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select * from summary_transients(
                      (select "offset" from __transactions where ix = 3),
                      (select "offset" from __transactions where ix = 7));"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 3
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 3
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 7
        And:
          Postgres.Session query {
            sql"""select * from summary_transients();"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument"        | "interface" | 3
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | "template"  | 3
          }
      ,
      funcTest("summary queries: Daml choice exercises counts in offset range"):
        Given:
          upAndRunning
        Then:
          // Binds to `[oldest_offset(), latest_offset()]` range by default
          Postgres query {
            sql"""select * from summary_exercises() order by template_fqn, choice_fqn;"""
          } `returns` table {
            s"${interfaces.name}:Interfaces:INameDocument" | s"${interfaces.name}:Interfaces:INameDocument:INameDocumentNameChange" | "INameDocumentNameChange" | true | 2
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:Archive" | "Archive" | true | 1
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:BirthCertificateNameChange" | "BirthCertificateNameChange" | true | 2
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:DoNothing" | "DoNothing" | false | 2
          }
        And:
          // ... or, can supply offset range explicitly
          Postgres query {
            sql"""select * from summary_exercises(
                        (select "offset" from __transactions where ix = 3),
                        (select "offset" from __transactions where ix = 5))
                      order by template_fqn, choice_fqn;"""
          } `returns` table {
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:Archive" | "Archive" | true | 1
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:BirthCertificateNameChange" | "BirthCertificateNameChange" | true | 1
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:DoNothing" | "DoNothing" | false | 1
          }
        When:
          Postgres.Session.init
        And:
          // ... or, implicitly through session
          Postgres.Session `setOldest` 3
        And:
          Postgres.Session `setLatest` 5
        And:
          Postgres.Session query {
            sql"""select * from summary_exercises() order by template_fqn, choice_fqn;"""
          } `returns` table {
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:Archive" | "Archive" | true | 1
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:BirthCertificateNameChange" | "BirthCertificateNameChange" | true | 1
            s"${nameRegistry.name}:NameRegistry:BirthCertificate" | s"${nameRegistry.name}:NameRegistry:BirthCertificate:DoNothing" | "DoNothing" | false | 1
          }
      ,
      funcTest("range queries exclude prior transaction when from_offset falls in an offset gap"):
        Given:
          upAndRunning

        val offsetAt5 = Capture[OffsetType]
        val offsetAt6 = Capture[OffsetType]
        Then:
          // Capture offsets at ix=5 and ix=6 before creating the gap
          Postgres query {
            sql"""select (select "offset" from __transactions where ix = 5),
                         (select "offset" from __transactions where ix = 6);"""
          } `returns` table { offsetAt5.capture | offsetAt6.capture }
        When:
          // Create a gap between ix=5 and ix=6 by shifting offsets from ix=6 onwards
          Postgres `shiftOffsets` (6L, 10L) `returns` anything
        Then:
          // Using the exact offset at ix=6 as from_offset correctly excludes Bill Myers (ix=5)
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from creates('NameRegistry:BirthCertificate',
                                 ${offsetAt6.get + 10L},
                                 latest_offset())
                    order by created_at_offset;"""
          } `returns` table {
            "id-bill" | "Bill" | "Taylor"
            "id-bill" | "Bill" | "Doe"
            "id-jane" | "Jane" | "Smith"
            "id-bill" | "Bill" | "Kirk"
            "id-jill" | "Jill" | "Brown"
          }
        // Using an offset in the gap: __nearest_ix_ceil rounds up to ix=6,
        // correctly excluding Bill Myers (ix=5) whose offset is before from_offset
        And:
          Postgres query {
            sql"""select payload->>'user_id', payload->>'firstName', payload->>'lastName'
                    from creates('NameRegistry:BirthCertificate',
                                 ${offsetAt5.get + 1L},
                                 latest_offset())
                    order by created_at_offset;"""
          } `returns` table {
            "id-bill" | "Bill" | "Taylor"
            "id-bill" | "Bill" | "Doe"
            "id-jane" | "Jane" | "Smith"
            "id-bill" | "Bill" | "Kirk"
            "id-jill" | "Jill" | "Brown"
          }
        And:
          // A range that stays entirely inside the synthetic offset gap should behave like an empty interval,
          // not fail while constructing the transient-summary range bounds.
          Postgres query {
            sql"""select * from summary_transients(
                         ${offsetAt5.get + 1L},
                         ${offsetAt6.get + 9L});"""
          } `returns` Table.empty
    )
  )
end QueryingSpec
