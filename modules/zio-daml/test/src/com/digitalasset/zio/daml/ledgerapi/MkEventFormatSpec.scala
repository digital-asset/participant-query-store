// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.transaction_filter.*
import com.digitalasset.canonical.UserRight.AsAnyParty
import com.digitalasset.canonical.{ContractFilter, MetadataFilter, Party, UserRight}
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml.KnownEntityIdentifiers
import zio.test.*

/** Verifies the filter shapes produced by [[mkEventFormat]].
  *
  * The suite covers the 2 x 3 x 2 matrix formed by:
  *
  *   - contract filter: AcceptAll vs Selective
  *   - metadata filter: AcceptAll vs RejectAll vs Selective
  *   - schema: with interfaces vs without interfaces
  *
  * The cases are grouped by the two production branches:
  *
  *   - `AcceptAll` contracts keeps the subscription open to new packages with a [[WildcardFilter]].
  *   - selective contracts produce closed subscriptions via [[TemplateFilter]] and [[InterfaceFilter]].
  */
object MkEventFormatSpec extends ZIOSpecDefault:

  private val alice: Party = Party("Alice")
  private val bob: Party   = Party("Bob")

  private val interface1 =
    Identifier(
      PackageId("pkg1"),
      PackageName("Interfaces"),
      PackageVersion("1.0.0"),
      ModuleName("Interfaces"),
      EntityName("IInterface1")
    )
  private val interface2 =
    Identifier(
      PackageId("pkg1"),
      PackageName("Interfaces"),
      PackageVersion("1.0.0"),
      ModuleName("Interfaces"),
      EntityName("IInterface2")
    )
  private val template1 =
    Identifier(
      PackageId("pkg2"),
      PackageName("Sample"),
      PackageVersion("1.0.0"),
      ModuleName("Sample"),
      EntityName("TTemplate1")
    )
  private val template2 =
    Identifier(
      PackageId("pkg2"),
      PackageName("Sample"),
      PackageVersion("1.0.0"),
      ModuleName("Sample"),
      EntityName("TTemplate2")
    )

  /** Schema with interfaces:
    *
    *   - TTemplate1 implements IInterface1 + IInterface2;
    *   - TTemplate2 has no interfaces.
    */
  private val schemaWithInterfaces: Seq[Template[Unit]] = Seq(
    mkInterface(interface1),
    mkInterface(interface2),
    mkTemplate(template1, Seq(interface1, interface2)),
    mkTemplate(template2)
  )

  /** Schema without interfaces: TTemplate1 and TTemplate2 are plain templates. */
  private val schemaWithoutInterfaces: Seq[Template[Unit]] = Seq(
    mkTemplate(template1),
    mkTemplate(template2)
  )

  private val acceptAllContracts: ContractFilter = ContractFilter(IdentifierFilter.AcceptAll)
  private val acceptAllMetadata: MetadataFilter  = MetadataFilter(IdentifierFilter.AcceptAll)
  private val rejectAllMetadata: MetadataFilter  = MetadataFilter(IdentifierFilter.RejectAll)

  def spec: Spec[Any, Nothing] = suite("mkEventFormat")(
    suite("IF branch: AcceptAll contracts (open subscription)")(
      test("Case 1: AcceptAll + AcceptAll metadata, no interfaces") {
        val knownIds = new KnownEntityIdentifiers(schemaWithoutInterfaces, acceptAllContracts, acceptAllMetadata)

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.WildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = true)
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 2: AcceptAll + AcceptAll metadata, with interfaces") {
        val knownIds = new KnownEntityIdentifiers(schemaWithInterfaces, acceptAllContracts, acceptAllMetadata)

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.WildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = true)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface1.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = true
                      )
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface2.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = true
                      )
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 3: AcceptAll + RejectAll metadata, no interfaces") {
        val knownIds = new KnownEntityIdentifiers(schemaWithoutInterfaces, acceptAllContracts, rejectAllMetadata)

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.WildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = false)
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 4: AcceptAll + RejectAll metadata, with interfaces") {
        val knownIds = new KnownEntityIdentifiers(schemaWithInterfaces, acceptAllContracts, rejectAllMetadata)

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.WildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = false)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface1.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = false
                      )
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface2.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = false
                      )
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 5: AcceptAll + Selective metadata (TTemplate2), no interfaces") {
        val knownIds =
          new KnownEntityIdentifiers(schemaWithoutInterfaces, acceptAllContracts, selectiveMetadata(template2))

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.WildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = false)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template2.toRefId), includeCreatedEventBlob = true)
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 6: AcceptAll + Selective metadata (IInterface2 | TTemplate2), with interfaces") {
        val knownIds =
          new KnownEntityIdentifiers(schemaWithInterfaces, acceptAllContracts, selectiveMetadata(interface2, template2))

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.WildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = false)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template2.toRefId), includeCreatedEventBlob = true)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface1.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = false
                      )
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface2.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = true
                      )
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      }
    ),
    suite("ELSE branch: Selective contracts (closed subscription)")(
      test("Case 7: Selective (TTemplate1) + AcceptAll metadata, no interfaces") {
        val knownIds =
          new KnownEntityIdentifiers(schemaWithoutInterfaces, selectiveContracts(template1), acceptAllMetadata)

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template1.toRefId), includeCreatedEventBlob = true)
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 8: Selective (TTemplate1 | IInterface1 | IInterface2) + AcceptAll metadata, with interfaces") {
        val knownIds = new KnownEntityIdentifiers(
          schemaWithInterfaces,
          selectiveContracts(template1, interface1, interface2),
          acceptAllMetadata
        )

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template1.toRefId), includeCreatedEventBlob = true)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface1.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = true
                      )
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface2.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = true
                      )
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 9: Selective (TTemplate1) + RejectAll metadata, no interfaces") {
        val knownIds =
          new KnownEntityIdentifiers(schemaWithoutInterfaces, selectiveContracts(template1), rejectAllMetadata)

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template1.toRefId), includeCreatedEventBlob = false)
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 10: Selective (TTemplate1 | IInterface1 | IInterface2) + RejectAll metadata, with interfaces") {
        val knownIds = new KnownEntityIdentifiers(
          schemaWithInterfaces,
          selectiveContracts(template1, interface1, interface2),
          rejectAllMetadata
        )

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template1.toRefId), includeCreatedEventBlob = false)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface1.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = false
                      )
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface2.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = false
                      )
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test("Case 11: Selective (TTemplate1) + Selective metadata (TTemplate1), no interfaces") {
        val knownIds = new KnownEntityIdentifiers(
          schemaWithoutInterfaces,
          selectiveContracts(template1),
          selectiveMetadata(template1)
        )

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template1.toRefId), includeCreatedEventBlob = true)
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      },
      test(
        "Case 12: Selective (TTemplate1 | IInterface1 | IInterface2) + Selective metadata (IInterface2), with interfaces"
      ) {
        val knownIds = new KnownEntityIdentifiers(
          schemaWithInterfaces,
          selectiveContracts(template1, interface1, interface2),
          selectiveMetadata(interface2)
        )

        assertTrue(
          mkEventFormat(AsAnyParty, knownIds) == EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(
              Filters(
                Seq(
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.TemplateFilter(
                      TemplateFilter(templateId = Some(template1.toRefId), includeCreatedEventBlob = false)
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface1.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = false
                      )
                    )
                  ),
                  CumulativeFilter(
                    CumulativeFilter.IdentifierFilter.InterfaceFilter(
                      InterfaceFilter(
                        interfaceId = Some(interface2.toRefId),
                        includeInterfaceView = true,
                        includeCreatedEventBlob = true
                      )
                    )
                  )
                )
              )
            ),
            verbose = false
          )
        )
      }
    ),
    suite("UserRight wiring")(
      test("AsParties populates filtersByParty for every party and leaves filtersForAnyParty empty") {
        val userRight = UserRight.AsParties(Set(alice, bob))
        val knownIds =
          new KnownEntityIdentifiers(schemaWithInterfaces, acceptAllContracts, selectiveMetadata(interface2, template2))

        val expectedFilters = Filters(
          Seq(
            CumulativeFilter(
              CumulativeFilter.IdentifierFilter.WildcardFilter(
                WildcardFilter(includeCreatedEventBlob = false)
              )
            ),
            CumulativeFilter(
              CumulativeFilter.IdentifierFilter.TemplateFilter(
                TemplateFilter(templateId = Some(template2.toRefId), includeCreatedEventBlob = true)
              )
            ),
            CumulativeFilter(
              CumulativeFilter.IdentifierFilter.InterfaceFilter(
                InterfaceFilter(
                  interfaceId = Some(interface1.toRefId),
                  includeInterfaceView = true,
                  includeCreatedEventBlob = false
                )
              )
            ),
            CumulativeFilter(
              CumulativeFilter.IdentifierFilter.InterfaceFilter(
                InterfaceFilter(
                  interfaceId = Some(interface2.toRefId),
                  includeInterfaceView = true,
                  includeCreatedEventBlob = true
                )
              )
            )
          )
        )

        assertTrue(
          mkEventFormat(userRight, knownIds) == EventFormat(
            filtersByParty = Map(
              alice.toString -> expectedFilters,
              bob.toString   -> expectedFilters
            ),
            filtersForAnyParty = None,
            verbose = false
          )
        )
      }
    )
  )

  private def mkTemplate(id: Identifier, implements: Seq[Identifier] = Seq.empty): Template[Unit] =
    Template[Unit](
      templateId = id,
      payload = (),
      key = None,
      isInterface = false,
      implements = implements,
      choices = Seq.empty
    )

  private def mkInterface(id: Identifier): Template[Unit] =
    Template[Unit](
      templateId = id,
      payload = (),
      key = None,
      isInterface = true,
      implements = Seq.empty,
      choices = Seq.empty
    )

  private def selectiveContracts(ids: Identifier*): ContractFilter =
    ContractFilter(IdentifierFilter.assertFromString(ids.map(filterName).mkString(" | ")))

  private def selectiveMetadata(ids: Identifier*): MetadataFilter =
    MetadataFilter(IdentifierFilter.assertFromString(ids.map(filterName).mkString(" | ")))

  private def filterName(id: Identifier): String = s"${id.moduleName}.${id.entityName}"
