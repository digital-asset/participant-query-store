// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.upgrades

import com.digitalasset.scribe.SharedLedgerAndPostgresTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Database.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import zio.ZIO
import com.digitalasset.transcode.schema.packageName
import scala.language.{implicitConversions, postfixOps}

object FailedInterfaceViewsSpec extends SharedLedgerAndPostgresTest:
  private val issuer = Party("Issuer")
  private val owner  = Party("Owner")

  private val assetV1 = DamlSource(
    "InterfacesV1" -> """module InterfacesV1 where
                        |
                        |interface AssetV1
                        |  where
                        |    viewtype AssetView
                        |
                        |data AssetView = AssetView with issuer: Party
                        |  deriving (Eq, Ord, Show)
                        |""".stripMargin
  )

  private val assetV2 = DamlSource(
    "InterfacesV2" ->
      """module InterfacesV2 where
        |
        |interface AssetV2
        |  where
        |    viewtype AssetViewV2
        |
        |data AssetViewV2 = AssetViewV2 with issuer: Party, owner: Party
        |  deriving (Eq, Ord, Show)
        |""".stripMargin
  )

  // Creates a unique version of the package
  // Uniqueness is ensured by the suffix which is added to the name of the package
  private def createToken(suffix: String) = DamlSource(
    "Token" -> """module Token where
                 |
                 |import Daml.Script
                 |import DA.Functor (void)
                 |
                 |import InterfacesV1
                 |
                 |template Token
                 |  with
                 |    issuer: Party
                 |  where
                 |    signatory issuer
                 |
                 |    interface instance AssetV1 for Token where
                 |        view = AssetView with issuer = issuer
                 |
                 |create: Party -> Script ()
                 |create issuer = void do
                 |  submit issuer $ createCmd Token with issuer = issuer
                 |""".stripMargin
  ).withNameSuffix(suffix).dependsOn(assetV1)

  private def createTokenUpgrade(baseToken: DamlSource) = DamlSource(
    "Token" -> """module Token where
                 |
                 |import Daml.Script
                 |import DA.Functor (void)
                 |
                 |import InterfacesV1
                 |import InterfacesV2
                 |
                 |template Token
                 |  with
                 |    issuer: Party
                 |    owner: Optional Party
                 |  where
                 |    signatory issuer
                 |    observer case owner of
                 |      None -> [issuer]
                 |      Some o -> [o]
                 |
                 |    interface instance AssetV1 for Token where
                 |        view = error "deprecated"
                 |
                 |    interface instance AssetV2 for Token where
                 |        view = AssetViewV2
                 |          with
                 |            issuer = issuer
                 |            owner = case owner of
                 |              None -> issuer
                 |              Some o -> o
                 |
                 |create: (Party, Party) -> Script ()
                 |create parties = void do
                 |  let (issuer, owner) = parties
                 |  submit issuer $ createCmd Token with issuer = issuer, owner = Some owner
                 |""".stripMargin
  ).upgrades(baseToken).dependsOn(assetV1, assetV2)

  def testFailedView(acsStream: Boolean) =
    funcTest(
      s"Failed interface views are gracefully ignored - ${if acsStream then "ACS stream" else "updates stream"} handling"
    ) {
      val token        = createToken(if acsStream then "acs-stream" else "updates-stream")
      val tokenUpgrade = createTokenUpgrade(token)

      val tokenV1Dar = Capture[DeployedDar]
      val tokenV2Dar = Capture[DeployedDar]

      Given:
        (DamlSdk.dar(token) >+> DamlSdk.deploy)
          ++ DamlSdk.parties(issuer, owner) ++ Postgres.database

      And:
        tokenV1Dar.captureFromService
      And:
        DamlSdk.runScript("Token:create", issuer.id)

      When:
        DamlSdk.dar(tokenUpgrade) >+> DamlSdk.deploy >+> DamlSdk.runScript("Token:create", issuer.id <&> owner.id)
      And:
        tokenV2Dar.captureFromService

      And:
        Scribe.pipeline(
          "--pipeline-datasource=TransactionStream",
          s"--pipeline-ledger-start=${if acsStream then "Latest" else "Genesis"}",
          "--pipeline-ledger-stop=Latest"
        )

      lazy val assetV1PkgId = tokenV1Dar.get.dar.packageInfo
        .collectFirst {
          case (pkgName, _pkvVer, pkgId) if pkgName.packageName.contains("assetV1") => pkgId
        }
        .getOrElse(sys.error("unavailable"))

      lazy val assetV2PkgId = tokenV2Dar.get.dar.packageInfo
        .collectFirst {
          case (pkgName, _pkvVer, pkgId) if pkgName.packageName.contains("assetV2") => pkgId
        }
        .getOrElse(sys.error("unavailable"))

      lazy val tokenPkgIdV1 = tokenV1Dar.get.dar.packageId
      lazy val tokenPkgIdV2 = tokenV2Dar.get.dar.packageId

      val contract1Cid = Capture[String]
      val contract2Cid = Capture[String]

      val cid1_assetV2_payload = Capture[String]
      val cid1_token_payload   = Capture[String]
      val cid2_assetV2_payload = Capture[String]
      val cid2_token_payload   = Capture[String]

      lazy val expectedPayloadsTbl = table {
        tokenPkgIdV1 | s"${assetV2.name}:InterfacesV2:AssetV2" | "interface" | contract1Cid.capture | cid1_assetV2_payload.capture
        tokenPkgIdV1 | s"${token.name}:Token:Token" | "template" | contract1Cid.capture | cid1_token_payload.capture
        tokenPkgIdV2 | s"${assetV2.name}:InterfacesV2:AssetV2" | "interface" | contract2Cid.capture | cid2_assetV2_payload.capture
        tokenPkgIdV2 | s"${tokenUpgrade.name}:Token:Token" | "template" | contract2Cid.capture | cid2_token_payload.capture
      }

      if acsStream then
        // TODO: Replace this unordered assertion once we have a stable ordering for ACS-populated events.
        //       Reason for using it: There's no discriminator on which we can order the returned result so that they are stable for ACS-populated events.
        //       More generally this is due to the fact the created_at_ix, the creation offset and record time depend on how the events were ingested
        Expect:
          active(additionalColumns = Seq("payload")).map { tbl =>
            zio.test.assert(tbl.rows.toList)(matchesTableUnordered(expectedPayloadsTbl))
          }
      else
        Expect:
          active(additionalColumns = Seq("payload")) `returns` expectedPayloadsTbl atTheEndOfTheDay

      Expect:
        ZIO.from(cid1_assetV2_payload.get) `is` stringMatching(
          """\{"owner": "Issuer.*?", "issuer": "Issuer.*?"\}"""
        )

      And:
        ZIO.from(cid1_token_payload.get) `is` stringMatching("""\{"issuer": "Issuer.*?"\}""")

      And:
        ZIO.from(cid2_assetV2_payload.get) `is` stringMatching("""\{"owner": "Owner.*?", "issuer": "Issuer.*?"\}""")

      And:
        ZIO.from(cid2_token_payload.get) `is` stringMatching("""\{"owner": "Owner.*?", "issuer": "Issuer.*?"\}""")
    }

  def spec = suite("Interface Views")(
    testFailedView(acsStream = true),
    testFailedView(acsStream = false)
  )
