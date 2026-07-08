// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.features.upgrades

import com.digitalasset.scribe.docker.{Docker, Service}
import com.digitalasset.scribe.functest.{FTEnv, FuncTestStandalone}
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.functest.table.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.daml.DamlSdk.{onlyCantonVersion, onlyPostgresVersion}
import com.digitalasset.scribe.services.postgres.*
import com.digitalasset.scribe.services.scribe.{Pipeline, Scribe}
import zio.*
import zio.jdbc.SqlFragment.Segment.Syntax
import zio.jdbc.sqlInterpolator
import zio.test.*

import scala.language.implicitConversions

/** This test must remain standalone because it starts a ledger with 2 participants and a custom bootstrap script.
  */
object RpidTwoParticipantSpec extends FuncTestStandalone:

  override protected def layerTimeout = 10.minutes

  private val packageV1 = DamlSource(
    "RpidTest" ->
      """module RpidTest where
        |
        |template SimpleContract
        |  with
        |    owner : Party
        |  where
        |    signatory owner
        |    choice Consume : ()
        |      controller owner
        |      do pure ()
        |""".stripMargin
  )

  private val packageV2 = DamlSource(
    "RpidTest" ->
      """module RpidTest where
        |
        |import Daml.Script
        |
        |template SimpleContract
        |  with
        |    owner : Party
        |    description : Optional Text
        |  where
        |    signatory owner
        |    choice Consume : ()
        |      controller owner
        |      do pure ()
        |
        |create : Party -> Script ()
        |create p = do
        |  _ <- submit p $ createCmd SimpleContract with owner = p, description = None
        |  pure ()
        |
        |consumeAll : Party -> Script ()
        |consumeAll p = do
        |  contracts <- query @SimpleContract p
        |  mapA (\(cid, _) -> submit p $ exerciseCmd cid Consume) contracts
        |  pure ()
        |""".stripMargin
  ).upgrades(packageV1)

  private val alice = Party("Alice")

  def spec = suite("RpidTwoParticipantSpec")(
    funcTest("creation_package_id differs from representative_package_id in two-participant setup"):
      lazy val v1Dar = Capture[DarFile]
      lazy val v2Dar = Capture[DarFile]
      Given:
        Postgres.instance
      And:
        DamlSdk.dar(packageV1)
      And:
        v1Dar.captureFromService
      And:
        DamlSdk.dar(packageV2)
      And:
        v2Dar.captureFromService
      And:
        cantonParticipantWithACSImportContract(v1Dar.get, v2Dar.get) ++ Postgres.database
      When:
        Scribe.pipeline(
          "--pipeline-datasource=TransactionStream",
          "--pipeline-filter-parties=*",
          s"--pipeline-filter-contracts=${Pipeline.allContractsWithoutAdminWorkflows}"
        )
      And:
        Scribe `hasProcessedAtLeastTransactions` 1
      And:
        DamlSdk.runScript("RpidTest:create", alice.id)
      And:
        Scribe `hasProcessedAtLeastTransactions` 2
      Expect:
        apiCreates() `returns` table {
          "package_id"        | "creation_package_id"
          ---                 | ---
          v2Dar.get.packageId | v1Dar.get.packageId
          v2Dar.get.packageId | v2Dar.get.packageId
        }
      And:
        apiActive() `returns` table {
          "package_id"        | "creation_package_id"
          ---                 | ---
          v2Dar.get.packageId | v1Dar.get.packageId
          v2Dar.get.packageId | v2Dar.get.packageId
        }
      And:
        storageContracts() `returns` table {
          "representative_package_id" | "creation_package_id" | "status"
          ---                         | ---                   | ---
          v2Dar.get.packageId         | v1Dar.get.packageId   | "active"
          v2Dar.get.packageId         | null                  | "active"
        }
      When:
        DamlSdk.runScript("RpidTest:consumeAll", alice.id)
      And:
        Scribe `hasProcessedAtLeastTransactions` 4
      Expect:
        apiArchives() `returns` table {
          "package_id"        | "creation_package_id"
          ---                 | ---
          v2Dar.get.packageId | v1Dar.get.packageId
          v2Dar.get.packageId | v2Dar.get.packageId
        }
      And:
        apiActive() `returns` Table.empty
      And:
        storageContracts() `returns` table {
          "representative_package_id" | "creation_package_id" | "status"
          ---                         | ---                   | ---
          v2Dar.get.packageId         | v1Dar.get.packageId   | "archived"
          v2Dar.get.packageId         | null                  | "archived"
        }
  ) @@ onlyPostgresVersion(">=14") @@ onlyCantonVersion(">=3.5")

  /** Two-participant Canton layer with Postgres storage (required for ACS import).
    *
    * Uses the captured v1 and v2 DARs, generates two-participant HOCON config + bootstrap script, and starts Canton.
    * Participant2 is on port 6865 (Scribe-facing), participant1 on port 7865 (internal only).
    *
    * The bootstrap script uses canonical offline party replication:
    *   - Creates the synchronizer and connects both participants
    *   - Uploads v1 to participant1, v2 to participant2
    *   - Allocates Alice on participant1
    *   - Creates a contract on participant1 using v1 template (before replication)
    *   - Replicates Alice to participant2 with onboarding flag (target proposes, disconnect, source proposes)
    *   - Exports ACS via parties.export_party_acs, imports via parties.import_party_acs
    *   - Reconnects participant2 and clears onboarding flag
    *
    * This results in participant2 having a contract with v2 as representative package and v1 as creation package.
    */
  private def cantonParticipantWithACSImportContract(
      v1Dar: DarFile,
      v2Dar: DarFile
  ): RLayer[FTEnv & Docker & Postgres, Service[Ledger] & DeployedDar & Parties] =
    ZLayer
      .fromZIO(
        for
          ftEnv <- ZIO.service[FTEnv]
          pg    <- ZIO.service[Postgres]
          pgHostname = pg.container.hostName
          cnt <- Docker.share("rpid_canton_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
          hostname = s"rpid-canton-$cnt"
          dbP1     = s"canton_p1_$cnt"
          dbP2     = s"canton_p2_$cnt"
          dbSeq    = s"canton_seq_$cnt"
          dbMed    = s"canton_med_$cnt"
          _ <- pg.adminDatabase.transaction(
            sql"""CREATE DATABASE "${Syntax(dbP1)}"""".execute *>
              sql"""CREATE DATABASE "${Syntax(dbP2)}"""".execute *>
              sql"""CREATE DATABASE "${Syntax(dbSeq)}"""".execute *>
              sql"""CREATE DATABASE "${Syntax(dbMed)}"""".execute
          )
          ca              <- Docker.certificateAuthority
          participantCert <- ca.generate("participant", Seq(hostname, "localhost", "127.0.0.1", "0.0.0.0"))
          adminCert       <- ca.generate("participant", Seq(hostname, "127.0.0.1"))
          domainCert      <- ca.generate("participant", Seq(hostname, "127.0.0.1"))
          pgClientCert    <- ca.generate("postgresclient")
          certFiles = Seq(
            os.root / "tls" / "root-ca.crt"      -> ca.certificate.crt,
            os.root / "tls" / "participant.pem"  -> participantCert.certificate.pem,
            os.root / "tls" / "participant.crt"  -> participantCert.certificate.crt,
            os.root / "tls" / "admin-client.pem" -> adminCert.certificate.pem,
            os.root / "tls" / "admin-client.crt" -> adminCert.certificate.crt,
            os.root / "tls" / "domain.pem"       -> domainCert.certificate.pem,
            os.root / "tls" / "domain.crt"       -> domainCert.certificate.crt,
            os.root / "tls" / "pg-client.crt"    -> pgClientCert.certificate.crt,
            os.root / "tls" / "pg-client.der"    -> pgClientCert.certificate.der
          )
          cantonConf <- CantonConf()
          appConf = cantonConf.twoParticipantConfig(
            ftEnv.cantonProtocolVersion,
            pgHostname,
            Postgres.port,
            dbP1,
            dbP2,
            dbSeq,
            dbMed
          )
          bootstrapSc = bootstrapScript("rpiddomain")
          prepopulateFiles = certFiles ++ Seq(
            os.root / "app" / "app.conf"     -> appConf,
            os.root / "app" / "bootstrap.sc" -> bootstrapSc,
            os.root / "dars" / "v1.dar"      -> v1Dar.darBytes,
            os.root / "dars" / "v2.dar"      -> v2Dar.darBytes
          )
          svc = Docker
            .service[Ledger](
              image = cantonConf.cantonDockerImage,
              exposePorts = Set(Ledger.participantPort),
              prepopulateFiles = prepopulateFiles,
              hostname = Some(hostname),
              env = cantonConf.cantonEnvVarMap,
              user = Some(1001),
              suppressOutput = !ftEnv.showCantonLogs
            )("daemon")
            .tap(_.get.blockUntilStdOut(_.contains("=== Bootstrapping complete ===")))
        yield svc
      )
      .flatten >+> (DamlSdk.allocatedParties(alice) ++ ZLayer.succeed(DeployedDar(v2Dar)))

  private def storageContracts() = Postgres `query`
    sql"""SELECT
            p.id as representative_package_id,
            c.creation_package_id,
            CASE WHEN c.archived_at_ix IS NULL THEN 'active' ELSE 'archived' END as status
          FROM __contracts c
          JOIN __packages p ON c.package_pk = p.pk
          ORDER BY created_at_ix"""

  private def apiActive() = Postgres `query`
    sql"""SELECT
            package_id,
            creation_package_id
          FROM active()
          ORDER BY created_at_ix"""

  private def apiCreates() = Postgres `query`
    sql"""SELECT
            package_id,
            creation_package_id
          FROM creates()
          ORDER BY created_at_ix"""

  private def apiArchives() = Postgres `query`
    sql"""SELECT
            package_id,
            creation_package_id
          FROM archives()
          ORDER BY created_at_ix"""

  private def bootstrapScript(synchronizer: String): String =
    s"""import com.digitalasset.canton.version.ProtocolVersion
       |import com.digitalasset.canton.config
       |
       |def main() = {
       |  nodes.local.start()
       |
       |  // 1. Create synchronizer
       |  val synchronizerId = bootstrap.synchronizer(
       |    synchronizerName = "$synchronizer",
       |    sequencers = Seq(sequencer1),
       |    mediators = Seq(mediator1),
       |    synchronizerOwners = Seq(sequencer1),
       |    synchronizerThreshold = PositiveInt.one,
       |    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer)
       |  )
       |
       |  // Set reconciliation interval to 10 years to avoid ACS commitment mismatch warnings
       |  val longReconciliationInterval = config.PositiveDurationSeconds.ofHours(24 * 365 * 10)
       |  sequencer1.topology.synchronizer_parameters
       |    .propose_update(synchronizerId.logical, _.update(reconciliationInterval = longReconciliationInterval))
       |
       |  // 2. Connect both participants
       |  logger.info("=== connecting participants to synchronizer ===")
       |  participant1.synchronizers.connect_local(sequencer1, alias = "$synchronizer")
       |  participant2.synchronizers.connect_local(sequencer1, alias = "$synchronizer")
       |  utils.retry_until_true { participant1.synchronizers.active("$synchronizer") }
       |  utils.retry_until_true { participant2.synchronizers.active("$synchronizer") }
       |
       |  // 3. Upload DARs: v1 to participant1, v2 to participant2
       |  logger.info("=== uploading DARs ===")
       |  val v1PkgId = participant1.dars.upload("/dars/v1.dar")
       |  participant2.dars.upload("/dars/v2.dar")
       |
       |  // 4. Allocate Alice on participant1
       |  val alice = participant1.parties.enable("Alice")
       |
       |  // 5. Create contract on participant1 using v1 template (before replication)
       |  logger.info("=== creating contract on participant1 ===")
       |  participant1.ledger_api.commands.submit(
       |    actAs = Seq(alice),
       |    commands = Seq(
       |      ledger_api_utils.create(
       |        v1PkgId,
       |        "RpidTest",
       |        "SimpleContract",
       |        Map[String, Any](
       |          "owner" -> alice,
       |        ),
       |      )
       |    ),
       |  )
       |
       |  // 6. Target (P2) authorizes hosting Alice with onboarding flag
       |  logger.info("=== replicating party to participant2 ===")
       |  participant2.topology.party_to_participant_mappings.propose_delta(
       |    party = alice,
       |    adds = List((participant2.id, ParticipantPermission.Submission)),
       |    store = synchronizerId,
       |    requiresPartyToBeOnboarded = true,
       |  )
       |
       |  // 7. Disconnect target
       |  participant2.synchronizers.disconnect_all()
       |
       |  // 8. Record source ledger end
       |  val sourceLedgerEnd = participant1.ledger_api.state.end()
       |
       |  // 9. Source (P1) authorizes hosting Alice on P2 with onboarding flag
       |  participant1.topology.party_to_participant_mappings.propose_delta(
       |    party = alice,
       |    adds = List((participant2.id, ParticipantPermission.Submission)),
       |    store = synchronizerId,
       |    requiresPartyToBeOnboarded = true,
       |  )
       |
       |  // 10. Export ACS from source
       |  logger.info("=== exporting ACS from participant1 ===")
       |  participant1.parties.export_party_acs(
       |    party = alice,
       |    synchronizerId = synchronizerId.logical,
       |    targetParticipantId = participant2.id,
       |    beginOffsetExclusive = sourceLedgerEnd,
       |    exportFilePath = "/app/acs-export.gz",
       |  )
       |
       |  // 11. Import ACS on target (P2 already disconnected from step 7)
       |  logger.info("=== importing ACS into participant2 ===")
       |  participant2.parties.import_party_acs(
       |    importFilePath = "/app/acs-export.gz",
       |    synchronizerId = synchronizerId.logical,
       |  )
       |
       |  // 12. Capture target ledger end (after import, before reconnect)
       |  val targetLedgerEnd = participant2.ledger_api.state.end()
       |
       |  // 13. Reconnect target
       |  participant2.synchronizers.reconnect_all()
       |  utils.retry_until_true { participant2.synchronizers.active("$synchronizer") }
       |
       |  // 14. Clear onboarding flag (poll until cleared)
       |  logger.info("=== clearing onboarding flag ===")
       |  utils.retry_until_true(timeout = 2.minutes, maxWaitPeriod = 1.minutes) {
       |    participant2.parties.clear_party_onboarding_flag(alice, synchronizerId.logical, targetLedgerEnd) match {
       |      case FlagSet(_) => false
       |      case FlagNotSet => true
       |    }
       |  }
       |
       |  // 15. Remove Alice from participant1 so she's only hosted on participant2
       |  logger.info("=== removing Alice from participant1 ===")
       |  participant1.topology.party_to_participant_mappings.propose_delta(
       |    party = alice,
       |    removes = List(participant1.id),
       |    store = synchronizerId,
       |    forceFlags = ForceFlags(ForceFlag.DisablePartyWithActiveContracts),
       |  )
       |  utils.retry_until_true {
       |    !participant1.parties.list(filterParticipant = participant1.id.filterString).exists(_.party == alice)
       |  }
       |
       |  // 16. Wait for party to be visible on P2
       |  logger.info("=== waiting for party activation ===")
       |  utils.retry_until_true {
       |    participant2.parties.list(filterParticipant = participant2.id.filterString).exists(_.party == alice)
       |  }
       |
       |  logger.info("=== Bootstrapping complete ===")
       |}
       |""".stripMargin

end RpidTwoParticipantSpec
