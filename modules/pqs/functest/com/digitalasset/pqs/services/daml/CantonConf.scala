// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.FTEnv
import com.digitalasset.pqs.services.inspectMaybe
import com.digitalasset.pqs.services.o11y.Collector
import com.digitalasset.pqs.services.oauth.OAuth
import org.semver4j.Semver
import os.{Path, Shellable}
import zio.ZIO

object CantonConf:
  val databaseNames: Seq[String]           = Seq("driver", "mediator", "sequencer", "participant")
  val maxRequestSize: Int                  = 30 * 1024 * 1024
  val participantPort: Int                 = 6865
  val participantAdmin                     = "participant_admin"
  val bootstrapCompleteMessage             = "=== Bootstrapping complete ==="
  val cantonAdditionalCmds: Seq[Shellable] = Seq("daemon")
  val cantonEnvVarMap: Map[String, String] =
    Map(
      "LOG_LEVEL_STDOUT" -> "INFO",
      "LOG_LEVEL_CANTON" -> "INFO",
      // Limit visible CPUs to prevent Canton from oversizing DB connection pools on high-CPU CI runners
      "JDK_JAVA_OPTIONS" -> "-XX:ActiveProcessorCount=4"
    )
  val user = 1001

  def load(): ZIO[FTEnv, Throwable, CantonConf] =
    for {
      cantonVersion   <- FTEnv.cantonVersion
      protocolVersion <- FTEnv.protocolVersion
      ver             <- ZIO.attempt(Semver.parse(cantonVersion))
      _               <- ZIO.logInfo(s"Using canton version $ver, major.minor ${ver.getMajor}.${ver.getMinor}")
    } yield CantonConf(cantonVersion, protocolVersion)

/** A set of values and auxiliary functions that differ between canton versions. Used to start canton container for
  * functest.
  */
final case class CantonConf(version: String, protocolVersion: Int):
  import CantonConf.*
  def cantonDockerImage: String = s"europe-docker.pkg.dev/da-images/public-all/docker/canton-base:$version"
  def oneParticipant(hostname: String, usingPostgres: Option[(String, Int)] = None) =
    def storage(name: String) = if usingPostgres.isDefined then Storage.Postgres(name) else Storage.Memory
    for (oauthInstance, collectorInstance, certFiles) <- commonSetup(hostname, usingPostgres = usingPostgres.isDefined)
    yield
      val config =
        s"""|${usingPostgres.fold("")((host, port) => postgresConfig(host, port))}
            |
            |canton {
            |  features {
            |    enable-preview-commands = yes
            |    enable-testing-commands = yes
            |  }
            |  parameters {
            |    manual-start = no
            |    non-standard-config = yes
            |    # Bumping because our topology state can get very large due to
            |    # a large number of participants.
            |    timeouts.processing.verify-active = 40.seconds
            |    timeouts.processing.slow-future-warn = 20.seconds
            |  }
            |
            |  # Bumping because our topology state can get very large due to
            |  # a large number of participants.
            |  monitoring.logging.delay-logging-threshold = 40.seconds
            |
            |  participants {
            |    ${participant("participant1", protocolVersion, storage = storage("participant"))}
            |  }
            |
            |  sequencers {
            |    ${sequencer("sequencer1", driverStorage = storage("driver"), sequencerStorage = storage("sequencer"))}
            |  }
            |
            |  mediators {
            |    ${mediator("mediator1", storage = storage("mediator"))}
            |  }
            |${collectorInstance.fold("")(monitoring)}
            |}
            |""".stripMargin
      val bootstrap =
        s"""|import com.digitalasset.canton.console.LocalInstanceReference
            |import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
            |import com.digitalasset.canton.version.ProtocolVersion
            |import cats.syntax.either._
            |import com.digitalasset.canton.config
            |
            |def main() = {
            |  nodes.local.start()
            |
            |  val synchronizerId = bootstrap.synchronizer(
            |    synchronizerName = "synchronizer1",
            |    sequencers = Seq(sequencer1),
            |    mediators = Seq(mediator1),
            |    synchronizerOwners = Seq(sequencer1),
            |    synchronizerThreshold = PositiveInt.one,
            |    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer)
            |  )
            |
            |  val initialReconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)
            |  sequencer1.topology.synchronizer_parameters
            |    .propose_update(synchronizerId.logical, _.update(reconciliationInterval = initialReconciliationInterval))
            |
            |  logger.info("=== connecting to synchronizer ===")
            |  participant1.synchronizers.connect_local(sequencer1, alias = "synchronizer1")
            |  utils.retry_until_true {
            |      participant1.synchronizers.active("synchronizer1")
            |  }
            |  logger.info("=== finished connecting to synchronizer ===")
            |
            |  // verify that the connection works
            |  participant1.health.ping(participant1)
            |
            |  logger.info("=== finishing participant bootstrap ===")
            |}
            |""".stripMargin
      certFiles ++ Seq(os.root / "app" / "app.conf" -> config, os.root / "app" / "bootstrap.sc" -> bootstrap)

  def twoParticipantsConfigOnly(pgHost: String, pgPort: Int, dbP1: String, dbP2: String): String =
    s"""|${postgresConfig(pgHost, pgPort)}
        |
        |canton {
        |  features {
        |    enable-preview-commands = yes
        |    enable-testing-commands = yes
        |  }
        |  parameters {
        |    manual-start = no
        |    non-standard-config = yes
        |    timeouts.processing.verify-active = 40.seconds
        |    timeouts.processing.slow-future-warn = 20.seconds
        |  }
        |
        |  monitoring.logging.delay-logging-threshold = 40.seconds
        |
        |  participants {
        |    ${participant("participant1", protocolVersion, 10014, 7865, Storage.Postgres(dbP1))}
        |    
        |    ${participant("participant2", protocolVersion, storage = Storage.Postgres(dbP2))}
        |  }
        |
        |  sequencers {
        |    ${sequencer("sequencer1")}
        |  }
        |
        |  mediators {
        |    ${mediator("mediator1")}
        |  }
        |}
        |""".stripMargin

  def twoSynchronizers(
      hostname: String,
      sync1: Synchronizer,
      sync2: Synchronizer
  ): ZIO[Docker, Throwable, Seq[(Path, String | Array[Byte])]] =
    for (oauthInstance, collectorInstance, certFiles) <- commonSetup(hostname, usingPostgres = false)
    yield
      val config =
        s"""|canton {
            |  features {
            |    enable-preview-commands = yes
            |    enable-testing-commands = yes
            |  }
            |  parameters {
            |    manual-start = no
            |    non-standard-config = yes
            |    timeouts.processing.verify-active = 40.seconds
            |    timeouts.processing.slow-future-warn = 20.seconds
            |  }
            |
            |  monitoring.logging.delay-logging-threshold = 40.seconds
            |
            |  participants {
            |    ${participant("participant1", protocolVersion)}
            |  }
            |
            |  sequencers {
            |    ${sequencer("sequencer1")}
            |    ${sequencer("sequencer2", 5010, 5011)}
            |  }
            |
            |  mediators {
            |    ${mediator("mediator1")}
            |    ${mediator("mediator2", 5012)}
            |  }
            |${collectorInstance.fold("")(monitoring)}
            |}
            |""".stripMargin
      val bootstrap =
        s"""|import com.digitalasset.canton.version.ProtocolVersion
            |import com.digitalasset.canton.config
            |
            |def main() = {
            |  nodes.local.start()
            |
            |  val synchronizer1Id = bootstrap.synchronizer(
            |    synchronizerName = "${sync1.name}",
            |    sequencers = Seq(sequencer1),
            |    mediators = Seq(mediator1),
            |    synchronizerOwners = Seq(sequencer1),
            |    synchronizerThreshold = PositiveInt.one,
            |    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer)
            |  )
            |  val synchronizer2Id = bootstrap.synchronizer(
            |    synchronizerName = "${sync2.name}",
            |    sequencers = Seq(sequencer2),
            |    mediators = Seq(mediator2),
            |    synchronizerOwners = Seq(sequencer2),
            |    synchronizerThreshold = PositiveInt.one,
            |    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer)
            |  )
            |
            |  val longReconciliationInterval = config.PositiveDurationSeconds.ofHours(24 * 365 * 10)
            |  sequencer1.topology.synchronizer_parameters
            |    .propose_update(synchronizer1Id.logical, _.update(reconciliationInterval = longReconciliationInterval))
            |  sequencer2.topology.synchronizer_parameters
            |    .propose_update(synchronizer2Id.logical, _.update(reconciliationInterval = longReconciliationInterval))
            |
            |  participant1.synchronizers.connect_local(sequencer1, alias = "${sync1.name}")
            |  participant1.topology.synchronizer_trust_certificates.propose(
            |    participant1,
            |    synchronizer1Id.logical,
            |    featureFlags = Seq(SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag.EnableMultiSynchronizer),
            |  )
            |
            |  participant1.synchronizers.connect_local(sequencer2, alias = "${sync2.name}")
            |  participant1.topology.synchronizer_trust_certificates.propose(
            |    participant1,
            |    synchronizer2Id.logical,
            |    featureFlags = Seq(SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag.EnableMultiSynchronizer),
            |  )
            |
            |  utils.retry_until_true { participant1.synchronizers.active("${sync1.name}") }
            |  utils.retry_until_true { participant1.synchronizers.active("${sync2.name}") }
            |  participant1.health.ping(participant1)
            |}
            |""".stripMargin
      certFiles ++ Seq(
        os.root / "app" / "app.conf"     -> config,
        os.root / "app" / "bootstrap.sc" -> bootstrap
      )

  private def postgresConfig(host: String, port: Int) =
    s"""|_storage {
        |  type = postgres
        |  config {
        |    dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
        |    properties {
        |      serverName = "$host"
        |      portNumber = $port
        |      user = postgres
        |      password = postgres
        |      ssl = true
        |      sslmode = "verify-ca"
        |      sslrootcert = "/tls/root-ca.crt"
        |      sslcert = "/tls/pg-client.crt"
        |      sslkey = "/tls/pg-client.der"
        |    }
        |  }
        |  parameters.migrate-and-start = yes
        |}""".stripMargin

  /** Generate all the certificates needed to set up env, oauth instance and otel collector instance if configured.
    */
  private def commonSetup(
      hostname: String,
      usingPostgres: Boolean
  ): ZIO[
    Docker,
    Throwable,
    (Option[Service[OAuth.Instance]], Option[Service[Collector.Instance]], Seq[(Path, String | Array[Byte])])
  ] = for
    ca                <- Docker.certificateAuthority
    participantCert   <- ca.generate("participant", Seq(hostname, "localhost", "127.0.0.1", "0.0.0.0"))
    adminCert         <- ca.generate("participant", Seq(hostname, "127.0.0.1"))
    domainCert        <- ca.generate("participant", Seq(hostname, "127.0.0.1"))
    collectorInstance <- inspectMaybe[Service[Collector.Instance]]
    oauthInstance     <- inspectMaybe[Service[OAuth.Instance]]
    oAuthCertOpt <- ZIO.whenCase(oauthInstance) {
      case Some(oauth) => oauth.getFileContents(os.root / "data" / "oauth-certificate.crt")
    }
    postgresCertOpt <- ZIO.when(usingPostgres)(ca.generate("postgresclient"))
  yield (
    oauthInstance,
    collectorInstance,
    Seq(
      os.root / "tls" / "root-ca.crt"      -> ca.certificate.crt,
      os.root / "tls" / "participant.pem"  -> participantCert.certificate.pem,
      os.root / "tls" / "participant.crt"  -> participantCert.certificate.crt,
      os.root / "tls" / "admin-client.pem" -> adminCert.certificate.pem,
      os.root / "tls" / "admin-client.crt" -> adminCert.certificate.crt,
      os.root / "tls" / "domain.pem"       -> domainCert.certificate.pem,
      os.root / "tls" / "domain.crt"       -> domainCert.certificate.crt
    ) ++ oAuthCertOpt.fold(Seq.empty) { cert => Seq(os.root / "data" / "oauth-certificate.crt" -> cert) }
      ++ postgresCertOpt.fold(Seq.empty) { cert =>
        Seq(
          os.root / "tls" / "pg-client.crt" -> cert.certificate.crt,
          os.root / "tls" / "pg-client.der" -> cert.certificate.der
        )
      }
  )

  private def oauthCantonConfig(oauthInstance: Option[Service[OAuth.Instance]]): String =
    if oauthInstance.isDefined then s"""|        auth-services = [{
                                        |          type = jwt-rs-256-crt
                                        |          certificate = "/data/oauth-certificate.crt"
                                        |        }]
                                        |""".stripMargin
    else ""

  private def participant(
      name: String,
      protocolVersion: Int,
      adminApiPort: Int = 1012,
      ledgerApiPort: Int = participantPort,
      storage: Storage = Storage.Memory
  ): String =
    s"""|$name {
        |  $storage
        |  admin-api {
        |    address = "0.0.0.0"
        |    port = $adminApiPort
        |  }
        |  init {
        |    ledger-api.max-deduplication-duration = 0s
        |  }
        |  http-ledger-api.enabled = false
        |  ledger-api {
        |    address = "0.0.0.0"
        |    port = $ledgerApiPort
        |    tls {
        |      cert-chain-file = "/tls/participant.crt"
        |      private-key-file = "/tls/participant.pem"
        |      trust-collection-file = "/tls/root-ca.crt"
        |      client-auth {
        |        type = require
        |        admin-client {
        |          cert-chain-file = "/tls/admin-client.crt"
        |          private-key-file = "/tls/admin-client.pem"
        |        }
        |      }
        |    }
        |  }
        |  parameters {
        |    initial-protocol-version = $protocolVersion
        |    minimum-protocol-version = $protocolVersion
        |  }
        |  topology.broadcast-batch-size = 1
        |}
        |""".stripMargin

  private def sequencer(
      name: String,
      publicApiPort: Int = 5008,
      adminApiPort: Int = 5009,
      driverStorage: Storage = Storage.Memory,
      sequencerStorage: Storage = Storage.Memory
  ): String =
    s"""|$name {
        |  $driverStorage
        |  sequencer {
        |    config {
        |      $sequencerStorage
        |    }
        |    type = "reference"
        |  }
        |  public-api {
        |    address = "0.0.0.0"
        |    port = $publicApiPort
        |  }
        |  admin-api {
        |    address = "0.0.0.0"
        |    port = $adminApiPort
        |  }
        |}
        |""".stripMargin

  private def mediator(name: String, adminApiPort: Int = 5007, storage: Storage = Storage.Memory): String =
    s"""|$name {
        |  $storage
        |  admin-api {
        |    address = "0.0.0.0"
        |    port = $adminApiPort
        |  }
        |}
        |""".stripMargin

  private def monitoring(collector: Service[Collector.Instance]): String =
    s"""|monitoring {
        |  tracing {
        |    propagation = enabled
        |    tracer {
        |      exporter {
        |        type = otlp
        |        address = ${collector.container.hostName}
        |        port = ${Collector.Instance.otlp}
        |      }
        |      sampler {
        |        type = always-on
        |        parent-based = true
        |      }
        |    }
        |  }
        |}
        |""".stripMargin

  private enum Storage:
    case Memory
    case Postgres(db: String)

    override def toString: String = this match
      case Memory => "storage.type = memory"
      case Postgres(db) =>
        s"""|storage = $${_storage}
            |storage.config.properties.databaseName = "$db"""".stripMargin

end CantonConf
