// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.FTEnv
import com.digitalasset.pqs.services.o11y.Collector
import com.digitalasset.pqs.services.oauth.OAuth
import org.semver4j.Semver
import os.{Path, Shellable}
import zio.{ZIO, ZLayer}
import zio.ZIO.{logInfo, whenCase}

/** A set of values and auxiliary functions that differ between canton versions. Used to start canton container for
  * functest.
  */
trait CantonConf:
  val version: String

  val cantonDockerImage: String =
    s"europe-docker.pkg.dev/da-images/public-all/docker/canton-base:$version"

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

  def apply(
      hostname: String,
      participantPort: Int,
      domain: String,
      publicApiPort: Int,
      protocolVersion: Int,
      maxRequestSize: Long = 4 * 1024 * 1024
  ): ZIO[Docker, Throwable, Seq[(Path, String | Array[Byte])]]

  def twoParticipantConfig(
      protocolVersion: Int,
      pgHost: String,
      pgPort: Int,
      dbP1: String,
      dbP2: String
  ): String

  def twoSynchronizerConfig(
      hostname: String,
      protocolVersion: Int
  ): ZIO[Docker, Throwable, Seq[(Path, String | Array[Byte])]]

object CantonConf:
  def apply(): ZIO[FTEnv, Throwable, CantonConf] =
    for {
      cantonVersion <- FTEnv.cantonVersion
      ver           <- ZIO.attempt(Semver.parse(cantonVersion))
      _             <- logInfo(s"Using canton version $ver, major.minor ${ver.getMajor}.${ver.getMinor}")
    } yield (ver.getMajor, ver.getMinor) match {
      case (3, 4) => Canton34(cantonVersion)
      case (3, 5) => Canton35Plus(cantonVersion)
      case (3, 6) => Canton35Plus(cantonVersion)
      case _      => sys.error(s"unsupported Canton version $ver")
    }

  val layer: ZLayer[FTEnv, Throwable, CantonConf] =
    ZLayer.fromZIO(CantonConf())

  /** Generate all the certificates needed to set up env, oauth instance and otel collector instance if configured.
    */
  private def commonSetup(
      hostname: String
  ): ZIO[
    Docker,
    Throwable,
    (Option[Service[OAuth.Instance]], Option[Service[Collector.Instance]], Seq[(Path, String | Array[Byte])])
  ] = for
    ca                <- Docker.certificateAuthority
    participantCert   <- ca.generate("participant", Seq(hostname, "localhost", "127.0.0.1", "0.0.0.0"))
    adminCert         <- ca.generate("participant", Seq(hostname, "127.0.0.1"))
    domainCert        <- ca.generate("participant", Seq(hostname, "127.0.0.1"))
    collectorInstance <- Docker.inspectMaybe[Collector.Instance]
    oauthInstance     <- Docker.inspectMaybe[OAuth.Instance]
    maybeOAuthCert <- whenCase(oauthInstance) {
      case Some(oauth) => oauth.getFileContents(os.root / "data" / "oauth-certificate.crt")
    }
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
    ) ++ maybeOAuthCert.fold(Seq.empty) { cert => Seq(os.root / "data" / "oauth-certificate.crt" -> cert) }
  )

  private def oauthCantonConfig(oauthInstance: Option[Service[OAuth.Instance]]): String =
    if oauthInstance.isDefined then
      s"""|        auth-services = [{
          |          type = jwt-rs-256-crt
          |          certificate = "/data/oauth-certificate.crt"
          |        }]
          |""".stripMargin
    else ""

  final case class Canton34(version: String) extends CantonConf:
    override def apply(
        hostname: String,
        participantPort: Int,
        synchronizer: String,
        publicApiPort: Int,
        protocolVersion: Int,
        maxRequestSize: Long
    ): ZIO[Docker, Throwable, Seq[(Path, String | Array[Byte])]] =
      for (oauthInstance, collectorInstance, certFiles) <- commonSetup(hostname)
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
              |    participant1 {
              |      monitoring.grpc-health-server {
              |        address = "0.0.0.0"
              |        port = 5061
              |      }
              |
              |      storage {
              |        type = memory
              |      }
              |
              |      admin-api {
              |        address = "0.0.0.0"
              |        port = 10012
              |      }
              |
              |      init {
              |        ledger-api.max-deduplication-duration = 0s
              |      }
              |
              |      sequencer-client {
              |        override-max-request-size = $maxRequestSize
              |      }
              |
              |      ledger-api {
              |        max-inbound-message-size = $maxRequestSize
              |        address = "0.0.0.0"
              |        port = $participantPort
              |        ${oauthCantonConfig(oauthInstance)}
              |        tls {
              |          cert-chain-file = "/tls/participant.crt"
              |          private-key-file = "/tls/participant.pem"
              |          trust-collection-file = "/tls/root-ca.crt"
              |          client-auth {
              |            type=require
              |            admin-client {
              |              cert-chain-file = "/tls/admin-client.crt"
              |              private-key-file = "/tls/admin-client.pem"
              |            }
              |          }
              |        }
              |        # We need to bump this because we run one stream per user +
              |        # polling for domain connections which can add up quite a bit
              |        # once you're around ~100 users.
              |        rate-limit.max-api-services-queue-size = 80000
              |        interactive-submission-service {
              |          enable-verbose-hashing = true
              |        }
              |      }
              |
              |      http-ledger-api {
              |        port = 7575
              |        address = 0.0.0.0
              |      }
              |
              |      parameters {
              |        initial-protocol-version = $protocolVersion
              |        minimum-protocol-version = $protocolVersion
              |        # tune the synchronisation protocols contract store cache
              |        caching {
              |          contract-store {
              |            maximum-size = 1000 # default 1e6
              |            expire-after-access = 120s # default 10 minutes
              |          }
              |        }
              |        # Bump ACS pruning interval to make sure ACS snapshots are available for longer
              |        journal-garbage-collection-delay = 24h
              |      }
              |
              |      # from https://docs.daml.com/2.8.0/canton/usermanual/performance.html#configuration
              |      # tune caching configs of the ledger api server
              |      ledger-api {
              |        index-service {
              |          max-contract-state-cache-size = 1000 # default 1e4
              |          max-contract-key-state-cache-size = 1000 # default 1e4
              |
              |          # The in-memory fan-out will serve the transaction streams from memory as they are finalized, rather than
              |          # using the database. Therefore, you should choose this buffer to be large enough such that the likeliness of
              |          # applications having to stream transactions from the database is low. Generally, having a 10s buffer is
              |          # sensible. Therefore, if you expect e.g. a throughput of 20 tx/s, then setting this number to 200 is sensible.
              |          # The default setting assumes 100 tx/s.
              |          max-transactions-in-memory-fan-out-buffer-size = 200 # default 1000
              |        }
              |        # Restrict the command submission rate (mainly for SV participants, since they are granted unlimited traffic)
              |        command-service {
              |          max-commands-in-flight = 30 # default = 256
              |        }
              |      }
              |
              |      topology.broadcast-batch-size = 1
              |    }
              |  }
              |
              |  sequencers {
              |    ${sequencer("sequencer1")}
              |  }
              |
              |  mediators {
              |    ${mediator("mediator1")}
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
              |    synchronizerName = "$synchronizer",
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
              |  participant1.synchronizers.connect_local(sequencer1, alias = "$synchronizer")
              |  utils.retry_until_true {
              |      participant1.synchronizers.active("$synchronizer")
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

    override def twoParticipantConfig(
        protocolVersion: Int,
        pgHost: String,
        pgPort: Int,
        dbP1: String,
        dbP2: String
    ): String = throw new NotImplementedError("not tested on Canton 3.4")

    override def twoSynchronizerConfig(
        hostname: String,
        protocolVersion: Int
    ): ZIO[Docker, Throwable, Seq[(Path, String | Array[Byte])]] =
      ZIO.fail(new NotImplementedError("not tested on Canton 3.4"))
  end Canton34

  final case class Canton35Plus(version: String) extends CantonConf:
    override def apply(
        hostname: String,
        participantPort: Int,
        synchronizer: String,
        publicApiPort: Int,
        protocolVersion: Int,
        maxRequestSize: Long = 4 * 1024 * 1024
    ) =
      for (oauthInstance, collectorInstance, certFiles) <- commonSetup(hostname)
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
              |    participant1 {
              |      monitoring.grpc-health-server {
              |        address = "0.0.0.0"
              |        port = 5061
              |      }
              |
              |      storage {
              |        type = memory
              |      }
              |
              |      admin-api {
              |        address = "0.0.0.0"
              |        port = 10012
              |      }
              |
              |      init {
              |        ledger-api.max-deduplication-duration = 0s
              |      }
              |
              |      sequencer-client {
              |        override-max-request-size = $maxRequestSize
              |      }
              |
              |      http-ledger-api.enabled = false
              |
              |      ledger-api {
              |        max-inbound-message-size = $maxRequestSize
              |        address = "0.0.0.0"
              |        port = $participantPort
              |        ${oauthCantonConfig(oauthInstance)}
              |        tls {
              |          cert-chain-file = "/tls/participant.crt"
              |          private-key-file = "/tls/participant.pem"
              |          trust-collection-file = "/tls/root-ca.crt"
              |          client-auth {
              |            type = require
              |            admin-client {
              |              cert-chain-file = "/tls/admin-client.crt"
              |              private-key-file = "/tls/admin-client.pem"
              |            }
              |          }
              |        }
              |        # We need to bump this because we run one stream per user +
              |        # polling for domain connections which can add up quite a bit
              |        # once you're around ~100 users.
              |        rate-limit.max-api-services-queue-size = 80000
              |        interactive-submission-service {
              |          enable-verbose-hashing = true
              |        }
              |      }
              |
              |      parameters {
              |        initial-protocol-version = $protocolVersion
              |        minimum-protocol-version = $protocolVersion
              |        # tune the synchronisation protocols contract store cache
              |        caching {
              |          contract-store {
              |            maximum-size = 1000 # default 1e6
              |            expire-after-access = 120s # default 10 minutes
              |          }
              |        }
              |        # Bump ACS pruning interval to make sure ACS snapshots are available for longer
              |        journal-garbage-collection-delay = 24h
              |      }
              |
              |      # from https://docs.daml.com/2.8.0/canton/usermanual/performance.html#configuration
              |      # tune caching configs of the ledger api server
              |      ledger-api {
              |        index-service {
              |          max-contract-state-cache-size = 1000 # default 1e4
              |          max-contract-key-state-cache-size = 1000 # default 1e4
              |
              |          # The in-memory fan-out will serve the transaction streams from memory as they are finalized, rather than
              |          # using the database. Therefore, you should choose this buffer to be large enough such that the likeliness of
              |          # applications having to stream transactions from the database is low. Generally, having a 10s buffer is
              |          # sensible. Therefore, if you expect e.g. a throughput of 20 tx/s, then setting this number to 200 is sensible.
              |          # The default setting assumes 100 tx/s.
              |          max-transactions-in-memory-fan-out-buffer-size = 200 # default 1000
              |        }
              |        # Restrict the command submission rate (mainly for SV participants, since they are granted unlimited traffic)
              |        command-service {
              |          max-commands-in-flight = 30 # default = 256
              |        }
              |      }
              |
              |      topology.broadcast-batch-size = 1
              |    }
              |  }
              |
              |  sequencers {
              |    ${sequencer("sequencer1")}
              |  }
              |
              |  mediators {
              |    ${mediator("mediator1")}
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
              |    synchronizerName = "$synchronizer",
              |    sequencers = Seq(sequencer1),
              |    mediators = Seq(mediator1),
              |    synchronizerOwners = Seq(sequencer1),
              |    synchronizerThreshold = PositiveInt.one,
              |    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer)
              |  )
              |
              |  val initialReconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)
              |  sequencer1.topology.synchronizer_parameters
              |    .propose_update(synchronizerId.logical, _.update(
              |      reconciliationInterval = initialReconciliationInterval,
              |      // The TrafficControlParameters used here have no default values,
              |      // so using the ones from com.digitalasset.canton.sequencing.TrafficControlParameters
              |      trafficControl = Some(TrafficControlParameters(
              |        maxBaseTrafficAmount = NonNegativeLong.tryCreate(204800L),
              |        readVsWriteScalingFactor = PositiveInt.tryCreate(200),
              |        maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofMinutes(10L),
              |        setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(2L),
              |        enforceRateLimiting = true,
              |        baseEventCost = NonNegativeLong.tryCreate(0L),
              |        freeConfirmationResponses = false
              |      ))
              |    ))
              |
              |  logger.info("=== connecting to synchronizer ===")
              |  participant1.synchronizers.connect_local(sequencer1, alias = "$synchronizer")
              |  utils.retry_until_true {
              |      participant1.synchronizers.active("$synchronizer")
              |  }
              |  logger.info("=== finished connecting to synchronizer ===")
              |
              |  // Set a high enough traffic balance for the participant to avoid hitting traffic limits during tests.
              |  sequencer1.traffic_control.set_traffic_balance(
              |    participant1.id, PositiveInt.one, NonNegativeLong.tryCreate(100000000L)
              |  )
              |
              |  // verify that the connection works
              |  participant1.health.ping(participant1)
              |
              |  logger.info("=== finishing participant bootstrap ===")
              |}
              |""".stripMargin
        certFiles ++ Seq(os.root / "app" / "app.conf" -> config, os.root / "app" / "bootstrap.sc" -> bootstrap)

    override def twoParticipantConfig(
        protocolVersion: Int,
        pgHost: String,
        pgPort: Int,
        dbP1: String,
        dbP2: String
    ): String =
      s"""_storage {
         |  type = postgres
         |  config {
         |    dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
         |    properties {
         |      serverName = "$pgHost"
         |      portNumber = $pgPort
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
         |}
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
         |    ${participantWithPGStorage("participant1", dbP1, 10012, 7865, protocolVersion)}
         |    
         |    ${participantWithPGStorage("participant2", dbP2, 10014, Ledger.participantPort, protocolVersion)}
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

    override def twoSynchronizerConfig(
        hostname: String,
        protocolVersion: Int
    ): ZIO[Docker, Throwable, Seq[(Path, String | Array[Byte])]] =
      for (oauthInstance, collectorInstance, certFiles) <- commonSetup(hostname)
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
              |    participant1 {
              |      storage.type = memory
              |      admin-api {
              |        address = "0.0.0.0"
              |        port = 10012
              |      }
              |      init {
              |        ledger-api.max-deduplication-duration = 0s
              |      }
              |      http-ledger-api.enabled = false
              |      ledger-api {
              |        address = "0.0.0.0"
              |        port = ${Ledger.participantPort}
              |        ${oauthCantonConfig(oauthInstance)}
              |        tls {
              |          cert-chain-file = "/tls/participant.crt"
              |          private-key-file = "/tls/participant.pem"
              |          trust-collection-file = "/tls/root-ca.crt"
              |          client-auth {
              |            type = require
              |            admin-client {
              |              cert-chain-file = "/tls/admin-client.crt"
              |              private-key-file = "/tls/admin-client.pem"
              |            }
              |          }
              |        }
              |      }
              |      parameters {
              |        initial-protocol-version = $protocolVersion
              |        minimum-protocol-version = $protocolVersion
              |      }
              |      topology.broadcast-batch-size = 1
              |    }
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
              |    synchronizerName = "synchronizer1",
              |    sequencers = Seq(sequencer1),
              |    mediators = Seq(mediator1),
              |    synchronizerOwners = Seq(sequencer1),
              |    synchronizerThreshold = PositiveInt.one,
              |    staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer)
              |  )
              |  val synchronizer2Id = bootstrap.synchronizer(
              |    synchronizerName = "synchronizer2",
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
              |  participant1.synchronizers.connect_local(sequencer1, alias = "synchronizer1")
              |  participant1.synchronizers.connect_local(sequencer2, alias = "synchronizer2")
              |  utils.retry_until_true { participant1.synchronizers.active("synchronizer1") }
              |  utils.retry_until_true { participant1.synchronizers.active("synchronizer2") }
              |  participant1.health.ping(participant1)
              |}
              |""".stripMargin
        certFiles ++ Seq(
          os.root / "app" / "app.conf" -> config,
          os.root / "app" / "bootstrap.sc" -> bootstrap
        )
  end Canton35Plus

  // PG storage is required by the ACS import test in RpidTwoParticipantSpec
  private def participantWithPGStorage(
      name: String,
      db: String,
      adminApiPort: Int,
      ledgerApiPort: Int,
      protocolVersion: Int
  ): String =
    s"""|$name {
        |      storage = $${_storage}
        |      storage.config.properties.databaseName = "$db"
        |      admin-api {
        |        address = "0.0.0.0"
        |        port = $adminApiPort
        |      }
        |      init {
        |        ledger-api.max-deduplication-duration = 0s
        |      }
        |      http-ledger-api.enabled = false
        |      ledger-api {
        |        address = "0.0.0.0"
        |        port = $ledgerApiPort
        |        tls {
        |          cert-chain-file = "/tls/participant.crt"
        |          private-key-file = "/tls/participant.pem"
        |          trust-collection-file = "/tls/root-ca.crt"
        |          client-auth {
        |            type = require
        |            admin-client {
        |              cert-chain-file = "/tls/admin-client.crt"
        |              private-key-file = "/tls/admin-client.pem"
        |            }
        |          }
        |        }
        |      }
        |      parameters {
        |        initial-protocol-version = $protocolVersion
        |        minimum-protocol-version = $protocolVersion
        |      }
        |      topology.broadcast-batch-size = 1
        |    }
        |""".stripMargin

  private def sequencer(name: String, publicApiPort: Int = 5008, adminApiPort: Int = 5009): String =
    s"""|$name {
        |      storage.type = memory
        |      public-api {
        |        address = "0.0.0.0"
        |        port = $publicApiPort
        |      }
        |      admin-api {
        |        address = "0.0.0.0"
        |        port = $adminApiPort
        |      }
        |    }
        |""".stripMargin

private def mediator(name: String, adminApiPort: Int = 5007): String =
  s"""|$name {
      |      storage.type = memory
      |      admin-api {
      |        address = "0.0.0.0"
      |        port = $adminApiPort
      |      }
      |    }
      |""".stripMargin

private def monitoring(collector: Service[Collector.Instance]): String =
  s"""  monitoring {
     |    tracing {
     |      propagation = enabled
     |      tracer {
     |        exporter {
     |          type = otlp
     |          address = ${collector.container.hostName}
     |          port = ${Collector.Instance.otlp}
     |        }
     |        sampler {
     |          type = always-on
     |          parent-based = true
     |        }
     |      }
     |    }
     |  }
     |""".stripMargin
