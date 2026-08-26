// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import com.digitalasset.scribe.docker.{Docker, Service}
import com.digitalasset.scribe.services.daml.Ledger
import com.digitalasset.scribe.services.oauth.OAuth
import com.digitalasset.scribe.services.postgres.*
import com.digitalasset.scribe.services.scribe.{CliRun, localScribeDockerImage}
import zio.{ZIO, ZLayer}

package object errors:
  def runScribe(args: String*) =
    ZLayer
      .service[Conf.Conf]
      .project { conf =>
        Docker
          .service[Unit](
            image = localScribeDockerImage,
            env = conf._1,
            prepopulateFiles = conf._2
          )(args)
          .flatMap(CliRun.fromSvc)
      }
      .flatten

  object Conf:
    type Conf = (Map[String, Any], Seq[(os.Path, Array[Byte] | String)])

    def buildConf[IN](conf: ZIO[Docker & IN, Throwable, Conf]*): ZIO[Docker & IN, Throwable, Conf] =
      for
        ca    <- Docker.certificateAuthority
        confs <- ZIO.collectAll(conf)
      yield {
        val (vars, files) = confs.unzip
        (
          vars.fold(Map.empty)(_ ++ _),
          files.fold(Seq.empty)(_ ++ _) :+ (os.root / "tls" / "root-ca.crt", ca.certificate.crt)
        )
      }

    def ledger(prefix: String = "") =
      for
        ca                <- Docker.certificateAuthority
        clientCertificate <- ca.generate("scribe")
        ledger            <- Docker.inspect[Ledger]
        oauthInstance     <- Docker.inspectMaybe[OAuth.Instance]
        _prefix = if prefix.isEmpty then prefix else s"${prefix}_".toUpperCase
        base = Map(
          s"SCRIBE_${_prefix}LEDGER_HOST"       -> ledger.container.hostName,
          s"SCRIBE_${_prefix}LEDGER_PORT"       -> Ledger.participantPort,
          s"SCRIBE_${_prefix}LEDGER_TLS_KEY"    -> "/tls/client.pem",
          s"SCRIBE_${_prefix}LEDGER_TLS_CERT"   -> "/tls/client.crt",
          s"SCRIBE_${_prefix}LEDGER_TLS_CAFILE" -> "/tls/root-ca.crt"
        )

        oauth = oauthInstance.fold(Map.empty) { oauth =>
          Map(
            "SCRIBE_PIPELINE_OAUTH_CLIENTSECRET" -> "clientsecret",
            "SCRIBE_PIPELINE_OAUTH_ENDPOINT"     -> s"https://${oauth.container.hostName}:${OAuth.port}/issuer1/token",
            "SCRIBE_PIPELINE_OAUTH_CAFILE"       -> "/tls/root-ca.crt",
            "SCRIBE_SOURCE_LEDGER_AUTH"          -> "OAuth"
          )
        }
      yield (
        base ++ oauth,
        Seq(
          os.root / "tls" / "client.pem" -> clientCertificate.certificate.pem,
          os.root / "tls" / "client.crt" -> clientCertificate.certificate.crt
        )
      )

    def pg(prefix: String = "") =
      for
        ca                <- Docker.certificateAuthority
        pg                <- ZIO.service[Postgres]
        dbName            <- ZIO.service[Database]
        clientCertificate <- ca.generate("scribe")
        _prefix = if prefix.isEmpty then prefix else s"${prefix}_".toUpperCase
        base = Map(
          s"SCRIBE_${_prefix}POSTGRES_HOST"       -> pg.container.hostName,
          s"SCRIBE_${_prefix}POSTGRES_PORT"       -> Postgres.port,
          s"SCRIBE_${_prefix}POSTGRES_DATABASE"   -> dbName.name,
          s"SCRIBE_${_prefix}POSTGRES_USERNAME"   -> "postgres",
          s"SCRIBE_${_prefix}POSTGRES_PASSWORD"   -> "postgres",
          s"SCRIBE_${_prefix}POSTGRES_TLS_MODE"   -> "VerifyFull",
          s"SCRIBE_${_prefix}POSTGRES_TLS_KEY"    -> "/tls/client-pg.der",
          s"SCRIBE_${_prefix}POSTGRES_TLS_CERT"   -> "/tls/client-pg.crt",
          s"SCRIBE_${_prefix}POSTGRES_TLS_CAFILE" -> "/tls/root-ca.crt"
        )
      yield (
        base,
        Seq(
          os.root / "tls" / "client-pg.der" -> clientCertificate.certificate.der,
          os.root / "tls" / "client-pg.crt" -> clientCertificate.certificate.crt
        )
      )

    val failfast = ZIO.succeed((Map("SCRIBE_RETRY_COUNTER_ATTEMPTS" -> "0"), Seq.empty))

    val pipeline = ZLayer.fromZIO(
      buildConf[Docker & Postgres & Database & Service[Ledger]](
        ledger("source"),
        pg("target"),
        failfast
      )
    )

    val datastore = ZLayer.fromZIO(
      buildConf[Docker & Postgres & Database & Service[Ledger]](
        ledger(),
        pg()
      )
    )
  end Conf
end errors
