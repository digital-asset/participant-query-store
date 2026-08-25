// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import mill._
import mill.api.Ctx
import mill.scalalib._

package object millbuild {

  /** Versions */
  object V {
    val jdk   = "17"
    val scala = "3.7.1"

    // Latest snapshot: https://console.cloud.google.com/artifacts/docker/da-images/europe/public-unstable/components%2Fdamlc
    // Latest stable: https://console.cloud.google.com/artifacts/docker/da-images/europe/public/components%2Fdamlc
    val damlc = "3.6.0-snapshot.20260818.14788.0.v64638935"

    // Latest snapshot: https://console.cloud.google.com/artifacts/docker/da-images/europe/public-unstable/components%2Fcanton-open-source
    // Latest stable: https://console.cloud.google.com/artifacts/docker/da-images/europe/public/components%2Fcanton-open-source
    val canton = "3.6.0-snapshot.20260818.20026.0.v41046c3b"

    val dockerClient = "3.4.0"
    val flyway = "12.10.0"

    // Think twice before changing this version, check with Canton's dependencies for a particular release line
    // for compatibility: https://github.com/DACH-NY/canton/blob/main/shared_dependencies.json
    val grpc  = "1.81.0"

    // force specific version to address vulns
    val nettyVersion = "4.2.16.Final"

    val openTelemetryAgent = "2.28.1"
    val scalaPB = "0.11.19"

    val zio                  = "2.1.26"
    val zioConfig            = "3.0.7"
    val zioLogging           = "2.3.0"
    val zioMetricsConnectors = "2.3.1"
    val zioHttp              = "3.11.3"
    val zioMock              = "1.0.0-RC12"
    val sttpClient           = "4.0.26"
  }

  /** Library dependencies */
  object L {
    object scalapb {
      val compiler = ivy"com.thesamet.scalapb::compilerplugin:${V.scalaPB}"
    }

    val protoJava = ivy"com.google.protobuf:protobuf-java:4.35.1"

    object netty {
      val codecHttp = ivy"io.netty:netty-codec-http:${V.nettyVersion}"
      val handlerProxy = ivy"io.netty:netty-handler-proxy:${V.nettyVersion}"
      val pkiTesting = ivy"io.netty:netty-pkitesting:${V.nettyVersion}"
      val transportNativeEpoll = ivy"io.netty:netty-transport-native-epoll:${V.nettyVersion}"
      val transportNativeKqueue = ivy"io.netty:netty-transport-native-kqueue:${V.nettyVersion}"
    }

    object canton {
      val ledgerApiProto = ivy"com.daml:ledger-api-proto:${V.canton}"
      val ledgerApi      = ivy"com.daml::ledger-api-scala:${V.canton}".withDottyCompat(V.scala)

      def archiveReader(implicit ctx: Ctx) = ivy"com.daml::daml-lf-archive:${V.canton}"
        .withDottyCompat(V.scala)
        .exclude(
          "org.scala-lang" -> "scala-compiler",
          "org.scala-lang" -> "scala-reflect"
        )
    }

    val fastparse  = ivy"com.lihaoyi::fastparse:3.1.1"
    val pprint     = ivy"com.lihaoyi::pprint:0.9.0"
    val oslib      = ivy"com.lihaoyi::os-lib:0.10.3"
    val ujson      = ivy"com.lihaoyi::ujson:3.3.1"
    val upickle    = ivy"com.lihaoyi::upickle:3.3.1"
    val sourceCode = ivy"com.lihaoyi::sourcecode:0.4.2"
    val jwt        = ivy"com.nimbusds:nimbus-jose-jwt:9.40"
    val semver     = ivy"org.semver4j:semver4j:5.3.0"

    object dockerClient {
      val core             = ivy"com.github.docker-java:docker-java-core:${V.dockerClient}"
      val zerodepTransport = ivy"com.github.docker-java:docker-java-transport-zerodep:${V.dockerClient}"
    }

    object grpc {
      val api   = ivy"io.grpc:grpc-api:${V.grpc}".forceVersion()
      val netty = ivy"io.grpc:grpc-netty-shaded:${V.grpc}"
    }

    object transcode {
      def schema = ivy"com.daml::transcode-schema:${V.canton}"
      def json   = ivy"com.daml::transcode-codec-json:${V.canton}"
      def proto  = ivy"com.daml::transcode-codec-proto-scala:${V.canton}"
      def daml   = ivy"com.daml::transcode-daml-lf:${V.canton}"
    }

    object zio {
      val zio           = ivy"dev.zio::zio:${V.zio}"
      val streams       = ivy"dev.zio::zio-streams:${V.zio}"
      val http          = ivy"dev.zio::zio-http:${V.zioHttp}".excludeOrg("io.netty")
      val jdbc          = ivy"dev.zio::zio-jdbc:0.1.2"
      val opentelemetry = ivy"dev.zio::zio-opentelemetry:3.1.18"
      val process       = ivy"dev.zio::zio-process:0.8.0"

      object config {
        val core     = ivy"dev.zio::zio-config:${V.zioConfig}"
        val typesafe = ivy"dev.zio::zio-config-typesafe:${V.zioConfig}" // HOCON config support
        val magnolia = ivy"dev.zio::zio-config-magnolia:${V.zioConfig}" // automatic derivation
      }

      object logging {
        val logging     = ivy"dev.zio::zio-logging:${V.zioLogging}"
        val slf4jBridge = ivy"dev.zio::zio-logging-slf4j2-bridge:${V.zioLogging}"
      }

      object metrics {
        val connectors          = ivy"dev.zio::zio-metrics-connectors:${V.zioMetricsConnectors}"
        val micrometerConnector = ivy"dev.zio::zio-metrics-connectors-micrometer:${V.zioMetricsConnectors}"
      }

      object test {
        val test     = ivy"dev.zio::zio-test:${V.zio}"
        val sbt      = ivy"dev.zio::zio-test-sbt:${V.zio}"
        val magnolia = ivy"dev.zio::zio-test-magnolia:${V.zio}"
        val http     = ivy"dev.zio::zio-http-testkit:${V.zioHttp}".excludeOrg("io.netty")
        val mock     = ivy"dev.zio::zio-mock:${V.zioMock}"
      }
    }

    object sttp {
      val zio    = ivy"com.softwaremill.sttp.client4::zio:${V.sttpClient}"
      val okhttp = ivy"com.softwaremill.sttp.client4::okhttp-backend:${V.sttpClient}"
    }

    val sbtTestInterface = ivy"org.scala-sbt:test-interface:1.0"

    object jdbc {
      val postgres = ivy"org.postgresql:postgresql:42.7.11"
    }

    val bouncyCastle = ivy"org.bouncycastle:bcpkix-jdk18on:1.78.1"

    val wartRemover = ivy"org.wartremover::wartremover:3.2.0"

    object flyway {
      val core           = ivy"org.flywaydb:flyway-core:${V.flyway}"
      val driverPostgres = ivy"org.flywaydb:flyway-database-postgresql:${V.flyway}"
    }

    val classgraph = ivy"io.github.classgraph:classgraph:4.8.174"

    object commons {
      val text  = ivy"org.apache.commons:commons-text:1.12.0"
      val lang3 = ivy"org.apache.commons:commons-lang3:3.18.0"
    }

    object openTelemetry {
      val api = ivy"io.opentelemetry:opentelemetry-api:1.62.0"
    }

    object metrics {
      // Keep in sync with the version shipped with `zio-metrics-connectors-micrometer`
      val micrometerCore = ivy"io.micrometer:micrometer-core:1.11.0"
    }
  }
}
