// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.auth.Config.AuthMode
import com.digitalasset.scribe.configuration.ISO8601Duration
import zio.config.magnolia.{describe, name}
import zio.durationInt

import java.io.File
import scala.language.implicitConversions

case class Config(
    @describe("Ledger API host")
    host: String = "localhost",
    @describe("Ledger API port")
    port: Int = 6865,
    tls: TlsConfig,
    @describe("Authorisation mode")
    auth: AuthMode = AuthMode.NoAuth,
    keepAlive: KeepAlive,
    @describe("Buffer size for gRPC channel")
    bufferSize: Int = 128,
    @describe("Cache Directory")
    cacheDir: File = File("/tmp/scribe")
)

case class TlsConfig(
    @describe("Trusted Certificate Authority (CA) certificate")
    @name("cafile")
    caCertificate: Option[File] = None,
    @describe("Client's private key (leave empty for server-only TLS)")
    @name("key")
    privateKey: Option[File] = None,
    @describe("Client's certificate (leave empty if embedded into private key file)")
    @name("cert")
    certificate: Option[File] = None
)

case class KeepAlive(
    @describe("Duration (ISO 8601) of interval between ping frames (PT0S to disable)")
    time: ISO8601Duration = 40.seconds,
    @describe("Duration (ISO 8601) of timeout for a ping frame to be acknowledged")
    timeout: ISO8601Duration = 20.seconds
)
