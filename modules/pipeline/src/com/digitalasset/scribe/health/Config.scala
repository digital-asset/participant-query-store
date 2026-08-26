// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.health

import zio.config.magnolia.describe

case class Config(
    @describe("Hostname or IP to bind HTTP health info service to")
    address: String = "127.0.0.1",
    @describe("HTTP port to use to expose application health info")
    port: Int = 8080
)
