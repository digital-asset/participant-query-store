// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline

import zio.config.magnolia.describe

// TODO remove
case class Config(
    @describe("Pipeline config")
    pipeline: com.digitalasset.scribe.pipeline.pipeline.Config,
    @describe("Ledger config")
    ledger: com.digitalasset.zio.daml.Config,
    @describe("Postgres config")
    postgres: com.digitalasset.scribe.postgres.backend.PostgresConfig
)
