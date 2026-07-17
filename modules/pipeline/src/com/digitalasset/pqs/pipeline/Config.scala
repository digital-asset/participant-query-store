// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import zio.config.magnolia.describe

// TODO remove
case class Config(
    @describe("Pipeline config")
    pipeline: com.digitalasset.pqs.pipeline.pipeline.Config,
    @describe("Ledger config")
    ledger: com.digitalasset.zio.daml.Config,
    @describe("Postgres config")
    postgres: com.digitalasset.pqs.postgres.backend.PostgresConfig
)
