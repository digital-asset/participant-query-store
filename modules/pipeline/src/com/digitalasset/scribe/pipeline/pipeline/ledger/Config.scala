// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline.pipeline.ledger

import com.digitalasset.scribe.pipeline.pipeline.ledger.specific.Config.{CliStartOffset, CliStopOffset}
import zio.config.magnolia.describe

case class Config(
    @describe("Start offset")
    start: CliStartOffset = CliStartOffset.Latest,
    @describe("Stop offset")
    stop: CliStopOffset = CliStopOffset.Never
)
