// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline.pipeline.ledger

import zio.config.magnolia.{Descriptor, describe}

case class Config(
    @describe("Start offset")
    start: CliStartOffset = CliStartOffset.Latest,
    @describe("Stop offset")
    stop: CliStopOffset = CliStopOffset.Never
)

enum CliStartOffset:
  case Genesis, Oldest, Latest
  case Absolute(offset: Long)

object CliStartOffset:
  given descrAbsolute: Descriptor[CliStartOffset.Absolute] =
    Descriptor.from(Descriptor[Long].transform[CliStartOffset.Absolute](CliStartOffset.Absolute.apply, _.offset))

enum CliStopOffset:
  case Latest, Never
  case Absolute(offset: Long)

object CliStopOffset:
  given descrAbsolute: Descriptor[Absolute] =
    Descriptor.from(Descriptor[Long].transform[Absolute](Absolute.apply, _.offset))
