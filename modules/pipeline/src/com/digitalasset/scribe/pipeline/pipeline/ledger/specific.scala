// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline.pipeline.ledger

import zio.config.magnolia.Descriptor

object specific:
  object Config:
    sealed trait CliStartOffset
    object CliStartOffset:
      case object Genesis                     extends CliStartOffset
      case object Oldest                      extends CliStartOffset
      case object Latest                      extends CliStartOffset
      final case class Absolute(offset: Long) extends CliStartOffset

      given descrAbsolute: Descriptor[CliStartOffset.Absolute] =
        Descriptor.from(Descriptor[Long].transform[CliStartOffset.Absolute](CliStartOffset.Absolute.apply, _.offset))
    end CliStartOffset

    sealed trait CliStopOffset
    object CliStopOffset:
      case object Latest                      extends CliStopOffset
      case object Never                       extends CliStopOffset
      final case class Absolute(offset: Long) extends CliStopOffset
      given descrAbsolute: Descriptor[Absolute] =
        Descriptor.from(Descriptor[Long].transform[Absolute](Absolute.apply, _.offset))
    end CliStopOffset

  end Config
