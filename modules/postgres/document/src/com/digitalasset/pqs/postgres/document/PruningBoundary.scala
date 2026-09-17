// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.Offset
import zio.config.magnolia.Descriptor

import java.time.ZonedDateTime
import scala.util.Try

enum PruningBoundary:
  case OffsetBoundary(offset: Offset.Absolute)
  case TimeBoundary(time: ZonedDateTime)
  case DurationBoundary(duration: java.time.Duration)

  override def toString: String = this match
    case OffsetBoundary(offset)     => offset.toString
    case TimeBoundary(time)         => time.toString
    case DurationBoundary(duration) => duration.toString

object PruningBoundary:
  given Descriptor[PruningBoundary] = Descriptor.from(
    Descriptor[String].transform[PruningBoundary](
      s =>
        // try parsing the pruning boundary as an offset, timestamp or duration
        def tryDuration = Try(java.time.Duration.parse(s)).map(DurationBoundary.apply)
        def tryTime     = Try(ZonedDateTime.parse(s)).map(TimeBoundary.apply)
        def offset      = OffsetBoundary(Offset.Absolute(s.toLong))
        tryDuration orElse tryTime getOrElse offset
      ,
      {
        case OffsetBoundary(offset)     => offset.toString
        case TimeBoundary(time)         => time.toString
        case DurationBoundary(duration) => duration.toString
      }
    )
  )
