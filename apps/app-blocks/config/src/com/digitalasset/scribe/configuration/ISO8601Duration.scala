// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.configuration

import zio.config.magnolia.Descriptor

import scala.util.Try

opaque type ISO8601Duration = java.time.Duration

object ISO8601Duration:
  given duration2iso: Conversion[java.time.Duration, ISO8601Duration] = t => t
  given iso2duration: Conversion[ISO8601Duration, java.time.Duration] = t => t
  given descr: Descriptor[ISO8601Duration] =
    Descriptor.from(
      Descriptor[String].transformOrFail[ISO8601Duration](
        x => Try { java.time.Duration.parse(x) }.toEither.left.map(_.getMessage),
        x => Right(x.toString)
      )
    )
