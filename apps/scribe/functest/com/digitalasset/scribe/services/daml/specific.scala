// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.daml

import com.digitalasset.canonical.specific.Offset
import zio.Task
import zio.ZIO

object specific:
  extension (offset: String | Long)
    def toOffset: Task[Offset.Absolute] =
      offset match
        case s: String => ZIO.fail(Throwable("No string offset accepted in this context"))
        case l: Long   => ZIO.succeed(Offset.Absolute(l))
