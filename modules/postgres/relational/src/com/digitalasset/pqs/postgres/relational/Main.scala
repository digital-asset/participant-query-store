// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.app.*
import zio.Console.printLine
import zio.ExitCode

object Main extends ComposableApp:
  def app = (
    "postgres-relational"
      @@ Command("Perform operations supporting Postgres database (w/ relational payload representation)")
      `as` printLine(s"posgtres-relational not yet unimplemented").ignore.as(ExitCode.failure)
  )
end Main
