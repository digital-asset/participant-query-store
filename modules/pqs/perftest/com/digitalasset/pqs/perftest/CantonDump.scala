// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.Dump
import com.digitalasset.pqs.services.daml.CantonConf

object CantonDumps:
  // Mill forks test JVMs with a sandboxed working directory, so `os.pwd` does not point at the repo checkout.
  // Mill sets `MILL_WORKSPACE_ROOT` on forked processes precisely to work around this
  private val workspaceRoot: os.Path = sys.env.get("MILL_WORKSPACE_ROOT").fold(os.pwd)(os.Path(_))

  private val dumpsDir: os.Path = workspaceRoot / "modules" / "pqs" / "perftest" / "dumps"

  val all = CantonConf.databaseNames.map(name => Dump(name, dumpsDir / s"$name.dump"))
