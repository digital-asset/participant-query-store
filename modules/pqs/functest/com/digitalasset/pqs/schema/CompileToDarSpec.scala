// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema

import com.digitalasset.pqs.Dars
import com.digitalasset.pqs.functest.FuncTestDefault
import com.digitalasset.pqs.services.daml.{DamlSdk, DarFile}
import zio.test.*

object CompileToDarSpec extends FuncTestDefault:

  def spec = funcTest("compile small model to dar"):
    Expect:
      DamlSdk.buildDar(Dars.pingSenderReceiver).map { dar =>
        assertTrue(
          dar.darBytes.nonEmpty,
          dar.packageId.length == 64,
          os.exists(dar.mainPackageDir)
        )
      }
