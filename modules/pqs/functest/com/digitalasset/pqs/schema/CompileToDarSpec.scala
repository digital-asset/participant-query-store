// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.schema

import com.digitalasset.pqs.functest.FuncTestDefault
import com.digitalasset.pqs.services.daml.{DamlSdk, DamlSource, DarFile}
import zio.ZIO.serviceWith
import zio.test.*

object CompileToDarSpec extends FuncTestDefault:
  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |template Ping
                    |  with
                    |    sender: Party
                    |    receiver: Party
                    |  where
                    |    signatory sender
                    |    observer receiver
                    |""".stripMargin
  )

  def spec = funcTest("compile small model to dar"):
    Given:
      DamlSdk.dar(pingPong)

    Expect:
      serviceWith[DarFile](dar =>
        assertTrue(
          dar.darBytes.nonEmpty,
          dar.packageId.length == 64,
          os.exists(dar.mainPackageDir)
        )
      )

end CompileToDarSpec
