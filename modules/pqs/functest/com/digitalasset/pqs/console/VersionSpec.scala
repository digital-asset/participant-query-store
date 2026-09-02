// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.console

import com.digitalasset.pqs.functest.FuncTestDefault
import com.digitalasset.pqs.functest.matchers.*
import com.digitalasset.pqs.services.pqs.Pqs
import zio.ExitCode

object VersionSpec extends FuncTestDefault:
  def spec = funcTest("version output"):
    When:
      Pqs `run` "--version"
    Then:
      Pqs.exitCode `is` ExitCode.success
    And:
      Pqs.stderr `is` empty
    And:
      Pqs.stdout `is` stringMatching("""^pqs, version: (.*)
                                       |daml-sdk.version: (\d+\.\d+\.\d+)(-.*)?
                                       |postgres-document.schema: (\d{3})$""".stripMargin)
end VersionSpec
