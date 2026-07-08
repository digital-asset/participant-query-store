// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.console

import com.digitalasset.scribe.functest.FuncTestDefault
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.scribe.Scribe
import zio.ExitCode

object VersionSpec extends FuncTestDefault:
  def spec = funcTest("version output"):
    When:
      Scribe `run` "--version"
    Then:
      Scribe.exitCode `is` ExitCode.success
    And:
      Scribe.stderr `is` empty
    And:
      Scribe.stdout `is` stringMatching("""^scribe, version: (.*)
                                          |daml-sdk.version: (\d+\.\d+\.\d+)(-.*)?
                                          |postgres-document.schema: (\d{3})$""".stripMargin)
end VersionSpec
