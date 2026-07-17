// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.appversion

import com.digitalasset.pqs.app.*
import zio.Console.printLine

import java.lang.System.lineSeparator

object Main extends ComposableApp:
  def app = ("-v" | "--version").as(
    getVersion.flatMap { (title, version, props) =>
      printLine(s"$title, version: $version${render(props, start = lineSeparator, sep = lineSeparator)}")
    }
  )

end Main
