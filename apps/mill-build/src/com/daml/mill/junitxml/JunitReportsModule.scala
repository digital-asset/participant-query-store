// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.mill.junitxml

import mill.scalalib.TestModule
import mill.testrunner.TestResult
import mill.{T, Task}

trait JunitReportsModule extends TestModule {
  override protected def testTask(
      args: Task[Seq[String]],
      globSelectors: Task[Seq[String]]
  ): Task[(String, Seq[TestResult])] = T.task {
    val (msg, results) = super.testTask(args, globSelectors)()
    saveJunitReport(
      millModuleSegments.render,
      s"${millModuleSegments.parts.last.capitalize}s for ${millModuleSegments.parts.dropRight(1).mkString("/")}",
      results,
      T.dest / "results.xml"
    )
    (msg, results)
  }

}
