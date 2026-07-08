// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest.sbt

import sbt.testing.*
import zio.test.sbt.{ZTestRunner, ZioSpecFingerprint}

final class FTFramework extends Framework:
  override val name: String              = s"${Console.UNDERLINED}FT${Console.RESET}"
  def fingerprints(): Array[Fingerprint] = Array(FTFingerprint)
  def runner(args: Array[String], remoteArgs: Array[String], testClassLoader: ClassLoader): Runner =
    val zRunner = new ZTestRunner(args, remoteArgs, testClassLoader)
    args.sliding(2, 2).collectFirst { case Array("--pools", v) => Some(v.toInt) }.foreach(FTSpec.forceResourcePools = _)
    args.sliding(2, 2).collectFirst { case Array("--lanes", v) => Some(v.toInt) }.foreach(FTSpec.forcePoolLanes = _)
    new Runner:
      def done(): String              = zRunner.done()
      def remoteArgs(): Array[String] = zRunner.remoteArgs()
      def args(): Array[String]       = zRunner.args()
      def tasks(taskDefs: Array[TaskDef]): Array[Task] =
        FTSpec.tasks = taskDefs
        val task = new TaskDef(classOf[FTSpec.type].getName, ZioSpecFingerprint, true, Array(new SuiteSelector))
        zRunner.tasks(Array(task))
end FTFramework
