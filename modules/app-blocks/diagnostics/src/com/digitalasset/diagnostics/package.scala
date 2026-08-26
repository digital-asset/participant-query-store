// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import java.io.PrintStream

package object diagnostics:
  def log(s: String*): Unit   = logPadded(Console.out, None, s*)
  def warn(s: String*): Unit  = logPadded(Console.err, Some("WARN:"), s*)
  def error(s: String*): Unit = logPadded(Console.err, Some("ERROR:"), s*)

  private def logPadded(ps: PrintStream, prefix: Option[String], s: String*): Unit =
    val start = Seq("[diagnostics]") ++ prefix.toList
    Console.withOut(ps) {
      println(s.map(s => (start ++ Seq(s)).mkString(" ")).mkString(System.lineSeparator))
    }
