// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.functest.sbt

import com.digitalasset.pqs.functest.FuncTest
import sbt.testing.SubclassFingerprint

object FTFingerprint extends SubclassFingerprint:
  def superclassName(): String           = classOf[FuncTest[Any]].getName
  def isModule: Boolean                  = true
  def requireNoArgConstructor(): Boolean = false
