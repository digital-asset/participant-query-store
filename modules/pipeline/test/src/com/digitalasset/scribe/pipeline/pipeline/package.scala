// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.pipeline

import com.digitalasset.canonical.specific.Offset.Absolute

package object pipeline:
  def absolute(ix: Int) = Absolute(ix.toLong)

  def formatExpectedValue(value: Int): String = value.toString
