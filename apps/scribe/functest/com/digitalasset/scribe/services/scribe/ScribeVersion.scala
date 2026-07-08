// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.scribe

import org.semver4j.Semver

case class ScribeVersion(semVer: Semver):
  override def toString: String = semVer.getVersion

object ScribeVersion:
  val Latest: ScribeVersion                 = ScribeVersion(Semver.of(Int.MaxValue, Int.MaxValue, Int.MaxValue).build)
  def apply(version: String): ScribeVersion = ScribeVersion(Semver.parse(version))
