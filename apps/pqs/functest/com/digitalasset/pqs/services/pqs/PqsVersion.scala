// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.pqs

import org.semver4j.Semver

case class PqsVersion(semVer: Semver):
  override def toString: String = semVer.getVersion

object PqsVersion:
  val Latest: PqsVersion                 = PqsVersion(Semver.of(Int.MaxValue, Int.MaxValue, Int.MaxValue).build)
  def apply(version: String): PqsVersion = PqsVersion(Semver.parse(version))
