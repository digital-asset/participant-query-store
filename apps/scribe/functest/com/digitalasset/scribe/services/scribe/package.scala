// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services

package object scribe:
  private val suffix: String         = sys.env.getOrElse("SCRIBE_IMAGE_TAG_SUFFIX", "")
  val localScribeDockerImage: String = s"scribe$suffix"
