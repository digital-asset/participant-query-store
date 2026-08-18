// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services

package object pqs:
  private val suffix: String      = sys.env.getOrElse("PQS_IMAGE_TAG_SUFFIX", "")
  val localPqsDockerImage: String = s"pqs$suffix"
