// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y

import zio.ZIOAspect

package object logs:
  def tag(key: String, value: String): OAspect = ZIOAspect.annotated(key, value)
  def tag(tags: (String, String)*): OAspect    = ZIOAspect.annotated(tags*)
