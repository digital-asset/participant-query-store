// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.configuration

import zio.config.magnolia.Descriptor

class AccessToken(val value: String) {
  override def toString: String = "********"
}

object AccessToken:
  given descr: Descriptor[AccessToken] =
    Descriptor.from(Descriptor[String].transform[AccessToken](new AccessToken(_), _.toString))
