// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.configuration

import zio.config.magnolia.Descriptor

class Secret(val value: String) {
  override def toString: String = "********"
}

object Secret:
  given descr: Descriptor[Secret] =
    Descriptor.from(Descriptor[String].transform[Secret](new Secret(_), _.toString))
