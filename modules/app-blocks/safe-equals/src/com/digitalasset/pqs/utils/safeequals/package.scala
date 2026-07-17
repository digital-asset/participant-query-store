// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.utils

import scala.annotation.targetName

package object safeequals:
  extension [A](self: A)
    @targetName("_equal")
    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def ===(other: A): Boolean = self == other
    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    @targetName("_not_equal")
    def =/=(other: A): Boolean = self != other
end safeequals
