// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package zio

import com.digitalasset.pqs.o11y.traces.{Api, Noop}

package object shims:
  val tracesApi: FiberRef[Api] = FiberRef.unsafe.make[Api](Noop)(using Unsafe.unsafe)
