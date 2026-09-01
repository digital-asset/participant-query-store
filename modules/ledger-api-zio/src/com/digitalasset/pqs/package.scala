// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import zio.{Tag, ZLayer}

package object pqs:
  extension [R, E, A](layer: ZLayer[R, E, A]) def as[A1 >: A: Tag]: ZLayer[R, E, A1] = layer.map(_.prune[A1])
