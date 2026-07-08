// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.grpc

import io.grpc.StatusException
import zio.IO

trait ZClientInterceptor {
  def intercept(metadata: SafeMetadata): IO[StatusException, Any]
}
object ZClientInterceptor {
  def intercept(effect: SafeMetadata => IO[StatusException, Any]): ZClientInterceptor =
    (metadata: SafeMetadata) => effect(metadata)
}
