// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import zio.{Cause, Trace, UIO, ZIO, ZIOAspect}

package object o11y:
  type OAspect = ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any]
  object OAspect {
    def apply(success: UIO[Unit]): OAspect = apply(_ => ZIO.unit, success)

    def apply(failure: Cause[?] => UIO[Unit], success: UIO[Unit]): OAspect = new OAspect {
      def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] = zio.foldCauseZIO(
        cause => failure(cause) *> ZIO.refailCause(cause),
        a => success *> ZIO.succeed(a)
      )
    }
  }
