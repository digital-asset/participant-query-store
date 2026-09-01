// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.o11y

import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.metrics.{Metric, MetricLabel}
import zio.{Cause, Clock, Trace, ZIO, ZIOAspect}

package object metrics:
  def latency(
      name: String,
      description: String,
      boundaries: Boundaries = Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
  ): OAspect = new ZIOAspect[Nothing, Any, Nothing, Any, Nothing, Any] {
    def apply[R, E, A](zio: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
      Clock.nanoTime.flatMap(start =>
        zio.foldCauseZIO(
          err => Clock.nanoTime.flatMap(end => metric.update((end - start) -> Some(err)) *> ZIO.refailCause(err)),
          a => Clock.nanoTime.flatMap(end => metric.update((end - start) -> None) *> ZIO.succeed(a))
        )
      )

    private val metric = Metric
      .histogram(name, description, boundaries)
      .contramap[(Long, Option[Cause[?]])]((duration, cause) => duration.toDouble / 1e9)
      .taggedWith((duration, cause) => Set(MetricLabel("status", getStatus(cause))))

    private def getStatus(cause: Option[Cause[?]]): String = cause match
      case None                            => "success"
      case Some(Cause.Empty)               => "empty"
      case Some(_: Cause.Fail[_])          => "fail"
      case Some(_: Cause.Die)              => "die"
      case Some(_: Cause.Interrupt)        => "interrupt"
      case Some(Cause.Stackless(cause, _)) => getStatus(Some(cause))
      case Some(Cause.Then(left, right))   => s"${getStatus(Some(left))};${getStatus(Some(right))}"
      case Some(Cause.Both(left, right))   => s"${getStatus(Some(left))}&${getStatus(Some(right))}"

  }
