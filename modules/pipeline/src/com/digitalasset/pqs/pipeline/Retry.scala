// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.pqs.configuration.ISO8601Duration
import org.apache.commons.lang3.time.DurationFormatUtils
import zio.Schedule.{Decision, Interval}
import zio.ZIO.logInfoCause
import zio.config.magnolia.describe
import zio.metrics.{Metric, MetricLabel}
import zio.{Cause, Duration, Schedule, Trace, ZIO, duration2DurationOps, durationInt}

import java.time.OffsetDateTime
import scala.language.implicitConversions

object Retry:
  case class Config(
      backoff: Backoff,
      counter: Counter
  )
  case class Backoff(
      @describe("Base time (ISO 8601) for backoff retry strategy")
      base: ISO8601Duration = 1.second,
      @describe("Factor for backoff retry strategy")
      factor: Double = 2,
      @describe("Max duration (ISO 8601) between attempts")
      cap: ISO8601Duration = 1.minute
  )
  case class Counter(
      @describe("Max attempts before giving up")
      attempts: Option[Int] = None,
      @describe("Time limit (ISO 8601) before giving up")
      duration: Option[ISO8601Duration] = None,
      @describe("Reset retry counters after period (ISO 8601) of stability")
      reset: ISO8601Duration = 10.minute
  )

  private val restartCounter =
    Metric.counter("app_restarts", "Number of total app restarts due to recoverable errors")
  def retryRecoverable(conf: Config)(
      recoverable: PartialFunction[Throwable, Throwable]
  ): Schedule[Any, Throwable, Any] =
    val backoff     = Schedule.exponential(conf.backoff.base, conf.backoff.factor) || Schedule.spaced(conf.backoff.cap)
    val maxAttempts = conf.counter.attempts.fold(Schedule.forever)(Schedule.recurs)
    val maxTime     = conf.counter.duration.fold(Schedule.forever)(Schedule.upTo(_))

    def pretty(duration: Duration) = DurationFormatUtils.formatDurationWords(duration.toMillis, true, true)
    def getCauses(ex: Throwable): List[Throwable] = Option(ex).fold(Nil)(x => x :: getCauses(x.getCause))
    val onlyRecoverable =
      (Schedule.elapsed && Schedule.count && Schedule.identity[Throwable]).whileOutputZIO { (elapsed, cnt, x) =>
        getCauses(x).collectFirst(recoverable) match
          case Some(ex) =>
            logInfoCause(
              Seq(
                Some(ex.getMessage),
                Some(s"Attempt ${cnt + 1}, unstable for ${pretty(elapsed)}."),
                conf.counter.attempts.map(max => s"Remaining attempts: ${max - cnt}."),
                conf.counter.duration.map(max => s"Remaining time: ${pretty(max.minus(elapsed `min` max))}.")
              ).flatten.mkString(" "),
              Cause.stackless(Cause.fail(ex))
            ) *> restartCounter.tagged(MetricLabel("exception", ex.getMessage)).increment.as(true)
          case None =>
            ZIO.succeed(false)
      }

    val elapsed: Schedule[Any, Any, Duration] = new Schedule[Any, Any, Duration]:
      override type State = Option[OffsetDateTime]
      override def initial = None
      override def step(now: OffsetDateTime, in: Any, state: State)(implicit trace: Trace) = state match
        case None =>
          ZIO.succeed(Some(now), Duration.Zero, Decision.Continue(Interval(now, OffsetDateTime.MAX)))
        case Some(value) =>
          val duration = Duration.fromInterval(value, now)
          ZIO.succeed(Some(now), duration, Decision.Continue(Interval(now, OffsetDateTime.MAX)))

    (backoff *> maxAttempts *> maxTime *> onlyRecoverable *> elapsed).resetWhen(_ > conf.counter.reset)
  end retryRecoverable
