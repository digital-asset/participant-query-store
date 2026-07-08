// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.diagnostics.metrics.micrometer

import com.digitalasset.diagnostics
import com.digitalasset.diagnostics.metrics.OpenMetricsStorage
import com.digitalasset.diagnostics.metrics.OpenMetricsStorage.{Time, Value}
import io.micrometer.core.instrument.*
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.cumulative.*
import io.micrometer.core.instrument.distribution.pause.PauseDetector
import io.micrometer.core.instrument.distribution.{CountAtBucket, DistributionStatisticConfig}
import io.micrometer.core.instrument.internal.{DefaultGauge, DefaultMeter}
import io.micrometer.core.instrument.push.{PushMeterRegistry, PushRegistryConfig}
import org.apache.commons.lang3.concurrent.BasicThreadFactory

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.{ToDoubleFunction, ToLongFunction}
import java.lang
import scala.jdk.CollectionConverters.*

/** An implementation of meter registry that periodically exports MicroMeter-backed metrics into storage capable of
  * rendering them in OpenMetrics format on demand.
  *
  * @param conf
  *   the configuration for the registry
  */
class OpenMetricsBridge(conf: OpenMetricsBridge.Config, storage: OpenMetricsStorage)
    extends PushMeterRegistry(conf, Clock.SYSTEM):
  diagnostics.log(s"Starting Micrometer to OpenMetrics bridge: interval = ${conf.step()}")

  config.namingConvention(NamingConvention.snakeCase)
  start(
    new BasicThreadFactory.Builder()
      .namingPattern("diagnostics-metrics-collector-%d")
      .daemon(true)
      .priority(Thread.MAX_PRIORITY)
      .build()
  )
  new io.micrometer.core.instrument.binder.jvm.JvmGcMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.system.UptimeMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.system.ProcessorMetrics().bindTo(this)
  new io.micrometer.core.instrument.binder.system.FileDescriptorMetrics().bindTo(this)

  override def close(): Unit =
    diagnostics.log(s"Shutting down Micrometer to OpenMetrics bridge")
    super.close()

  override def publish(): Unit =
    val time = Time(clock.wallTime() / 1e3)
    if conf.enabled() then
      getMeters.forEach { m =>
        val meta = convert(m)
        m.use(
          gauge => storage.ingestGauge(meta)(time, Value(gauge.value)),
          counter => storage.ingestCounter(meta)(time, Value(counter.count)),
          timer =>
            val snap = timer.takeSnapshot
            storage.ingestHistogram(meta)(time, snap.count, snap.total, convertBuckets(snap.histogramCounts))
          ,
          summary =>
            val snap = summary.takeSnapshot
            storage.ingestHistogram(meta)(time, snap.count, snap.total, convertBuckets(snap.histogramCounts))
            // `_max` needs to be a standalone gauge, not part of the histogram family itself as per OpenMetrics spec
            storage.ingestGauge(meta.copy(suffixes = Seq("max")))(time, Value(snap.max))
          ,
          // The ones below are funky so we map them to Unknown until we actually see them in the wild
          longTaskTimer => storage.ingestUnknown(meta)(time, Value(longTaskTimer.takeSnapshot.count.toDouble)),
          timeGauge => storage.ingestUnknown(meta)(time, Value(timeGauge.value)),
          fnCounter => storage.ingestUnknown(meta)(time, Value(fnCounter.count)),
          fnTimer => storage.ingestUnknown(meta)(time, Value(fnTimer.count)),
          fallback =>
            val value = fallback.measure.asScala.toList.headOption.map(_.getValue).getOrElse(0.0)
            storage.ingestUnknown(meta)(time, Value(value))
        )
      }

  private def convert(m: Meter) =
    OpenMetricsStorage.Metadata(
      name = getConventionName(m.getId),
      description = Option(m.getId.getDescription).filter(_.trim.nonEmpty),
      unit = Option(m.getId.getBaseUnit).filter(_.trim.nonEmpty),
      labels = getConventionTags(m.getId).asScala.map(t => t.getKey -> t.getValue).toSet
    )

  private def convertBuckets(buckets: Array[CountAtBucket]) =
    buckets.toSeq.map(cab => cab.bucket -> Value(cab.count))

  override def newGauge[T](id: Meter.Id, obj: T, valueFunction: ToDoubleFunction[T]): Gauge =
    new DefaultGauge[T](id, obj, valueFunction)

  override def newCounter(id: Meter.Id): Counter =
    new CumulativeCounter(id)

  override def newTimer(id: Meter.Id, dsconfig: DistributionStatisticConfig, pauseDetector: PauseDetector): Timer =
    new CumulativeTimer(id, clock, dsconfig, pauseDetector, getBaseTimeUnit, false)

  override def newDistributionSummary(
      id: Meter.Id,
      dsconfig: DistributionStatisticConfig,
      scale: Double
  ): DistributionSummary =
    new CumulativeDistributionSummarySupportsDoubles(id, clock, dsconfig, scale, false)

  override def newMeter(id: Meter.Id, meterType: Meter.Type, measurements: lang.Iterable[Measurement]): Meter =
    new DefaultMeter(id, meterType, measurements)

  override def newFunctionTimer[T](
      id: Meter.Id,
      obj: T,
      countFunction: ToLongFunction[T],
      totalTimeFunction: ToDoubleFunction[T],
      totalTimeFunctionUnit: TimeUnit
  ): FunctionTimer =
    new CumulativeFunctionTimer[T](id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit, getBaseTimeUnit)

  override def newFunctionCounter[T](id: Meter.Id, obj: T, countFunction: ToDoubleFunction[T]): FunctionCounter =
    new CumulativeFunctionCounter[T](id, obj, countFunction)

  override def getBaseTimeUnit: TimeUnit = TimeUnit.SECONDS

  override def defaultHistogramConfig: DistributionStatisticConfig =
    DistributionStatisticConfig.builder
      .minimumExpectedValue(1e-6)
      .expiry(Duration.ofDays(100L * 365L))
      .bufferLength(1)
      .build
      .merge(DistributionStatisticConfig.DEFAULT)

end OpenMetricsBridge

object OpenMetricsBridge:
  abstract class Config extends PushRegistryConfig:
    override def prefix(): String = "diagnostics"
  end Config

  object Config:
    def apply(step: Duration): Config =
      apply(s"${step.toSeconds}s")

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def apply(step: String): Config =
      case "diagnostics.step" => step
      case _                  => null
  end Config
end OpenMetricsBridge
