// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.micrometer.core.instrument.distribution

import io.micrometer.core.instrument.Clock

import java.util.Objects
import java.{lang, util}

/** A re-implementation of io.micrometer.core.instrument.distribution.TimeWindowFixedBoundaryHistogram that does not
  * truncate doubles during recording.
  *
  * @param clock
  *   the clock
  * @param config
  *   the configuration
  * @param supportsAggregablePercentiles
  *   whether the histogram supports aggregable percentiles
  * @param isCumulativeBucketCounts
  *   whether the bucket counts are cumulative
  * @see
  *   io.micrometer.core.instrument.distribution.TimeWindowFixedBoundaryHistogram
  */
class TimeWindowFixedBoundaryHistogramSupportsDoubles(
    clock: Clock,
    config: DistributionStatisticConfig,
    supportsAggregablePercentiles: Boolean,
    isCumulativeBucketCounts: Boolean
) extends AbstractTimeWindowHistogram[FixedBoundaryHistogramSupportsDoubles, Unit](
      clock,
      config,
      classOf[FixedBoundaryHistogramSupportsDoubles],
      supportsAggregablePercentiles
    ):
  private val histogramBuckets    = distributionStatisticConfig.getHistogramBuckets(supportsAggregablePercentiles)
  private val percentileHistogram = distributionStatisticConfig.isPercentileHistogram
  if Objects.nonNull(percentileHistogram) && percentileHistogram then
    histogramBuckets.addAll(PercentileHistogramBuckets.buckets(distributionStatisticConfig))
  private val buckets = histogramBuckets.stream.filter(Objects.nonNull).mapToDouble(_.doubleValue).toArray
  initRingBuffer()

  override def newBucket(): FixedBoundaryHistogramSupportsDoubles =
    new FixedBoundaryHistogramSupportsDoubles(buckets, isCumulativeBucketCounts)

  override def recordLong(bucket: FixedBoundaryHistogramSupportsDoubles, value: Long): Unit =
    recordDouble(bucket, value.toDouble)

  override def recordDouble(bucket: FixedBoundaryHistogramSupportsDoubles, value: Double): Unit =
    bucket.record(value)

  override def resetBucket(bucket: FixedBoundaryHistogramSupportsDoubles): Unit = bucket.reset()

  override def newAccumulatedHistogram(ringBuffer: Array[FixedBoundaryHistogramSupportsDoubles]): Unit = ()

  override def accumulate(): Unit = ()

  override def resetAccumulatedHistogram(): Unit = ()

  override def valueAtPercentile(percentile: Double): Double = 0.0

  override def countsAtValues(values: util.Iterator[lang.Double]): util.Iterator[CountAtBucket] =
    currentHistogram.countsAtValues(values)
