// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.micrometer.core.instrument.distribution

import java.util
import java.util.concurrent.atomic.AtomicLongArray

/** A re-implementation of io.micrometer.core.instrument.distribution.FixedBoundaryHistogram that actually enables
  * tracking of doubles instead of longs.
  *
  * @param buckets
  *   the bucket boundaries
  * @param isCumulativeBucketCounts
  *   whether the bucket counts are cumulative
  * @see
  *   io.micrometer.core.instrument.distribution.FixedBoundaryHistogram
  */
class FixedBoundaryHistogramSupportsDoubles(buckets: Array[Double], isCumulativeBucketCounts: Boolean = false):
  private val values = new AtomicLongArray(buckets.length)

  def reset(): Unit =
    for (i <- 0 until values.length)
      values.set(i, 0)

  def record(value: Double): Unit =
    val index = leastLessThanOrEqualTo(value)
    if index > -1 then values.incrementAndGet(index)

  /** Returns the array of {@link CountAtBucket} for each of the buckets tracked by this histogram.
    */
  def getCountAtBuckets(): Array[CountAtBucket] =
    val countAtBuckets  = new Array[CountAtBucket](this.buckets.length)
    var cumulativeCount = 0.0

    for i <- 0.until(buckets.length) do
      val valueAtCurrentBucket = values.get(i)
      if isCumulativeBucketCounts then
        cumulativeCount += valueAtCurrentBucket
        countAtBuckets(i) = new CountAtBucket(buckets(i), cumulativeCount)
      else countAtBuckets(i) = new CountAtBucket(buckets(i), valueAtCurrentBucket.toDouble)
    countAtBuckets

  @SuppressWarnings(Array("org.wartremover.warts.While", "org.wartremover.warts.Return"))
  private def leastLessThanOrEqualTo(key: Double): Int =
    var low  = 0
    var high = buckets.length - 1
    while low <= high do
      (low + high) >>> 1 match
        case mid if buckets(mid) < key => low = mid + 1
        case mid if buckets(mid) > key => high = mid - 1
        case mid                       => return mid // exact match
    if low < buckets.length then low else -1

  private def countAtValue(value: Double): Long =
    val index = util.Arrays.binarySearch(buckets, value)
    if index < 0 then 0 else values.get(index)
