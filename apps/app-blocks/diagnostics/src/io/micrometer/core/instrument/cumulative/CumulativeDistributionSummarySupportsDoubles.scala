// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.micrometer.core.instrument.cumulative

import io.micrometer.core.instrument.distribution.{
  DistributionStatisticConfig,
  TimeWindowFixedBoundaryHistogramSupportsDoubles
}
import io.micrometer.core.instrument.{Clock, Meter}

class CumulativeDistributionSummarySupportsDoubles(
    id: Meter.Id,
    clock: Clock,
    dsconfig: DistributionStatisticConfig,
    scale: Double,
    supportsAggregablePercentiles: Boolean
) extends CumulativeDistributionSummary(
      id,
      clock,
      dsconfig,
      scale,
      new TimeWindowFixedBoundaryHistogramSupportsDoubles(clock, dsconfig, supportsAggregablePercentiles, true)
    )
