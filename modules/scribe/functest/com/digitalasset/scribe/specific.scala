// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import zio.jdbc.{SqlFragment, sqlInterpolator}

object specific:
  type OffsetType = Long

  val eventIdSqlType  = "USER-DEFINED"
  val offsetSqlType   = "bigint"
  val offsetScalaType = "long"

  val biggestOffset: OffsetType  = Long.MaxValue
  val smallestOffset: OffsetType = 0

  def offsetSqlFragment(value: String) = sql"$value::bigint"

  val nonExistenceEventId = s"($biggestOffset,${Int.MaxValue})"
