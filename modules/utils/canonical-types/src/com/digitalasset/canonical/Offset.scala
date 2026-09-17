// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canonical

import com.digitalasset.pqs.utils.safeequals.===

enum Offset extends Ordered[Offset]:
  case Genesis
  case Absolute(offset: Long)
  case Infinity

  override def compare(that: Offset): Int =
    (this, that) match
      case (a, b) if a === b                        => 0
      case (Offset.Genesis, _)                      => -1
      case (_, Offset.Genesis)                      => 1
      case (Offset.Absolute(a), Offset.Absolute(b)) => a.compareTo(b)
      case (Offset.Infinity, _)                     => 1
      case (_, Offset.Infinity)                     => -1

  override def toString: String = this match
    case Offset.Genesis          => "GENESIS"
    case Offset.Infinity         => "INFINITY"
    case Offset.Absolute(offset) => offset.toString

  def toEndLedgerOffset: Option[Long] = Option(toLong).filter(_ != Long.MaxValue)

  def toLong: Long = this match
    case Offset.Genesis          => 0L
    case Offset.Absolute(offset) => offset
    case Offset.Infinity         => Long.MaxValue
