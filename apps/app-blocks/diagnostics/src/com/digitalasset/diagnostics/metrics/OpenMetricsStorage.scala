// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.diagnostics.metrics

import com.digitalasset.diagnostics
import com.digitalasset.diagnostics.metrics.OpenMetricsStorage.*
import com.digitalasset.diagnostics.util.Ring

import scala.collection.mutable

/** A type of storage for metrics that exposes ingested data according to the rules of OpenMetrics format. Each metric
  * series is backed by a ring buffer of fixed size.
  *
  * @param bufferSize
  *   the number of samples to store for each metric
  * @see
  *   https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md
  */
class OpenMetricsStorage(bufferSize: Int, commonLabels: Labels = Set.empty):
  diagnostics.log(s"Initialising OpenMetrics storage: samples = $bufferSize, labels = $commonLabels")

  private val registry = new mutable.TreeMap[MetricFamilyName, MetricFamily]()
  private val storage  = new mutable.LinkedHashMap[(MetricFamilyName, MetricName), Ring[(Time, Value)]]()

  def ingestUnknown(meta: Metadata): (Time, Value) => Unit = ingest(meta, MetricType.Unknown)
  def ingestGauge(meta: Metadata): (Time, Value) => Unit   = ingest(meta, MetricType.Gauge)

  private def ingest(meta: Metadata, metricType: MetricType)(time: Time, value: Value): Unit =
    val Metadata(name, description, unit, labels, suffixes) = meta
    val f      = MetricFamily(name, metricType, description, unit, suffixes)
    val family = registry.getOrElseUpdate(f.name, f)
    record(family.name, mkMetricName(family.name, labels ++ commonLabels), time, value)

  def ingestCounter(meta: Metadata)(time: Time, value: Value): Unit =
    val Metadata(name, description, unit, labels, suffixes) = meta
    val f      = MetricFamily(name.stripSuffix("_total"), MetricType.Counter, description, unit, suffixes)
    val family = registry.getOrElseUpdate(f.name, f)
    record(family.name, mkMetricName(s"${family.name}_total", labels ++ commonLabels), time, value)

  def ingestHistogram(
      meta: Metadata
  )(time: Time, count: Long, sum: Double, buckets: Seq[(HistogramBucketBoundary, Value)]): Unit =
    val Metadata(name, description, unit, labels, suffixes) = meta
    val f           = MetricFamily(name, MetricType.Histogram, description, unit, suffixes)
    val family      = registry.getOrElseUpdate(f.name, f)
    val finalLabels = labels ++ commonLabels
    (if buckets.nonEmpty then buckets.sortBy(_._1) else Seq(Double.MaxValue -> Value(count.toDouble)))
      .foreach {
        case (bucket, value) =>
          val le =
            if (Double.MaxValue - bucket).abs < 1e6 then "+Inf"
            else bucket.toString
          record(family.name, mkMetricName(s"${family.name}_bucket", finalLabels + ("le" -> le)), time, value)
      }
    record(family.name, mkMetricName(s"${family.name}_count", finalLabels), time, Value(count.toDouble))
    record(family.name, mkMetricName(s"${family.name}_sum", finalLabels), time, Value(sum))

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def expose(): Seq[String] =
    registry.toSeq.flatMap {
      case (_, family) =>
        Seq(s"# TYPE ${family.name} ${family.`type`.toString.toLowerCase}") ++
          family.unit.map(x => s"# UNIT ${family.name} $x").toList ++
          family.description.map(x => s"# HELP ${family.name} $x") ++
          storage.collect {
            case ((familyName, metricName), buffer) if familyName == family.name =>
              buffer.iterator.map { case (time, value) => s"$metricName $value ${f"$time%.3f"}" }
          }.flatten
    } ++ Seq("# EOF")

  private def mkMetricName(name: MetricFamilyName, labels: Labels): MetricName =
    name + labels.toSeq.sorted.map { case (k, v) => s"""$k="$v"""" }.mkString("{", ",", "}")

  private def buffer(family: MetricFamilyName, metric: MetricName) =
    storage.getOrElse(family -> metric, Ring.empty(bufferSize))

  private def record(family: MetricFamilyName, metric: MetricName, time: Time, value: Value): Unit =
    storage.update(family -> metric, buffer(family, metric).push(time -> value)._2)

end OpenMetricsStorage

object OpenMetricsStorage:
  opaque type Time = Double
  object Time:
    def apply(d: Double): Time               = d
    extension (t: Time) def toDouble: Double = t

  opaque type Value = Double
  object Value:
    def apply(d: Double): Value               = d
    extension (v: Value) def toDouble: Double = v

  type Label                   = (String, String)
  type Labels                  = Set[Label]
  type MetricName              = String
  type MetricFamilyName        = String
  type HistogramBucketBoundary = Double

  case class Metadata(
      name: MetricFamilyName,
      description: Option[String] = None,
      unit: Option[String] = None,
      labels: Labels = Set.empty,
      suffixes: Seq[String] = Seq.empty
  )

  private enum MetricType:
    // These are defined by spec but nothing emits them yet in our env - stateset, info, gaugehistogram, summary
    // (future work will be required to support them if such  need arises)
    case Unknown, Gauge, Counter, Histogram

  private case class MetricFamily(
      name: MetricFamilyName,
      `type`: MetricType,
      description: Option[String],
      unit: Option[String]
  )

  private object MetricFamily:
    def apply(
        name: MetricFamilyName,
        `type`: MetricType = MetricType.Unknown,
        description: Option[String] = None,
        unit: Option[String] = None,
        suffixes: Seq[String] = Seq.empty
    ): MetricFamily =
      val mfUnit      = unit.map(_.toLowerCase)
      val finalSuffix = (mfUnit.toList ++ suffixes).map(_.toLowerCase)
      val mfName      = name + (if finalSuffix.nonEmpty then finalSuffix.mkString("_", "_", "") else "")
      new MetricFamily(mfName, `type`, description, mfUnit)

end OpenMetricsStorage
