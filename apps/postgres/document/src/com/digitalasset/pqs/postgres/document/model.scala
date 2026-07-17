// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.*
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import com.digitalasset.pqs.postgres.document.specific
import com.digitalasset.pqs.postgres.document.specific.ValueConverter
import io.opentelemetry.api.trace.SpanContext
import org.apache.commons.text.translate.LookupTranslator
import org.postgresql.PGConnection
import ujson.Value
import zio.ZIO.logTrace
import zio.jdbc.ZConnection
import zio.jdbc.shims.postgres.PGRestorableConnection
import zio.metrics.{Metric, MetricLabel}
import zio.{Chunk, ChunkBuilder, ZIO}

import java.io.StringReader
import java.lang.System.lineSeparator
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

object model {
  sealed trait Model { def labels: Set[MetricLabel] }
  sealed trait Copy extends Model { def _table: Table; def _sql: String; def _row: String }
  final case class Watermark(
      ix: Long,
      offset: Offset,
      seenAts: Seq[Long],
      txSpans: Seq[DetachedSpan] = Seq.empty,
      persistSpans: Seq[SpanContext] = Seq.empty
  ) extends Model {
    val labels = l("type" -> "watermark")
  }

  given watermarkOrdering: Ordering[Watermark] = Ordering.by(_.ix)
  private def l(kv: (String, Any)*)            = kv.map((k, v) => MetricLabel(k, v.toString)).toSet

  object Model {
    private val counter = Metric.counter("pipeline_events", "Processed ledger events")

    def prepareStatement(all: Iterable[Model]): ZIO[ZConnection, Throwable, Chunk[Watermark]] = {

      val copies                  = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[String]]
      val watermarks              = ChunkBuilder.make[Watermark]()
      val txs                     = all.onlyTransactions()
      val batchContents           = all.onlyCopies().groupMapReduce(_._table)(_ => 1L)(_ + _)
      def statAttribute(t: Table) = s"pqs.${t.name}.rows_count" -> batchContents.getOrElse(t, 0L)
      all.foreach {
        case c: Copy      => copies.getOrElseUpdate(c._sql, mutable.ListBuffer.empty).addOne(c._row)
        case w: Watermark => watermarks.addOne(w.copy(txSpans = txs.find(_.ix == w.ix).flatMap(_.span).toList))
      }

      val forcedCopies = copies.view
        .map { (sql, rows) => (sql, rows.view.mkString(lineSeparator())) }
        .toSeq
        .sortBy(_._1)
      val copyIO = ZIO.serviceWithZIO[ZConnection](
        _.access { conn =>
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val api = conn.asInstanceOf[PGRestorableConnection].underlying.asInstanceOf[PGConnection].getCopyAPI
          forcedCopies.foreach { (sql, rows) => api.copyIn(sql, StringReader(rows)) }
        } <* logTrace(
          s"SQL:${lineSeparator()}" +
            s"${forcedCopies.map(x => s"${x._1}${lineSeparator()}${x._2}").mkString(lineSeparator())}"
        )
      )

      val metricsIO = ZIO.foreachDiscard(
        all.view.filter(_.labels.nonEmpty).groupMapReduce(_.labels)(_ => 1)(_ + _)
      )(
        counter.tagged(_).update(_)
      )

      traces.span("execute SQL") {
        copyIO @@ traces.attributes(
          statAttribute(Table.Transactions),
          statAttribute(Table.Events),
          statAttribute(Table.Contracts),
          statAttribute(Table.Exercises),
          statAttribute(Table.Archives)
        )
      } *>
        metricsIO *>
        ZIO.foreachDiscard(txs.flatMap(_.span)) { s =>
          s.linkToCurrentSpan("target" -> "↧ persist to datastore") *>
            s.addEvent("flushed transaction model SQL to datastore")
        } *>
        traces.currentSpan().map { s => watermarks.result().map(_.copy(persistSpans = Seq(s.getSpanContext))) }
    }
  }

  enum Table(val name: String):
    case Transactions extends Table("__transactions")
    case Events       extends Table("__events")
    case Contracts    extends Table("__contracts")
    case Exercises    extends Table("__exercises")
    case Archives     extends Table("__archives")

  final class Transaction(tx: specific.Transaction, val span: Option[DetachedSpan] = None) extends Copy:
    val _table                   = Table.Transactions
    val _sql                     = s"/*0*/ copy ${_table.name} (${tx.columns.mkString(", ")}) from stdin"
    val _row                     = tx.rowValues
    val labels: Set[MetricLabel] = l("type" -> "transaction")
    val ix: Long                 = tx.ix
    val offset: Offset           = tx.offset

  type EntityTypePk = Long
  type PackagePk    = Long

  final class Event(ev: specific.Event) extends Copy:
    val _table = Table.Events
    val _sql   = s"/*1*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels = Set.empty

  final class Contract(ev: specific.Contract) extends Copy:
    val _table = Table.Contracts
    val _sql   = s"/*2*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels: Set[MetricLabel] =
      l("type" -> "create", "template" -> ev.qualifiedName)

  final class Exercise(ev: specific.Exercise) extends Copy:
    val _table = Table.Exercises
    val _sql   = s"/*3*/ copy ${_table.name} (${ev.columns.mkString(", ")}) from stdin"
    val _row   = ev.rowValues
    val labels: Set[MetricLabel] =
      l("type" -> "exercise", "template" -> ev.qualifiedName, "choice" -> ev.choiceName)

  final class Archive(
      qualifiedName: String,
      entityType: EntityTypePk,
      eventPk: IdPlaceholder,
      txIx: Long,
      contractId: ContractId,
      packagePk: PackagePk
  ) extends Copy {
    val _table = Table.Archives
    val _sql =
      s"/*4*/ copy ${_table.name} (archive_event_pk, archived_at_ix, contract_id, tpe_pk, package_pk) from stdin"
    val _row = values(eventPk)(txIx)(contractId)(entityType)(packagePk)
    val labels: Set[MetricLabel] =
      l("type" -> "archive", "template" -> qualifiedName)
  }

  extension (models: Iterable[Model])
    def onlyTransactions(): Iterable[Transaction] = models.view.collect { case t: Transaction => t }
    def onlyCopies(): Iterable[Copy]              = models.view.collect { case t: Copy => t }

  extension (tx: Transaction)
    def ifTraced[R, E, A](zio: DetachedSpan => ZIO[R, E, A]) = ZIO.whenCase(tx.span) { case Some(s) => zio(s) }

  // utils

  private[document] def values: RowValues           = RowValues()
  private given conv: Conversion[RowValues, String] = _.toString
  private[document] class RowValues {
    private val sb                = StringBuilder()
    override def toString: String = sb.result()
    def apply[A](value: A)(using conv: ValueConverter[A]): RowValues =
      if sb.nonEmpty then sb.append("\t")
      sb.append(conv.convert(value))
      this
  }

  // https://www.postgresql.org/docs/current/sql-copy.html
  private val escaper = new LookupTranslator(
    Map(
      "\b"     -> "\\b",
      "\f"     -> "\\f",
      "\n"     -> "\\n",
      "\r"     -> "\\r",
      "\t"     -> "\\t",
      "\u000b" -> "\\v",
      "\\"     -> "\\\\"
    ).asJava
  )

  private[document] given booleanConverter: ValueConverter[Boolean]       = value => value.toString
  private[document] given numericConverter[A: Numeric]: ValueConverter[A] = value => value.toString
  private[document] given stringConverter: ValueConverter[String]         = value => escaper.translate(value)
  private[document] given contractIdConverter: ValueConverter[ContractId] = value => value
  private[document] given domainIdConverter: ValueConverter[DomainId]     = value => value
  private[document] given partyConverter: ValueConverter[Party]           = value => value
  private[document] given idConverter: ValueConverter[IdPlaceholder]      = value => value.id.toString
  private[document] given jsonConverter: ValueConverter[Value]            = value => escaper.translate(value.toString)
  private[document] given tuple2Converter[A]: ValueConverter[(A, A)] = value => s"(\"${value._1}\",\"${value._2}\")"

  private[document] given byteArrayConverter: ValueConverter[Array[Byte]] =
    val HEX_DIGITS = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    value =>
      if value.isEmpty then ""
      else
        val sb = StringBuilder()
        sb.append("\\\\x")
        value.foreach(b => sb.append(HEX_DIGITS(b >> 4 & 15)).append(HEX_DIGITS(b & 15)))
        sb.result()

  private[document] given instantConverter: ValueConverter[Instant] =
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXX")
    value => value.atZone(ZoneOffset.UTC).format(fmt)

  private[document] given optionalConverter[A: ValueConverter]: ValueConverter[Option[A]] =
    case Some(value) => implicitly[ValueConverter[A]].convert(value)
    case None        => "\\N"

  private[document] given iterableConverter[A: ValueConverter]: ValueConverter[Seq[A]] =
    value => value.map(implicitly[ValueConverter[A]].convert).mkString("{", ",", "}")

  enum EventType:
    case Create; case Archive; case Exercise
  private[document] given eventTypeConverter: ValueConverter[EventType] =
    case EventType.Create   => "create"
    case EventType.Archive  => "archive"
    case EventType.Exercise => "exercise"
}
