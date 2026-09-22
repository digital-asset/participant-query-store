// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.*
import com.digitalasset.canonical.{EventId, NodeId}
import com.digitalasset.transcode.schema.ChoiceName
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import io.opentelemetry.api.trace.SpanContext
import org.apache.commons.text.translate.LookupTranslator
import org.postgresql.PGConnection
import zio.ZIO.logTrace
import zio.jdbc.{JdbcDecoder, ZConnection}
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

enum EventType:
  case Create, Archive, Exercise, Assign, Unassign

enum ReassignmentType:
  case Assign, Unassign

sealed trait Model:
  def labels: Set[MetricLabel]

sealed trait Copy extends Model:
  def table: Table
  def row: String

final case class Watermark(
    ix: Long,
    offset: Offset,
    seenAts: Seq[Long],
    txSpans: Seq[DetachedSpan] = Seq.empty,
    persistSpans: Seq[SpanContext] = Seq.empty
) extends Model:
  val labels = l("type" -> "watermark")

given watermarkOrdering: Ordering[Watermark] = Ordering.by(_.ix)
private def l(kv: (String, Any)*)            = kv.map((k, v) => MetricLabel(k, v.toString)).toSet

object Model {
  private val counter = Metric.counter("pipeline_events", "Processed ledger events")

  def prepareStatement(all: Iterable[Model]): ZIO[ZConnection, Throwable, Chunk[Watermark]] = {

    val copies                  = mutable.LinkedHashMap.empty[Table, mutable.ListBuffer[String]]
    val watermarks              = ChunkBuilder.make[Watermark]()
    val txs                     = all.onlyTransactions()
    val batchContents           = all.onlyCopies().groupMapReduce(_.table)(_ => 1L)(_ + _)
    def statAttribute(t: Table) = s"pqs.${t.name}.rows_count" -> batchContents.getOrElse(t, 0L)
    all.foreach {
      case c: Copy      => copies.getOrElseUpdate(c.table, mutable.ListBuffer.empty).addOne(c.row)
      case w: Watermark => watermarks.addOne(w.copy(txSpans = txs.find(_.ix == w.ix).flatMap(_.span).toList))
    }

    val forcedCopies = copies.view
      .map { (table, rows) => (table, rows.view.mkString(lineSeparator())) }
      .toSeq
      .sortBy(_._1.insertOrder)
    val copyIO = ZIO.serviceWithZIO[ZConnection](
      _.access { conn =>
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val api = conn.asInstanceOf[PGRestorableConnection].underlying.asInstanceOf[PGConnection].getCopyAPI
        forcedCopies.foreach { (table, rows) => api.copyIn(table.copyQuery, StringReader(rows)) }
      } <* logTrace(
        s"SQL:$lineSeparator" +
          forcedCopies.map((table, rows) => s"${table.copyQuery}$lineSeparator$rows").mkString(lineSeparator)
      )
    )

    val metricsIO = ZIO.foreachDiscard(
      all.view.filter(_.labels.nonEmpty).groupMapReduce(_.labels)(_ => 1)(_ + _)
    )(
      counter.tagged(_).update(_)
    )

    traces.span("execute SQL") {
      copyIO @@ traces.attributes(
        statAttribute(Transaction),
        statAttribute(Event),
        statAttribute(Contract),
        statAttribute(Exercise),
        statAttribute(DeactivatedContract),
        statAttribute(Reassignment)
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

opaque type EntityTypePk <: Long = Long
object EntityTypePk:
  inline def apply(value: Long): EntityTypePk = value
  given JdbcDecoder[EntityTypePk]             = JdbcDecoder.longDecoder.map(apply)

type PackagePk = Long

sealed trait Table(val name: String, columns: Seq[String], val insertOrder: Int):
  val copyQuery = s"copy $name (${columns.mkString(", ")}) from stdin"

final class Transaction(
    val ix: Long,
    val offset: Offset,
    transactionId: Option[String] = None,
    effectiveAt: Option[Instant] = None,
    synchronizerId: Option[SynchronizerId] = None,
    workflowId: Option[String] = None,
    remoteSpan: Option[(String, String)] = None,
    externalTransactionHash: Option[Array[Byte]] = None,
    paidTrafficCost: Option[Long] = None,
    val span: Option[DetachedSpan] = None
) extends Copy:
  def table = Transaction
  val row = buildRow(
    ix,
    offset.toLong,
    transactionId,
    effectiveAt,
    synchronizerId,
    workflowId,
    remoteSpan,
    externalTransactionHash,
    paidTrafficCost
  )
  val labels: Set[MetricLabel] = l("type" -> "transaction")

object Transaction
    extends Table(
      "__transactions",
      Seq(
        "ix",
        "\"offset\"",
        "transaction_id",
        "effective_at",
        "synchronizer_id",
        "workflow_id",
        "trace_context",
        "external_transaction_hash",
        "paid_traffic_cost"
      ),
      insertOrder = 0
    )

final class Event(
    pk: IdPlaceholder,
    txIx: Long,
    eventId: EventId,
    eventType: EventType
) extends Copy:
  def table  = Event
  val row    = buildRow(pk, txIx, eventId, eventType)
  val labels = Set.empty

object Event
    extends Table(
      "__events",
      Seq("pk", "tx_ix", "event_id", "type"),
      insertOrder = 1
    )

final class Contract(
    qualifiedName: String,
    entityType: EntityTypePk,
    createEventPk: Option[IdPlaceholder],
    createdAtIx: Option[Long],
    assignEventPk: Option[IdPlaceholder],
    assignedAtIx: Option[Long],
    contractId: ContractId,
    synchronizerId: SynchronizerId,
    reassignmentCounter: Long,
    signatories: Seq[Party],
    observers: Seq[Party],
    witnesses: Seq[Party],
    payload: ujson.Value,
    contractKey: Option[ujson.Value],
    contractKeyHash: Option[Array[Byte]],
    metadata: Option[Array[Byte]],
    acsDelta: Boolean,
    packagePk: PackagePk,
    creationPackageId: Option[String]
) extends Copy:
  def table = Contract
  val row = buildRow(
    entityType,
    createEventPk,
    createdAtIx,
    assignEventPk,
    assignedAtIx,
    contractId,
    synchronizerId,
    reassignmentCounter,
    payload,
    contractKey,
    contractKeyHash,
    metadata,
    packagePk,
    creationPackageId,
    signatories,
    observers,
    witnesses,
    !acsDelta
  )
  val labels: Set[MetricLabel] =
    val tpe = if createdAtIx.isDefined then "create" else "assign"
    l("type" -> tpe, "template" -> qualifiedName)

object Contract
    extends Table(
      "__contracts",
      Seq(
        "tpe_pk",
        "create_event_pk",
        "created_at_ix",
        "assign_event_pk",
        "assigned_at_ix",
        "contract_id",
        "synchronizer_id",
        "reassignment_counter",
        "payload",
        "contract_key",
        "contract_key_hash",
        "metadata",
        "package_pk",
        "creation_package_id",
        "signatories",
        "observers",
        "witnesses",
        "divulged_only"
      ),
      insertOrder = 2
    )

final class Exercise(
    qualifiedName: String,
    entityType: EntityTypePk,
    contractEntityType: EntityTypePk,
    exerciseEventPk: IdPlaceholder,
    exercisedAt: Long,
    contractId: ContractId,
    choiceName: ChoiceName,
    argument: ujson.Value,
    result: ujson.Value,
    controllers: Seq[Party],
    witnesses: Seq[Party],
    lastDescendant: NodeId,
    packagePk: PackagePk
) extends Copy:
  def table = Exercise
  val row = buildRow(
    entityType,
    contractEntityType,
    exerciseEventPk,
    exercisedAt,
    contractId,
    argument,
    result,
    controllers,
    witnesses,
    lastDescendant,
    packagePk
  )
  val labels: Set[MetricLabel] = l("type" -> "exercise", "template" -> qualifiedName, "choice" -> choiceName)

object Exercise
    extends Table(
      "__exercises",
      Seq(
        "tpe_pk",
        "contract_tpe_pk",
        "exercise_event_pk",
        "exercised_at_ix",
        "contract_id",
        "argument",
        "result",
        "controllers",
        "witnesses",
        "last_descendant_node_id",
        "package_pk"
      ),
      insertOrder = 3
    )

// Represents a contract deactivation: either an archive or an unassign.
final class DeactivatedContract(
    qualifiedName: String,
    entityType: EntityTypePk,
    contractId: ContractId,
    archiveEventPk: Option[IdPlaceholder],
    archivedAtIx: Option[Long],
    unassignEventPk: Option[IdPlaceholder],
    unassignedAtIx: Option[Long],
    synchronizerId: SynchronizerId
) extends Copy:
  def table = DeactivatedContract
  val row = buildRow(
    entityType,
    contractId,
    archiveEventPk,
    archivedAtIx,
    unassignEventPk,
    unassignedAtIx,
    synchronizerId
  )
  val labels =
    val tpe = if archiveEventPk.isDefined then "archive" else "unassign"
    l("type" -> tpe, "template" -> qualifiedName)

// As opposed to the other tables, __tmp_deactivated_contracts is a staging table
// the underlying __contracts is updated sequentially by __update_watermark_fn SQL function
object DeactivatedContract
    extends Table(
      "__tmp_deactivated_contracts",
      Seq(
        "tpe_pk",
        "contract_id",
        "archive_event_pk",
        "archived_at_ix",
        "unassign_event_pk",
        "unassigned_at_ix",
        "synchronizer_id"
      ),
      insertOrder = 4
    )

final class Reassignment(
    entityType: EntityTypePk,
    reassignmentEventPk: IdPlaceholder,
    reassignedAtIx: Long,
    reassignmentType: ReassignmentType,
    contractId: ContractId,
    reassignmentId: String,
    source: SynchronizerId,
    target: SynchronizerId,
    submitter: Option[Party],
    reassignmentCounter: Long,
    witnesses: Seq[Party],
    assignmentExclusivity: Option[Instant]
) extends Copy:
  def table = Reassignment
  val row = buildRow(
    entityType,
    reassignmentEventPk,
    reassignedAtIx,
    reassignmentType,
    contractId,
    reassignmentId,
    source,
    target,
    submitter,
    reassignmentCounter,
    witnesses,
    assignmentExclusivity
  )
  val labels: Set[MetricLabel] = l("type" -> reassignmentType.toString.toLowerCase())

object Reassignment
    extends Table(
      "__reassignments",
      Seq(
        "contract_tpe_pk",
        "reassign_event_pk",
        "reassigned_at_ix",
        "type",
        "contract_id",
        "reassignment_id",
        "source_synchronizer_id",
        "target_synchronizer_id",
        "submitter",
        "reassignment_counter",
        "witnesses",
        "assignment_exclusivity"
      ),
      insertOrder = 5
    )

extension (models: Iterable[Model])
  def onlyTransactions(): Iterable[Transaction] = models.view.collect { case t: Transaction => t }
  def onlyCopies(): Iterable[Copy]              = models.view.collect { case t: Copy => t }

extension (tx: Transaction)
  def ifTraced[R, E, A](zio: DetachedSpan => ZIO[R, E, A]) = ZIO.whenCase(tx.span) { case Some(s) => zio(s) }

// utils
private opaque type RowValue <: String = String
private object RowValue:
  inline def apply(value: String): RowValue = value

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

  type Converter[A] = Conversion[A, RowValue]

  given Converter[EntityTypePk]    = value => RowValue(value.toString)
  given Converter[EventId]         = value => RowValue(value.toString)
  given Converter[Boolean]         = value => RowValue(value.toString)
  given [A: Numeric]: Converter[A] = value => RowValue(value.toString)
  given Converter[String]          = value => RowValue(escaper.translate(value))
  given Converter[ContractId]      = value => RowValue(value)
  given Converter[SynchronizerId]  = value => RowValue(value)
  given Converter[Party]           = value => RowValue(value)
  given Converter[IdPlaceholder]   = value => RowValue(value.id.toString)
  given Converter[ujson.Value]     = value => RowValue(escaper.translate(value.toString))
  given [A]: Converter[(A, A)]     = value => RowValue(s"(\"${value._1}\",\"${value._2}\")")

  given Converter[Array[Byte]] =
    val HEX_DIGITS = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    value =>
      if value.isEmpty then ""
      else
        val sb = StringBuilder()
        sb.append("\\\\x")
        value.foreach(b => sb.append(HEX_DIGITS(b >> 4 & 15)).append(HEX_DIGITS(b & 15)))
        RowValue(sb.result())

  given Converter[Instant] =
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXX")
    value => RowValue(value.atZone(ZoneOffset.UTC).format(fmt))

  given [A: Converter]: Converter[Option[A]] =
    case Some(value) => value: RowValue
    case None        => "\\N"

  given [A: Converter]: Converter[Seq[A]] =
    value => value.map(v => v: RowValue).mkString("{", ",", "}")

  given Converter[EventType] =
    case EventType.Create   => RowValue("create")
    case EventType.Archive  => RowValue("archive")
    case EventType.Exercise => RowValue("exercise")
    case EventType.Assign   => RowValue("assign")
    case EventType.Unassign => RowValue("unassign")

  given Converter[ReassignmentType] =
    case ReassignmentType.Assign   => RowValue("assign")
    case ReassignmentType.Unassign => RowValue("unassign")

private def buildRow(values: RowValue*): String = values.mkString("\t")
