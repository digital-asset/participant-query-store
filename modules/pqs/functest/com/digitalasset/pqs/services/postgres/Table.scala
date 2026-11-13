// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

import com.digitalasset.pqs.functest.matchers.Capture
import com.digitalasset.pqs.utils.safeequals.===
import zio.test.Assertion
import zio.test.diff.{Diff, DiffResult}
import zio.jdbc.*

import scala.collection.mutable
import scala.language.implicitConversions

final case class Table private (rows: Seq[Row]):
  require(
    rows.map(_.cells.size).maxOption === rows.map(_.cells.size).minOption,
    s"Table is not rectangular:\n$toString"
  )
  override def toString: String =
    if rows.isEmpty then "<empty>"
    else rows.map(r => r.cells.map(_.value).mkString(" | ")).mkString("\n")
  def transpose: Table   = Table(rows.map(_.cells.map(_.value)).transpose.map(r => Row(r.map(Cell.apply))))
  def map(f: Any => Any) = Table(rows.map(r => Row(r.cells.map(c => Cell(f(c.value))))))

object Table:
  val empty = Table(Seq.empty)

  given Diff[Table] = (x: Table, y: Table) =>
    if x === y
    then DiffResult.Identical(x)
    else if x.toString === y.toString
    then
      val withClass = (v: Any) => s"${v.toString}(${v.getClass})"
      DiffResult.Different(
        x,
        y,
        Some(zio.test.internal.myers.MyersDiff.diff(x.map(withClass).toString, y.map(withClass).toString).toString)
      )
    else DiffResult.Different(x, y, Some(zio.test.internal.myers.MyersDiff.diff(x.toString, y.toString).toString))

  def matchesUnordered(expected: Table): Assertion[Iterable[Any]] =
    expected.rows.map(Assertion.contains).reduceOption(_ && _).getOrElse(Assertion.anything)

  def build(rows: Seq[Row]): Table = new Table(rows)

  /** Table DSL. Usage example:
    * {{{
    * Table {
    *   "column1" | "column2" | "column3"
    *   ---       | ---       | ---
    *   "value 1" | true      | 123
    *   "value 3" | true      | 321
    * }
    * }}}
    *
    * All the rows above the separator are ignored, and can be used as a table header for description purposes.
    */
  def apply(init: Builder ?=> Builder): Table =
    val builder: Builder = Builder()
    init(using builder)
    builder.build()

  class Builder private[Table] ():
    private val rows       = mutable.Buffer.empty[Row]
    private val currentRow = mutable.Buffer.empty[Cell]

    extension (left: Any)
      def |(right: Any): this.type =
        newRow(left)
        addCell(right)

      /** Alternative definition of column separator such that it's possible to express `Int | Int` (or equivalent),
        * which otherwise resolves to bitwise OR.
        */
      def <|>(right: Any): this.type = |(right)

    def |(right: Any): this.type = addCell(right)

    private[Table] def newRow(left: Any): this.type =
      if currentRow.nonEmpty then rows.append(Row(currentRow.toSeq))
      currentRow.clear()
      addCell(left)
    private[Table] def addCell(value: Any): this.type =
      currentRow.append(Cell(value))
      this
    private[Table] def build(): Table =
      if currentRow.nonEmpty then rows.append(Row(currentRow.toSeq))
      new Table(rows.toSeq)
  end Builder

  object Builder:
    given (using ctx: Builder): Conversion[Any, Builder] = any => ctx.newRow(any)
end Table

final case class Row(cells: Seq[Cell])

object Row:
  given JdbcDecoder[Row] = { (ix, rs) =>
    val cells = mutable.Buffer.empty[Cell]
    for ix <- 1 to rs.getMetaData.getColumnCount do {
      val value = rs.getMetaData.getColumnType(ix) match
        case java.sql.Types.BOOLEAN => rs.getBoolean(ix)
        case java.sql.Types.BIT     => rs.getBoolean(ix)
        case java.sql.Types.VARCHAR => rs.getString(ix)
        case java.sql.Types.INTEGER => rs.getInt(ix)
        case java.sql.Types.BIGINT  => rs.getLong(ix)
        case other                  => rs.getString(ix)
      cells.append(Cell(value))
    }
    (rs.getMetaData.getColumnCount, Row(cells.toSeq))
  }

final case class Cell(value: Any | AnyRef):
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  override def equals(that: Any): Boolean = (value, that) match
    case (a: Assertion[Any @unchecked], Cell(v)) => a.test(v)
    case (v, Cell(a: Assertion[Any @unchecked])) => a.test(v)
    case (c: Capture[Any @unchecked], Cell(v))   => c.get == v
    case (v, Cell(c: Capture[Any @unchecked]))   => c.get == v
    case (a, Cell(b))                            => a == b
    case _                                       => false
  override def hashCode(): Int = 0
