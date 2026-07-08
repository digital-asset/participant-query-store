// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest

import com.digitalasset.scribe.functest.matchers.Capture
import com.digitalasset.scribe.utils.safeequals.===
import zio.test.Assertion
import zio.test.diff.{Diff, DiffResult}

import scala.collection.mutable
import scala.language.implicitConversions

package object table:

  implicit val tableDiff: Diff[Table] = (x: Table, y: Table) =>
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

  final case class Table(rows: Seq[Row]) {
    require(
      rows.map(_.cells.size).maxOption === rows.map(_.cells.size).minOption,
      s"Table is not rectangular:\n$toString"
    )
    override def toString: String =
      if rows.isEmpty then "<empty>"
      else rows.map(r => r.cells.map(_.value).mkString(" | ")).mkString("\n")
    def transpose: Table   = Table(rows.map(_.cells.map(_.value)).transpose.map(r => Row(r.map(Cell.apply))))
    def map(f: Any => Any) = Table(rows.map(r => Row(r.cells.map(c => Cell(f(c.value))))))
  }
  object Table:
    val empty = Table(Seq.empty)

  final case class Row(cells: Seq[Cell])
  final case class Cell(value: Any | AnyRef) {
    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    override def equals(that: Any): Boolean = (value, that) match
      case (a: Assertion[Any @unchecked], Cell(v)) => a.test(v)
      case (v, Cell(a: Assertion[Any @unchecked])) => a.test(v)
      case (c: Capture[Any @unchecked], Cell(v))   => c.get == v
      case (v, Cell(c: Capture[Any @unchecked]))   => c.get == v
      case (a, Cell(b))                            => a == b
      case _                                       => false
    override def hashCode(): Int = 0
  }

  /** Table DSL. Usage example:
    * {{{
    * import com.digitalasset.scribe.functest.table.*
    * //
    * table {
    *   "column1" | "column2" | "column3"
    *   ---       | ---       | ---
    *   "value 1" | true      | 123
    *   "value 2" | false     | 0         |? TestAspect.ignore
    *   "value 3" | true      | 321
    * }
    * }}}
    *
    * All the rows above the separator are ignored, and can be used as a table header for description purposes.
    *
    * Rows can be conditionally included using `|?` at the end of the row followed by a test aspect.
    */
  def table(init: Ctx ?=> RowDummy.type): Table =
    val rows       = mutable.Buffer.empty[Row]
    val currentRow = mutable.Buffer.empty[Cell]
    given ctx: Ctx = new Ctx:
      def newRow(): Unit =
        if currentRow.nonEmpty then
          if currentRow.forall(_.value === ---) then rows.clear() // header
          else rows.append(Row(currentRow.toSeq))
        currentRow.clear()
      def clearRow(): Unit =
        currentRow.clear()
      def addCell(value: Any): Unit =
        currentRow.append(Cell(value))
    end ctx
    init
    ctx.newRow()
    Table(rows.toSeq)

  extension (dummy: RowDummy.type) //
    def |(right: Any)(using ctx: Ctx): RowDummy.type          = { ctx.addCell(right); dummy }
    def |?(onlyIf: => Boolean)(using ctx: Ctx): RowDummy.type = { if !onlyIf then ctx.clearRow(); dummy }

    /** Alternative definition of column separator such that it's possible to express `Int | Int` (or equivalent), which
      * otherwise resolves to bitwise OR.
      * @param right
      *   the right operand of the column separator
      * @param ctx
      *   the context
      * @return
      *   the dummy row definition
      */
    def <|>(right: Any)(using ctx: Ctx): RowDummy.type = |(right)

  case object `---`
  case object RowDummy:
    given anyToDummy(using ctx: Ctx): Conversion[Any, RowDummy.type] with
      def apply(any: Any): RowDummy.type = any match {
        case RowDummy => RowDummy // safeguard against accidental duplicate implicit conversion
        case _        => ctx.newRow(); ctx.addCell(any); RowDummy
      }

  trait Ctx:
    private[table] def newRow(): Unit
    private[table] def clearRow(): Unit
    private[table] def addCell(value: Any): Unit
  end Ctx

end table
