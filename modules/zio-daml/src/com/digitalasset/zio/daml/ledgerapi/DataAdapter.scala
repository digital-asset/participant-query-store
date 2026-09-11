// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.trace_context.TraceContext
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.*
import com.google.protobuf.timestamp.Timestamp

private[ledgerapi] sealed trait DataAdapter[T]:
  def source: T
  def sourceType: String
  def transactionId: String
  def offset: Long
  def commandId: String
  def workflowId: String
  def effectiveAt: Option[Timestamp]
  def externalTransactionHash: Option[Array[Byte]]
  def paidTrafficCost: Option[Long]
  def traceContext: TraceContext
  def eventsSize: Int

private[ledgerapi] object DataAdapter:
  final case class TransactionAdapter(tx: Transaction, txShape: TransactionShape) extends DataAdapter[Transaction]:
    override def source: Transaction = tx
    override def sourceType: String =
      val shape = txShape match
        case TRANSACTION_SHAPE_ACS_DELTA      => "ACS delta"
        case TRANSACTION_SHAPE_LEDGER_EFFECTS => "ledger effects"
        case TRANSACTION_SHAPE_UNSPECIFIED    => "unspecified"
        case Unrecognized(x)                  => s"unrecognized: $x"
      s"transaction ($shape)"
    override def transactionId: String                        = tx.updateId
    override def offset: Long                                 = tx.offset
    override def commandId: String                            = tx.commandId
    override def workflowId: String                           = tx.workflowId
    override def effectiveAt: Option[Timestamp]               = tx.effectiveAt
    override def externalTransactionHash: Option[Array[Byte]] = extractors.externalTransactionHash(tx)
    override def paidTrafficCost: Option[Long]                = tx.paidTrafficCost
    override def traceContext: TraceContext                   = tx.getTraceContext
    override def eventsSize: Int                              = tx.events.size

  final case class ReassignmentAdapter(reassignment: Reassignment) extends DataAdapter[Reassignment]:
    override def source: Reassignment  = reassignment
    override def sourceType: String    = "reassignment"
    override def transactionId: String = reassignment.updateId
    override def offset: Long          = reassignment.offset
    override def commandId: String     = reassignment.commandId
    override def workflowId: String    = reassignment.workflowId
    // A Reassignment carries no ledger effective time: no Daml code is interpreted, so there is
    // nothing for one to be the answer to. `record_time` is a different quantity, set by the
    // synchronizer rather than the submitting participant, and is deliberately not substituted
    // here — see `Effective time` in the design.
    override def effectiveAt: Option[Timestamp]               = None
    override def externalTransactionHash: Option[Array[Byte]] = None // no such field on a Reassignment
    override def paidTrafficCost: Option[Long]                = reassignment.paidTrafficCost
    override def traceContext: TraceContext                   = reassignment.getTraceContext
    override def eventsSize: Int                              = reassignment.events.size
