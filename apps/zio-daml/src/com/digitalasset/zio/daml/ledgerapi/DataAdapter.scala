// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

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
  def effectiveAt: Timestamp
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
    override def effectiveAt: Timestamp                       = tx.getEffectiveAt
    override def externalTransactionHash: Option[Array[Byte]] = extractors.externalTransactionHash(tx)
    override def paidTrafficCost: Option[Long]                = tx.paidTrafficCost
    override def traceContext: TraceContext                   = tx.getTraceContext
    override def eventsSize: Int                              = tx.events.size
