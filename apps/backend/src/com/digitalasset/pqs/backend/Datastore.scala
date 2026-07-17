// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.backend

import com.digitalasset.canonical.ReassignmentEvent
import com.digitalasset.canonical.specific.{Event, Offset, Transaction, TreeEvent}
import com.digitalasset.pqs.backend.Datastore.ProcessingSink
import zio.Task
import zio.stream.ZSink

/** Defines common interface for pluggable data stores. The bridge will ask for the last known position of the data
  * store.
  *
  * The data store should return None if no check point is present (i.e. it is fresh). In this case bridge will ask to
  * process ACS first to rehydrate the state.
  *
  * The bridge will then ask to process the remaining (potentially infinite) stream of transactions starting from the
  * last known offset or offset acquired in the ACS.
  */
trait Datastore:
  /** Register this instance as active writer and removes partial transactions after the latest watermark/checkpoint.
    *
    * Abrupt termination of PQS can leave partial transactions there, so cleaning them up ensures transactions can be
    * safely re-inserted from the latest watermark onwards.
    */
  def registerActiveWriterAndCleanupTransactions: Task[Unit]

  /** Retrieve first known position of the data store if present. This is offset and ordinal index of the first
    * transaction.
    */
  def getFirstCheckpoint: Task[Datastore.Checkpoint]

  /** Retrieve last known position of the data store if present. This is offset and ordinal index of the last
    * transaction.
    */
  def getLastCheckpoint: Task[Datastore.Checkpoint]

  /** Process contract payloads from the ACS. The first item is Genesis, the last item is the absolute offset, which is
    * expected to be returned in the subsequent call to `getLastCheckpoint`
    */
  def processAcs: ProcessingSink[Event.Created | Offset]

  /** Process the remaining transactions */
  def processTransactions
      : ProcessingSink[(Transaction[Event | TreeEvent | ReassignmentEvent], Datastore.TransactionIndex)]
end Datastore

object Datastore:
  type TransactionIndex  = Long
  type Checkpoint        = (Offset, TransactionIndex)
  type ProcessingSink[A] = ZSink[Any, Throwable, A, Nothing, Unit]
end Datastore
