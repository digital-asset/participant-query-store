// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction.Transaction

object extractors:
  def externalTransactionHash(tx: Transaction): Option[Array[Byte]] =
    tx.externalTransactionHash.map(_.toByteArray)

  def acsDelta(evt: CreatedEvent): Boolean = evt.acsDelta
