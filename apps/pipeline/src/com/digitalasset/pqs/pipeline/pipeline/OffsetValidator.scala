// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline.pipeline

import com.digitalasset.canonical.specific.Offset
import com.digitalasset.canonical.specific.Offset.order.*
import com.digitalasset.pqs.utils.safeequals.===
import zio.{Task, ZIO}

object OffsetValidator {

  private def verify(cond: Boolean, msg: String): Task[Unit] = ZIO.fail(Throwable(msg)).unless(cond).unit
  def validate(
      requestedStart: Offset,
      requestedEnd: Offset,
      dbStart: Offset,
      dbEnd: Offset,
      ledgerStart: Offset,
      ledgerEnd: Offset
  ): Task[Unit] =
    ZIO.collectAllDiscard(
      Seq(
        verify(
          requestedStart <= requestedEnd,
          s"Requested end '$requestedEnd' must be greater than or equal to start '$requestedStart'."
        ),
        verify(
          ledgerStart <= requestedStart && requestedStart <= ledgerEnd,
          s"Requested start '$requestedStart' is outside of ledger history '$ledgerStart...$ledgerEnd'."
        ),
        verify(
          ledgerStart <= requestedEnd && requestedEnd <= ledgerEnd || requestedEnd === Offset.Infinity,
          s"Requested end '$requestedEnd' is outside of ledger history '$ledgerStart...$ledgerEnd'."
        ),
        verify(
          requestedStart >= dbStart,
          s"Cannot prepend to existing datastore. Requested start '$requestedStart', datastore start '$dbStart'."
        ),
        verify(
          requestedStart <= dbEnd || dbEnd == Offset.Genesis,
          s"Requested offsets '$requestedStart...$requestedEnd' will produce gap in datastore history '$dbStart...$dbEnd'."
        ),
        verify(
          dbEnd >= ledgerStart || dbEnd == Offset.Genesis,
          s"There is an unrecoverable gap between datastore end '$dbEnd' and ledger start '$ledgerStart'."
        )
      )
    )
}
