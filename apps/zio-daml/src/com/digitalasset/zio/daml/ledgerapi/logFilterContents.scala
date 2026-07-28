// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.digitalasset.zio.daml.KnownEntityIdentifiers
import zio.ZIO
import zio.ZIO.{logDebug, logInfo}

object logFilterContents:
  def apply(knownIds: KnownEntityIdentifiers) = {
    val ids = knownIds.filtered
    logInfo(s"Contract filter inclusive of ${ids.templates.size} templates and ${ids.interfaces.size} interfaces")
      *> ZIO.foreachDiscard(ids.templates)(id => logDebug(s"Including template ${id.uniqueName}"))
      *> ZIO.foreachDiscard(ids.interfaces)(id => logDebug(s"Including interface ${id.uniqueName}"))
      *> ZIO.foreachDiscard(ids.metadata)(id => logDebug(s"Capturing metadata for ${id.uniqueName}"))
  }
