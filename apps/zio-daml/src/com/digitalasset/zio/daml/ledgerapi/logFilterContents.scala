// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.digitalasset.zio.daml.KnownEntityIdentifiers
import zio.ZIO
import zio.ZIO.{logDebug, logInfo}

object logFilterContents:
  def apply(knownIds: KnownEntityIdentifiers) = {
    val ids = knownIds.filtered
    val summary =
      if knownIds.includesAll then
        s"Contract filter: wildcard (all ${ids.templates.size} templates, ${ids.interfaces.size} interfaces)"
      else s"Contract filter inclusive of ${ids.templates.size} templates and ${ids.interfaces.size} interfaces"
    logInfo(summary)
      *> ZIO.foreachDiscard(ids.templates)(id => logDebug(s"Including template ${id.uniqueName}"))
      *> ZIO.foreachDiscard(ids.interfaces)(id => logDebug(s"Including interface ${id.uniqueName}"))
      *> ZIO.foreachDiscard(ids.metadata)(id => logDebug(s"Capturing metadata for ${id.uniqueName}"))
  }
