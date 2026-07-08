// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import com.digitalasset.scribe.app.*
import com.digitalasset.scribe.postgres.document

object Main extends ComposableApp:
  private val Scribe                          = "scribe"
  override def executableName: Option[String] = Some(Scribe)

  def app =
    Scribe @@ Command("An efficient ledger data exporting tool")
      - (pipeline.Main.app | Datastore.app | appversion.Main.app)

  // TODO make backend options discoverable at runtime
  private object Datastore extends ComposableApp:
    def app =
      "datastore" @@ Command("Perform operations supporting a certified data store")
        - document.Main.app /* | relational.Main.app */
  end Datastore

end Main
