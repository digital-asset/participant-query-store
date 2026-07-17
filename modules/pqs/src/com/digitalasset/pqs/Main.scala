// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.app.*
import com.digitalasset.pqs.postgres.document

object Main extends ComposableApp:
  private val Pqs                             = "pqs"
  override def executableName: Option[String] = Some(Pqs)

  def app =
    Pqs @@ Command("An efficient ledger data exporting tool")
      - (pipeline.Main.app | Datastore.app | appversion.Main.app)

  // TODO make backend options discoverable at runtime
  private object Datastore extends ComposableApp:
    def app =
      "datastore" @@ Command("Perform operations supporting a certified data store")
        - document.Main.app /* | relational.Main.app */
  end Datastore

end Main
