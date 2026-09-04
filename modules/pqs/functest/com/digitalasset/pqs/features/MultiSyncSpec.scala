// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.features.nuck

import com.digitalasset.pqs.functest.FuncTestStandalone
import com.digitalasset.pqs.services.daml.DamlSdk
import com.digitalasset.pqs.services.daml.DamlSdk.onlyCantonVersion

object MultiSyncSpec extends FuncTestStandalone:
  def spec = suite("Multi-Sync")(
    funcTest("Contract is created, reassigned and archived") {
      Given:
        DamlSdk.multiSyncLedger
    }
  ) @@ onlyCantonVersion(">=3.5")
