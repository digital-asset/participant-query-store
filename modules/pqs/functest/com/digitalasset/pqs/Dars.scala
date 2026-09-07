// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.services.daml.DamlSource

object Dars:
  private[pqs] val pingPongTransact = DamlSource(
    "PingPong" ->
      """module PingPong where
        |
        |import Daml.Script
        |import DA.Functor (void)
        |
        |template Ping
        |  with
        |    owner: Party
        |  where
        |    signatory owner
        |
        |transact : Party -> Script ()
        |transact party = void do
        |  submit party $ createCmd Ping with owner = party
        |""".stripMargin
  )
