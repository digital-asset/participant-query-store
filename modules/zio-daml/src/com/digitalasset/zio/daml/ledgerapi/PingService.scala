// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.digitalasset.scribe.grpc.ZManagedChannel
import zio.{ZIO, ZLayer}

object PingService:
  val ping: ZIO[ZManagedChannel, Throwable, Unit] =
    VersionServiceClient.live(VersionServiceClient.getLedgerApiVersion(GetLedgerApiVersionRequest()).unit)
