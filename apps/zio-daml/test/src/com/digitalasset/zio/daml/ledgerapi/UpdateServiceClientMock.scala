// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.daml.ledger.api.v2.update_service.*
import zio.mock.*
import zio.stream.ZStream
import zio.*
import io.grpc.StatusException

object UpdateServiceClientMock extends Mock[UpdateServiceClient]:
  object GetUpdates        extends Stream[GetUpdatesRequest, StatusException, GetUpdatesResponse]
  object GetUpdateByOffset extends Effect[GetUpdateByOffsetRequest, StatusException, GetUpdateResponse]
  object GetUpdateById     extends Effect[GetUpdateByIdRequest, StatusException, GetUpdateResponse]
  object GetUpdatePages    extends Effect[GetUpdatesPageRequest, StatusException, GetUpdatesPageResponse]
  object GetUpdateByHash   extends Effect[GetUpdateByHashRequest, StatusException, GetUpdateResponse]

  override val compose: URLayer[Proxy, UpdateServiceClient] =
    ZLayer {
      for proxy <- ZIO.service[Proxy]
      yield new UpdateServiceClient:
        override def getUpdates(request: GetUpdatesRequest)               = ZStream.unwrap(proxy(GetUpdates, request))
        override def getUpdateByOffset(request: GetUpdateByOffsetRequest) = proxy(GetUpdateByOffset, request)
        override def getUpdateById(request: GetUpdateByIdRequest)         = proxy(GetUpdateById, request)
        override def getUpdatesPage(request: GetUpdatesPageRequest): IO[StatusException, GetUpdatesPageResponse] =
          proxy(GetUpdatePages, request)
        override def getUpdateByHash(request: GetUpdateByHashRequest) = proxy(GetUpdateByHash, request)
    }
