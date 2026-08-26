// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.digitalasset.canonical.UserRight.AsAnyParty
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.canonical.{ContractFilter, MetadataFilter}
import com.digitalasset.zio.daml.ledgerapi.specific.Codecs
import com.digitalasset.transcode.schema.{Dictionary, IdentifierFilter}
import com.digitalasset.zio.daml.KnownEntityIdentifiers
import com.digitalasset.zio.daml.ledgerapi.UpdateServiceClientMock.GetUpdates
import io.grpc.{Status, StatusException}
import zio.*
import zio.mock.Expectation.value
import zio.stream.{Take, ZStream}
import zio.test.*
import zio.test.Assertion.{anything, assertion, equalTo, fails}

object UpdateServiceSpec extends ZIOSpecDefault:
  private val first  = 10L
  private val second = 11L
  private val third  = 12L

  private def offset(l: Long) = Offset.Absolute(l)

  private def response(offset: Long): GetUpdatesResponse =
    GetUpdatesResponse.defaultInstance.withTransaction(Transaction.defaultInstance.withOffset(offset))

  private val emptyDictionaryLayer: ULayer[Codecs] = ZLayer.succeed(Dictionary(Seq.empty))

  private val emptyKnownIdsLayer: ULayer[KnownEntityIdentifiers] = ZLayer.succeed {
    new KnownEntityIdentifiers(
      schema = Seq.empty,
      contractFilter = ContractFilter(IdentifierFilter.AcceptAll),
      metadataFilter = MetadataFilter(IdentifierFilter.AcceptAll)
    )
  }

  private val dummyRight = AsAnyParty

  private def serviceLayer(updateServiceClientLayer: ULayer[UpdateServiceClient]) =
    (updateServiceClientLayer ++ emptyDictionaryLayer ++ emptyKnownIdsLayer)
      >>> ZLayer.fromFunction(UpdateService.apply)

  def spec = suite("UpdateService")(
    suite("retry logic")(
      test("restarts from the last offset after token expiry"):
        val failingWithTokenExpired = ZStream(
          Take.single(response(first)),
          Take.single(response(second)),
          Take.fail(new StatusException(Status.ABORTED.withDescription("ACCESS_TOKEN_EXPIRED")))
        ).flattenTake
        val retryStream = ZStream.succeed(response(third))

        val expectationToRetry =
          GetUpdates(
            assertion(s"first call starts at ${Offset.Genesis.toLongOffset}")(
              _.beginExclusive == Offset.Genesis.toLongOffset
            ),
            value(failingWithTokenExpired)
          ) ++
            GetUpdates(assertion(s"second call starts at $second")(_.beginExclusive == second), value(retryStream))
        (for
          service <- ZIO.service[UpdateService]
          result <- service
            .getTransactions(dummyRight, Offset.Genesis, offset(999L))
            .map(_.offset) // keep just the offsets
            .runCollect
        yield assertTrue(
          result == Chunk(offset(first), offset(second), offset(third))
        )).provideLayer(serviceLayer(expectationToRetry.toLayer))
      ,
      test("does not retry on different error rather than token expired - Status.INTERNAL"):
        val internalError = new StatusException(Status.INTERNAL.withDescription("SOME_ERROR"))
        val failingWithInternalError =
          ZStream.succeed(response(first)) ++ ZStream.succeed(response(second)) ++ ZStream.fail(internalError)

        val expectationDONTRetry =
          GetUpdates(anything, value(failingWithInternalError))
        (for
          svc <- ZIO.service[UpdateService]
          exit <- svc
            .getTransactions(dummyRight, offset(1), offset(999))
            .runDrain
            .exit
        yield assert(exit)(fails(equalTo(internalError)))).provideLayer(serviceLayer(expectationDONTRetry.toLayer))
    ),
    suite("happy path case")(
      test("getTransactions - end-inclusive terminates the stream - shape is SHAPE_ACS_DELTA"):
        val streamResponse = ZStream.succeed(response(first)) ++ ZStream.succeed(response(second))

        val expectations = GetUpdates(
          assertion("request uses SHAPE_ACS_DELTA")(
            _.updateFormat
              .flatMap(_.includeTransactions)
              .exists(_.transactionShape == TransactionShape.TRANSACTION_SHAPE_ACS_DELTA)
          ),
          value(streamResponse)
        ).twice
        (for
          service <- ZIO.service[UpdateService]
          result <- service
            .getTransactions(dummyRight, offset(first), offset(second))
            .map(_.offset)
            .runCollect
          completed <- service
            .getTransactions(dummyRight, offset(first), offset(second))
            .runDrain
            .timeout(1.second)
        yield assertTrue(
          result == Chunk(offset(first), offset(second)),
          completed.isDefined
        )).provideLayer(serviceLayer(expectations.toLayer))
      ,
      test("getTransactionTrees - works with the same offsets - shape is LEDGER_EFFECTS"):
        val streamResponse = ZStream.succeed(response(first)) ++ ZStream.succeed(response(second))

        val expectations = GetUpdates(
          assertion("request uses LEDGER_EFFECTS")(
            _.updateFormat
              .flatMap(_.includeTransactions)
              .exists(_.transactionShape == TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS)
          ),
          value(streamResponse)
        )
        (for
          service <- ZIO.service[UpdateService]
          result <- service
            .getTransactionTrees(dummyRight, offset(first), offset(second))
            .map(_.offset)
            .runCollect
        yield assertTrue(
          result == Chunk(offset(first), offset(second))
        )).provideLayer(serviceLayer(expectations.toLayer))
    )
  )
