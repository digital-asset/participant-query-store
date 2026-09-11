// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent,
  Reassignment,
  ReassignmentEvent as ProtoReassignmentEvent,
  UnassignedEvent
}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.update_service.ZioUpdateService.UpdateServiceClient
import com.digitalasset.canonical.ReassignmentEvent
import com.digitalasset.canonical.UserRight.AsAnyParty
import com.digitalasset.canonical.specific.EventId
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.canonical.{
  CommandId,
  ContractFilter,
  ContractId,
  DomainId,
  MetadataFilter,
  Party,
  TransactionId,
  WorkflowId
}
import com.digitalasset.zio.daml.ledgerapi.specific.Codecs
import com.digitalasset.transcode.schema.{Dictionary, IdentifierFilter}
import com.digitalasset.transcode.schema.{
  EntityName,
  Identifier,
  ModuleName,
  PackageId,
  PackageName,
  PackageVersion,
  Template
}
import com.digitalasset.zio.daml.KnownEntityIdentifiers
import com.digitalasset.zio.daml.ledgerapi.UpdateServiceClientMock.GetUpdates
import com.google.protobuf.timestamp.Timestamp
import io.grpc.{Status, StatusException}
import scalapb.TimestampConverters
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

  private val pingId =
    Identifier(
      PackageId("pkg1"),
      PackageName("PingPong"),
      PackageVersion("1.0.0"),
      ModuleName("PingPong"),
      EntityName("Ping")
    )

  private val protoPingId =
    com.daml.ledger.api.v2.value.Identifier("pkg1", "PingPong", "Ping")

  private val pingKnownIdsLayer: ULayer[KnownEntityIdentifiers] = ZLayer.succeed {
    new KnownEntityIdentifiers(
      schema = Seq(
        Template[Unit](
          templateId = pingId,
          payload = (),
          key = None,
          isInterface = false,
          implements = Seq.empty,
          choices = Seq.empty
        )
      ),
      contractFilter = ContractFilter(IdentifierFilter.AcceptAll),
      metadataFilter = MetadataFilter(IdentifierFilter.AcceptAll)
    )
  }

  private val recordTime = Timestamp.of(1_700_000_000L, 0)

  private def unassignedEvent(nodeId: Int) =
    ProtoReassignmentEvent.defaultInstance.withUnassigned(
      UnassignedEvent.defaultInstance
        .withReassignmentId("reassignment-1")
        .withContractId("contract-1")
        .withTemplateId(protoPingId)
        .withSource("sync1")
        .withTarget("sync2")
        .withSubmitter("Alice")
        .withReassignmentCounter(1L)
        .withWitnessParties(Seq("Alice"))
        .withNodeId(nodeId)
    )

  private def assignedEvent(nodeId: Int) =
    ProtoReassignmentEvent.defaultInstance.withAssigned(
      AssignedEvent.defaultInstance
        .withReassignmentId("reassignment-1")
        .withSource("sync1")
        .withTarget("sync2")
        .withSubmitter("Alice")
        .withReassignmentCounter(1L)
        .withCreatedEvent(
          CreatedEvent.defaultInstance
            .withContractId("contract-1")
            .withTemplateId(protoPingId)
            .withWitnessParties(Seq("Alice"))
            .withNodeId(nodeId)
        )
    )

  private def reassignmentResponse(offset: Long, events: ProtoReassignmentEvent*): GetUpdatesResponse =
    GetUpdatesResponse.defaultInstance.withReassignment(
      Reassignment.defaultInstance
        .withUpdateId("update-1")
        .withCommandId("command-1")
        .withWorkflowId("workflow-1")
        .withOffset(offset)
        .withRecordTime(recordTime)
        .withSynchronizerId("sync2")
        .withEvents(events)
    )

  private val dummyRight = AsAnyParty

  private def serviceLayer(updateServiceClientLayer: ULayer[UpdateServiceClient]) =
    (updateServiceClientLayer ++ emptyDictionaryLayer ++ emptyKnownIdsLayer)
      >>> ZLayer.fromFunction(UpdateService.apply)

  private def pingServiceLayer(updateServiceClientLayer: ULayer[UpdateServiceClient]) =
    (updateServiceClientLayer ++ emptyDictionaryLayer ++ pingKnownIdsLayer)
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
    ),
    suite("reassignments")(
      test("request subscribes to reassignments alongside transactions"):
        val expectations = GetUpdates(
          assertion("request includes reassignments and transactions")(req =>
            req.updateFormat.exists(f => f.includeReassignments.isDefined && f.includeTransactions.isDefined)
          ),
          value(ZStream.succeed(response(first)))
        )
        (for
          service <- ZIO.service[UpdateService]
          _       <- service.getTransactions(dummyRight, offset(first), offset(first)).runDrain
        yield assertCompletes).provideLayer(pingServiceLayer(expectations.toLayer))
      ,
      test("an unassigned event maps to a canonical transaction carrying ReassignmentEvent.Unassigned"):
        val expectations = GetUpdates(
          anything,
          value(ZStream.succeed(reassignmentResponse(first, unassignedEvent(0))))
        )
        (for
          service <- ZIO.service[UpdateService]
          result  <- service.getTransactions(dummyRight, offset(first), offset(first)).runCollect
        yield
          val tx = result.head
          assertTrue(
            result.length == 1,
            tx.transactionId == TransactionId("update-1"),
            tx.commandId == CommandId("command-1"),
            tx.workflowId == WorkflowId("workflow-1"),
            tx.offset == offset(first),
            // A Reassignment has no ledger effective time, and the canonical model says so.
            tx.effectiveAt.isEmpty,
            tx.domainId.isEmpty,
            tx.events == Chunk(
              ReassignmentEvent.Unassigned(
                eventId = EventId(first, 0),
                reassignmentId = "reassignment-1",
                source = DomainId("sync1"),
                target = DomainId("sync2"),
                submitter = Some(Party("Alice")),
                reassignmentCounter = 1L,
                contractId = ContractId("contract-1"),
                templateId = pingId,
                witnesses = Chunk(Party("Alice")),
                assignmentExclusivity = None
              )
            )
          )
        ).provideLayer(pingServiceLayer(expectations.toLayer))
      ,
      test("an assigned event maps to ReassignmentEvent.Assigned, reading node_id from the created event"):
        val expectations = GetUpdates(
          anything,
          value(ZStream.succeed(reassignmentResponse(first, assignedEvent(3))))
        )
        (for
          service <- ZIO.service[UpdateService]
          result  <- service.getTransactions(dummyRight, offset(first), offset(first)).runCollect
        yield assertTrue(
          result.head.events == Chunk(
            ReassignmentEvent.Assigned(
              eventId = EventId(first, 3),
              reassignmentId = "reassignment-1",
              source = DomainId("sync1"),
              target = DomainId("sync2"),
              submitter = Some(Party("Alice")),
              reassignmentCounter = 1L,
              contractId = ContractId("contract-1"),
              templateId = pingId,
              witnesses = Chunk(Party("Alice"))
            )
          )
        )).provideLayer(pingServiceLayer(expectations.toLayer))
      ,
      test("a reassignment is represented identically in both stream modes"):
        val expectations = GetUpdates(
          anything,
          value(ZStream.succeed(reassignmentResponse(first, unassignedEvent(0))))
        ).twice
        (for
          service  <- ZIO.service[UpdateService]
          acsDelta <- service.getTransactions(dummyRight, offset(first), offset(first)).runCollect
          ledgerFx <- service.getTransactionTrees(dummyRight, offset(first), offset(first)).runCollect
        yield assertTrue(
          acsDelta.map(_.events) == ledgerFx.map(_.events),
          acsDelta.map(_.effectiveAt) == ledgerFx.map(_.effectiveAt)
        )).provideLayer(pingServiceLayer(expectations.toLayer))
      ,
      test("a transaction still carries its ledger effective time"):
        // Guards the widening to Option[Instant]: it must not silently drop the time for the
        // update kind that does have one.
        val effectiveAt = Timestamp.of(1_700_000_500L, 0)
        val expectations = GetUpdates(
          anything,
          value(
            ZStream.succeed(
              GetUpdatesResponse.defaultInstance.withTransaction(
                Transaction.defaultInstance.withOffset(first).withEffectiveAt(effectiveAt)
              )
            )
          )
        )
        (for
          service <- ZIO.service[UpdateService]
          result  <- service.getTransactions(dummyRight, offset(first), offset(first)).runCollect
        yield assertTrue(
          result.head.effectiveAt.contains(TimestampConverters.asJavaInstant(effectiveAt))
        )).provideLayer(pingServiceLayer(expectations.toLayer))
      ,
      test("a batched reassignment yields one event per element with distinct event ids"):
        val expectations = GetUpdates(
          anything,
          value(ZStream.succeed(reassignmentResponse(first, unassignedEvent(0), unassignedEvent(1))))
        )
        (for
          service <- ZIO.service[UpdateService]
          result  <- service.getTransactions(dummyRight, offset(first), offset(first)).runCollect
        yield
          val ids = result.head.events.collect { case e: ReassignmentEvent.Unassigned => e.eventId }
          assertTrue(
            result.length == 1,
            result.head.events.length == 2,
            ids == Chunk(EventId(first, 0), EventId(first, 1))
          )
        ).provideLayer(pingServiceLayer(expectations.toLayer))
    )
  )
