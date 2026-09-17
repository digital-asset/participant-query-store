// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.value.Value
import com.digitalasset.canonical.*
import com.digitalasset.transcode.schema
import com.digitalasset.transcode.schema.*
import com.digitalasset.pqs.utils.safeequals.=/=
import com.digitalasset.zio.daml.{ProtobufCodecs, DamlSchema}
import com.google.rpc.error_details.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import io.grpc.Status.Code
import scalapb.TimestampConverters
import zio.ZIO.{logDebug, logInfo}
import zio.{Chunk, Task, ZIO}

object eventConverters:

  def convertEvent(
      event: com.daml.ledger.api.v2.event.Event,
      synchronizerId: SynchronizerId
  )(using ProtobufCodecs, DamlSchema): Task[TransactionEvent] = event.event match
    case com.daml.ledger.api.v2.event.Event.Event.Created(evt) =>
      convertCreatedEvent(evt, synchronizerId)
    case com.daml.ledger.api.v2.event.Event.Event.Archived(evt) =>
      convertArchivedEvent(evt)
    case com.daml.ledger.api.v2.event.Event.Event.Exercised(evt) =>
      convertExercisedEvent(evt)
    case unsupported =>
      ZIO.fail(new RuntimeException(s"Unsupported event type: ${unsupported.getClass}"))

  def convertReassignmentEvent(
      event: com.daml.ledger.api.v2.reassignment.ReassignmentEvent,
      synchronizerId: SynchronizerId
  )(using ProtobufCodecs, DamlSchema): Task[ReassignmentEvent] = event.event match
    case com.daml.ledger.api.v2.reassignment.ReassignmentEvent.Event.Unassigned(evt) =>
      convertUnassignedEvent(evt)
    case com.daml.ledger.api.v2.reassignment.ReassignmentEvent.Event.Assigned(evt) =>
      convertAssignedEvent(evt, synchronizerId)
    case unsupported =>
      ZIO.fail(new RuntimeException(s"Unsupported reassignment event type: ${unsupported.getClass}"))

  def convertCreatedEvent(
      evt: com.daml.ledger.api.v2.event.CreatedEvent,
      synchronizerId: SynchronizerId
  )(using codecs: ProtobufCodecs)(using DamlSchema): Task[Event.Created] =
    toContract(evt).map(Event.Created(EventId(evt.offset, evt.nodeId), synchronizerId, _))

  private def convertInterfaceView(
      contractId: String,
      view: com.daml.ledger.api.v2.event.InterfaceView
  )(using ProtobufCodecs, DamlSchema): Task[Option[(schema.Identifier, com.daml.ledger.api.v2.value.Record)]] =
    view match {
      case com.daml.ledger.api.v2.event
            .InterfaceView(Some(interfaceId), Some(viewStatus), Some(viewValue), _implementationPackageId)
          if viewStatus.code == Code.OK.value() =>
        interfaceId.toIdentifier().map(identifier => Some(identifier -> viewValue))
      case failedView if failedView.getViewStatus.code != Code.OK.value() =>
        for
          prettyInterfaceId <- failedView.getInterfaceId.toIdentifier().map(_.uniqueName)
          prettyStatusCode = pprint(PrintableGrpcStatus.fromStatus(failedView.getViewStatus))
          _ <- logInfo(
            s"""Ignored an interface view value for contractId ($contractId), interfaceId ($prettyInterfaceId)
               |because it was received with a non-OK status code: $prettyStatusCode.""".stripMargin
          )
        yield None
      case malformedView =>
        val prettyViewStatus = pprint(PrintableGrpcStatus.fromStatus(malformedView.getViewStatus))
        for
          prettyInterfaceId <- malformedView.getInterfaceId.toIdentifier().map(_.uniqueName)
          _ <- logInfo(
            s"""Ignored a malformed interface view for contractId ($contractId), interfaceId ($prettyInterfaceId) and view status ($prettyViewStatus).
               |The view is expected to have an interfaceId, a viewStatus and a viewValue.""".stripMargin
          )
          _ <- logDebug(
            s"""Malformed interface view for contractId ($contractId) and interfaceId ($prettyInterfaceId): ${pprint(
                malformedView,
                height = Int.MaxValue
              )}"""
          )
        yield None
    }

  private def convertArchivedEvent(
      evt: com.daml.ledger.api.v2.event.ArchivedEvent
  )(using DamlSchema): Task[Event.Archived] =
    for templateId <- evt.getTemplateId.toIdentifier()
    yield Event.Archived(
      eventId = EventId(evt.offset, evt.nodeId),
      templateId = templateId,
      contractId = ContractId(evt.contractId)
    )

  private def convertUnassignedEvent(
      evt: com.daml.ledger.api.v2.reassignment.UnassignedEvent
  )(using DamlSchema): Task[Event.Unassigned] =
    for templateId <- evt.getTemplateId.toIdentifier()
    yield Event.Unassigned(
      eventId = EventId(evt.offset, evt.nodeId),
      reassignmentId = evt.reassignmentId,
      source = SynchronizerId(evt.source),
      target = SynchronizerId(evt.target),
      submitter = Option.when(evt.submitter.nonEmpty)(Party(evt.submitter)),
      reassignmentCounter = evt.reassignmentCounter,
      contractId = ContractId(evt.contractId),
      templateId = templateId,
      witnesses = evt.witnessParties.to(Chunk).map(Party),
      assignmentExclusivity = evt.assignmentExclusivity.map(TimestampConverters.asJavaInstant)
    )

  private def convertAssignedEvent(
      evt: com.daml.ledger.api.v2.reassignment.AssignedEvent,
      synchronizerId: SynchronizerId
  )(using ProtobufCodecs, DamlSchema): Task[Event.Assigned] =
    // An AssignedEvent has no offset or node_id of its own: the proto puts them on the embedded
    // created event ("The offset of this event refers to the offset of the assignment, while the
    // node_id is the index of within the batch"). The contract's identity lives there too.
    val created = evt.getCreatedEvent
    toContract(created).map { contract =>
      Event.Assigned(
        eventId = EventId(created.offset, created.nodeId),
        reassignmentId = evt.reassignmentId,
        source = SynchronizerId(evt.source),
        target = SynchronizerId(evt.target),
        submitter = Option.when(evt.submitter.nonEmpty)(Party(evt.submitter)),
        reassignmentCounter = evt.reassignmentCounter,
        synchronizerId = synchronizerId,
        contract = contract
      )
    }

  private def convertExercisedEvent(
      evt: com.daml.ledger.api.v2.event.ExercisedEvent
  )(using codecs: ProtobufCodecs)(using DamlSchema): Task[Event.Exercised] =
    val entityIdentifier = evt.interfaceId.getOrElse(evt.getTemplateId)
    for
      entitySchemaId <- entityIdentifier.toIdentifier()
      templateId     <- evt.getTemplateId.toIdentifier()
      choiceName = schema.ChoiceName(evt.choice)
    yield Event.Exercised(
      eventId = EventId(evt.offset, evt.nodeId),
      templateId = templateId,
      entityId = entitySchemaId,
      choice = choiceName,
      consuming = evt.consuming,
      contractId = ContractId(evt.contractId),
      arg = codecs.choiceArgument(entitySchemaId, choiceName).toDynamicValue(evt.getChoiceArgument),
      result = codecs.choiceResult(entitySchemaId, choiceName).toDynamicValue(evt.getExerciseResult),
      controllers = evt.actingParties.to(Chunk).map(Party),
      witnesses = evt.witnessParties.to(Chunk).map(Party),
      lastDescendant = evt.lastDescendantNodeId
    )

  private def toContract(
      evt: com.daml.ledger.api.v2.event.CreatedEvent
  )(using codecs: ProtobufCodecs)(using DamlSchema) =
    for
      templateId <- evt.getTemplateId.toIdentifier(Some(evt.representativePackageId))
      interfaces <- ZIO
        .foreach(evt.interfaceViews)(convertInterfaceView(evt.contractId, _).map(_.toList))
        .map(_.flatten)
    yield
      val template = evt.createArguments.map { tmpl => (templateId, tmpl) }
      val payloads = template.toList ++ interfaces
      Contract(
        representativePackageId = PackageId(evt.representativePackageId),
        templateQualifiedName = templateId.qualifiedName,
        contractId = ContractId(evt.contractId),
        contractKey = codecs.getTemplateKey(templateId).map(_.toDynamicValue(evt.getContractKey)),
        contractKeyHash = Option.when(!evt.contractKeyHash.isEmpty)(evt.contractKeyHash.toByteArray),
        payloads = payloads.to(Chunk).map { (id, payload) =>
          id -> codecs.template(id).toDynamicValue(Value.of(Value.Sum.Record(payload)))
        },
        signatories = evt.signatories.to(Chunk).map(Party),
        observers = evt.observers.to(Chunk).map(Party),
        witnesses = evt.witnessParties.to(Chunk).map(Party),
        created_at = evt.createdAt.map(TimestampConverters.asJavaInstant),
        metadata = Option.when(payloads.map(_._1).exists(_.isMetadataIncluded))(evt.createdEventBlob.toByteArray),
        acsDelta = extractors.acsDelta(evt),
        // Storage optimization: only store it when it's different.
        // It can be derived from the representative package id, and in the common case it is the same.
        creationPackageId =
          Option.when(evt.getTemplateId.packageId =/= evt.representativePackageId)(evt.getTemplateId.packageId)
      )

  // TODO: Use DecodedCantonError from Canton
  private case class PrintableGrpcStatus(
      grpcStatusCode: Int,
      grpcStatusMessage: String,
      requestInfo: Option[RequestInfo],
      errorInfo: Option[ErrorInfo],
      resourceInfo: Option[ResourceInfo],
      retryInfo: Option[RetryInfo]
  )

  private object PrintableGrpcStatus:
    def fromStatus(status: com.google.rpc.status.Status): PrintableGrpcStatus =
      PrintableGrpcStatus(
        grpcStatusCode = status.code,
        grpcStatusMessage = status.message,
        requestInfo = status.details.collectFirst { case any if any.is[RequestInfo] => any.unpack[RequestInfo] },
        errorInfo = status.details.collectFirst { case any if any.is[ErrorInfo] => any.unpack[ErrorInfo] },
        resourceInfo = status.details.collectFirst { case any if any.is[ResourceInfo] => any.unpack[ResourceInfo] },
        retryInfo = status.details.collectFirst { case any if any.is[RetryInfo] => any.unpack[RetryInfo] }
      )
