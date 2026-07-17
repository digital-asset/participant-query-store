// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.value.Value
import com.digitalasset.canonical.*
import com.digitalasset.canonical.specific.{Event, EventId, TransactionEvent}
import com.digitalasset.transcode.{Codec, schema}
import com.digitalasset.transcode.schema.*
import com.digitalasset.pqs.utils.safeequals.=/=
import com.digitalasset.zio.daml.KnownEntityIdentifiers
import com.google.rpc.error_details.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import io.grpc.Status.Code
import scalapb.TimestampConverters
import zio.ZIO.{logDebug, logInfo}
import zio.{Chunk, Task, ZIO}

object specific:
  type Codecs = Dictionary[Codec[com.digitalasset.transcode.codec.proto.Value]]

  def convertEvent(
      event: com.daml.ledger.api.v2.event.Event,
      offsetLong: Long,
      rights: UserRight
  )(using Codecs, KnownEntityIdentifiers): Task[TransactionEvent] = event.event match
    case com.daml.ledger.api.v2.event.Event.Event.Created(evt) =>
      convertCreatedEvent(evt)
    case com.daml.ledger.api.v2.event.Event.Event.Archived(evt) =>
      convertArchivedEvent(evt)
    case com.daml.ledger.api.v2.event.Event.Event.Exercised(evt) =>
      convertExercisedEvent(evt)
    case unsupported =>
      ZIO.fail(new RuntimeException(s"Unsupported event type: ${unsupported.getClass}"))

  def convertCreatedEvent(
      evt: com.daml.ledger.api.v2.event.CreatedEvent
  )(using Codecs, KnownEntityIdentifiers): Task[Event.Created] =
    for {
      templateId <- evt.getTemplateId.toIdentifier(Some(evt.representativePackageId))
      template = evt.createArguments.map { tmpl => (templateId, tmpl) }
      interfaces <- ZIO
        .foreach(evt.interfaceViews)(convertInterfaceView(evt.contractId, _).map(_.toList))
        .map(_.flatten)
      payloads = template.toList ++ interfaces
    } yield Event.Created(
      eventId = EventId(evt.offset, evt.nodeId),
      representativePackageId = PackageId(evt.representativePackageId),
      templateQualifiedName = templateId.qualifiedName,
      contractId = ContractId(evt.contractId),
      contractKey = summon[Codecs].getTemplateKey(templateId).map(_.toDynamicValue(evt.getContractKey)),
      contractKeyHash = Option.when(!evt.contractKeyHash.isEmpty)(evt.contractKeyHash.toByteArray),
      payloads = payloads.to(Chunk).map { (id, payload) =>
        id -> summon[Codecs].template(id).toDynamicValue(Value.of(Value.Sum.Record(payload)))
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

  private def convertInterfaceView(
      contractId: String,
      view: com.daml.ledger.api.v2.event.InterfaceView
  )(using Codecs, KnownEntityIdentifiers): Task[Option[(schema.Identifier, com.daml.ledger.api.v2.value.Record)]] =
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
  )(using KnownEntityIdentifiers): Task[Event.Archived] =
    for templateId <- evt.getTemplateId.toIdentifier()
    yield Event.Archived(
      eventId = EventId(evt.offset, evt.nodeId),
      templateId = templateId,
      contractId = ContractId(evt.contractId)
    )

  private def convertExercisedEvent(
      evt: com.daml.ledger.api.v2.event.ExercisedEvent
  )(using Codecs, KnownEntityIdentifiers): Task[Event.Exercised] =
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
      arg = summon[Codecs].choiceArgument(entitySchemaId, choiceName).toDynamicValue(evt.getChoiceArgument),
      result = summon[Codecs].choiceResult(entitySchemaId, choiceName).toDynamicValue(evt.getExerciseResult),
      controllers = evt.actingParties.to(Chunk).map(Party),
      witnesses = evt.witnessParties.to(Chunk).map(Party),
      lastDescendant = evt.lastDescendantNodeId
    )

  // TODO: Use DecodedCantonError from Canton
  case class PrintableGrpcStatus(
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
