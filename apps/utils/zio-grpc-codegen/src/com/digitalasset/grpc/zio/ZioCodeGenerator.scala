// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.zio

import com.google.protobuf.Descriptors.{FileDescriptor, MethodDescriptor, ServiceDescriptor}
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse
import protocgen.{CodeGenApp, CodeGenRequest, CodeGenResponse}
import scalapb.compiler.*
import scalapb.compiler.StreamType.{Bidirectional, ClientStreaming, ServerStreaming, Unary}
import scalapb.options.Scalapb

import scala.jdk.CollectionConverters.*

object ZioCodeGenerator extends CodeGenApp {
  override def registerExtensions(registry: ExtensionRegistry): Unit =
    Scalapb.registerAllExtensions(registry)

  def process(request: CodeGenRequest): CodeGenResponse = ProtobufGenerator.parseParameters(request.parameter) match
    case Right(params) =>
      val implicits = DescriptorImplicits.fromCodeGenRequest(params, request)
      CodeGenResponse.succeed(
        request.filesToGenerate.collect {
          case file if !file.getServices.isEmpty => new ZioFilePrinter(implicits, file).result()
        },
        Set(CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL)
      )

    case Left(error) =>
      CodeGenResponse.fail(error)
}

class ZioFilePrinter(implicits: DescriptorImplicits, file: FileDescriptor) {
  import implicits.*

  def result(): CodeGeneratorResponse.File =
    CodeGeneratorResponse.File.newBuilder().setName(scalaFileName).setContent(content).build()

  private def scalaFileName = OuterObject.fullName.replace('.', '/') + ".scala"

  private def content: String = new FunctionalPrinter()
    .add(
      s"package ${file.scalaPackage.fullName}",
      "",
      s"object ${OuterObject.name} {"
    )
    .indent
    .print(file.getServices.asScala)((fp, s) => new ServicePrinter(s).print(fp))
    .outdent
    .add("}")
    .result()

  private val DtoObject   = file.scalaPackage / s"${NameUtils.snakeCaseToCamelCase(baseName(file.getName), true)}Grpc"
  private val OuterObject = file.scalaPackage / s"Zio${NameUtils.snakeCaseToCamelCase(baseName(file.getName), true)}"

  private val Channel         = "_root_.com.digitalasset.scribe.grpc.ZManagedChannel"
  private val ZLayer          = "_root_.zio.ZLayer"
  private val ZStream         = "_root_.zio.stream.ZStream"
  private val Stream          = "_root_.zio.stream.Stream"
  private val ZIO             = "_root_.zio.ZIO"
  private val IO              = "_root_.zio.IO"
  private val Any             = "_root_.scala.Any"
  private val Nothing         = "_root_.scala.Nothing"
  private val StatusException = "_root_.io.grpc.StatusException"

  private class ServicePrinter(service: ServiceDescriptor) {
    private val methods   = service.getMethods.asScala.toVector
    private val traitName = OuterObject / s"${service.name}Client"

    def print(fp: FunctionalPrinter) =
      fp.add(s"trait ${traitName.name} {")
        .indent
        .print(methods)(printMethodSignature)
        .outdent
        .add("}")
        .add(s"object ${traitName.name} {")
        .indent
        .add(
          s"val live: $ZLayer[$Channel, Throwable, ${traitName.name}] = $ZLayer.fromFunction { (__channel__ : $Channel) =>"
        )
        .indent
        .add(s"new ${traitName.name} {")
        .indent
        .print(methods)(printMethodImpl)
        .outdent
        .add("}")
        .outdent
        .add("}")
        .print(methods)(printMethodAccessor)
        .outdent
        .add("}")

    private def printMethodSignature(fp: FunctionalPrinter, m: MethodDescriptor) =
      fp.add(methodSignature(m, None, Some(StatusException)))

    private def printMethodImpl(fp: FunctionalPrinter, method: MethodDescriptor) =
      val signature = methodSignature(method, None, Some(StatusException))
      val methodD   = s"${DtoObject.fullNameWithMaybeRoot}.METHOD_${NameUtils.toAllCaps(method.getName)}"
      val clientCall = method.streamType match
        case Unary           => s"__channel__.unaryCall($methodD, request)"
        case ClientStreaming => s"__channel__.clientStreamingCall($methodD, request)"
        case ServerStreaming => s"__channel__.serverStreamingCall($methodD, request)"
        case Bidirectional   => s"__channel__.bidiCall($methodD, request)"
      fp.add(s"$signature = $clientCall")

    private def printMethodAccessor(fp: FunctionalPrinter, method: MethodDescriptor) =
      val signature = methodSignature(method, Some(traitName.name), Some(StatusException))
      val clientCall = method.streamType match
        case Unary           => s"$ZIO.serviceWithZIO[${traitName.name}]"
        case ClientStreaming => s"$ZIO.serviceWithZIO[${traitName.name}]"
        case ServerStreaming => s"$ZStream.serviceWithStream[${traitName.name}]"
        case Bidirectional   => s"$ZStream.serviceWithStream[${traitName.name}]"
      val innerCall = s"_.${method.name}(request)"
      fp.add(s"$signature = $clientCall($innerCall)")

    private def methodSignature(method: MethodDescriptor, contextType: Option[String], errorType: Option[String]) =
      val scalaOutType = method.outputType.scalaType
      val context      = contextType.getOrElse(Any)
      val errorParam   = errorType.getOrElse(Nothing)
      val reqType      = methodInType(method, StatusException)
      val signature = method.streamType match
        case Unary           => s"(request: $reqType): ${io(scalaOutType, errorParam, context)}"
        case ClientStreaming => s"(request: $reqType): ${io(scalaOutType, errorParam, context)}"
        case ServerStreaming => s"(request: $reqType): ${stream(scalaOutType, errorParam, context)}"
        case Bidirectional   => s"(request: $reqType): ${stream(scalaOutType, errorParam, context)}"
      s"def ${method.name}$signature"

    private def methodInType(method: MethodDescriptor, errorType: String): String =
      val scalaInType = method.inputType.scalaType
      method.streamType match
        case Unary           => scalaInType
        case ClientStreaming => stream(scalaInType, errorType, Any)
        case ServerStreaming => scalaInType
        case Bidirectional   => stream(scalaInType, errorType, Any)

    private def stream(res: String, errType: String, envType: String) = envType match
      case Any => s"$Stream[$errType, $res]"
      case r   => s"$ZStream[$r, $errType, $res]"

    private def io(res: String, errType: String, envType: String) = envType match
      case Any => s"$IO[$errType, $res]"
      case r   => s"$ZIO[$r, $errType, $res]"
  }

}
