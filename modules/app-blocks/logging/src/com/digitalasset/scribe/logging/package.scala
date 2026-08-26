// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import zio.config.magnolia.{Descriptor, describe}
import zio.logging.{LogFilter, LogFormat}
import zio.{LogLevel, ULayer, ZLayer}

import java.io.File
import java.util.UUID

package object logging:
  object ConsoleLogging:
    val default: ULayer[Config] = ZLayer.succeed(Config())
    final case class Config(
        @describe("Log level")
        level: Level = Level.Info,
        @describe("Log pattern")
        pattern: Pattern = Pattern.Plain,
        @describe("Log output format")
        format: ConsoleFormat = ConsoleFormat.Plain,
        @describe("Custom mappings for log levels")
        mappings: Map[String, Level] = Map.empty
    ) extends Formatable,
          Filterable
  end ConsoleLogging

  object FileLogging:
    val default: ULayer[Config] = ZLayer.succeed(Config())
    final case class Config(
        @describe("Log output file")
        destination: File = new File("output.log"),
        @describe("Log level")
        level: Level = Level.Info,
        @describe("Log pattern")
        pattern: Pattern = Pattern.Plain,
        @describe("Log output format")
        format: FileFormat = FileFormat.Plain,
        @describe("Custom mappings for log levels")
        mappings: Map[String, Level] = Map.empty
    ) extends Formatable,
          Filterable
  end FileLogging

  sealed trait Level
  object Level:
    case object All     extends Level
    case object Fatal   extends Level
    case object Error   extends Level
    case object Warning extends Level
    case object Info    extends Level
    case object Debug   extends Level
    case object Trace   extends Level
    case object None    extends Level
  end Level

  sealed trait Pattern:
    def pattern: String
  end Pattern
  object Pattern:
    case object Plain extends Pattern:
      override def pattern: String = "%timestamp{HH:mm:ss.SSS} %highlight{%fixed{1}{%level}} [%fiberId] %name:%line" +
        " %highlight{%message} %highlight{%cause} %kvs"

    case object Standard extends Pattern:
      val instanceUuid: String = UUID.randomUUID.toString
      override def pattern: String =
        s"%label{component}{scribe} %label{instance_uuid}{$instanceUuid} %label{timestamp}{%timestamp{yyyy-MM-dd'T'HH:mm:ss.SSSZ}}" +
          " %label{level}{%level} %label{description}{%message} %label{cause}{%cause} %label{scribe}{%kvs}"

    case object Structured extends Pattern:
      override def pattern: String = "%label{timestamp}{%timestamp{yyyy-MM-dd'T'HH:mm:ssZ}} %label{level}{%level}" +
        " %label{thread}{%fiberId} %label{location}{%name:%line} %label{message}{%message} %label{cause}{%cause} %kvs"

    final case class Custom(pattern: String) extends Pattern

    given descrCustom: Descriptor[Custom] =
      Descriptor.from(Descriptor[String].transform[Custom](Custom.apply, _.pattern))
  end Pattern

  sealed trait ConsoleFormat
  object ConsoleFormat:
    case object Plain extends ConsoleFormat
    case object Json  extends ConsoleFormat
  end ConsoleFormat

  sealed trait FileFormat
  object FileFormat:
    case object Plain      extends FileFormat
    case object PlainAsync extends FileFormat
    case object Json       extends FileFormat
    case object JsonAsync  extends FileFormat
  end FileFormat

  sealed trait Filterable:
    def level: Level

    def mappings: Map[String, Level]

    def toLogFilterConfig: LogFilter.LogLevelByNameConfig =
      val levels = mappings.map((k, v) => (k, toLogLevel(v)))
      LogFilter.LogLevelByNameConfig(toLogLevel(level), levels)

    def toLogFilter: LogFilter[String] = toLogFilterConfig.toFilter

    private def toLogLevel(l: Level) = l match
      case Level.All     => LogLevel.All
      case Level.Fatal   => LogLevel.Fatal
      case Level.Error   => LogLevel.Error
      case Level.Warning => LogLevel.Warning
      case Level.Info    => LogLevel.Info
      case Level.Debug   => LogLevel.Debug
      case Level.Trace   => LogLevel.Trace
      case Level.None    => LogLevel.None
  end Filterable

  sealed trait Formatable:
    def pattern: Pattern

    def toLogFormat: Option[LogFormat] = LogFormat.Pattern.parse(pattern.pattern).map(_.toLogFormat).toOption
  end Formatable

end logging
