// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import zio.Console.{printLine, printLineError}
import zio.ZIO.logInfo
import zio.config.*
import zio.config.ConfigSource.*
import zio.config.magnolia.{Descriptor, describe}
import zio.config.typesafe.*
import zio.*

import java.io.File

package object configuration:

  case class OptionInfo(
      description: String,
      longKey: String,
      shortKey: Option[String] = Option.empty,
      dataType: Option[String] = Option.empty,
      defaultValue: Option[String] = Option.empty,
      envVar: Option[String] = Option.empty,
      sysProp: Option[String] = Option.empty,
      variants: List[String] = List.empty
  )

  val EnvVarPrefix       = "PQS"
  val LegacyEnvVarPrefix = "SCRIBE"

  val OptionConfigFromFile = OptionInfo(
    "Path to configuration overrides via an external HOCON file",
    "--config",
    None,
    Some("file"),
    Some("None"),
    Some(s"${EnvVarPrefix}_CONFIG"),
    Some("config"),
    List.empty
  )

  def apply[T: Tag: Descriptor]: ZLayer[ZIOAppArgs, Nothing, T] = {
    ZLayer.fromZIO(
      (for
        descriptor <- getDescriptor
        conf       <- zio.config.read(descriptor)
        // print configuration to console
        _ <- zio.config
          .write(descriptor, conf)
          .fold(
            _ => ZIO.unit,
            tree => logInfo(s"Applied configuration:${java.lang.System.lineSeparator}${tree.toHoconString}")
          )
          .provide(zio.Runtime.removeDefaultLoggers)
      yield conf).catchAll(logError)
    )
  }

  def configOptions[T: Descriptor]: ZIO[ZIOAppArgs, Nothing, List[OptionInfo]] = {
    import PrettyPrinter.ConfigDescriptor.prettyOptions
    getDescriptor[T]
      .mapAttempt(_.prettyOptions(EnvVarPrefix))
      .map(_.prepended(OptionConfigFromFile))
      .catchAll(logError)
  }

  private def logError(e: Throwable) = {
    import PrettyPrinter.ReadError.pretty
    e match {
      case error: ReadError[Any] =>
        printLineError(error.pretty).ignore *>
          printLine("Please run with a help flag (--help | --help-verbose) for available options").ignore *>
          ZIO.failCause(Cause.Empty)
      case other =>
        printLine("Could not process application configuration").ignore *>
          ZIO.failCause(Cause.Empty)
    }
  }

  private def ensureConfigFileExists(file: File) =
    (
      printLineError(s"[ERROR] Config file does not exist: ${file.getPath}") *>
        ZIO.failCause(Cause.Empty)
    ).whenZIO(ZIO.attemptBlockingIO(!file.isFile))

  // Configuration options that affect the configuration parsing itself, such as config source files
  private final case class ConfigConfig(config: Option[File])
  private object ConfigConfig:
    private[configuration] val descriptor = zio.config.magnolia.descriptor[ConfigConfig]

  private def warnIfScribePrefixUsed: ZIO[Any, Throwable, Unit] =
    ZIO
      .attempt(sys.env.keys.filter(_.startsWith(s"${LegacyEnvVarPrefix}_")).toSeq)
      .flatMap { legacyKeys =>
        ZIO
          .when(legacyKeys.nonEmpty) {
            ZIO.logWarning(
              s"""|Environment variables with the '$LegacyEnvVarPrefix' prefix are deprecated.
                  |Found ${legacyKeys.map(key => s"'$key'").take(10).mkString(", ")}.
                  |Please use the '$EnvVarPrefix' prefix instead.
            """.stripMargin
            )
          }
          .unit
      }

  private def getDescriptor[T: Descriptor] = for
    args <- ZIO.service[ZIOAppArgs]
    cmdArgs = fromCommandLineArgs(args.getArgs.toList, Some('-'), Some(',')).mapKeys(_.toLowerCase())
    sysProp = fromSystemProps(Some('.'), Some(','))
    pqsEnv = fromSystemEnv(Some('_'), Some(','), _.startsWith(EnvVarPrefix))
      .at(PropertyTreePath.$(EnvVarPrefix))
      .mapKeys(_.toUpperCase())
    scribeEnv = fromSystemEnv(Some('_'), Some(','), _.startsWith(LegacyEnvVarPrefix))
      .at(PropertyTreePath.$(LegacyEnvVarPrefix))
      .mapKeys(_.toUpperCase())
    _ <- warnIfScribePrefixUsed
    env        = pqsEnv.orElse(scribeEnv)
    baseSource = cmdArgs <> sysProp <> env
    // use the baseSource to create a ConfigConfig. Add any additional config paths
    configConfig <- zio.config.read(ConfigConfig.descriptor from baseSource)
    _            <- configConfig.config.fold(ZIO.unit)(ensureConfigFileExists)
    source = configConfig.config.map(TypesafeConfigSource.fromHoconFile).fold(baseSource)(baseSource <> _)
  yield zio.config.magnolia.descriptor[T] from source

end configuration
