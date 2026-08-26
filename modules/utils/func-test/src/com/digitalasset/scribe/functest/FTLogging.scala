// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest

import com.digitalasset.scribe
import com.digitalasset.scribe.docker.{ContainerImage, SuiteName}
import zio.*
import zio.Runtime.removeDefaultLoggers
import zio.logging.*

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import scala.io.AnsiColor
import scala.jdk.CollectionConverters.*
import zio.test.*

object FTLogging:
  val layer: ZLayer[Any, Nothing, Unit] = removeDefaultLoggers ++ ZLayer
    .fromZIO(
      zio.System.env("FT_LOG_FILE").orDie.map {
        case None       => consoleLogger(consoleLogConfig)
        case Some(file) => fileLogger(fileLogConfig(file))
      }
    )
    .flatten

  /** Runs a test block silently, forwarding logs to the console only if the test fails.
    */
  def silentTest = new TestAspect.PerTest[Nothing, Any, Throwable, Any]:
    override def perTest[R, E >: Throwable](test: ZIO[R, TestFailure[E], TestSuccess])(implicit
        trace: Trace
    ): ZIO[R, TestFailure[E], TestSuccess] =
      val bufferingLogger = new BufferingLogger
      for result <- FiberRefAccess.currentLoggers
          .locallyWith { loggers =>
            bufferingLogger.setLoggers(loggers)
            Set(bufferingLogger)
          }(test)
          .tapError(_ => bufferingLogger.forwardLogs.mapError(TestFailure.die))
      yield result

  private def fileLogConfig(file: String) =
    import zio.logging.LogFormat.*
    def anno[A](ann: zio.logging.LogAnnotation[A]) = make { (builder, _, _, _, _, _, fiberRefs, _, _) =>
      fiberRefs
        .get(zio.logging.logContext)
        .foreach { context =>
          context.get(ann).foreach { value =>
            val rendered = ann.render(value)
            if rendered.nonEmpty then
              builder.appendText(" ")
              builder.appendText(rendered)
          }
        }
    }

    val format = timestampForwarder
      + anno(SuiteName)
      + anno(ContainerImage)
      |-| level
      |-| line
      + cause
    FileLoggerConfig(
      os.Path(file, os.pwd).toNIO.toAbsolutePath,
      format,
      LogFilter.LogLevelByNameConfig(LogLevel.Debug)
    )
  end fileLogConfig

  private def consoleLogConfig =
    import zio.logging.LogFormat.*
    val cols      = Seq(AnsiColor.GREEN, AnsiColor.YELLOW, AnsiColor.BLUE, AnsiColor.MAGENTA, AnsiColor.CYAN)
    val modifiers = Seq("", AnsiColor.BOLD, AnsiColor.UNDERLINED, AnsiColor.BOLD + AnsiColor.UNDERLINED)
    val colors    = (cols zip modifiers).map(_ + _).toIndexedSeq

    def anno[A](ann: zio.logging.LogAnnotation[A]) = make { (builder, _, _, _, _, _, fiberRefs, _, _) =>
      fiberRefs
        .get(zio.logging.logContext)
        .foreach { context =>
          context.get(ann).foreach { value =>
            val rendered = ann.render(value)
            if rendered.nonEmpty then
              builder.appendText(" ")
              val color = colors(rendered.hashCode.abs % colors.size)
              builder.appendText(color)
              builder.appendText(rendered)
              builder.appendText(AnsiColor.RESET)
          }
        }
    }

    val format = timestampForwarder
      + anno(SuiteName)
      + anno(ContainerImage)
      |-| level.fixed(1).highlight
      |-| line
      + cause
    ConsoleLoggerConfig(format, LogFilter.LogLevelByNameConfig(LogLevel.Debug))
  end consoleLogConfig

  private def timestampForwarder = LogFormat.make { (builder, _, _, _, _, _, _, _, annotations) =>
    val ts = annotations.getOrElse("timestamp", currentTimestamp())
    builder.appendText(ts.take(12))
  }

  private type Log = (Trace, FiberId, LogLevel, () => String, Cause[Any], FiberRefs, List[LogSpan], Map[String, String])
  private class BufferingLogger extends ZLogger[String, Unit]:
    private val buffer: ConcurrentLinkedQueue[Log]        = new ConcurrentLinkedQueue[Log]
    private var currentLoggers: Set[ZLogger[String, Any]] = Set.empty

    def setLoggers(loggers: Set[ZLogger[String, Any]]): Unit = currentLoggers = loggers
    def apply(
        trace: Trace,
        fiberId: FiberId,
        logLevel: LogLevel,
        message: () => String,
        cause: Cause[Any],
        fiberRefs: FiberRefs,
        spans: List[LogSpan],
        annotations: Map[String, String]
    ): Unit =
      val ts = currentTimestamp()
      buffer.add((trace, fiberId, logLevel, message, cause, fiberRefs, spans, annotations + ("timestamp" -> ts)))

    def forwardLogs = ZIO.attempt {
      buffer.iterator.asScala.foreach {
        case (trace, fiberId, logLevel, message, cause, context, spans, annotations) =>
          currentLoggers.foreach(_.apply(trace, fiberId, logLevel, message, cause, context, spans, annotations))
      }
    }
  end BufferingLogger

  private def currentTimestamp(): String = DateTimeFormatter.ISO_LOCAL_TIME.format(ZonedDateTime.now())
