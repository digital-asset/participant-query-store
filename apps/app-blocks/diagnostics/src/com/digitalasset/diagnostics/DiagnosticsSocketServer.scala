// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.diagnostics

import com.digitalasset.diagnostics.DiagnosticsSocketServer.Config
import com.digitalasset.diagnostics.metrics.OpenMetricsStorage
import com.digitalasset.diagnostics.metrics.micrometer.OpenMetricsBridge
import com.digitalasset.diagnostics.threads.ThreadDumpsCollector
import io.micrometer.core.instrument.Metrics
import org.apache.commons.lang3.concurrent.BasicThreadFactory

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, ServerSocketChannel}
import java.nio.file.*
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.Locale
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.compiletime.uninitialized
import scala.util.Using

/** A diagnostics server that is meant to be embedded into a JVM application to monitor the app's most critical
  * operational domains (such as configuration, metrics, thread dumps, etc) in a self-contained manner. It listens on a
  * socket and responds to incoming connections with a health dump in the form of a .zip file containing separate
  * entries for each monitored domain over an extended timeframe. On JVM shutdown, the latest health dump is written to
  * a directory if configured to do so.
  *
  * @param conf
  *   configuration for the diagnostics server
  * @param openModules
  *   whether automatic opening of necessary JDK modules is requested
  */
private class DiagnosticsSocketServer(conf: Config, openModules: Boolean):
  log("Initialising diagnostics server with configuration:")
  pprint.pprintln(conf)

  // init services
  private val executorService = Executors.newSingleThreadExecutor(
    BasicThreadFactory
      .builder()
      .namingPattern("diagnostics-socket-server-%d")
      .daemon(true)
      .priority(Thread.MAX_PRIORITY)
      .build()
  )
  private val metricsStorage = new OpenMetricsStorage(conf.metrics.bufferSize, conf.metrics.tags.toSet)
  private val metricsBridge  = new OpenMetricsBridge(OpenMetricsBridge.Config(conf.metrics.interval), metricsStorage)
  private val threadDumpsCollector =
    new ThreadDumpsCollector(conf.threads.interval, conf.threads.bufferSize, openModules)
  Metrics.addRegistry(metricsBridge)

  // establish clean-up
  conf.dumpPath.foreach { path =>
    log(s"Will write health dump on JVM exit to $path")
    Runtime.getRuntime.addShutdownHook(new Thread(() => write(path, healthDump())))
  }
  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdownServices()))

  // start connections loop
  executorService.execute(() => acceptConnections(conf.host, conf.port))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.While"))
  private def acceptConnections(host: String, port: Int): Unit =
    try
      val acceptSelector = SelectorProvider.provider.openSelector()
      val ssc            = ServerSocketChannel.open()
      ssc.configureBlocking(false)
      val isa = new InetSocketAddress(host, port)
      ssc.socket.bind(isa)
      log(s"Server socket listening on ${ssc.socket.getLocalSocketAddress}")
      val acceptKey = ssc.register(acceptSelector, SelectionKey.OP_ACCEPT)
      while acceptSelector.select() > 0 do
        val readyKeys = acceptSelector.selectedKeys.iterator()
        while readyKeys.hasNext do
          val key = readyKeys.next()
          readyKeys.remove()
          try
            Using.resource(key.channel.asInstanceOf[ServerSocketChannel].accept()) { channel =>
              log(s"Writing to $channel")
              val buffer       = ByteBuffer.wrap(healthDump())
              var bytesWritten = 0
              while
                bytesWritten = channel.write(buffer)
                bytesWritten > 0
              do log(s"Wrote $bytesWritten bytes to socket channel")
            }
          catch
            case e: Throwable =>
              error(s"Failed to write health dump to socket: ${e.getMessage}")
              e.printStackTrace()
    catch
      case e: Throwable =>
        error(s"Failed to start listening on $host:$port, caused by: ${e.getMessage}")
        e.printStackTrace()

  private def shutdownServices(): Unit =
    log(s"Diagnostics server shutting down now...")
    try
      metricsBridge.close()
      threadDumpsCollector.close()
      executorService.shutdownNow()
      if !executorService.awaitTermination(5, TimeUnit.SECONDS) then
        warn("Executor service (for diagnostics socket server) did not terminate promptly")
    catch
      case e: InterruptedException =>
        // (re-)cancel if current thread also interrupted
        executorService.shutdownNow()
        // preserve interrupt status
        Thread.currentThread.interrupt()

  private def healthDump(): Array[Byte] =
    Using.resource(new ByteArrayOutputStream()) { baos =>
      Using.resource(new ZipOutputStream(baos)) { zos =>
        zos.putNextEntry(new ZipEntry(s"metrics.openmetrics"))
        zos.write(metricsStorage.expose().mkString(System.lineSeparator).getBytes)
        zos.closeEntry()
        threadDumpsCollector.get().foreach {
          case (time, dump) =>
            zos.putNextEntry(new ZipEntry(s"threads-$time.zip"))
            zos.write(dump)
            zos.closeEntry()
        }
      }
      baos.toByteArray
    }

  private def write(path: Path, contents: => Array[Byte]): Unit =
    val now     = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ENGLISH))
    val outPath = path.resolve(s"$now.zip")
    log(s"Writing health dump to $outPath")
    Files.write(outPath, contents, StandardOpenOption.CREATE_NEW)
    ()

end DiagnosticsSocketServer

object DiagnosticsSocketServer:
  private var serverInstance: DiagnosticsSocketServer = uninitialized

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  def start(conf: Config = DiagnosticsSocketServer.configure(), openModules: Boolean = false): Unit =
    if !conf.enabled
    then warn("Diagnostics server is disabled")
    else if serverInstance != null
    then warn("Diagnostics server is already running")
    else
      try serverInstance = new DiagnosticsSocketServer(conf, openModules)
      catch
        case e: Throwable =>
          error(s"Failed to start diagnostics server: ${e.getMessage}")
          e.printStackTrace()

  case class MetricsConfig(interval: Duration, bufferSize: Int, tags: Seq[(String, String)])
  case class ThreadsConfig(interval: Duration, bufferSize: Int)
  case class Config(
      enabled: Boolean,
      host: String,
      port: Int,
      dumpPath: Option[Path],
      metrics: MetricsConfig,
      threads: ThreadsConfig
  )

  private enum Defaults[T](val key: String, val value: String, val mapper: String => T):
    case Enabled           extends Defaults("enabled", "true", _.toBoolean)
    case Host              extends Defaults("host", "127.0.0.1", identity)
    case Port              extends Defaults("port", "0", _.toInt.abs)
    case DumpPath          extends Defaults("dump.path", "", toPath)
    case MetricsInterval   extends Defaults("metrics.interval", "PT10S", Duration.parse(_).abs)
    case MetricsBufferSize extends Defaults("metrics.buffer.size", "60", _.toInt.abs)
    case MetricsTags       extends Defaults("metrics.tags", "", identity)
    case ThreadsInterval   extends Defaults("threads.interval", "PT1M", Duration.parse(_).abs)
    case ThreadsBufferSize extends Defaults("threads.buffer.size", "10", _.toInt.abs)

  private enum EnvVars(val key: String):
    case Enabled           extends EnvVars(envVarName(Defaults.Enabled))
    case Host              extends EnvVars(envVarName(Defaults.Host))
    case Port              extends EnvVars(envVarName(Defaults.Port))
    case DumpPath          extends EnvVars(envVarName(Defaults.DumpPath))
    case MetricsInterval   extends EnvVars(envVarName(Defaults.MetricsInterval))
    case MetricsBufferSize extends EnvVars(envVarName(Defaults.MetricsBufferSize))
    case MetricsTags       extends EnvVars(envVarName(Defaults.MetricsTags))
    case ThreadsInterval   extends EnvVars(envVarName(Defaults.ThreadsInterval))
    case ThreadsBufferSize extends EnvVars(envVarName(Defaults.ThreadsBufferSize))

  private enum SysProps(val key: String):
    case Enabled           extends SysProps(sysPropName(Defaults.Enabled))
    case Host              extends SysProps(sysPropName(Defaults.Host))
    case Port              extends SysProps(sysPropName(Defaults.Port))
    case DumpPath          extends SysProps(sysPropName(Defaults.DumpPath))
    case MetricsInterval   extends SysProps(sysPropName(Defaults.MetricsInterval))
    case MetricsBufferSize extends SysProps(sysPropName(Defaults.MetricsBufferSize))
    case MetricsTags       extends SysProps(sysPropName(Defaults.MetricsTags))
    case ThreadsInterval   extends SysProps(sysPropName(Defaults.ThreadsInterval))
    case ThreadsBufferSize extends SysProps(sysPropName(Defaults.ThreadsBufferSize))

  /** Inspects the environment to produce diagnostics server configuration based on the following sources (in order of
    * priority):
    *   - System properties
    *   - Environment variables
    *   - Default values
    *
    * If the supplied values are invalid (couldn't parse duration, for example), sensible default values are used so
    * this function always returns a valid configuration.
    *
    * @return
    *   an initialised configuration
    */
  def configure(): Config =
    import DiagnosticsSocketServer.{Defaults as D, EnvVars as EV, SysProps as SP}

    import scala.util.Properties

    def probe(sp: SysProps, ev: => EnvVars, alt: => Defaults[?]) = probeSourced(sp, ev, alt)._2

    def probeSourced(sp: SysProps, ev: => EnvVars, alt: => Defaults[?]) =
      Properties
        .propOrNone(sp.key)
        .map(sp.key -> _)
        .orElse(Properties.envOrNone(ev.key).map(ev.key -> _))
        .getOrElse(alt.key -> alt.value)

    def get[T](sp: SysProps, ev: => EnvVars, alt: => Defaults[T]) =
      val (source, value) = probeSourced(sp, ev, alt)
      try alt.mapper(value)
      catch
        case ex =>
          def emptyValue(v: String) = if v.isEmpty then "<empty>" else v
          val original              = s"$source=${emptyValue(value)}"
          warn(s"Error configuring `$original`: ${ex.getMessage}. Defaulting to ${emptyValue(alt.value)}.")
          alt.mapper(alt.value)

    val metricsTags = probe(SP.MetricsTags, EV.MetricsTags, D.MetricsTags)
      .split(",")
      .filter(_.nonEmpty)
      .toSeq
      .map { x =>
        x.indexOf('=') match
          case -1 => (x, "")
          case ix => (x.substring(0, ix), x.substring(ix + 1))
      }

    Config(
      enabled = get(SP.Enabled, EV.Enabled, D.Enabled),
      host = probe(SP.Host, EV.Host, D.Host),
      port = get(SP.Port, EV.Port, D.Port),
      dumpPath = get(SP.DumpPath, EV.DumpPath, D.DumpPath),
      metrics = MetricsConfig(
        interval = get(SP.MetricsInterval, EV.MetricsInterval, D.MetricsInterval),
        bufferSize = get(SP.MetricsBufferSize, EV.MetricsBufferSize, D.MetricsBufferSize),
        tags = metricsTags
      ),
      threads = ThreadsConfig(
        interval = get(SP.ThreadsInterval, EV.ThreadsInterval, D.ThreadsInterval),
        bufferSize = get(SP.ThreadsBufferSize, EV.ThreadsBufferSize, D.ThreadsBufferSize)
      )
    )
  end configure

  private def envVarName(d: Defaults[?])  = s"DA_DIAGNOSTICS_${d.key.toUpperCase.replace('.', '_')}"
  private def sysPropName(d: Defaults[?]) = envVarName(d).toLowerCase.replace('_', '.')

  private def toPath(s: String) =
    Option(s).filter(_.trim.nonEmpty).map(Paths.get(_)).map {
      case p if Files.notExists(p)    => throw new IllegalArgumentException(s"Path $p does not exist")
      case p if !Files.isDirectory(p) => throw new IllegalArgumentException(s"Path $p is not a directory")
      case p if !Files.isWritable(p)  => throw new IllegalArgumentException(s"Path $p is not writable")
      case p                          => p.toAbsolutePath.toRealPath().normalize
    }

end DiagnosticsSocketServer
