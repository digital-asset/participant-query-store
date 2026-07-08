// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.diagnostics.threads

import com.digitalasset.diagnostics
import com.digitalasset.diagnostics.util.Ring
import com.digitalasset.jdk.ModulesOpener.{Mode, openForPackages}
import org.apache.commons.lang3.concurrent.BasicThreadFactory

import java.io.{ByteArrayOutputStream, IOException}
import java.lang.System.lineSeparator
import java.lang.management.ManagementFactory
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.{Locale, Objects}
import scala.util.Using

/** A collector that periodically captures thread dumps of the running JVM and stores them in a ring buffer for later
  * retrieval.
  *
  * @param interval
  *   the periodicity of thread dumps capture
  * @param bufferSize
  *   the number of thread dumps to store
  * @param openModules
  *   whether automatic opening of necessary JDK modules is requested
  */
class ThreadDumpsCollector(interval: Duration, bufferSize: Int, openModules: Boolean):
  diagnostics.log(s"Starting thread dumps collector: interval = $interval, samples = $bufferSize")

  private var storage = Ring.empty[(String, Array[Byte])](bufferSize)
  private val executorService = Executors.newSingleThreadScheduledExecutor(
    new BasicThreadFactory.Builder()
      .namingPattern("diagnostics-thread-dumps-collector-%d")
      .daemon(true)
      .priority(Thread.MAX_PRIORITY)
      .build()
  )
  private val source      = ThreadDumpSource(openModules)
  private val dtFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ENGLISH)

  executorService.scheduleAtFixedRate(() => collect(), interval.toSeconds, interval.toSeconds, TimeUnit.SECONDS)

  def collect(): Unit =
    val now          = LocalDateTime.now().format(dtFormatter)
    val (_, newring) = storage.push(now -> pack(s"threads-$now.tdump", source.collect()))
    storage = newring

  def get(): Seq[(String, Array[Byte])] = storage.iterator.toSeq

  def close(): Unit =
    diagnostics.log(s"Shutting down thread dumps collector")
    executorService.shutdownNow()
    if !executorService.awaitTermination(5, TimeUnit.SECONDS) then
      diagnostics.warn("Executor service (for diagnostics thread dumps collector) did not terminate promptly")
    source.close()

  private def pack(name: String, contents: String): Array[Byte] =
    Using.resource(new ByteArrayOutputStream()) { baos =>
      Using.resource(new ZipOutputStream(baos)) { zos =>
        zos.putNextEntry(new ZipEntry(name))
        zos.write(contents.getBytes)
        zos.closeEntry()
      }
      baos.toByteArray
    }

end ThreadDumpsCollector

private trait ThreadDumpSource:
  def collect(): String
  def close(): Unit

private object ThreadDumpSource:
  def apply(openModules: Boolean): ThreadDumpSource =
    try
      if openModules then openForPackages(Set("sun.tools.attach"), Mode.ExportToAllUnnamed)
      new VmAttachSource
    catch
      case ex: NoClassDefFoundError if ex.getMessage.equalsIgnoreCase("com/sun/tools/attach/VirtualMachine") =>
        diagnostics.warn(
          "This JVM instance lacks `jdk.attach` module (is vanilla JRE distro in use?)",
          "Falling back to low fidelity thread dump renderer",
          "Resolve this warning by building custom JRE with necessary module(s), learn more at:",
          "  - https://adoptium.net/en-GB/blog/2021/10/jlink-to-produce-own-runtime/",
          "  - https://hub.docker.com/_/eclipse-temurin (section: Creating a JRE using jlink)"
        )
        new TdaHandcraftedSource
      case ex: IllegalAccessError
          if ex.getMessage.contains("because module jdk.attach does not export sun.tools.attach") =>
        diagnostics.warn(
          "Could not self-attach to JVM",
          "Falling back to low fidelity thread dump renderer",
          "Resolve this warning by providing `--add-exports=jdk.attach/sun.tools.attach=ALL-UNNAMED` flag to JVM"
        )
        new TdaHandcraftedSource
      case ex: IOException if ex.getMessage.equalsIgnoreCase("Can not attach to current VM") =>
        diagnostics.warn(
          "Could not self-attach to JVM",
          "Falling back to low fidelity thread dump renderer",
          "Resolve this warning by providing `-Djdk.attach.allowAttachSelf` flag to JVM"
        )
        new TdaHandcraftedSource
      case ex: Throwable =>
        diagnostics.warn(s"Could not self-attach to JVM ($ex)", "Falling back to low fidelity thread dump renderer")
        new TdaHandcraftedSource

  /** Source that uses `jdk.attach` module's capabilities to produce a rich thread dump equivalent to `kill -3 <pid>`
    * output.
    */
  private class VmAttachSource extends ThreadDumpSource:
    import com.sun.tools.attach.VirtualMachine
    import sun.tools.attach.HotSpotVirtualMachine

    private val vmName = ManagementFactory.getRuntimeMXBean.getName
    private val pid    = vmName.substring(0, vmName.indexOf('@'))
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    private val vm = VirtualMachine.attach(pid).asInstanceOf[HotSpotVirtualMachine]

    override def collect(): String =
      scala.io.Source.fromInputStream(vm.remoteDataDump()).mkString

    override def close(): Unit =
      diagnostics.log(s"Detaching from JVM")
      vm.detach()

  end VmAttachSource

  /** Source that produces a low fidelity thread dump that is still functional enough to be consumed by VisualVM's TDA
    * plugin to perform analysis.
    * @see
    *   https://github.com/mkbrv/tda/blob/master/src/main/java/com/pironet/tda/jconsole/MBeanDumper.java
    * @see
    *   https://github.com/irockel/tda/blob/master/tda/src/java/com/pironet/tda/SunJDKParser.java
    */
  private class TdaHandcraftedSource extends ThreadDumpSource:
    import java.lang.management.{LockInfo, ThreadInfo}

    private val indent: String = " ".repeat(4)
    private val mxbRuntime     = ManagementFactory.getRuntimeMXBean
    private val mxbThread      = ManagementFactory.getThreadMXBean
    private val dtFormatter    = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

    override def collect(): String =
      val sb = new StringBuilder()
      sb.append(LocalDateTime.now().format(dtFormatter)).append(lineSeparator)
      sb.append(s"Full thread dump ${mxbRuntime.getVmName} (${mxbRuntime.getVmVersion}):")
        .append(lineSeparator)
        .append(lineSeparator)
      if mxbThread.isObjectMonitorUsageSupported && mxbThread.isSynchronizerUsageSupported
      then dumpThreadInfoWithLocks(sb)
      else dumpThreadInfo(sb)
      sb.append(lineSeparator).result

    override def close(): Unit = ()

    private def dumpThreadInfo(sb: StringBuilder): Unit =
      mxbThread.getAllThreadIds.foreach { tid =>
        printThreadInfo(mxbThread.getThreadInfo(tid, Int.MaxValue), sb)
      }

    private def dumpThreadInfoWithLocks(sb: StringBuilder): Unit =
      mxbThread
        .dumpAllThreads(true, true)
        .foreach { ti =>
          printThreadInfo(ti, sb)
          printLockInfo(ti.getLockedSynchronizers, sb)
        }

    private def printThreadInfo(ti: ThreadInfo, sb: StringBuilder): Unit =
      printThread(ti, sb)
      val stacktrace = ti.getStackTrace
      val monitors   = ti.getLockedMonitors
      for i <- stacktrace.indices do
        sb.append(s"${indent}at ${stacktrace(i)}").append(lineSeparator)
        for (j <- 1 until monitors.length) do
          val mi = monitors(j)
          if mi.getLockedStackDepth == i then sb.append(s"$indent  - locked $mi").append(lineSeparator)
      sb.append(lineSeparator)

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    private def printThread(ti: ThreadInfo, sb: StringBuilder): Unit =
      val s = new StringBuilder(s""""${ti.getThreadName}" nid=${ti.getThreadId} state=${ti.getThreadState}""")
      if Objects.nonNull(ti.getLockName) && ti.getThreadState != Thread.State.BLOCKED then
        val lockInfo = ti.getLockName.split("@")
        s.append(lineSeparator).append(s"$indent- waiting on <0x${lockInfo(1)}> (a ${lockInfo(0)})")
        s.append(lineSeparator).append(s"$indent- locked <0x${lockInfo(1)}> (a ${lockInfo(0)})")
      else if Objects.nonNull(ti.getLockName) && ti.getThreadState == Thread.State.BLOCKED then
        val lockInfo = ti.getLockName.split("@")
        s.append(lineSeparator).append(s"$indent- waiting to lock <0x${lockInfo(1)}> (a ${lockInfo(0)})")
      if ti.isSuspended then s.append(" (suspended)")
      if ti.isInNative then s.append(" (running in native)")
      sb.append(s.result).append(lineSeparator)
      if Objects.nonNull(ti.getLockOwnerName) then
        sb.append(s"$indent owned by ${ti.getLockOwnerName} id=${ti.getLockOwnerId}").append(lineSeparator)

    private def printLockInfo(locks: Array[LockInfo], sb: StringBuilder) =
      sb.append(s"${indent}Locked synchronizers: count = ${locks.length}").append(lineSeparator)
      locks.foreach(li => sb.append(s"$indent  - $li").append(lineSeparator))
      sb.append(lineSeparator)

  end TdaHandcraftedSource

end ThreadDumpSource
