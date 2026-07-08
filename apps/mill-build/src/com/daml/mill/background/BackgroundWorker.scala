// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.mill.background

import mill.api.Ctx
import mill.api.Result
import mill.util.Jvm.{javaExe, jdkTool}
import os.Shellable
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class BackgroundWorker private (private val env: Map[String, String], private val workerCtx: Ctx) {
  def env(kv: (String, String)): BackgroundWorker = new BackgroundWorker(env + kv, workerCtx)

  def run(args: Shellable*)(implicit taskCtx: Ctx): Unit = {
    val proc = spawn(args)(taskCtx)
    proc.join()
    if (proc.exitCode() != 0) throw new RuntimeException(s"Failed with exit code ${proc.exitCode()}")
  }

  def command(args: Shellable*)(implicit taskCtx: Ctx): Result[Unit] = {
    val proc = spawn(args)(taskCtx)
    proc.join()
    if (proc.exitCode() == 0) Result.Success(())
    else Result.Failure(s"Failed with exit code ${proc.exitCode()}")
  }

  def spawn(args: Shellable*)(implicit taskCtx: Ctx): os.SubProcess = BackgroundWorker.synchronized {
    /*
    The algo is the following:
     * Try to atomically create a .lock folder
     * If it's successful:
       - spawn the process
       - periodically update modified time of .lock
       - monitor for deletion of .lock or appearance of .lock/stop. Either should start shutdown of the child process
       - when shutdown is complete, remove .lock folder
     * If it's unsuccessful,
       - Create a .lock/stop file
       - If modification of .lock folder is old enough, consider it abandoned and remove it, start from beginning
       - If creation of .lock folder didn't occur within timeout, fail.
     */

    val taskName = (taskCtx.dest relativeTo workerCtx.workspace).segments.drop(1).mkString(".").dropRight(5)
    os.makeDir.all(taskPath)

    val theArgs =
      Vector(javaExe) ++
        Vector("-cp", runnerDir.toIO.getCanonicalPath, "BackgroundWrapper", lockDir.toIO.getCanonicalPath) ++
        args.flatMap(_.value)

    workerCtx.log.debug(s"Run subprocess with args: ${theArgs.map(a => s"'$a'").mkString(" ")}")
    val barrier = Promise[Unit]()
    val proc = os
      .proc(theArgs)
      .spawn(
        cwd = taskCtx.dest,
        env = env,
        stdout =
          os.ProcessOutput.Readlines(x => taskCtx.log.info(s"[${fansi.Bold.On(taskName)}] ${fansi.Str.Strip(x)}")),
        stderr = os.ProcessOutput.Readlines(x =>
          if (x == "=Lock acquired=") barrier.trySuccess(())
          else taskCtx.log.error(taskCtx.dest.toString() + ": " + fansi.Str.Strip(x).plainText)
        )
      )
    proc.wrapped.onExit().thenRun(() => barrier.tryFailure(new IllegalStateException("Process exited prematurely")))
    taskCtx.log.debug(s"${taskCtx.dest}: Waiting for lock")
    Await.result(barrier.future, 1.minute)
    taskCtx.log.debug(s"${taskCtx.dest}: Lock acquired")
    proc
  }

  def stopAll(): Unit = BackgroundWorker.synchronized {
    def locks = Try(os.walk(procDir).filter(_.toIO.getName == ".lock")).getOrElse(Seq.empty)
    locks.foreach(lock => os.write.over(lock / "stop", ""))
    val finished = Future { while (locks.nonEmpty) Thread.sleep(200) }
    Await.ready(finished, 1.minute)
    os.remove.all(procDir)
  }

  def isAlive(implicit taskCtx: Ctx): Boolean = BackgroundWorker.synchronized {
    Try(
      lockDir.toIO.exists() &&
        !(lockDir / "stop").toIO.exists() &&
        (System.currentTimeMillis() - os.mtime(lockDir)).millis < 5.seconds
    ).getOrElse(false)
  }

  private val runnerDir                       = workerCtx.dest / "runner"
  private val procDir                         = workerCtx.dest / "proc"
  private def taskPath(implicit taskCtx: Ctx) = procDir / (taskCtx.dest relativeTo workerCtx.workspace)
  private def lockDir(implicit taskCtx: Ctx)  = taskPath / ".lock"

  BackgroundWorker.compileBgWrapperTo(runnerDir)(workerCtx)
}

object BackgroundWorker {
  def apply(env: Map[String, String] = Map.empty)(implicit ctx: Ctx) = new BackgroundWorker(env, ctx)

  private def compileBgWrapperTo(target: os.Path)(implicit ctx: Ctx): Unit = synchronized {
    if (!os.exists(target)) {
      os.makeDir.all(target)
      val sourceFile = target / "BackgroundWrapper.java"
      os.write.over(
        sourceFile,
        wrapperCode
      )
      val result =
        os.proc(jdkTool("javac"), "--target", "8", "--source", "8", sourceFile.toIO.getCanonicalPath).call().exitCode
      if (result != 0) throw new RuntimeException(s"Can't compile BackgroundWrapper ($result)")
    }
  }

  private val wrapperCode = """// Background Wrapper
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class BackgroundWrapper {
    public static void main(String[] args) throws Exception {
        final int argsN = 1;
        String lockdir = args[0];
        Path lockPath = Paths.get(lockdir);
        File lockDir = lockPath.toFile();
        File stopFile = new File(lockDir, "stop");
        if (!lockDir.getParentFile().isDirectory()) {
            System.err.println(lockDir.getCanonicalPath() + " isn't a directory");
            System.exit(1);
        }

        // Trying to acquire exclusive lock
        long start = System.currentTimeMillis();
        while (!lockDir.mkdir()) {
            long now = System.currentTimeMillis();
            long lastModified = java.nio.file.Files.getLastModifiedTime(lockPath, LinkOption.NOFOLLOW_LINKS).toMillis();
            if (now - start > 10000) { // Timeout
                System.err.println("Can't get exclusive lock");
                System.exit(1);
            }
            if (now - lastModified > 5000) { // .lock dir seems to be abandoned
                System.err.println("Lock seems to be abandoned, removing");
                deleteDir(lockPath);
            } else { // Notify previous process about intent to stop it
                stopFile.createNewFile();
            }
            Thread.sleep(200);
        }
        System.err.println("=Lock acquired=");

        String[] realArgs = new String[args.length - argsN];
        for (int i = 0; i < args.length - argsN; i++) {
            realArgs[i] = args[i + argsN];
        }
        Process proc = new ProcessBuilder(realArgs).inheritIO().redirectErrorStream(true).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (proc.isAlive()) {
                    System.err.println("Forcibly destroying the process");
                    proc.destroy();
                }
            } catch (Exception ignore) {
            }
            try {
                deleteDir(lockPath);
            } catch (Exception ignore) {
            }
        }));

        // monitor
        Thread monitor = new Thread(() -> {
            try {
                long lastModified = System.currentTimeMillis();
                while (true) {
                    if (!lockDir.exists() || stopFile.exists()) { // initiate shutdown
                        proc.destroy();
                        if (!proc.waitFor(2, TimeUnit.SECONDS)) {
                            System.err.println("Process didn't exit within specified time. Abandoning...");
                            System.exit(127);
                        }
                        System.exit(proc.exitValue());
                    }

                    if (System.currentTimeMillis() - lastModified > 1000) {
                        lastModified = System.currentTimeMillis();
                        Files.setLastModifiedTime(lockPath, java.nio.file.attribute.FileTime.fromMillis(lastModified));
                    }
                    Thread.sleep(200);
                }
            } catch (Exception ignore) {
                System.exit(1);
            }
        });
        monitor.setDaemon(true);
        monitor.start();

        System.exit(proc.waitFor());
    }


    private static void deleteDir(Path path) throws IOException {
        try (Stream<Path> pathStream = Files.walk(path)) {
            pathStream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }
}
"""
}
