// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.functest.sbt

import com.digitalasset.pqs.docker.{Docker, DockerResources}
import com.digitalasset.pqs.functest.{Dpm, FTEnv, FTLogging, FuncTest}
import sbt.testing.TaskDef
import zio.test.{Spec, TestAspect, TestEnvironment, TestFailure, TestSuccess, ZIOSpecDefault, assertTrue}
import zio.{Cause, Chunk, EnvironmentTag, ExecutionStrategy, IO, Ref, Scope, Unsafe, ZIO, ZLayer}
import zio.test.TestAspect.PerTest

import java.nio.file.{Files, Path, Paths}

object FTSpec extends ZIOSpecDefault:
  var tasks: Array[TaskDef] = Array()

  var forceResourcePools: Option[Int] = Option.empty[Int]
  var forcePoolLanes: Option[Int]     = Option.empty[Int]

  def spec: Spec[TestEnvironment & Scope, Any] =
    val memGb = dockerResources.availableMemoryBytes / 1e9
    if memGb < 4 then
      test(
        f"Docker has only $memGb%.1fGB available — functional tests require at least 4GB. " +
          "Increase Docker Desktop memory allocation in Settings → Resources."
      )(ZIO.succeed(assertTrue(false)))
    else
      import scala.math.Ordering.Implicits.seqOrdering
      val allTestCases                     = tasks.toSeq.flatMap(task => expandSpec(toSpec(task))).sortBy(_.labels)
      val (suiteTestTotals, millResultLog) = initProgress(allTestCases)
      val testsDone                        = Unsafe.unsafe(implicit u => Ref.unsafe.make(Map.empty[String, Int]))
      val pooledSpecs = allTestCases
        .groupBy(_.sharedLayer)
        .values
        .toSeq
        .sortBy(group => -group.size)
        .zipWithIndex
        .map((cases, ix) => flattenSpec(cases, ix, suiteTestTotals, millResultLog, testsDone))
      println("---")
      val layers = FTLogging.layer ++ Docker.live ++ (FTEnv.layer >+> Dpm.layer)
      Spec
        .exec(ExecutionStrategy.ParallelN(resourcePools), Spec.multiple(Chunk.fromIterable(pooledSpecs)))
        // Note: using zio.test.live here is necessary so FTEnv have access to all env variables that configure the test.
        .provideSomeLayerShared[TestEnvironment & Scope](
          ZLayer.fromZIOEnvironment(
            zio.test.live(layers.build.tap(_ => log(pooledSpecs)))
          )
        )
  end spec

  private val hostProcessors = Runtime.getRuntime.availableProcessors()
  private val hostMemory     = Runtime.getRuntime.maxMemory()

  /** Docker daemon resource limits, which on Docker Desktop are typically lower than the host OS. Falls back to
    * JVM/OS-level values if Docker is unreachable.
    */
  private val dockerResources: DockerResources =
    Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe
        .run(Docker.queryResources)
        .getOrElse(_ => DockerResources(hostProcessors, hostMemory, hostMemory))
    }

  /** Memory model constants derived from benchmarking across 4–16GB Docker configurations at 10 CPUs. memoryNeeded =
    * OverheadGb + pools × (PoolBaseGb + lanes × LaneCostGb)
    *
    * Validated against two full benchmark sweeps (2026-02-24). Key boundaries: 1×4 OOMs at 7GB / passes at 8GB; 2×4
    * OOMs at 12GB; 3×3 OOMs at 14GB; 3×4 passes at 16GB.
    */
  private val OverheadGb = 2.5 // Docker daemon, kernel, fixed overhead + safety margin
  private val PoolBaseGb = 2.0 // Per-pool: Canton (~1.5GB), Postgres (~200MB), misc
  private val LaneCostGb = 0.8 // Per-lane: Pqs/DamlSdk containers + GC/fragmentation overhead

  /** Compute pool and lane counts jointly to maximize total parallelism (pools × lanes) while staying within Docker
    * memory and CPU limits. When one or both dimensions are forced via CLI, the other is optimized within the remaining
    * budget.
    *
    * Memory model (from benchmarks at 10 CPUs across 4–16GB Docker): memoryNeeded = 2.5GB overhead + pools × (2.0GB
    * base + lanes × 0.8GB) CPU constraint: at least 2 cores per pool. Lanes capped at 4.
    */
  private lazy val autoParallelism: (Int, Int) =
    val maxCpuPools = 1 max (dockerResources.cpus / 2)
    val memGb       = dockerResources.availableMemoryBytes / 1e9
    val budget      = memGb - OverheadGb
    (forceResourcePools, forcePoolLanes) match
      case (Some(p), Some(l)) => (p, l)
      case (Some(p), None) =>
        val l = 1.max(((budget / p - PoolBaseGb) / LaneCostGb).intValue).min(4)
        (p, l)
      case (None, Some(l)) =>
        val p = 1 max (budget / (PoolBaseGb + l * LaneCostGb)).intValue min maxCpuPools
        (p, l)
      case (None, None) =>
        // Lookup table derived from benchmark sweeps (2026-02-24) at 10 CPUs.
        // Each tier is the highest known-safe (pools×lanes) for that memory range.
        // For ≥16GB the formula takes over, scaling with available resource headroom.
        val (p, l) = memGb match
          case m if m < 6  => (1, 1) //  4– 5 GB: minimal parallelism
          case m if m < 8  => (1, 2) //  6– 7 GB: 1×4 OOMs at 7GB
          case m if m < 10 => (1, 4) //  8– 9 GB: full lanes, single pool
          case m if m < 12 => (2, 2) // 10–11 GB: 2×3 borderline at 10GB
          case m if m < 14 => (2, 3) // 12–13 GB: 2×4 OOMs at 12GB
          case m if m < 16 => (2, 3) // 14–15 GB: 2×4 and 3x3 OOMs at 14GB
          case _ => // 16+   GB: scale with formula
            val candidates = for
              p <- 1 to maxCpuPools
              l <- 1 to 4
              if p * (PoolBaseGb + l * LaneCostGb) < budget
            yield (p, l)
            candidates.maxByOption((p, l) => (p * l, p)).getOrElse((3, 4))
        (p min maxCpuPools, l)

  private def resourcePools: Int = autoParallelism._1
  private def poolLanes: Int     = autoParallelism._2

  /** TestAspect that races each test against the Docker OOM signal. When any container is OOM-killed, all running tests
    * fail immediately with a descriptive error instead of spinning in retry loops.
    */
  private val oomGuard = new PerTest.AtLeastR[Docker]:
    def perTest[R <: Docker, E](
        test: ZIO[R, TestFailure[E], TestSuccess]
    )(implicit trace: zio.Trace): ZIO[R, TestFailure[E], TestSuccess] =
      ZIO.serviceWithZIO[Docker] { docker =>
        val oomWatch: IO[TestFailure[E], Nothing] =
          docker.oomSignal.promise.await.mapError(t => TestFailure.Runtime(Cause.die(t)))
        test.raceFirst(oomWatch)
      }

  private def initProgress(allTestCases: Seq[TestCase[Any]]): (Map[String, Int], Option[Path]) =
    val suiteTestTotals = allTestCases.groupBy(_.labels.headOption.getOrElse("?")).map((k, v) => k -> v.size)
    val millResultLog   = findMillResultLog()
    (suiteTestTotals, millResultLog)

  private def recordTestDone(
      suiteName: String,
      suiteTestTotals: Map[String, Int],
      millResultLog: Option[Path],
      testsDone: Ref[Map[String, Int]]
  ): ZIO[Any, Nothing, Unit] =
    val total = suiteTestTotals.getOrElse(suiteName, Int.MaxValue)
    testsDone
      .updateAndGet(m => m.updated(suiteName, m.getOrElse(suiteName, 0) + 1))
      .flatMap(m =>
        ZIO.when(m.getOrElse(suiteName, 0) >= total)(writeMillResultLog(m, suiteTestTotals, millResultLog)).unit
      )

  /** Mill's result.log for the functest test task. The ticker thread in the parent process polls this every 20ms. Mill
    * runs tests in a sandbox subdirectory of test.dest, and result.log is a sibling of sandbox/.
    */
  private def findMillResultLog(): Option[Path] =
    val workDir = Paths.get(System.getProperty("user.dir"))
    val path    = workDir.getParent.resolve("result.log")
    if Files.exists(path) then Some(path) else None

  /** Write suite completion count to Mill's result.log so the ticker shows real progress. Format matches Mill's
    * expected upickle serialization of (Long, Long): [success,failure]
    */
  private def writeMillResultLog(
      done: Map[String, Int],
      suiteTestTotals: Map[String, Int],
      millResultLog: Option[Path]
  ): ZIO[Any, Nothing, Unit] =
    millResultLog.fold(ZIO.unit) { path =>
      val completed = suiteTestTotals.count((suite, total) => done.getOrElse(suite, 0) >= total)
      ZIO.attempt(Files.writeString(path, s"[$completed,0]")).ignore
    }

  private def log(pooledSpecs: Iterable[Spec[?, ?]]) =
    zio.Console.ConsoleLive.printLine {
      val reservedGb = (dockerResources.memoryBytes - dockerResources.availableMemoryBytes) / 1e9
      val dockerStats =
        f"Docker: ${dockerResources.cpus} CPUs, ${dockerResources.memoryBytes / 1e9}%1.1fGb total, " +
          f"${dockerResources.availableMemoryBytes / 1e9}%1.1fGb available " +
          f"(${reservedGb}%1.1fGb reserved by other containers)"

      f"""Starting Func Test. Using $resourcePools pools with $poolLanes lanes in each
         |Host: $hostProcessors CPUs, ${hostMemory / 1e9}%1.1fGb JVM heap
         |$dockerStats
         |Detected groups of specs sharing infrastructure: ${pooledSpecs.size}""".stripMargin
    }

  private case class TestCase[FR](
      sharedLayer: ZLayer[FTEnv & Dpm & Docker, Throwable, FR],
      sharedTag: EnvironmentTag[FR],
      labels: Seq[String],
      spec: Spec[FR & FTEnv & Dpm & Docker & TestEnvironment & Scope, Any]
  ):
    def toSpec: Spec[FR & FTEnv & Dpm & Docker & TestEnvironment & Scope, Any] =
      labels.foldRight(spec)(Spec.labeled)

  private def expandSpec[FR](funcTest: FuncTest[FR]): Seq[TestCase[FR]] =
    def loop(
        labels: Seq[String],
        spec: Spec[FR & FTEnv & Dpm & Docker & TestEnvironment & Scope, Any]
    ): Seq[TestCase[FR]] = spec.caseValue match
      case Spec.MultipleCase(specs) =>
        specs.flatMap(loop(labels, _))
      case Spec.LabeledCase(label, spec) =>
        loop(labels :+ label, spec)
      case Spec.TestCase(_, _) =>
        Seq(TestCase(funcTest.shared, funcTest.sharedTag, labels, spec))
      case other =>
        throw UnsupportedOperationException(s"$other case is not supported")
    loop(Vector.empty, funcTest.spec)

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  private def flattenSpec[FR](
      testCases: Seq[TestCase[FR]],
      groupIx: Int,
      suiteTestTotals: Map[String, Int],
      millResultLog: Option[Path],
      testsDone: Ref[Map[String, Int]]
  ): Spec[TestEnvironment & Scope & FTEnv & Dpm & Docker, Any] =
    println(s"Group $groupIx:")
    val specs = testCases.map { testCase =>
      val suiteName = testCase.labels.headOption.getOrElse("?")
      println("  - " + testCase.labels.mkString(" / "))
      testCase.toSpec @@ TestAspect.after(recordTestDone(suiteName, suiteTestTotals, millResultLog, testsDone))
    }
    given EnvironmentTag[FR] = testCases.head.sharedTag
    Spec
      .exec(
        ExecutionStrategy.ParallelN(poolLanes),
        Spec.multiple(Chunk.fromIterable(specs))
      )
      .provideSomeLayerShared[TestEnvironment & Scope & FTEnv & Dpm & Docker](
        ZLayer.fromZIOEnvironment(
          // Using zio.test.live is necessary to access real time and compute duration
          zio.test.live(
            testCases.head.sharedLayer.build.timed
              .tap((duration, _) => ZIO.log(s"Initialized shared environment in ${duration.toMillis} ms"))
              .map((_, env) => env)
          )
        )
      ) @@ oomGuard @@ FTLogging.silentTest
  end flattenSpec

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def toSpec(taskDef: TaskDef) =
    import org.portablescala.reflect.*
    val fqn             = taskDef.fullyQualifiedName().stripSuffix("$") + "$"
    val testClassLoader = Thread.currentThread().getContextClassLoader
    Reflect
      .lookupLoadableModuleClass(fqn, testClassLoader)
      .getOrElse(throw new ClassNotFoundException("failed to load object: " + fqn))
      .loadModule()
      .asInstanceOf[FuncTest[Any]]
  end toSpec
