// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest

import com.digitalasset.scribe
import com.digitalasset.scribe.docker.{ContainerExitedException, Docker, SuiteName}
import com.digitalasset.scribe.functest.FuncTest.*
import zio.*
import zio.ZIO.{logDebug, logInfo}
import zio.internal.stacktracer.SourceLocation
import zio.test.*

import scala.annotation.implicitNotFound
import scala.collection.mutable
import scala.compiletime.uninitialized

object FuncTest:
  private[functest] val defaultLayerTimeout = 5.minutes
  private val instructionTimeout            = 30.seconds

  extension [R, E >: Throwable](io: ZIO[R, E, TestResult])
    def atTheEndOfTheDay: ZIO[R, E, TestResult] =
      val retryFailures = Schedule.identity[TestResult].whileInput(_.isFailure)
      val log = Schedule.identity[TestResult].tapOutput { tr =>
        lazy val assertions = FailureCase.getPath(tr.result).map((a, b) => s"$a → $b").mkString("; ")
        logDebug(s"Retrying [$assertions]").when(tr.isFailure)
      }
      val backoff  = Schedule.exponential(10.millis, 2) || Schedule.spaced(1.second)
      val upto     = Schedule.upTo(instructionTimeout)
      val schedule = retryFailures <* log <* backoff <* upto
      io.repeat(schedule)
    end atTheEndOfTheDay

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
abstract class FuncTest[FR: EnvironmentTag] extends ZIOSpec[FTEnv & Dpm & Docker & FR]:
  private type Instruction[R] = ZIO[R & Scope & FR & Docker, Throwable, TestResult]

  def shared: ZLayer[FTEnv & Dpm & Docker, Throwable, FR]
  val sharedTag = summon[EnvironmentTag[FR]]

  protected def layerTimeout: Duration = FuncTest.defaultLayerTimeout

  def specName: String = getClass.getSimpleName.replaceAll("\\$", "")

  override final def bootstrap: ZLayer[Any, Any, FTEnv & Dpm & Docker & FR] =
    FTLogging.layer >+> FTEnv.layer >+> Dpm.layer >+> Docker.live >+> shared

  def funcTest[RProv, RReq](label: String)(init: Ctx[RProv, RReq] ?=> Unit)(implicit
      @implicitNotFound("Environment requirement not satisfied.\nRequested:\n${RReq}\n\nProvided:\n${RProv}\n${FR}")
      ev: (RProv & FR & FTEnv & Dpm & Docker & Scope) <:< RReq,
      trace: Trace
  ): Spec[Scope & FTEnv & Dpm & Docker & FR, Throwable] = test(label):
    given ctx: Ctx[RProv, RReq] = new Ctx[RProv, RReq]
    init

    def loop(instr: List[Instruction[RReq]]): Instruction[Any] = instr match
      case head :: tail =>
        head
          .provideSomeEnvironment[Scope & FR & Docker](e => e.unionAll(ctx.requiredEnv))
          .flatMap(tr => if tr.isSuccess then loop(tail).map(tr && _) else ZIO.succeed(tr))
      case Nil => assertCompletesZIO

    execute(
      logInfo(label)
        *> ZIO.runtime.flatMap(runtime => ZIO.attempt(ctx.runtime = runtime.asInstanceOf[Runtime[RProv]]))
        *> loop(ctx.instructions.toList)
        <* logInfo(s"End of $label")
    )

  private def execute(spec: Instruction[Any]) = live(
    for
      counter <- Docker.share("_testCounter")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
      res     <- SuiteName(Some(counter))(spec)
    yield res
  )

  // setup layers
  private def setupLayer[DEP, R](
      layer: => ZLayer[DEP, Throwable, R]
  )(implicit ctx: Ctx[? <: R, ? <: DEP], trace: Trace) =
    ctx.instructions.append(
      ZIO
        .suspend(layer.build)
        .provideSomeEnvironment[Scope & DEP](e => e.unionAll(ctx.requiredEnv))
        .timeoutFail(TestTimeoutException("Layer setup timed out"))(layerTimeout)
        .retry(
          Schedule.recurWhile {
            case _: TestTimeoutException     => true
            case _: ContainerExitedException => true
            case _                           => false
          } *> Schedule.recurs(2) // retry 2 more times and give up
        )
        .mapAttempt { env =>
          ctx.env = ctx.env.unionAll(env)
          zio.test.assertCompletes
        }
    )

  def Given[R1](
      layer: => ZLayer[FTEnv & Dpm & Docker & FR & Scope, Throwable, R1]
  )(implicit ctx: Ctx[? <: R1, ? <: FTEnv & Dpm & Docker & FR & Scope], trace: Trace) =
    setupLayer(layer)
  def When[DEP, R1](
      layer: => ZLayer[DEP, Throwable, R1]
  )(implicit ctx: Ctx[? <: R1, ? <: DEP], trace: Trace, dummyImplicit: DummyImplicit) =
    setupLayer(layer)
  def And[DEP, R1](
      layer: => ZLayer[DEP, Throwable, R1]
  )(implicit ctx: Ctx[? <: R1, ? <: DEP], trace: Trace, dummyImplicit: DummyImplicit) =
    setupLayer(layer)

  // Interactions
  private def interactWithoutAssertion[R](io: => ZIO[R, Throwable, Any])(implicit ctx: Ctx[?, ? <: R], trace: Trace) =
    assert(io.as(zio.test.assertCompletes))
  private def assert[R](io: => ZIO[R, Throwable, TestResult])(implicit ctx: Ctx[?, ? <: R], trace: Trace) =
    ctx.instructions.append(
      ZIO.suspend(io.timeoutFail(TestTimeoutException("Test instruction timed out"))(instructionTimeout * 2))
    )
  def When[R](io: => ZIO[R, Throwable, Any])(implicit ctx: Ctx[?, ? <: R], trace: Trace) = interactWithoutAssertion(io)
  def Expect[R](io: => ZIO[R, Throwable, TestResult])(implicit ctx: Ctx[?, ? <: R], trace: Trace) = assert(io)
  def Then[R](io: => ZIO[R, Throwable, TestResult])(implicit ctx: Ctx[?, ? <: R], trace: Trace)   = assert(io)
  def And[R](io: => ZIO[R, Throwable, TestResult])(implicit ctx: Ctx[?, ? <: R], trace: Trace)    = assert(io)

  class Ctx[RProv, RReq]:
    private[FuncTest] var runtime: zio.Runtime[RProv] = uninitialized
    private[FuncTest] var env: ZEnvironment[RProv]    = ZEnvironment.empty.asInstanceOf[ZEnvironment[RProv]]
    private[FuncTest] def requiredEnv: ZEnvironment[RReq] =
      runtime.environment.unionAll(env).asInstanceOf[ZEnvironment[RReq]]
    private[FuncTest] val instructions = mutable.Buffer.empty[Instruction[RReq]]
  end Ctx

  extension [R, E >: Throwable](io: ZIO[R, E, TestResult])
    def atTheEndOfTheDay: ZIO[R, E, TestResult] = FuncTest.atTheEndOfTheDay(io)

end FuncTest
