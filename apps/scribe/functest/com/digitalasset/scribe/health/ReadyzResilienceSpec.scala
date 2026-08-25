// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.health

import com.digitalasset.scribe.docker.Client
import com.digitalasset.scribe.docker.Client.run
import com.digitalasset.scribe.docker.Docker
import com.digitalasset.scribe.functest.FuncTestStandalone
import com.digitalasset.scribe.functest.matchers.is
import com.digitalasset.scribe.health.HealthEndpoint.readyzStatusAndField
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.scribe.Scribe
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.SyncDockerCmd
import zio.{ZIO, durationInt}
import zio.http.Status
import zio.json.ast.Json

import scala.language.{implicitConversions, postfixOps}

/** This test must remain standalone because it stops and starts containers, which would interfere with any other tests
  * sharing the same infrastructure.
  */
object ReadyzResilienceSpec extends FuncTestStandalone:
  private val alice = Party("Alice")
  private val ping = DamlSource(
    "Ping" -> """module Ping where
                |
                |import Daml.Script
                |import DA.Functor (void)
                |
                |template Ping
                |  with
                |    sender: Party
                |    receiver: Party
                |  where
                |    signatory sender
                |    observer receiver
                |
                |transact1: Party -> Script ()
                |transact1 alice = void do
                |  submit alice $ createCmd Ping with sender = alice, receiver = alice
                |""".stripMargin
  )

  def spec = suite("readyz resilience")(
    funcTest("Postgres failure and recovery"):
      Given:
        DamlSdk.dar(ping) ++ DamlSdk.ledger ++ Postgres.instance
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        DamlSdk.runScript[String]("Ping:transact1", alice.id)
      And:
        Scribe.pipeline(
          "--health-address=0.0.0.0",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Never",
          "--target-postgres-probeinterval=PT2S"
        )
      Expect:
        readyzStatusAndField("jdbc_connection_pool_up") `is` Some((Status.Ok, Some(Json.Bool(true)))) retryUntilTimeout
      When:
        // We stop/start the Postgres container instead of pausing, because Docker pause freezes the process
        // but keeps TCP sockets open in kernel buffers, so the JDBC probe's SELECT 1 blocks indefinitely rather than
        // failing. Stop actually kills the process, breaking connections immediately.
        ZIO.serviceWith[Postgres](_.service).flatMap(pg => stopContainer(pg.container.containerId))
      Then:
        readyzStatusAndField("jdbc_connection_pool_up") `is`
          Some((Status.ServiceUnavailable, Some(Json.Bool(false)))) retryUntilTimeout

      When:
        ZIO.serviceWith[Postgres](_.service).flatMap(pg => startContainer(pg.container.containerId))
      Then:
        readyzStatusAndField("jdbc_connection_pool_up")
          .is(Some((Status.Ok, Some(Json.Bool(true)))))
          .retryUntilTimeout(50.seconds)
    ,
    funcTest("Canton failure and recovery"):
      Given:
        DamlSdk.dar(ping) ++ DamlSdk.ledger ++ Postgres.instance
      And:
        DamlSdk.deploy ++ DamlSdk.parties(alice) ++ Postgres.database
      And:
        DamlSdk.runScript[String]("Ping:transact1", alice.id)
      And:
        Scribe.pipeline(
          "--health-address=0.0.0.0",
          "--pipeline-ledger-start=Genesis",
          "--pipeline-ledger-stop=Never",
          "--source-ledger-keepalive-time=PT5S",
          "--source-ledger-keepalive-timeout=PT1S",
          "--retry-backoff-cap=PT3S"
        )
      Expect:
        readyzStatusAndField("grpc_up") `is`
          Some((Status.Ok, Some(Json.Bool(true)))) retryUntilTimeout

      When:
        // We pause/unpause the Canton container because stop would lose the uploaded DARs, requiring
        // a full redeploy. Pause works here because gRPC keepalive has its own timeout.
        Docker.inspect[Ledger].flatMap(l => pauseContainer(l.container.containerId))
      Then:
        readyzStatusAndField("grpc_up") `is`
          Some((Status.ServiceUnavailable, Some(Json.Bool(false)))) retryUntilTimeout

      When:
        Docker.inspect[Ledger].flatMap(l => unpauseContainer(l.container.containerId))
      Then:
        readyzStatusAndField("grpc_up") `is` Some((Status.Ok, Some(Json.Bool(true)))) retryUntilTimeout
  )

  private def containerCmd(cmd: DockerClient => SyncDockerCmd[?]) =
    ZIO
      .serviceWithZIO[DockerClient](client => cmd(client).run.unit)
      .provideLayer(Client.live)

  private def stopContainer(containerId: String)    = containerCmd(_.stopContainerCmd(containerId))
  private def startContainer(containerId: String)   = containerCmd(_.startContainerCmd(containerId))
  private def pauseContainer(containerId: String)   = containerCmd(_.pauseContainerCmd(containerId))
  private def unpauseContainer(containerId: String) = containerCmd(_.unpauseContainerCmd(containerId))
