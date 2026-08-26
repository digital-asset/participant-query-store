// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.auth

import com.digitalasset.scribe.docker.{Docker, Service}
import com.digitalasset.scribe.functest.FuncTest
import com.digitalasset.scribe.functest.matchers.*
import com.digitalasset.scribe.services.daml.*
import com.digitalasset.scribe.services.oauth.OAuth
import com.digitalasset.scribe.services.postgres.Postgres
import com.digitalasset.scribe.services.proxy.ForwardProxy
import com.digitalasset.scribe.services.scribe.{CliRun, Scribe}
import zio.*
import zio.test.*

import scala.language.implicitConversions

object OAuthProxySpec
    extends FuncTest[
      Service[OAuth.Instance] & Service[ForwardProxy.Instance] & TokenService & Service[Ledger] & Postgres & DeployedDar
    ]:

  // Starts OAuth and ForwardProxy, then creates a proxy network and moves OAuth off the main
  // network so it's only reachable through the proxy. Downstream layers only see the services
  // after network isolation is complete.
  private def networkIsolation(
      oauthLayer: RLayer[Docker, Service[OAuth.Instance]],
      proxyLayer: RLayer[Docker, Service[ForwardProxy.Instance]]
  ): RLayer[Docker, Service[OAuth.Instance] & Service[ForwardProxy.Instance]] =
    ZLayer.scopedEnvironment {
      for
        docker <- ZIO.service[Docker]
        dockerLayer = ZLayer.succeed(docker)
        oauthEnv  <- oauthLayer.build
        proxyEnv  <- proxyLayer.build
        networkId <- docker.createNetwork("proxy-isolation")
        _         <- docker.connectToNetwork[OAuth.Instance](networkId).provideEnvironment(oauthEnv)
        _         <- docker.connectToNetwork[ForwardProxy.Instance](networkId).provideEnvironment(proxyEnv)
        _         <- docker.disconnectFromNetwork[OAuth.Instance](docker.mainNetworkId).provideEnvironment(oauthEnv)
        _ <- ZIO.addFinalizer {
          // Disconnect containers from proxy network before createNetwork's finalizer removes it.
          // Without this, removeNetworkCmd fails with "network has active endpoints".
          (docker.disconnectFromNetwork[OAuth.Instance](networkId).provideEnvironment(oauthEnv) *>
            docker.disconnectFromNetwork[ForwardProxy.Instance](networkId).provideEnvironment(proxyEnv)).ignoreLogged
        }
      yield oauthEnv ++ proxyEnv
    }

  private val pingPong = DamlSource(
    "PingPong" -> """module PingPong where
                    |
                    |import Daml.Script
                    |import DA.Functor (void)
                    |
                    |template Ping
                    |  with
                    |    owner: Party
                    |  where
                    |    signatory owner
                    |
                    |transact : Party -> Script ()
                    |transact party = void do
                    |  submit party $ createCmd Ping with owner = party
                    |""".stripMargin
  )

  lazy val shared =
    networkIsolation(OAuth.instance, ForwardProxy.instance(OAuth.port)) >+> TokenService.liveViaProxy
      >+> DamlSdk.ledger ++ Postgres.instance
      >+> DamlSdk.dar(pingPong)
      >+> DamlSdk.deploy

  private val alice = Party("Alice")

  private val context =
    Postgres.database ++ DamlSdk.parties(alice)

  private val proxyUrl =
    Docker.inspect[ForwardProxy.Instance].map(i => s"http://${i.container.hostName}:${ForwardProxy.port}")

  private val oauthUrl =
    Docker.inspect[OAuth.Instance].map(i => s"https://${i.container.hostName}:${OAuth.port}/issuer1/token")

  def spec = suite("oauth-proxy")(
    funcTest("pipeline acquires token through proxy"):
      Given:
        context
      When:
        ZLayer.fromZIO(proxyUrl).flatMap { env =>
          Scribe.runPipeline(
            "--pipeline-oauth-clientid=participant_admin",
            s"--pipeline-oauth-proxy-url=${env.get[String]}",
            s"--pipeline-oauth-proxy-user=${ForwardProxy.defaultUser}",
            s"--pipeline-oauth-proxy-password=${ForwardProxy.defaultPassword}",
            "--pipeline-ledger-stop=Latest"
          )
        }
      Expect:
        Scribe.exitCode `is` ExitCode.success
    ,
    funcTest("pipeline fails without proxy when OAuth is network-isolated"):
      Given:
        context
      When:
        Scribe
          .attemptPipeline("--pipeline-oauth-clientid=participant_admin", "--pipeline-ledger-stop=Latest")
          .flatMap(CliRun.fromSvc)
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        oauthUrl.flatMap(url => Scribe.stderr `is` stringContaining(s"Exception when sending request: POST $url"))
    ,
    funcTest("pipeline fails with wrong proxy credentials"):
      Given:
        context
      When:
        ZLayer.fromZIO(proxyUrl).flatMap { env =>
          Scribe
            .attemptPipeline(
              "--pipeline-oauth-clientid=participant_admin",
              s"--pipeline-oauth-proxy-url=${env.get[String]}",
              "--pipeline-oauth-proxy-user=wronguser",
              "--pipeline-oauth-proxy-password=wrongpass",
              "--pipeline-ledger-stop=Latest"
            )
            .flatMap(CliRun.fromSvc)
        }
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        oauthUrl.flatMap(url => Scribe.stderr `is` stringContaining(s"Exception when sending request: POST $url"))
    ,
    funcTest("pipeline fails when proxy is unreachable"):
      Given:
        context
      When:
        Scribe
          .attemptPipeline(
            "--pipeline-oauth-clientid=participant_admin",
            "--pipeline-oauth-proxy-url=http://nonexistent-proxy:9999",
            "--pipeline-ledger-stop=Latest"
          )
          .flatMap(CliRun.fromSvc)
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        oauthUrl.flatMap(url => Scribe.stderr `is` stringContaining(s"Exception when sending request: POST $url"))
      And:
        Scribe.stderr.is(stringContaining("java.net.UnknownHostException: nonexistent-proxy"))
    ,
    funcTest("pipeline fails without proxy credentials"):
      Given:
        context
      When:
        ZLayer.fromZIO(proxyUrl).flatMap { env =>
          Scribe
            .attemptPipeline(
              "--pipeline-oauth-clientid=participant_admin",
              s"--pipeline-oauth-proxy-url=${env.get[String]}",
              "--pipeline-ledger-stop=Latest"
            )
            .flatMap(CliRun.fromSvc)
        }
      Expect:
        Scribe.exitCode `is` ExitCode.failure
      And:
        oauthUrl.flatMap(url => Scribe.stderr `is` stringContaining(s"Exception when sending request: POST $url"))
  )
