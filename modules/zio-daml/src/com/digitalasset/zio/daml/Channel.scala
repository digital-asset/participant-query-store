// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.auth.TokenService
import com.digitalasset.scribe.configuration.ISO8601Duration
import com.digitalasset.scribe.grpc.{ZClientInterceptor, ZManagedChannel}
import com.digitalasset.zio.daml.ledgerapi.PingService
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.{ManagedChannelBuilder, Metadata, StatusException}
import zio.ZIO.{logInfo, logWarningCause}
import zio.{Schedule, ZIO, ZLayer}

import java.util.concurrent.TimeUnit
import scala.language.implicitConversions

object Channel:
  val grpcUpGauge = zio.metrics.Metric.gauge("grpc_up", "Grpc channel is up")

  private val AuthHeader = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  val live: ZLayer[TokenService & Config, Throwable, ZManagedChannel] = ZLayer.scoped {
    for
      conf         <- ZIO.service[Config]
      tokenService <- ZIO.service[TokenService]
      mkBuilder = () => if requiresTls(conf.tls) then mkSecureChannel(conf) else mkInsecureChannel(conf)
      interceptor = ZClientInterceptor.intercept { md =>
        tokenService.getAccessToken.some
          .flatMap(t => md.put(AuthHeader, t))
          .unsome
          .mapError(ex => StatusException(io.grpc.Status.UNAUTHENTICATED.withCause(ex)))
      }
      channel <- ZManagedChannel(mkBuilder(), conf.bufferSize, interceptor).build

      _ <- ZIO.acquireRelease(grpcUpGauge.set(1))(_ => grpcUpGauge.set(0))

      // launch ping in background
      _ <- PingService.ping
        .timeoutFail(StatusException(io.grpc.Status.UNAVAILABLE.withDescription("Keep-alive ping timed out")))(
          zio.Duration.fromMillis(conf.keepAlive.timeout.toMillis)
        )
        .foldCauseZIO(
          cause =>
            grpcUpGauge.set(0) *>
              logWarningCause(s"Keep-alive (get ledger version) failed", cause),
          _ =>
            grpcUpGauge.set(1) *>
              logInfo("Keep-alive (get ledger version) successful")
        )
        .repeat(Schedule.spaced(conf.keepAlive.time))
        .provide(ZLayer.succeed(channel.get))
        .forkScoped
    yield channel.get
  }

  private def requiresTls(conf: TlsConfig) =
    conf.privateKey.nonEmpty || conf.caCertificate.nonEmpty

  private def mkInsecureChannel(conf: Config): ManagedChannelBuilder[?] =
    ManagedChannelBuilder
      .forAddress(conf.host, conf.port)
      .maxInboundMessageSize(Int.MaxValue)
      .keepAliveTime(conf.keepAlive.time.maxValueIfZero, TimeUnit.MILLISECONDS)
      .keepAliveTimeout(conf.keepAlive.timeout.toMillis, TimeUnit.MILLISECONDS)
      .usePlaintext

  private def mkSecureChannel(conf: Config) =
    NettyChannelBuilder
      .forAddress(conf.host, conf.port)
      .maxInboundMessageSize(Int.MaxValue)
      .keepAliveTime(conf.keepAlive.time.maxValueIfZero, TimeUnit.MILLISECONDS)
      .keepAliveTimeout(conf.keepAlive.timeout.toMillis, TimeUnit.MILLISECONDS)
      .useTransportSecurity
      .sslContext(mkSslContext(conf.tls))

  private def mkSslContext(conf: TlsConfig) = {
    GrpcSslContexts.forClient
      .modifyOpt(conf.caCertificate)(cacert => _.trustManager(cacert))
      .modifyOpt(conf.privateKey)(pkey => _.keyManager(conf.certificate.getOrElse(pkey), pkey))
      .build
  }

  extension [A](self: A) def modifyOpt[B](arg: Option[B])(f: B => A => A): A = arg.map(f(_)(self)).getOrElse(self)
  extension (duration: ISO8601Duration)
    def maxValueIfZero: Long = if duration.isZero then Long.MaxValue else duration.toMillis

end Channel
