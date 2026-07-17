// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.auth

import com.digitalasset.pqs.configuration.{ISO8601Duration, Secret}
import com.digitalasset.pqs.utils.safeequals.===
import okhttp3.{Credentials, OkHttpClient, Response as OkResponse, Route}
import sttp.capabilities.Effect
import sttp.client4.*
import sttp.client4.okhttp.OkHttpSyncBackend
import sttp.monad.MonadError
import zio.ZIO.{attempt, fail, logDebug, logError, logInfo}
import zio.stm.{TRef, ZSTM}
import zio.{Clock, Duration, RIO, RLayer, Schedule, Task, ULayer, ZIO, ZLayer, durationLong}

import java.io.{File, FileInputStream, IOException}
import java.net.{InetSocketAddress, Proxy, URI}
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.util.Base64
import java.util.concurrent.TimeUnit
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}
import scala.language.implicitConversions
import scala.util.{Try, Using}

trait TokenService:
  def getAccessToken: Task[Option[AccessToken]]

object TokenService:
  private val GrantType         = "grant_type"
  private val ClientCredentials = "client_credentials"
  private val Scope             = "scope"
  private val Params            = List(GrantType -> ClientCredentials)

  type Token = (AccessToken, Duration)

  val live: RLayer[Auth, TokenService] =
    ZLayer
      .service[Auth]
      .project {
        case conf: Auth.OAuth =>
          ZLayer.succeed(HttpClientParams(conf.caCertificate, conf.proxy))
            >>> httpClient
            ++ ZLayer.succeed(conf)
            >>> auth
        case Auth.AccessToken(token) => fromToken(Some(token))
        case Auth.NoAuth             => fromToken(None)
      }
      .flatten

  case class HttpClientParams(
      caCertificate: Option[File],
      proxy: Option[Config.ProxyConfig] = None
  )

  // OkHttp is used instead of the JDK's built-in HTTP clients because both java.net.http.HttpClient
  // and java.net.HttpURLConnection fail to handle proxy authentication (HTTP 407) during HTTPS
  // CONNECT tunneling. HttpClient silently drops the connection after the 407 challenge-response,
  // and HttpURLConnection corrupts its connection pool so all subsequent attempts get
  // "Connection refused". OkHttp's proxyAuthenticator handles CONNECT + Basic auth correctly.
  // The sync OkHttp backend is wrapped in ZIO.attemptBlocking to provide Backend[Task].
  lazy val httpClient: RLayer[HttpClientParams, Backend[Task]] =
    ZLayer.scoped {
      ZIO.service[HttpClientParams].flatMap { params =>
        val backend = buildBackend(params)
        ZIO.acquireRelease(ZIO.succeed(backend))(_.close().ignoreLogged)
      }
    }

  // Null: OkHttp's Authenticator interface requires returning null to signal "stop retrying".
  // Equals: header() returns nullable Java String; === doesn't support null comparisons.
  // AsInstanceOf: Effect[_] is a phantom type erased at runtime, so the cast is safe.
  @SuppressWarnings(
    Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null", "org.wartremover.warts.Equals")
  )
  private def buildBackend(params: HttpClientParams): Backend[Task] =
    val builder = new OkHttpClient.Builder()

    for
      proxy    <- params.proxy
      proxyUrl <- proxy.url
    do
      val port = proxyUrl.getPort match
        case -1 =>
          proxyUrl.getScheme match
            case "https" => 443
            case _       => 80
        case p => p
      builder.proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(proxyUrl.getHost, port)))
      proxy.user.foreach { user =>
        val password = proxy.password.fold("")(_.value)
        // OkHttp calls this authenticator on HTTP 407 responses during CONNECT tunneling,
        // which correctly handles the challenge-response cycle that the JDK clients cannot.
        // Return null when credentials were already sent to avoid an infinite retry loop.
        builder.proxyAuthenticator { (_: Route, response: OkResponse) =>
          if response.request().header("Proxy-Authorization") != null then null
          else response.request().newBuilder().header("Proxy-Authorization", Credentials.basic(user, password)).build()
        }
      }

    params.caCertificate.foreach { caFile =>
      val (sslCtx, tm) = buildSslContext(caFile)
      builder.sslSocketFactory(sslCtx.getSocketFactory, tm)
    }

    val syncBackend = OkHttpSyncBackend.usingClient(builder.build())
    // The asInstanceOf cast is safe because Effect[_] is a phantom type erased at runtime,
    // and only basic (non-streaming) requests are sent here. If a streaming request is ever
    // passed, the cast will succeed but the sync backend will fail at the sttp level.
    new Backend[Task]:
      override def send[T](request: GenericRequest[T, Effect[Task]]): Task[Response[T]] =
        ZIO.attemptBlocking(syncBackend.send(request.asInstanceOf))
      override def close(): Task[Unit]     = ZIO.attemptBlocking(syncBackend.close())
      override def monad: MonadError[Task] = new sttp.client4.impl.zio.RIOMonadAsyncError[Any]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def buildSslContext(caFile: File): (SSLContext, X509TrustManager) =
    val cf   = CertificateFactory.getInstance("X.509")
    val cert = Using.resource(FileInputStream(caFile))(cf.generateCertificate)
    val ks   = KeyStore.getInstance(KeyStore.getDefaultType)
    ks.load(null, null)
    ks.setCertificateEntry("ca", cert)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ks)
    val sslCtx = SSLContext.getInstance("TLS")
    sslCtx.init(null, tmf.getTrustManagers, null)
    val tm = tmf.getTrustManagers.collectFirst { case x: X509TrustManager => x }
    (sslCtx, tm.getOrElse(throw IllegalStateException("No X509TrustManager found")))

  private def fromToken(token: Option[AccessToken]): ULayer[TokenService] = ZLayer.succeed(
    new TokenService {
      def getAccessToken: Task[Option[AccessToken]] = ZIO.succeed(token.map(bearer))
    }
  )

  def auth: RLayer[Backend[Task] & Auth.OAuth, TokenService] = ZLayer.scoped(
    for
      conf     <- ZIO.service[Auth.OAuth]
      tokenRef <- TRef.makeCommit(Option.empty[Either[Throwable, Token]])

      getToken = tokenRef.get.collectSTM {
        case Some(Left(err))    => ZSTM.fail(err)
        case Some(Right(value)) => ZSTM.succeed(Some(value._1))
      }.commit

      retrySchedule = (Schedule.upTo(30.seconds) *> (Schedule.exponential(10.millis, 2) || Schedule.spaced(1.seconds)))
        .tapInput((ex: Throwable) => logInfo(s"Retrying auth token acquisition: ${ex.getMessage}"))

      refreshSchedule = Schedule.identity[Either[Throwable, Token]].addDelay {
        case Right((_, delay)) => delay `minus` conf.preemptExpiry
        case _                 => Duration.Zero
      }

      tokenEndpoint <- getTokenEndpoint(conf.issuer, conf.endpoint)

      _ <- acquireToken(conf.clientId, conf.clientSecret, tokenEndpoint, conf.scope, conf.parameters)
        .retry(retrySchedule)
        .either
        .tap(result => tokenRef.set(Some(result)).commit)
        .repeat(refreshSchedule)
        .forkScoped

      _ <- getToken
      _ <- logDebug(
        s"Initialised with tokenEndpoint=${conf.endpoint.toString}, clientId=${conf.clientId} " +
          s"and parameters=${conf.parameters.map((k, v) => s"$k=$v").mkString("(", ",", ")")}"
      )
    yield new TokenService {
      def getAccessToken: Task[Option[AccessToken]] = getToken
    }
  )

  def getTokenEndpoint(issuer: Option[URI], endpoint: Option[URI]): RIO[Backend[Task], URI] =
    (issuer, endpoint) match
      case (_, Some(endpoint)) => ZIO.succeed(endpoint)
      case (Some(issuer), _) =>
        for
          backend <- ZIO.service[Backend[Task]]
          response <- basicRequest
            .get(uri"${issuer.toString.stripSuffix("/")}/.well-known/openid-configuration")
            .response(asStringAlways)
            .send(backend)
          tokenEndpoint <-
            if response.code.isSuccess then parseOIDCProviderConfigForTokenEndpoint(response.body)
            else
              logError("Failed OpenID provider configuration response") *>
                fail(IOException("OpenID provider configuration returned an unexpected response"))
        yield tokenEndpoint
      case _ =>
        // this shouldn't actually happen because we validate this config elsewhere
        fail(IOException(s"No issuer or endpoint configured"))

  def acquireToken(
      clientId: String,
      clientSecret: Secret,
      endpoint: URI,
      scope: Option[String],
      parameters: Map[String, String]
  ): RIO[Backend[Task], Token] =
    for
      _ <- logInfo("Acquiring auth token")
      _ <- logDebug(
        s"acquireToken with tokenEndpoint=${endpoint.toString}, clientId=$clientId " +
          s"and parameters=${parameters.map((k, v) => s"$k=$v").mkString("(", ",", ")")}"
      )
      backend <- ZIO.service[Backend[Task]]
      params   = Params ++ scope.map(s => Scope -> s).toList ++ parameters.toList
      formBody = params.distinct
      _ <- logRequest(clientId, endpoint, formBody.map((k, v) => s"$k=$v").mkString("&"))
      response <- basicRequest
        .post(uri"${endpoint.toString}")
        .body(formBody*)
        .auth
        .basic(clientId, clientSecret.value)
        .response(asStringAlways)
        .send(backend)
      token <-
        if response.code.isSuccess then logInfo(s"Successful auth token response") *> parseToken(response.body)
        else if response.code.code === 400 then
          val detail = Try {
            val json = ujson.read(response.body)
            json("error").str + Try(":" + json("error_description").str).getOrElse("")
          }.getOrElse("unknown error")
          logError(s"Failed auth token response because of Bad Request ($detail)") *> fail(
            Throwable(s"Failed auth token response because of Bad Request ($detail)")
          )
        else
          fail(IOException(s"Auth token couldn't be acquired due to: HTTP ${response.code.code}"))
            .tapError(x => logError(x.getMessage))
      _ <- logInfo(s"Token acquired, expires in ${token._2}.")
    yield token
  end acquireToken

  private def parseToken(rawToken: String): Task[Token] =
    for
      now <- Clock.currentTime(TimeUnit.SECONDS)
      token <- attempt {
        ujson.read(rawToken)("access_token").str
      }
      expires <- attempt {
        val parts   = token.split("\\.")
        val payload = String(Base64.getDecoder.decode(parts(1).getBytes()))
        ujson.read(payload)("exp").num.toLong
      }
      accessToken  = bearer(token)
      refreshDelay = (expires - now).seconds
    yield accessToken -> refreshDelay
  end parseToken

  private def parseOIDCProviderConfigForTokenEndpoint(rawDocument: String): Task[URI] =
    for tokenEndpointURL <- attempt { ujson.read(rawDocument)("token_endpoint").str } yield URI(tokenEndpointURL)
  end parseOIDCProviderConfigForTokenEndpoint

  private def logRequest(clientId: String, endpoint: URI, body: String) =
    // Log with debug level to avoid logging sensitive information in production logs.
    logDebug(s"POST ${endpoint.toURL.toString} [$body] using clientId=$clientId")

  private def bearer(token: String) = (if token.startsWith("Bearer ") then token else "Bearer " + token).trim
end TokenService
