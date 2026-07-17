// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import com.digitalasset.transcode.schema.{PackageId, PackageName, PackageVersion}
import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.{Dpm, FTConfig, FTEnv}
import com.digitalasset.pqs.grpc.{ZClientInterceptor, ZManagedChannel}
import com.digitalasset.pqs.services.daml
import com.digitalasset.pqs.services.daml.specific.toOffset
import com.digitalasset.pqs.services.oauth.OAuth
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import org.semver4j.Semver
import zio.ZIO.{attemptBlocking, logInfo, suspend}
import zio.test.{Spec, TestAspectAtLeastR}
import zio.*

import java.io.ByteArrayInputStream
import java.util.jar.JarFile.MANIFEST_NAME
import java.util.zip.ZipInputStream
import scala.util.Using

object DamlSdk:
  private val maxRequestSize: Int = 30 * 1024 * 1024

  ////////////
  // Layers //
  ////////////

  /** Compile DAML sources and wrap them into DarFile layer */
  def dar(source: DamlSource): RLayer[Dpm & FTEnv, DarFile] = ZLayer.fromZIO(buildDar(source))

  /** Compile DAML sources and wrap them into DarFile layer */
  private def buildDar(source: DamlSource): ZIO[Dpm & FTEnv, Throwable, DarFile] =
    val packages = (source :: source.deps).distinct.toList
    for
      multiPackageDir <- FTEnv.createUniqueDirectory(s"build-dar-${source.name}")
      mainPackageDir = multiPackageDir / source.name
      _ <- writeMultiPackage(multiPackageDir, packages)
      _ <- Dpm.buildDar(mainPackageDir)
      darPath = mainPackageDir / ".daml" / "dist" / s"${source.name}-${source.version}.dar"
      darBytes <- attemptBlocking {
        os.read.bytes(darPath)
      }
      packageInfo <- attemptBlocking {
        Using
          .Manager { use =>
            val dar = use(ZipInputStream(use(ByteArrayInputStream(darBytes))))
            LazyList.continually(dar.getNextEntry).takeWhile(e => !MANIFEST_NAME.equalsIgnoreCase(e.getName)).force
            val manifest = new java.util.jar.Manifest()
            manifest.read(ByteArrayInputStream(dar.readAllBytes()))
            dar.closeEntry()
            val extract      = "(.+?)-([^-]+)-([^-]+).dalf".r
            val packageNames = packages.map(_.name)
            manifest.getMainAttributes.getValue("Dalfs").split("\\s*,\\s*").toList.map(_.split("/").last).collect {
              case extract(name, version, id) if packageNames.contains(PackageName(name)) =>
                (PackageName(name), PackageVersion(version), PackageId(id))
            }
          }
          .fold(x => throw new IllegalStateException("Can't determine dar's package id", x), identity)
      }
      darFile = DarFile(source, mainPackageDir, darPath, darBytes, packageInfo)
    yield darFile

  private def writeMultiPackage(
      multiPackageDir: os.Path,
      packages: List[DamlSource]
  ): ZIO[FTEnv, Throwable, Unit] =
    for
      _ <- ZIO.foreachDiscard(packages)(pkg => writePackage(multiPackageDir / pkg.name, pkg))
      multiPackageYaml =
        s"""|packages:
            |${packages.map(p => s"  - ./${p.name}").mkString("\n")}
            |""".stripMargin
      _ <- attemptBlocking(os.write(multiPackageDir / "multi-package.yaml", multiPackageYaml))
    yield ()

  private def writePackage(packageDir: os.Path, pkg: DamlSource): ZIO[FTEnv, Throwable, Unit] =
    for
      damlVersion  <- FTEnv.damlSdkVersion
      damlLfTarget <- FTEnv.damlLfTarget
      _ <- ZIO.foreachDiscard(pkg.contents) { (module, contents) =>
        val modulePath = packageDir / "daml" / os.RelPath(module.replace('.', '/') + ".daml")
        attemptBlocking(os.write(modulePath, contents, createFolders = true))
      }
      // We use override-components because we don't have a full dpm SDK version,
      // but only the daml version for the daml components
      damlYaml =
        s"""|override-components:
            |  damlc:
            |    version: $damlVersion
            |  daml-script:
            |    version: $damlVersion
            |name: ${pkg.name}
            |source: daml
            |version: ${pkg.version}
            |build-options:
            |  - "--target=$damlLfTarget"
            |  $buildOptions
            |dependencies:
            |  - daml-prim
            |  - daml-stdlib
            |  - daml-script
            |${pkg.deps.map(d => s"  - ../${d.name}/.daml/dist/${d.name}-${d.version}.dar").mkString("\n")}
            |""".stripMargin
      _ <- attemptBlocking(os.write(packageDir / "daml.yaml", damlYaml))
    yield ()

  private def semverSatisfies(version: String)(x: String): Boolean =
    Semver.coerce(x).withClearedPreReleaseAndBuild().satisfies(version)

  def onlyDamlLfVersion(version: String)   = onlyConfigValue(_.damlLfTarget, semverSatisfies(version))
  def onlyPostgresVersion(version: String) = onlyConfigValue(_.postgresVersion, semverSatisfies(version))
  def onlyCantonVersion(version: String)   = onlyConfigValue(_.cantonVersion, semverSatisfies(version))

  def onlyConfigValue[A](f: FTConfig => A, matcher: A => Boolean): TestAspectAtLeastR[FTEnv] =
    new TestAspectAtLeastR[FTEnv]:
      def some[R >: Nothing <: FTEnv, E >: Nothing <: Any](spec: Spec[R, E])(implicit trace: Trace): Spec[R, E] =
        spec.whenZIO(ZIO.service[FTEnv].map(env => f(env.config)).map(matcher))

  val ledger: RLayer[FTEnv & Docker, Service[Ledger]] =
    CantonConf.layer >+> canton("canton")

  private def canton(
      prefix: String,
      suppressOutput: Boolean = true
  ): RLayer[FTEnv & CantonConf & Docker, Service[Ledger]] =
    ZLayer
      .fromZIO(
        for
          version         <- FTEnv.cantonVersion
          protocolVersion <- FTEnv.cantonProtocolVersion
          cnt             <- Docker.share(s"${prefix}_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
          hostname = s"$prefix-$cnt"
          cantonConf <- ZIO.service[CantonConf]
          files      <- cantonConf(hostname, Ledger.participantPort, "mydomain", 5081, protocolVersion, maxRequestSize)
          ftEnv      <- ZIO.service[FTEnv]
          svc = Docker
            .service[Ledger](
              image = cantonConf.cantonDockerImage,
              exposePorts = Set(Ledger.participantPort),
              prepopulateFiles = files,
              hostname = Some(hostname),
              env = cantonConf.cantonEnvVarMap,
              user = Some(cantonConf.user),
              suppressOutput = !ftEnv.showCantonLogs
            )(cantonConf.cantonAdditionalCmds*)
            .tap(_.get.blockUntilStdOut(_.contains(cantonConf.bootstrapCompleteMessage)))
        yield svc
      )
      .flatten

  val deploy: RLayer[Docker & Service[Ledger] & DarFile, DeployedDar] =
    ZLayer
      .fromZIO(
        for
          dar   <- ZIO.service[DarFile]
          mutex <- Docker.share("upload_dar" -> dar.packageId)(Semaphore.make(1))
          alreadyExists = api.listPackageIds.map(_.toSet.contains(dar.packageId))
          upload        = suspend(api.uploadDar(dar))
          _ <- mutex.withPermit(upload.unlessZIO(alreadyExists))
        yield DeployedDar(dar)
      )

  /** Allocate parties on ledger and wrap them in a layer */
  def parties(parties: Party*): RLayer[Docker & Service[Ledger], Parties] =
    ZLayer.fromZIO(for
      partyCounter <- Docker.share("party_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
      hints = parties.map(party => s"${party.prefix}_$partyCounter")
      ids           <- ZIO.foreach(hints) { hint => api.allocateParty(hint) }
      oauthInstance <- Docker.inspectMaybe[OAuth.Instance]
      ps <- ZIO.foreach(parties zip hints zip ids) {
        case ((p, hint), id) =>
          p._name.set(Some(hint)) *> p._id.set(Some(id))
            *> ZIO.unless(oauthInstance.isEmpty) { api.grantRights(id) }.as(p)
      }
    yield Parties(ps))

  /** Discover already-allocated parties on the participant by prefix match. */
  def allocatedParties(parties: Party*): RLayer[Docker & Service[Ledger], Parties] =
    ZLayer.fromZIO(
      for
        knownParties <- api.listKnownParties
        ps <- ZIO.foreach(parties) { party =>
          ZIO
            .fromOption(knownParties.find(_.party.startsWith(s"${party.prefix}::")))
            .flatMap(details => party._name.set(Some(party.prefix)) *> party._id.set(Some(details.party)).as(party))
            .orElseFail(Throwable(s"Allocated party ${party.prefix} not found on participant"))
        }
      yield Parties(ps)
    )

  def users(users: daml.User*): RLayer[Docker & Service[Ledger], Users] =
    ZLayer.fromZIO(ZIO.foreach(users)(createUser).map(Users.apply))

  private def createUser(user: daml.User) =
    for
      primaryParty <- user.primaryParty.id
      userId       <- user.id
      canActAs     <- ZIO.foreach(user.canActAs)(_.id)
      canReadAs    <- ZIO.foreach(user.canReadAs)(_.id)
      _            <- api.createUser(userId, primaryParty, primaryParty +: canActAs, canReadAs, user.canReadAsAnyParty)
    yield user

  class PrunedTo(offset: String | Long)

  /** Prune ledger up to offset */
  def pruneLedger(upToOffset: String | Long): RLayer[Docker & Service[Ledger], PrunedTo] =
    ZLayer.fromZIO(upToOffset.toOffset.flatMap(api.pruneLedger).as(PrunedTo(upToOffset)))

  /** Run script and store result in the layer */
  def runScript[IN: upickle.default.Writer](
      name: String,
      args: Task[IN]
  ): ZLayer[Docker & Service[Ledger] & DarFile & FTEnv & Dpm, Throwable, Unit] =
    ZLayer.scoped(for
      scriptDir         <- FTEnv.createUniqueDirectory(s"runscript-$name")
      ca                <- Docker.certificateAuthority
      cert              <- ca.generate("runscript")
      rootCaCrt         <- writeFile(scriptDir / "tls" / "root-ca.crt", ca.certificate.crt)
      participantPem    <- writeFile(scriptDir / "tls" / "participant.pem", cert.certificate.pem)
      participantCrt    <- writeFile(scriptDir / "tls" / "participant.crt", cert.certificate.crt)
      ledger            <- ZIO.service[Service[Ledger]]
      adminTokenService <- inspectMaybe[TokenService]
      dar               <- ZIO.service[DarFile]
      argsV             <- args
      version           <- FTEnv.damlSdkVersion
      inputJson         <- ZIO.attempt(upickle.default.write(argsV, escapeUnicode = true))
      inputFile         <- writeFile(scriptDir / "input.json", inputJson)
      outputFile = scriptDir / "output.json"
      participantAdminToken <- ZIO.whenCase(adminTokenService) {
        case Some(ts) =>
          ts.getParticipantAdminToken
            .flatMap(token => writeFile(scriptDir / "access.token", token.stripPrefix("Bearer ")))
      }
      _ <- logInfo(s"Running script [$name] with arguments: $inputJson")
      _ <- Dpm.runScript(
        packageDir = dar.mainPackageDir,
        ledgerHost = ledger.exposedAddress,
        ledgerPort = ledger.exposedPorts(Ledger.participantPort),
        scriptName = name,
        darPath = dar.darPath,
        maxRequestSize = maxRequestSize,
        inputFile = inputFile,
        outputFile = outputFile,
        crt = participantCrt,
        pem = participantPem,
        cacrt = rootCaCrt,
        accessTokenFile = participantAdminToken
      )
    yield ())

  private def writeFile(file: os.Path, content: String): Task[os.Path] =
    ZIO.attemptBlocking(os.write(file, content, createFolders = true)).as(file)

  ///////////////
  // Internals //
  ///////////////

  private lazy val participantChannel: RLayer[Docker & Service[Ledger], ZManagedChannel] =
    val authHeader = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
    ZLayer.scoped(for
      svc               <- Docker.inspect[Ledger]
      adminTokenService <- inspectMaybe[TokenService]
      ca                <- Docker.certificateAuthority
      cert              <- ca.generate("participant")
      mkBuilder = () =>
        NettyChannelBuilder
          .forAddress(svc.exposedAddress, svc.exposedPorts(Ledger.participantPort))
          .useTransportSecurity()
          .sslContext(
            GrpcSslContexts.forClient
              .keyManager(cert.certificate.privateKey, cert.certificate.certificate)
              .trustManager(ca.certificate.certificate)
              .build()
          )
      interceptor = ZClientInterceptor.intercept { md =>
        ZIO
          .whenCase(adminTokenService) {
            case Some(ts) => ts.getParticipantAdminToken.flatMap { token => md.put(authHeader, token) }
          }
          .orDie
      }
      channel <- ZManagedChannel(mkBuilder(), 128, interceptor).build
    yield channel.get)

  lazy val api = Api(participantChannel)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def inspectMaybe[T: Tag]: UIO[Option[T]] =
    ZIO.environment[Any].mapAttempt(_.asInstanceOf[ZEnvironment[T]].get[T]).option
end DamlSdk
