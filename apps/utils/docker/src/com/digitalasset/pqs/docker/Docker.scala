// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.docker

import com.digitalasset.pqs.docker.Client.{run, stream}
import com.digitalasset.pqs.docker.tls.CertificateAuthority
import com.digitalasset.pqs.utils.safeequals.===
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.InspectContainerResponse
import com.github.dockerjava.api.model.StreamType.{STDERR, STDOUT}
import com.github.dockerjava.api.model.{ExposedPort, Frame, HostConfig, Mount, MountType, PortBinding}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream}
import org.apache.commons.io.output.ByteArrayOutputStream
import os.Shellable
import zio.*
import zio.ZIO.*
import zio.stream.ZPipeline

import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters.*
import scala.util.Try

trait Docker:
  def share[R, E, A](key: Serializable)(compute: ZIO[R & Scope, E, A]): ZIO[R, E, A]

  def service[T: Tag](
      image: String,
      exposePorts: Set[Int] = Set.empty,
      env: Map[String, Any] = Map.empty,
      prepopulateFiles: Seq[(os.Path, String | Array[Byte])] = Seq.empty,
      hostname: Option[String] = None,
      user: Option[Int] = None,
      suppressOutput: Boolean = false
  )(cmd: Shellable*): ZLayer[Any, Throwable, Service[T]]

  def run[T: Tag](
      image: String,
      env: Map[String, Any] = Map.empty,
      prepopulateFiles: Seq[(os.Path, String | Array[Byte])] = Seq.empty,
      user: Option[Int]
  )(cmd: Shellable*): ZIO[Scope, Throwable, Service[T]]

  def inspect[T: Tag]: URIO[Service[T], Service[T]]

  def inspectMaybe[T: Tag]: UIO[Option[Service[T]]]

  def certificateAuthority: CertificateAuthority

  /** The ID of the main session network all containers are connected to by default. */
  def mainNetworkId: String

  /** Create an additional scoped Docker network. Removed when scope closes. */
  def createNetwork(name: String): ZIO[Scope, Throwable, String]

  /** Connect a running container to an additional network. */
  def connectToNetwork[T: Tag](networkId: String): ZIO[Service[T], Throwable, Unit]

  /** Disconnect a container from a network. */
  def disconnectFromNetwork[T: Tag](networkId: String): ZIO[Service[T], Throwable, Unit]

  /** Promise that is failed when any container managed by this Docker instance is OOM-killed. Tests can race against
    * this to fail instantly on OOM rather than spinning in retry loops.
    */
  def oomSignal: OomSignal

  /** Safety net: removes any containers that weren't cleaned up by individual scope finalizers.
    */
  def cleanupAllContainers: UIO[Unit]
end Docker

object Docker:
  private val newInstance =
    (sharedStorage ++ Client.live)
      >+> mode
      >+> ZLayer
        .service[Mode]
        .flatMap(m =>
          CertificateAuthority.generate(m.get match
            case Mode.Local => Seq.empty
            case Mode.CI    => (1 to 255).map(ix => s"172.17.0.$ix")
          )
        )
      >+> dockerSession
      >+> oomSignalLayer
      >>> ZLayer.fromFunction(Impl.apply)

  /** Provides docker container runtime. Please note that for shared storage to work, the user of this layer must ensure
    * that only one instance is created (by properly choosing ZIO Scope). This is particularly important with tests, as
    * running tests in parallel will result in separate shared storage for each test if not configured to be shared
    * between test classes.
    */
  val live: ZLayer[Any, Throwable, Docker] =
    ZLayer.scopedEnvironment {
      for
        env <- newInstance.build
        // Safety net: on scope close, remove any containers with our RunId prefix that
        // weren't cleaned up by individual scope finalizers (e.g., due to OOM race conditions).
        _ <- ZIO.addFinalizer {
          env.get[Docker].cleanupAllContainers
        }
      yield env
    }

  def share[R, E, A](key: Serializable)(compute: ZIO[R & Scope, E, A]): ZIO[R & Docker, E, A] =
    serviceWithZIO[Docker](_.share(key)(compute))

  def service[T: Tag](
      image: String,
      exposePorts: Set[Int] = Set.empty,
      env: Map[String, Any] = Map.empty,
      prepopulateFiles: Seq[(os.Path, String | Array[Byte])] = Seq.empty,
      hostname: Option[String] = None,
      user: Option[Int] = None,
      suppressOutput: Boolean = false
  )(cmd: Shellable*): ZLayer[Docker, Throwable, Service[T]] =
    ZLayer
      .service[Docker]
      .flatMap(_.get.service(image, exposePorts, env, prepopulateFiles, hostname, user, suppressOutput)(cmd))

  def run[T: Tag](
      image: String,
      env: Map[String, Any] = Map.empty,
      prepopulateFiles: Seq[(os.Path, String | Array[Byte])] = Seq.empty,
      user: Option[Int] = None
  )(cmd: Shellable*): ZIO[Docker & Scope, Throwable, Service[T]] =
    serviceWithZIO[Docker](_.run(image, env, prepopulateFiles, user)(cmd))

  def inspect[T: Tag]: URIO[Docker & Service[T], Service[T]] =
    serviceWithZIO[Docker](_.inspect[T])

  def inspectMaybe[T: Tag]: URIO[Docker, Option[Service[T]]] =
    serviceWithZIO[Docker](_.inspectMaybe[T])

  def certificateAuthority: URIO[Docker, CertificateAuthority] =
    serviceWith[Docker](_.certificateAuthority)

  def mainNetworkId: URIO[Docker, String] =
    serviceWith[Docker](_.mainNetworkId)

  def createNetwork(name: String): ZIO[Docker & Scope, Throwable, String] =
    serviceWithZIO[Docker](_.createNetwork(name))

  def connectToNetwork[T: Tag](networkId: String): ZIO[Docker & Service[T], Throwable, Unit] =
    serviceWithZIO[Docker](_.connectToNetwork[T](networkId))

  def disconnectFromNetwork[T: Tag](networkId: String): ZIO[Docker & Service[T], Throwable, Unit] =
    serviceWithZIO[Docker](_.disconnectFromNetwork[T](networkId))

  /** Query Docker daemon for available CPUs and memory. Uses a short-lived client connection. Accounts for memory
    * already consumed by running containers by querying their actual usage.
    */
  def queryResources: Task[DockerResources] =
    ZIO.scoped {
      Client.live.build.flatMap { env =>
        val client = env.get[DockerClient]
        for
          info <- client.infoCmd().run
          totalMemory = info.getMemTotal.longValue
          cpus        = info.getNCPU.intValue
          containers <- listContainers(client)
          containerMemory <- ZIO.foreachPar(containers) { c =>
            client
              .statsCmd(c.getId)
              .withNoStream(true)
              .stream
              .runHead
              .map(stats =>
                (for
                  s <- stats
                  m <- Option(s.getMemoryStats)
                  u <- Option(m.getUsage)
                yield u.longValue).getOrElse(0L)
              )
              .timeout(5.seconds)
              .map(_.getOrElse(0L))
          }
          reservedMemory  = containerMemory.sum
          availableMemory = 0L.max(totalMemory - reservedMemory)
        yield DockerResources(cpus, totalMemory, availableMemory)
      }
    }

  private case class Impl(
      client: DockerClient,
      sharedScope: Scope,
      sharedStorage: Ref.Synchronized[Map[Serializable, Promise[?, ?]]],
      mode: Mode,
      certificateAuthority: CertificateAuthority,
      session: DockerSession,
      oomSignal: OomSignal
  ) extends Docker:

    /** Provides a K -> V storage that is shared between all ZIOs using this layer. The typical use case is to store
      * synchronized reft to integers here that are used as counters to generate unique docker container names. Let's
      * say that both TestA and TestB want to create independent postgres container each. The name of the container
      * should be postgres-X, where X is some unique number. To achieve that, both can generate a unique container name
      * this way:
      *
      * unique <- share((s"postgresCounter", containerBaseName))(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ +
      * 1)) name = s"postgres-$unique"
      */
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    override def share[R, E, A](key: Serializable)(compute: ZIO[R & Scope, E, A]): ZIO[R, E, A] =
      sharedStorage
        .updateSomeAndGetZIO {
          case storage if !storage.contains(key) =>
            for
              promise <- Promise.make[E, A]
              _       <- sharedScope.extend(compute).exit.flatMap(promise.done).forkIn(sharedScope)
            yield storage.updated(key, promise)
        }
        .flatMap(_(key).asInstanceOf[Promise[E, A]].await)

    override def service[T: Tag](
        image: String,
        exposePorts: Set[Int],
        env: Map[String, Any],
        prepopulateFiles: Seq[(os.Path, String | Array[Byte])],
        hostname: Option[String],
        user: Option[Int],
        suppressOutput: Boolean
    )(
        cmd: Shellable*
    ): ZLayer[Any, Throwable, Service[T]] =
      ZLayer.scoped(ContainerImage(image)(for
        container <- mkContainer(image, env, prepopulateFiles, exposePorts, hostname, user)(cmd)
        svc       <- startContainer[T](container, suppressOutput)
      yield svc))

    override def run[T: Tag](
        image: String,
        env: Map[String, Any],
        prepopulateFiles: Seq[(os.Path, String | Array[Byte])],
        user: Option[Int]
    )(
        cmd: Shellable*
    ): ZIO[Scope, Throwable, Service[T]] = for
      scope    <- ZIO.scope
      svc      <- service[T](image, env = env, prepopulateFiles = prepopulateFiles, user = user)(cmd).build(scope)
      exitCode <- svc.get.exitCode
      _ <- ZIO.fail(RuntimeException(s"Process failed with code $exitCode")).unless(exitCode === ExitCode.success)
    yield svc.get

    def inspect[T: Tag]: URIO[Service[T], Service[T]] =
      ZIO.service[Service[T]]

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def inspectMaybe[T: Tag]: UIO[Option[Service[T]]] =
      ZIO.environment[Any].mapAttempt(_.asInstanceOf[ZEnvironment[Service[T]]].get[Service[T]]).option

    override def mainNetworkId: String = session.dockerNetworkId

    override def createNetwork(name: String): ZIO[Scope, Throwable, String] =
      acquireRelease(
        client.createNetworkCmd().withName(s"${session.prefix}-$name").run.map(_.getId)
      )(id => client.removeNetworkCmd(id).run.ignoreLogged)

    override def connectToNetwork[T: Tag](networkId: String): ZIO[Service[T], Throwable, Unit] =
      for
        svc <- ZIO.service[Service[T]]
        _   <- client.connectToNetworkCmd.withContainerId(svc.container.containerId).withNetworkId(networkId).run
      yield ()

    override def disconnectFromNetwork[T: Tag](networkId: String): ZIO[Service[T], Throwable, Unit] =
      for
        svc <- ZIO.service[Service[T]]
        _ <- client
          .disconnectFromNetworkCmd()
          .withContainerId(svc.container.containerId)
          .withNetworkId(networkId)
          .run
      yield ()

    ///////////////

    private def getFileContents(containerId: String, path: os.Path)(implicit trace: Trace): Task[Array[Byte]] =
      acquireReleaseWith( // acquire
        client.copyArchiveFromContainerCmd(containerId, path.toString).run
      )(is => // release
        attemptBlocking(is.close()).ignoreLogged
      )(is => // use
        attemptBlocking {
          val tis   = new TarArchiveInputStream(is)
          val entry = tis.getNextTarEntry
          tis.readAllBytes()
        }
      )

    private def copyToContainer(
        containerId: String,
        files: Seq[(os.Path, String | Array[Byte])],
        user: Option[Int]
    )(implicit
        trace: Trace
    ): Task[Unit] =
      acquireReleaseWith(attemptBlocking { // acquire
        val bos = new ByteArrayOutputStream()
        val tos = new TarArchiveOutputStream(bos)
        tos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)

        // directories
        val directories = files
          .flatMap((path, _) => (path / os.up).segments.scanLeft(os.root)(_ / _))
          .distinct
          .sortBy(_.segmentCount)
        directories.foreach { path =>
          val entry = new TarArchiveEntry(s"$path/")
          entry.setMode(0x1c0) // 0700 permissions (octal in hex)
          user.foreach(entry.setUserId)
          tos.putArchiveEntry(entry)
          tos.closeArchiveEntry()
        }

        // files
        files.foreach { (path, contents) =>
          val entry = new TarArchiveEntry(path.toString)
          val bytes = contents match
            case ba: Array[Byte] => ba
            case s: String       => s.getBytes(UTF_8)
          entry.setSize(bytes.length)
          entry.setMode(0x180) // 0600 permissions (octal in hex)
          user.foreach(entry.setUserId)
          tos.putArchiveEntry(entry)
          tos.write(bytes)
          tos.closeArchiveEntry()
        }
        tos.finish()
        bos.toInputStream
      })(stream => // release
        attemptBlocking(stream.close()).ignoreLogged
      )(stream => // use
        client.copyArchiveToContainerCmd(containerId).withRemotePath("/").withTarInputStream(stream).run
      ).unless(files.isEmpty).unit

    private def mkContainer(
        image: String,
        env: Map[String, Any],
        prepopulateFiles: Seq[(os.Path, String | Array[Byte])],
        exposePorts: Set[Int],
        hostname: Option[String],
        user: Option[Int]
    )(
        cmd: Shellable*
    )(implicit
        trace: Trace
    ): ZIO[Scope, Throwable, Container] = for
      _ <- pullImageIfNecessary(image)

      unpackedCmd = cmd.flatMap(_.value)
      _ <- acquireRelease( //
        logInfo(s"Creating container [$image] with cmd: [${unpackedCmd.mkString(" ")}]")
      )(_ => //
        logInfo(s"Destroying container [$image]")
      )

      containerBaseName = image.split('/').takeRight(1).mkString.replaceAll("[^a-z]+", "")
      cnt <- share((s"hostnameCounter", containerBaseName))(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
      containerName = s"${session.prefix}-$containerBaseName-$cnt"
      theHostname   = hostname.getOrElse(s"$containerBaseName-$cnt")

      createCmd = client
        .createContainerCmd(image)
        .withEnv(env.toSeq.map((k, v) => s"$k=$v")*)
        .withExposedPorts(exposePorts.toSeq.map(p => new ExposedPort(p))*)
        .withCmd(unpackedCmd*)
        .withName(containerName)
        .withHostName(theHostname)
        .withHostConfig(
          HostConfig
            .newHostConfig()
            .withPortBindings(exposePorts.toSeq.map(p => PortBinding.parse(s"127.0.0.1:$p"))*)
            .withMounts(Seq(Mount().withSource(session.volumeName).withTarget("/ft").withType(MountType.VOLUME)).asJava)
        )

      // create container
      containerId <- acquireRelease( // acquire
        createCmd.run.map(_.getId)
      )(cid =>
        // Try to stop the container gracefully. This allows PQS to close its database connections properly.
        client
          .inspectContainerCmd(cid)
          .run
          .flatMap { info =>
            val running = Option(info.getState.getRunning).exists(_.booleanValue)
            ZIO.when(running)(client.stopContainerCmd(cid).withTimeout(30).run)
          }
          .ignoreLogged *>
          client.removeContainerCmd(cid).withRemoveVolumes(true).withForce(true).run.ignoreLogged
      )

      // connect to docker network
      _ <- acquireRelease( // acquire
        client.connectToNetworkCmd.withContainerId(containerId).withNetworkId(session.dockerNetworkId).run
      )(_ => // release
        client
          .disconnectFromNetworkCmd()
          .withContainerId(containerId)
          .withNetworkId(session.dockerNetworkId)
          .run
          .ignoreLogged
      )

      // prepopulate files
      _ <- copyToContainer(containerId, prepopulateFiles, user)
    yield Container(image, containerId, theHostname, exposePorts)

    /** Safety net: finds and removes all containers whose name starts with our RunId prefix. This catches containers
      * that leaked due to scope finalizer failures (e.g., OOM race conditions). Runs two passes with a brief delay
      * between them to catch containers whose creation was in-flight when the first pass ran (uninterruptible
      * acquireRelease completing after fiber interrupt).
      */
    override def cleanupAllContainers: UIO[Unit] =
      val removeOurs: Task[Int] =
        for
          containers <- listContainers(client, showAll = true)
          ourContainers = containers.filter(_.getNames.exists(_.startsWith(s"/${session.prefix}-")))
          _ <- ZIO.foreachDiscard(ourContainers) { c =>
            val name = c.getNames.headOption.getOrElse(c.getId)
            logWarning(s"Safety-net cleanup: removing container $name") *>
              client.removeContainerCmd(c.getId).withForce(true).withRemoveVolumes(true).run.ignoreLogged
          }
        yield ourContainers.size
      (for
        removed <- removeOurs
        // If we found any, do a second pass after a delay to catch stragglers
        // from in-flight container creations that raced with our first listing.
        _ <- (ZIO.sleep(1.second) *> removeOurs).when(removed > 0)
      yield ()).ignoreLogged

    /** Inspects a container after it exits: checks for OOM-kill and returns the authoritative exit code.
      *
      * The exit code from `waitContainerCmd` is passed as `waitCode` for fallback, but we prefer the value from
      * `inspectContainerCmd` because `waitContainerCmd`'s streaming callback can occasionally report incorrect exit
      * codes for fast-exiting containers (observed as a flake on CI).
      */
    private def verifyExitAndCheckOom(container: Container, waitCode: Int)(implicit trace: Trace): Task[ExitCode] =
      client.inspectContainerCmd(container.containerId).run.flatMap { info =>
        val inspectCode = Option(info.getState.getExitCodeLong).fold(waitCode)(_.intValue)
        ZIO.when(inspectCode != waitCode)(
          logInfo(
            s"Container ${container.hostName}: waitContainerCmd reported exit code $waitCode, " +
              s"inspectContainerCmd says $inspectCode (inspect wins)"
          )
        ) *>
          ZIO
            .fail(
              RuntimeException(
                s"Container ${container.hostName} (${container.image}) was killed by the OOM killer. " +
                  s"Docker does not have enough memory for this level of test parallelism."
              )
            )
            .when(Option(info.getState.getOOMKilled).exists(_.booleanValue))
            .as(ExitCode(inspectCode))
      }

    private def startContainer[T](container: Container, suppressOutput: Boolean)(implicit
        trace: Trace
    ): ZIO[Scope, Throwable, Service[T]] =
      for
        _ <- client.startContainerCmd(container.containerId).run

        // Start tracking container exit early so we can race it against port-waiting.
        exitCode <- zio.Promise.make[Throwable, ExitCode]
        _ <- client
          .waitContainerCmd(container.containerId)
          .stream
          .map(_.getStatusCode.toInt)
          .runHead
          .someOrElse(1)
          .flatMap { waitCode =>
            // Verify exit code via inspectContainerCmd and check for OOM-kill.
            // Also fail the global oomSignal so tests abort instantly on OOM.
            verifyExitAndCheckOom(container, waitCode)
              .tapError(oomErr => oomSignal.promise.fail(oomErr).ignore)
          }
          .intoPromise(exitCode)
          .forkScoped

        portMap = (r: InspectContainerResponse) =>
          Try(
            r.getNetworkSettings.getPorts.getBindings.asScala.toSeq
              .flatMap((k, v) => v.map(p => k.getPort -> p.getHostPortSpec.toInt))
              .toMap
          ).getOrElse(Map.empty)
        expectedPortsAreExposed = (r: InspectContainerResponse) => {
          val remaining = container.ports -- portMap(r).keySet
          logInfo(
            s"Waiting for ports: ${remaining.mkString(", ")}. Inspection response: ${r.getNetworkSettings}"
          ) unless remaining.isEmpty as remaining.isEmpty
        }
        untilPortsAreExposed = Schedule.recurUntilZIO(expectedPortsAreExposed)
        backoff              = Schedule.exponential(10.millis, 2) || Schedule.spaced(1.second)
        waitForPorts = client.inspectContainerCmd(container.containerId).run.repeat(untilPortsAreExposed <* backoff)
        // If the container dies (e.g. OOM) while waiting for ports, fail immediately
        // instead of spinning in a retry loop forever.
        failOnDeath = exitCode.await.flatMap { code =>
          ZIO.fail(
            RuntimeException(
              s"Container ${container.hostName} (${container.image}) exited with code ${code.code} while waiting for ports to become available."
            )
          )
        }
        info <- waitForPorts.raceFirst(failOnDeath)
        exposedAddress =
          if mode === Mode.Local
          then "localhost"
          else info.getNetworkSettings.getNetworks.get("bridge").getIpAddress
        exposedPorts =
          if mode === Mode.Local
          then portMap(info)
          else portMap(info).keySet.map(k => k -> k).toMap

        // std IO
        process = (f: PartialFunction[Frame, Array[Byte]], g: String => StdIO) =>
          ZPipeline.collect(f.andThen(Chunk.fromArray))
            >>> ZPipeline.flattenChunks
            >>> ZPipeline.utf8Decode
            >>> ZPipeline.splitLines
            >>> ZPipeline.map((x: CharSequence) => Try(fansi.Str.Strip(x).plainText).getOrElse(x.toString))
            >>> ZPipeline.map(g)
        ioStream = client
          .logContainerCmd(container.containerId)
          .withStdOut(true)
          .withStdErr(true)
          .withFollowStream(true)
          .withTailAll()
          .stream
        stdErr = ioStream.via(process({ case f if f.getStreamType === STDERR => f.getPayload }, StdErr.apply))
        stdOut = ioStream.via(process({ case f if f.getStreamType === STDOUT => f.getPayload }, StdOut.apply))
        io     = stdErr merge stdOut

        _ <- ZIO.when(!suppressOutput)(io.foreach(logStdIO).forkScoped)
      yield Service[T](
        container,
        exposedAddress,
        exposedPorts,
        io,
        exitCode.await,
        getFileContents(container.containerId, _)
      )

    private def pullImageIfNecessary(image: String)(implicit trace: Trace) = ZIO.unlessZIO(
      client.inspectImageCmd(image).run.exit.map(_.isSuccess)
    )(
      client
        .pullImageCmd(image)
        .stream
        .foreach(rr =>
          Option(rr.getProgressDetail).fold {
            logInfo(rr.getStatus)
          } { pd =>
            val perc = if Option(pd.getTotal).isEmpty then "" else f", ${100.0 * pd.getCurrent / pd.getTotal}%2.2f%%"
            logDebug(s"${rr.getId} - ${rr.getStatus}$perc")
          }
        )
    )
  end Impl

  private def listContainers(client: DockerClient, showAll: Boolean = false) =
    client.listContainersCmd().withShowAll(showAll).run.map(Option(_).map(_.asScala.toSeq).getOrElse(Seq.empty))

  private def logStdIO(output: StdIO) = output match
    case StdOut(line) => logInfo(line)
    case StdErr(line) => logError(line)

  private def sharedStorage =
    ZLayer.fromZIO(Ref.Synchronized.make(Map.empty[Serializable, Promise[?, ?]]))

  private def mode = ZLayer.fromZIO(for
    client            <- ZIO.service[DockerClient]
    runningContainers <- listContainers(client)
    ciContainer = runningContainers.collectFirst { case c if c.getImage.contains("cimg/base") => c }
    mode        = ciContainer.fold[Mode](Mode.Local)(c => Mode.CI)
    _ <- logInfo(s"Running Docker service in $mode mode")
  yield mode)

  private def dockerSession = ZLayer.scoped(
    for
      client <- ZIO.service[DockerClient]
      seed   <- zio.Random.nextLong.map(rid => f"${math.abs(rid)}%x")
      prefix = s"pqs-ft-$seed"

      dockerNetworkId <- acquireRelease( // acquire
        client.createNetworkCmd().withName(s"$prefix-network").run.map(_.getId)
      )(id => // release
        client.removeNetworkCmd(id).run.ignoreLogged
      )

      volumeName <- acquireRelease( // acquire
        client.createVolumeCmd().withName(s"$prefix-volume").run.map(_.getName)
      )(name => // release
        client.removeVolumeCmd(name).run.ignoreLogged
      )
      _ <- logInfo(s"Created new docker layer [session=$prefix, network=$dockerNetworkId, volume=$volumeName]")
    yield DockerSession(prefix, dockerNetworkId, volumeName)
  )

  private def oomSignalLayer = ZLayer.fromZIO(
    Promise.make[Throwable, Nothing].map(OomSignal(_))
  )

end Docker
