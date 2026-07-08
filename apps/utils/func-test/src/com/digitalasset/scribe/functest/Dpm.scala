// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest

import zio.*
import zio.process.*

/** Witness that Dpm and the Daml SDK are installed */
class Dpm private ()

object Dpm:
  val layer: ZLayer[FTEnv, Throwable, Dpm] = ZLayer.fromZIO(installSdk)

  def buildDar(packageDir: os.Path): ZIO[Dpm, CommandError, Unit] =
    Command("dpm", "build").copy(workingDirectory = Some(packageDir.toIO)).successOrLogError

  def runScript(
      packageDir: os.Path,
      ledgerHost: String,
      ledgerPort: Int,
      scriptName: String,
      darPath: os.Path,
      maxRequestSize: Int,
      inputFile: os.Path,
      outputFile: os.Path,
      crt: os.Path,
      pem: os.Path,
      cacrt: os.Path,
      accessTokenFile: Option[os.Path]
  ): ZIO[Dpm, CommandError, Unit] =
    val ledgerArgs = Seq("--ledger-host", ledgerHost, "--ledger-port", ledgerPort.toString)
    val scriptArgs = Seq("--script-name", scriptName, "--dar", darPath.toString)
    val inputOutputArgs = Seq(
      "--max-inbound-message-size",
      maxRequestSize.toString,
      "--input-file",
      inputFile.toString,
      "--output-file",
      outputFile.toString
    )
    val certificateArgs = Seq("--crt", crt.toString, "--pem", pem.toString, "--cacrt", cacrt.toString)
    val accessTokenFileArgs =
      accessTokenFile.map(accessTokenFile => Seq("--access-token-file", accessTokenFile.toString)).getOrElse(Seq.empty)
    Command(
      "dpm",
      (Seq("script") ++ ledgerArgs ++ scriptArgs ++ inputOutputArgs ++ certificateArgs ++ accessTokenFileArgs)*
    )
      .copy(workingDirectory = Some(packageDir.toIO))
      .successOrLogError

  private def installSdk: ZIO[FTEnv, Throwable, Dpm] =
    for
      ftEnv <- ZIO.service[FTEnv]
      damlVersion = ftEnv.config.damlSdkVersion
      registry =
        if damlVersion.toLowerCase.contains("snapshot")
        then "europe-docker.pkg.dev/da-images/public-unstable"
        else "europe-docker.pkg.dev/da-images/public"
      _      <- ZIO.log(s"Installing daml components for $damlVersion")
      tmpDir <- ftEnv.createUniqueDirectory("dpm-install-sdk")
      // Create an empty daml project to install the daml components
      // We use override-components because we don't have a full dpm SDK version,
      // but only the daml version for the daml components
      damlYaml =
        s"""|override-components:
            |  damlc:
            |    version: $damlVersion
            |  daml-script:
            |    version: $damlVersion
            |""".stripMargin
      _ <- ZIO.attemptBlocking(os.write(tmpDir / "daml.yaml", damlYaml))
      _ <- Command("dpm", "install", "package")
        .copy(env = Map("DPM_REGISTRY" -> registry), workingDirectory = Some(tmpDir.toIO))
        .successOrLogError
    yield new Dpm()

end Dpm

extension (cmd: Command.Standard)
  private def successOrLogError: ZIO[Any, CommandError, Unit] =
    for
      process  <- cmd.run
      exitCode <- process.exitCode
      _ <- ZIO.when(exitCode.code != 0) {
        for
          stdout <- process.stdout.lines
          stderr <- process.stderr.lines
          _ <- ZIO.logError(
            s"""|${cmd.command.head} failed:
                |STDOUT: ${stdout.mkString("\n  ")}
                |STDERR: ${stderr.mkString("\n  ")}
                |""".stripMargin
          )
          _ <- ZIO.fail(CommandError.NonZeroErrorCode(exitCode))
        yield ()
      }
    yield ()
