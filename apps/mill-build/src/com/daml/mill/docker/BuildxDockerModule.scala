// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.mill.docker

import mill._
import mill.api.Ctx
import mill.scalalib.JavaModule
import os.Shellable.IterableShellable

trait BuildxDockerModule extends contrib.docker.DockerModule { outer: JavaModule =>
  trait BuildxDockerConfig extends DockerConfig {
    def moduleName: T[String] = T { outer.artifactNameParts().last }
    def arch: T[String]       = T.input { T.env.getOrElse[String]("MILL_DOCKER_IMAGE_ARCH", "arm64") }

    private def baseImageCacheBuster: T[(Boolean, Double)] = T.input {
      val pull = pullBaseImage()
      if (pull) (pull, Math.random()) else (pull, 0d)
    }

    // Enhances base module's DockerConfig#build logic to allow building an arbitrary Linux platform image,
    // and export it into a .tar file.
    def tar = T {
      useMultiarch()()
      val dest    = T.dest
      val asmPath = outer.assembly().path
      os.copy(asmPath, dest / asmPath.last)
      os.write(dest / "Dockerfile", dockerfile())

      val log            = T.log
      val (pull, _)      = baseImageCacheBuster()
      val pullLatestBase = IterableShellable(if (pull) Some("--pull") else None)

      val platformArg = arch() match {
        case "amd64" => Seq("--platform", "linux/amd64")
        case "arm64" => Seq("--platform", "linux/arm64")
        case other   => throw new Exception(s"Unsupported architecture: $other")
      }
      val name       = moduleName()
      val outputFile = dest / s"$name-${arch()}.tar"
      val outputArg  = Seq("--output", s"type=docker,dest=-")
      val tagArg     = Seq("--tag", s"$name:latest-${arch()}")

      log.info(s"Building image for platform ${platformArg.last} and exporting to $outputFile")
      val result = os
        .proc(executable(), "buildx", "build", tagArg, outputArg, platformArg, pullLatestBase, dest)
        .call(stdout = os.PathRedirect(outputFile), stderr = toInfo)

      log.info(s"Docker build completed ${
          if (result.exitCode == 0) "successfully"
          else "unsuccessfully"
        } with ${result.exitCode}")
      PathRef(outputFile)
    }

    private def useMultiarch() = T.command {
      val multiArchName = "multiarch"
      val use = os
        .proc(executable(), "buildx", "use", multiArchName)
        .call(stdout = toInfo, stderr = os.ProcessOutput.Readlines(_ => ()), check = false)
        .exitCode == 0
      if (!use) {
        os.proc(executable(), "buildx", "create", "--name", multiArchName, "--driver", "docker-container", "--use")
          .call(stdout = toInfo, stderr = toInfo)
      }
      {}
    }
  }

  private def toInfo(implicit ctx: Ctx) = os.ProcessOutput.Readlines(ctx.log.info)
}
