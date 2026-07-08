// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.mill.daml

import millbuild.V
import com.daml.mill.background.BackgroundWorker
import mill._
import mill.scalalib.OfflineSupportModule

trait DamlModule extends Module with OfflineSupportModule {
  def damlcVersion: T[String] = V.damlc

  def deps: T[Seq[String]]
  def sources            = Task.Sources("daml")
  def moduleName: String = millModuleSegments.parts.last
  def version: T[String] = "0.0.0"

  def damlYaml = T {
    s"""|override-components:
        |  damlc:
        |    version: ${damlcVersion()}
        |  daml-script:
        |    version: ${damlcVersion()}
        |name: $moduleName
        |source: daml
        |version: ${version()}
        |dependencies:
        |${deps().map(x => s"  - $x").mkString("\n")}
        |build-options:
        |- --enable-interfaces=yes
        |- -Wno-template-interface-depends-on-daml-script
        |""".stripMargin
  }

  def stage: T[PathRef] = T {
    val damlDir = T.dest / "daml"
    sources()
      .filter(_.path.toIO.exists())
      .foreach { source =>
        os.copy(source.path, damlDir, mergeFolders = true, copyAttributes = true, createFolders = true)
      }
    os.write.over(T.dest / "daml.yaml", damlYaml())
    PathRef(T.dest)
  }

  def dar: T[PathRef] = T.persistent {
    os.copy.over(stage().path, T.dest)
    val darFile = T.dest / s"$moduleName.dar"
    dpmWorker()
      .env("DAML_PROJECT" -> T.dest.toIO.getCanonicalPath)
      .run("dpm", "build", "--output", darFile)
    PathRef(darFile)
  }

  override def prepareOffline(all: mainargs.Flag) = T.command {
    dpmWorker()
    ()
  }

  private def dpmWorker = T.worker {
    val worker = BackgroundWorker(env = Map("DPM_REGISTRY" -> "europe-docker.pkg.dev/da-images/public-unstable"))
    os.write.over(
      T.dest / "daml.yaml",
      s"""|override-components:
          |  damlc:
          |    version: ${damlcVersion()}
          |  daml-script:
          |    version: ${damlcVersion()}
          |""".stripMargin
    )
    worker.run("dpm", "install", "package")
    worker
  }
}
