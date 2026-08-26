// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import coursier.core.Authentication
import coursier.maven.MavenRepository
import mill._
import mill.runner.MillBuildRootModule
import mill.scalalib._

import scala.util.Try

object `package` extends MillBuildRootModule {
  override def ivyDeps = T {
    super.ivyDeps() ++ Agg(
      ivy"com.lihaoyi::mill-contrib-docker:0.11.11",
      ivy"com.lihaoyi::mill-contrib-scalapblib:0.12.11"
    )
  }
}
