// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package wart

import millbuild.L
import mill._
import mill.scalalib._

trait WartRemoverModule extends ScalaModule {
  def warts: T[Seq[String]] = T { Seq.empty[String] }

  override def compileIvyDeps      = T { super.compileIvyDeps() ++ Agg(L.wartRemover) }
  override def scalacPluginIvyDeps = T { super.scalacPluginIvyDeps() ++ Agg(L.wartRemover) }

  override def scalacOptions = T {
    super.scalacOptions() ++ warts().map(w => s"-P:wartremover:traverser:org.wartremover.warts.$w")
  }
}
