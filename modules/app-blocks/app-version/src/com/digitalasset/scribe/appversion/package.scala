// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe

import zio.ZIO.logInfo
import zio.{ZIO, ZLayer}

import java.io.InputStream
import java.util.Properties
import java.util.jar.Attributes.Name.*
import java.util.jar.Manifest

package object appversion:
  val LogVersion: ZLayer[Any, Throwable, Unit] = ZLayer.fromZIO(
    getVersion.flatMap { (title, version, props) =>
      logInfo(s"$title, version: $version${render(props, start = " (", end = ")")}")
    }
  )

  val getVersion = getImplementation().flatMap { (title, version) =>
    getExtraProperties(title).map(extra => (title, version, extra))
  }

  private def getImplementation() = for
    manifest <- fromResource("META-INF/MANIFEST.MF")(is => new Manifest(is))
    title    <- ZIO.attempt(manifest.getMainAttributes.getValue(IMPLEMENTATION_TITLE))
    version  <- ZIO.attempt(manifest.getMainAttributes.getValue(IMPLEMENTATION_VERSION))
  yield (title, version)

  private def getExtraProperties(app: String) =
    fromResource(s"META-INF/$app-version.properties")(is => {
      val p = new Properties()
      p.load(is)
      p
    }).orElseSucceed(new Properties())

  private def fromResource[A](path: String)(f: InputStream => A) =
    ZIO.acquireReleaseWith( // acquire
      ZIO.attempt(Thread.currentThread().getContextClassLoader.getResourceAsStream(path))
    )(is => // release
      ZIO.attempt(is.close()).ignore
    )(is => // use
      ZIO.attempt(f(is))
    )

  private[appversion] def render(props: Properties, start: String = "", sep: String = ", ", end: String = "") =
    if !props.isEmpty then
      import scala.jdk.CollectionConverters.*
      props
        .stringPropertyNames()
        .asScala
        .map(key => s"$key: ${props.getProperty(key)}")
        .toList
        .sorted
        .mkString(start, sep, end)
    else ""

end appversion
