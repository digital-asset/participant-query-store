// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.featureflag

import zio.stm.TMap
import zio.{UIO, ZIO, ZLayer}

abstract class FeatureFlag[A](
    val defaultValue: A
)

trait FeatureFlagService:
  def get[A](flag: FeatureFlag[A]): UIO[A]
  def set[A](flag: FeatureFlag[A], value: A): UIO[Unit]

object FeatureFlagService:
  case class FeatureFlagInitializer[R, E, A](flag: FeatureFlag[A], getValue: ZIO[R, E, A])

  def live[R, E](initializers: FeatureFlagInitializer[R, E, ?]*): ZLayer[R, E, FeatureFlagService] =
    ZLayer.scoped(
      for
        initialValues <- ZIO.foreachPar(initializers)(fi => fi.getValue.map(value => fi.flag -> value))
        flags         <- TMap.make[Any, Any](initialValues*).commit
      yield new FeatureFlagService:
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        override def get[A](flag: FeatureFlag[A]): UIO[A] =
          flags.getOrElse(flag, flag.defaultValue).commit.asInstanceOf[UIO[A]]
        override def set[A](flag: FeatureFlag[A], value: A): UIO[Unit] = flags.put(flag, value).commit
    )

  def whenEnabled[R, E](flag: FeatureFlag[Boolean])(zio: ZIO[R, E, Any]): ZIO[R & FeatureFlagService, E, Unit] =
    for
      flags <- ZIO.service[FeatureFlagService]
      _     <- ZIO.whenZIO(flags.get(flag))(zio)
    yield ()

  def when[R, E, A](
      flag: FeatureFlag[Boolean]
  )(enabled: A, disabled: A): ZIO[R & FeatureFlagService, E, A] =
    whenZIO(flag)(ZIO.succeed(enabled), ZIO.succeed(disabled))

  def whenZIO[R, E, A](
      flag: FeatureFlag[Boolean]
  )(enabled: ZIO[R, E, A], disabled: ZIO[R, E, A]): ZIO[R & FeatureFlagService, E, A] =
    for
      flags       <- ZIO.service[FeatureFlagService]
      flagEnabled <- flags.get(flag)
      result      <- if flagEnabled then enabled else disabled
    yield result

  given [R, E, A]: Conversion[(FeatureFlag[A], ZIO[R, E, A]), FeatureFlagInitializer[R, E, A]] = (k, v) =>
    FeatureFlagInitializer(k, v)
