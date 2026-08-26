// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.mill.proguard

import mill._
import mill.api.PathRef
import mill.scalalib._
import mill.util.Jvm
import os.{Path, Shellable}

trait ProguardModule extends JavaModule {

  def proguardUpstreamJar: T[PathRef] = Task {
    val out = T.dest / "out.jar"
    Assembly.create(
      destJar = out,
      inputPaths = runClasspath().map(_.path),
      manifest = Jvm.createManifest(None),
      assemblyRules = assemblyRules
    )
    PathRef(out)
  }

  def proguardJar: T[PathRef] = Task {
    val proguardJar = T.dest / "proguard.jar"

    val libraryJars = (
      os.list(Path(sys.props("java.home")) / "lib", sort = false).filter(_.ext == "jar")
        ++ os
          .list(Path(sys.props("java.home")) / "jmods", sort = false)
          .filter(_.ext == "jmod")
          .map(x => s"<java.home>/jmods/${x.last}(!**.jar;!module-info.class)")
    ).mkString(java.io.File.pathSeparator)

    val args = Seq[Shellable](
      "-injars",
      proguardUpstreamJar().path,
      "-outjars",
      proguardJar,
      "-libraryjars",
      libraryJars,
      internalProguardOptions()
        ++ proguardOptions()
        ++ Seq(s"-keep public class ${finalMainClass()} { public static void main(java.lang.String[]); }")
    ).flatMap(_.value)

    Jvm.callProcess(
      mainClass = "proguard.ProGuard",
      jvmArgs = proguardJvmArgs(),
      classPath = proguardClasspath().map(_.path).toSeq,
      mainArgs = args,
      cwd = T.dest
    )

    PathRef(proguardJar)
  }

  override def assembly: T[PathRef] = Task {
    val finalJar = Assembly.create(
      destJar = T.dest / "out.jar",
      inputPaths = Agg.empty,
      manifest = manifest(),
      prependShellScript = Option(prependShellScript()).filter(_ != ""),
      base = Some(proguardJar().path),
      assemblyRules = Seq.empty
    )
    finalJar.pathRef
  }

  private def proguardClasspath: Target[Agg[PathRef]] = Task {
    Lib.resolveDependencies(
      allRepositories(),
      Agg(
        scalalib.Lib.depToBoundDep(
          ivy"com.guardsquare:proguard-base:${proguardVersion()}",
          mill.main.BuildInfo.scalaVersion
        )
      )
    )
  }

  private def internalProguardOptions: T[Seq[String]] = Task {
    Seq(
      Option.unless(proguardOptimize())("-dontoptimize"),
      Option.unless(proguarObfuscate())("-dontobfuscate"),
      Option.unless(proguardShrink())("-dontshrink"),
      Option.unless(proguardPreverify())("-dontpreverify"),
      Option.unless(proguardLogNotes())("-dontnote"),
      Option.unless(proguardLogWarnings())("-dontwarn")
    ).flatten ++
      proguardIgnore().map(what => s"-dontwarn $what") ++
      proguardKeep().map(what => s"-keep $what")
  }
  def proguardVersion: T[String]      = Task { "7.7.0" }
  def proguardOptimize: T[Boolean]    = true
  def proguarObfuscate: T[Boolean]    = false
  def proguardShrink: T[Boolean]      = true
  def proguardPreverify: T[Boolean]   = true
  def proguardLogNotes: T[Boolean]    = false
  def proguardLogWarnings: T[Boolean] = true
  def proguardIgnore: T[Seq[String]]  = Task { Seq.empty[String] }
  def proguardKeep: T[Seq[String]]    = Task { Seq.empty[String] }
  def proguardOptions: T[Seq[String]] = Task { Seq.empty[String] }
  def proguardJvmArgs: T[Seq[String]] = Task { Seq.empty[String] }
}
