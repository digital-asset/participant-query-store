// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import $meta._
import com.daml.mill.daml.DamlModule
import com.daml.mill.docker.BuildxDockerModule
import com.daml.mill.junitxml.JunitReportsModule
import com.daml.mill.proguard.ProguardModule
import coursier.maven.MavenRepository
import java.nio.file.FileSystems
import mill._
import mill.api.Ctx
import mill.contrib.scalapblib.{ScalaPBModule, ScalaPBWorkerApi}
import mill.javalib.Assembly
import mill.scalalib.Assembly.Rule
import mill.scalalib._
import mill.scalalib.publish._
import mill.scalalib.scalafmt._
import mill.testrunner.TestResult
import millbuild.{L, V}
import tasks.GlobalTasks
import wart.WartRemoverModule

import scala.annotation.nowarn

object `package` extends RootModule { root =>
  // All Mill modules are under modules/
  implicit def millModuleBasePath: define.Ctx.BasePath =
    define.Ctx.BasePath(super.millModuleBasePath.value / "modules")

  object all extends GlobalTasks

  val dockerImage = s"eclipse-temurin:${V.jdk}"

  def isCI: T[Boolean] = T { T.env.contains("CI") }

  /** Workspace-specific suffix for locally-built Docker images. Prevents different checkouts from clobbering each
    * other's images in the shared Docker daemon. Empty on CI.
    */
  def localImageSuffix: T[String] = Task.Input {
    if (isCI()) ""
    // This is the hash of the workspace directory, which should be unique per checkout.
    else s"-${T.workspace.toString.hashCode.toHexString}"
  }

  trait ScribeModule extends ScalaModule with ScalafmtModule with WartRemoverModule {
    outer =>
    override def scalaVersion = V.scala

    override def repositoriesTask = Task.Anon {
      super.repositoriesTask() ++ Seq {
        MavenRepository("https://europe-maven.pkg.dev/da-images/public-maven-unstable")
      }
    }

    override def mandatoryIvyDeps = T { super.mandatoryIvyDeps() ++ Agg(L.pprint) }

    def publishVersion: T[String] = Task.Input { T.env.getOrElse[String]("SCRIBE_VERSION", "UNSPECIFIED") }
    def publishVendor: T[String]  = "Digital Asset"

    override def warts = Seq(
      "ArrayEquals",
      "AnyVal",
      "AsInstanceOf",
      "EitherProjectionPartial",
      "Enumeration",
      "Equals",
      "ExplicitImplicitTypes",
      "FinalVal",
      "ImplicitConversion",
      "IsInstanceOf",
      "JavaConversions",
      "JavaSerializable",
      "LeakingSealed",
      "Null",
      "Option2Iterable",
      "OptionPartial",
      "Product",
      "Return",
      "Serializable",
      "StringPlusAny",
      "IterableOps",
      "TryPartial",
      "While"
    )

    override def scalacOptions = T {
      super.scalacOptions() ++ Seq(
        "-deprecation",
        "-feature",
        "-new-syntax",
        "-Wunused:imports",
        s"-Xtarget:${V.jdk}"
      ) ++ (if (isCI()) Seq("-Xfatal-warnings") else Seq.empty)
    }

    trait ScribeTests
        extends ScalaTests
        with TestModule
        with JunitReportsModule
        with ScalafmtModule
        with WartRemoverModule {
      override def testFramework: T[String] = "zio.test.sbt.ZTestFramework"

      override def ivyDeps: T[Agg[Dep]] = T {
        super.ivyDeps() ++ Agg(L.zio.test.test, L.zio.test.sbt, L.zio.test.magnolia, L.zio.test.http)
      }

      // Uses TLSv1.2 to avoid JDK-8221218 in forked test runs.
      // See also https://github.com/opensearch-project/security/issues/3299
      override def forkArgs = T { Seq("-Djdk.tls.client.protocols=TLSv1.2") ++ super.forkArgs() }
    }

  }

  //////////////////////////////////////////////
  // Scribe top-level entry point Application //
  //////////////////////////////////////////////
  object scribe extends ScribeModule with ProguardModule with BuildxDockerModule {
    override def forkArgs = T { Seq("-Djdk.attach.allowAttachSelf") ++ super.forkArgs() }

    // Create a pom.xml listing all runtime dependencies
    // Used by Blackduck to scan dependencies
    def pom = Task {
      val pom = Pom(
        Artifact("com.daml", artifactName(), publishVersion()),
        (transitiveRunIvyDeps() ++ transitiveIvyDeps()).map(x => Artifact.fromDep(x.toDep, "", "", "")),
        artifactName(),
        PomSettings("description", "org", "http://digitalasset.com", Seq.empty, VersionControl(), Seq.empty),
        Map.empty,
        PackagingType.Jar,
        parentProject = None,
      )
      val pomPath = T.dest / "pom.xml"
      os.write.over(pomPath, pom)
      PathRef(pomPath)
    }

    override val moduleDeps = Seq(
      pipeline,
      postgres.relational
    )

    override def mainClass = Some("com.digitalasset.scribe.Main")

    override def assemblyRules = super.assemblyRules ++ Seq(
      Rule.AppendPattern("META-INF/services/.*", separator = "\n"), // A.k.a. merge services - required for GRPC clients
      Assembly.Rule.ExcludePattern(".*\\.(js|proto|tasty|jar)"),
      Assembly.Rule.ExcludePattern("org/jline/.*"),
      Assembly.Rule.ExcludePattern("scala/tools/nsc/doc/html/resource/lib/.*"),
      Assembly.Rule.ExcludePattern("templates/.*"),
      Assembly.Rule.ExcludePattern("META-INF/maven/.*"),
      Assembly.Rule.ExcludePattern("META-INF/native/.*"),
      Assembly.Rule.ExcludePattern("META-INF/native-image/.*")
    )

    override def proguardKeep = Task {
      Seq(
        "class org.bouncycastle.jcajce.provider.digest.Keccak* { *; }",   // digest used in daml lf reader
        "class org.slf4j.** { *; }",                                      // logging
        "class zio.logging.slf4j.bridge.ZioSLF4JServiceProvider { *; }",  // logging
        "class io.grpc.**.*Provider { *; } ",                             // Grpc/Netty Dynamic Providers
        "class nonapi.io.github.classgraph.classloaderhandler.** { *; }", // classgraph
        "class org.flywaydb.** { *; } ", // flyway defines list of plugins in META-INF and then expects them
        "class io.opentelemetry.** { *; } ",
        "class io.micrometer.** { *; } ",
        "class io.grpc.** extends java.lang.Enum { *; } ",
        "class org.postgresql.** extends java.lang.Enum { *; } ",
        "class com.fasterxml.** extends java.lang.Enum { *; } ",
        "class io.netty.util.concurrent.ConcurrentSkipListIntObjMultimap { *; }",
        "class tools.jackson.databind.** extends java.lang.Enum { *; }",
        
      )
    }

    override def proguardIgnore = Seq(
      "**.annotations.**",  // annotations
      "org.aspectj.**",     // annotations
      "com.google.auto.**", // annotations
      "java.lang.invoke.MethodHandle",
      "java.lang.invoke.VarHandle",
      "scala.AnyKind",
      "org.postgresql.sspi.**", // PG windows client
      "org.postgresql.osgi.**", // PG osgi refs
      "com.google.rpc.**",
      "com.google.api.**",
      "org.flywaydb.core.**",
      "**.netty.**",
      "io.micrometer.**",
      "com.google.protobuf.compiler.**",
      "com.digitalasset.zio.daml.ledgerapi.**", // Proguard doesn't resolve DamlLf$Archive$
      "com.digitalasset.transcode.**",          // Proguard complains about Codec.from which is not used
      "scalapb.compiler.**",
      "scalapb.options.**",
      "zio.json.DeriveJson*",
      "zio.stream.**",
      "org.jline.**",
      "sttp.client4.internal.**", // sttp references java.lang.System$Logger$ not available in ProGuard
      "okhttp3.**",               // OkHttp references optional Android/Conscrypt/BouncyCastle JSSE classes
      "buildinfo.**",             // excluded zio-constraintless but ProGuard still warns about cross-references
      "zio.constraintless.**", // zio-constraintless JAR excluded from runClasspath; zio-schema refs are compile-time only
      "dotty.tools.dotc.transform.**",
      "scala.quoted.runtime.**",
      "lombok.Generated",
      "org.checkerframework.**",
      "org.osgi.annotation.bundle.Export",
      "com.google.protobuf.descriptor.FileOptions$",
    )

    override def proguardOptimize: T[Boolean] = false
    override def proguardShrink: T[Boolean]   = true

    def latestSchemaVersion = T {
      Lib
        .findSourceFiles(postgres.document.resources(), Seq("sql"))
        .map(PathRef(_))
        .map(_.path.last)
        .filter(_.startsWith("V"))
        .max
        .substring(1, 4)
    }

    override def resources = T {
      val metainf = T.dest / "META-INF"
      os.makeDir(metainf)
      // Write more properties here as needed
      os.write(
        metainf / s"${artifactName()}-version.properties",
        s"""${postgres.document.artifactName()}.schema=${latestSchemaVersion()}
           |daml-sdk.version=${V.damlc}
           |""".stripMargin
      )
      Seq(PathRef(T.dest))
    }

    override def manifest = T {
      import java.util.jar.Attributes.Name._
      super
        .manifest()
        .add(IMPLEMENTATION_VERSION.toString -> publishVersion())
        .add(IMPLEMENTATION_TITLE.toString -> artifactName())
        .add(IMPLEMENTATION_VENDOR.toString -> publishVendor())
    }

    object docker extends BuildxDockerConfig {
      def moduleName    = "scribe"
      override def tags = T { Seq(s"${moduleName()}${localImageSuffix()}") }
      def baseImage     = Task.Input { T.env.getOrElse[String]("SCRIBE_IMAGE_BASE", dockerImage) }
      def jvmOptions    = T { Seq("-Djdk.attach.allowAttachSelf") ++ super.jvmOptions() }

      def run = T {
        val mavenRepo = allRepositories()
          .collectFirst { case repo: MavenRepository => repo}
          .getOrElse(coursier.Repositories.central)
        super.run() ++ Seq(
          "mkdir -p /agent/extensions",
          s"wget ${mavenRepo.root}/io/opentelemetry/javaagent/opentelemetry-javaagent/${V.openTelemetryAgent}/opentelemetry-javaagent-${V.openTelemetryAgent}.jar -O /agent/otel.jar"
        )
      }
    }

    object test extends ScribeTests

    /** Control parallelism by passing in pools and lanes parameters, e.g. --pools 2 --lanes 4 */
    object functest extends ScribeTests with ScalafmtModule with WartRemoverModule {
      // There isn't single source of truth for env var names.
      // There is one in build system (here) and another on the test code side.
      // Please keep them in sync if making any changes.
      val PostgresVersionEnvVar       = "SCRIBE_POSTGRESVERSION"
      val DamlSdkVersionEnvVar        = "SCRIBE_DAMLSDKVERSION"
      val CantonVersionEnvVar         = "SCRIBE_CANTONVERSION"
      val CantonProtocolVersionEnvVar = "SCRIBE_CANTONPROTOCOLVERSION"
      val DamlLfTargetEnvVar          = "SCRIBE_DAMLLFTARGET"

      // This is needed to prevent mill caching from storing env values from previous invocations
      // https://mill-build.org/mill/fundamentals/tasks.html#_environment_variable_inputs
      def functestEnvInput = Task.Input {
        Task.env
      }

      override def testFramework: T[String] = "com.digitalasset.scribe.functest.sbt.FTFramework"

      override def forkEnv = T {
        scribe.docker.build()
        val commonOverrides = Map(
          PostgresVersionEnvVar     -> "17",
          DamlSdkVersionEnvVar      -> V.damlc,
          CantonVersionEnvVar       -> V.canton,
          "SCRIBE_IMAGE_TAG_SUFFIX" -> localImageSuffix()
        )
        val sdkOverrides = Map(
          CantonProtocolVersionEnvVar -> "35",
          DamlLfTargetEnvVar          -> "2.3"
        )
        commonOverrides ++ sdkOverrides ++ functestEnvInput() ++ super.forkEnv()
      }

      override def sources   = Task.Sources { super.sources().map(p => PathRef(p.path / os.up / os.up / "functest")) }

      override def ivyDeps = T { super.ivyDeps() ++ Agg(L.sourceCode) }
      override def moduleDeps = {
        super.moduleDeps ++ Seq(utils.`func-test`)
      }
    }
  }

  //////////////
  // Pipeline //
  //////////////

  object pipeline extends ScribeModule {
    override val moduleDeps = Seq(
      `app-blocks`.`app-version`,
      `app-blocks`.`bootstrap-o11y`,
      `app-blocks`.`composable-app`,
      `app-blocks`.config,
      `app-blocks`.diagnostics,
      `app-blocks`.o11y,
      backend,
      postgres.document
    )

    override def ivyDeps = Agg(
      L.zio.zio,
      L.netty.codecHttp,
      L.netty.handlerProxy,
      L.netty.pkiTesting,
      L.netty.transportNativeEpoll,
      L.netty.transportNativeKqueue,
      L.zio.http,
      L.zio.streams,
      // Bump transitive dep to resolve vulnerabilities
      L.protoJava,
    )

    object test extends ScribeTests
  }

  ////////////////////////
  // Datastore Backends //
  ////////////////////////

  object backend extends ScribeModule {
    override def ivyDeps = Agg(
      L.zio.zio,
      L.zio.streams
    )

    override def moduleDeps = Seq(
      utils.`canonical-types`
    )
  }

  object postgres extends Module {
    object backend extends ScribeModule {
      override def ivyDeps = Agg(
        L.zio.zio,
        L.zio.streams,
        L.zio.jdbc,
        L.jdbc.postgres
      )

      override def moduleDeps = Seq(
        `app-blocks`.config,
        `app-blocks`.o11y,
        root.backend
      )
    }

    object document extends ScribeModule {
      override def ivyDeps = Agg(
        L.commons.text,
        L.flyway.core,
        L.flyway.driverPostgres,
        L.classgraph,
        L.transcode.json
      )

      override val moduleDeps = Seq(
        `app-blocks`.`app-version`,
        `app-blocks`.`bootstrap-cli`,
        `app-blocks`.`composable-app`,
        `app-blocks`.config,
        `app-blocks`.o11y,
        auth,
        postgres.backend,
        `zio-daml` // needed to get schema from ledger
      )
    }

    object relational extends ScribeModule {
      override val moduleDeps = Seq(
        `app-blocks`.`composable-app`,
        postgres.backend
      )
    }
  }

  /////////////////
  // App blocks  //
  /////////////////

  object `app-blocks` extends Module {

    /** Show App Version and build metadata */
    object `app-version` extends ScribeModule {
      override def moduleDeps = Seq(
        `composable-app`
      )
    }

    /** Logging Bootstrap for user-interactive command line applications */
    object `bootstrap-cli` extends ScribeModule {
      override def moduleDeps = Seq(
        logging
      )

      override def ivyDeps = Agg(
        L.zio.zio,
        L.zio.logging.logging,
        L.zio.logging.slf4jBridge
      )
    }

    /** Logging Bootstrap for server applications with OpenTelemetry */
    object `bootstrap-o11y` extends ScribeModule {
      override def moduleDeps = Seq(
        o11y,
        `app-version`,
        logging
      )

      override def ivyDeps = Agg(
        L.zio.logging.logging,
        L.zio.logging.slf4jBridge,
        L.zio.metrics.connectors,
        L.zio.metrics.micrometerConnector,
        L.zio.opentelemetry
      )
    }

    /** Composable app with hierarchical menus */
    object `composable-app` extends ScribeModule {
      override def ivyDeps = Agg(
        L.zio.zio,
        L.zio.logging.logging
      )

      override def moduleDeps = Seq(`app-blocks`.config)

      object test extends ScribeTests
    }

    /** Parse configs from env, system properties, command line arguments */
    object config extends ScribeModule {
      override def ivyDeps = Agg(
        L.zio.zio,
        L.zio.config.core,
        L.zio.config.typesafe,
        L.zio.config.magnolia,
        L.fastparse,
        L.pprint
      )

      override def moduleDeps = Seq(`safe-equals`)

      object test extends ScribeTests
    }

    /** Provide unobtrusive features for diagnostics and troubleshooting */
    object diagnostics extends ScribeModule {
      override def moduleDeps = Seq(`jdk-helper`)

      override def ivyDeps = Agg(
        L.commons.lang3,
        L.metrics.micrometerCore,
        L.pprint
      )
    }

    /** Provide certain utilities for working with the JDK */
    object `jdk-helper` extends ScribeModule

    object `feature-flag` extends ScribeModule {
      override def moduleDeps = Seq(
        `safe-equals`
      )

      override def ivyDeps = Agg(
        L.zio.zio
      )
    }

    /** Logging utils */
    object logging extends ScribeModule {
      override def ivyDeps = Agg(
        L.zio.zio,
        L.zio.config.magnolia,
        L.zio.logging.logging
      )
    }

    /** OpenTelemetry */
    object o11y extends ScribeModule {
      override def ivyDeps = Agg(
        L.zio.zio,
        L.openTelemetry.api
      )
    }

    /** Type-safe equals operator */
    object `safe-equals` extends ScribeModule
  }

  ///////////
  // Tools //
  ///////////

  /** ZIO-grpc bindings */
  object `ledger-api-zio` extends ScalaPBModule {
    def scalaVersion   = V.scala
    def scalaPBVersion = V.scalaPB

    // Disable warnings for generated sources until source-scoped -Wconf rules are available.
    override def scalacOptions = T { super.scalacOptions() ++ Seq("-Wconf:any:silent") }

    override def repositoriesTask = Task.Anon {
      super.repositoriesTask() ++ Seq {
        MavenRepository("https://europe-maven.pkg.dev/da-images/public-maven-unstable")
      }
    }

    private def unpackProtoSources(
        jars: Agg[PathRef],
        includePattern: String = "glob:**/*.proto",
        excludePattern: String = "glob:{}"
    )(implicit ctx: Ctx.Dest): PathRef = {
      val unpacked       = ctx.dest / "unpacked"
      val target         = ctx.dest / "target"
      val includeMatcher = FileSystems.getDefault.getPathMatcher(includePattern)
      val excludeMatcher = FileSystems.getDefault.getPathMatcher(excludePattern)
      jars.iterator
        .foreach { path => mill.api.IO.unpackZip(path.path, unpacked.relativeTo(ctx.dest)) }
      os.walk(unpacked)
        .filter(os.isFile(_))
        .map(_.relativeTo(unpacked))
        .filter(f => includeMatcher.matches(f.toNIO) && !excludeMatcher.matches(f.toNIO))
        .foreach(f => os.copy(unpacked / f, target / f, createFolders = true))
      PathRef(target)
    }

    override def scalaPBSources = T {
      val damlJars = resolveDeps(T.task { Agg(L.canton.ledgerApiProto).map(bindDependency()) })()
      val includeProtos = Seq(
        "com/daml/**/*.proto",
        "google/protobuf/*.proto",
        "google/rpc/status.proto",
        "google/rpc/code.proto",
        "google/rpc/error_details.proto"
      )

      val damlProtos = unpackProtoSources(
        damlJars,
        excludePattern = "glob:com/daml/ledger/api/scalapb/package.proto",
        includePattern = s"glob:{${includeProtos.mkString(",")}}"
      )(T.dest / "daml")

      T.log.info("Done extracting scalaPBSources")

      Seq(damlProtos)
    }

    override def compileScalaPB: T[PathRef] = T.persistent {
      val zioPluginMain      = utils.`zio-grpc-codegen`.finalMainClass()
      val zioPluginClasspath = utils.`zio-grpc-codegen`.runClasspath()

      os.makeDir.all(T.dest / "zio")
      os.makeDir.all(T.dest / "scala-unused")

      ScalaPBWorkerApi
        .scalaPBWorker()
        .compile(
          scalaPBClasspath() ++ zioPluginClasspath,
          scalaPBSources().map(_.path),
          scalaPBOptions(),
          T.dest / "scala-unused",
          scalaPBCompileOptions() ++ Seq(
            s"--custom-gen=zio=$zioPluginMain",
            s"--zio_out=${T.dest}/zio"
          )
        )

      PathRef(T.dest)
    }

    override def generatedSources = Task {
      Seq(PathRef(compileScalaPB().path / "zio"))
    }

    override def ivyDeps = T {
      Agg(
        L.canton.ledgerApi,
        L.zio.zio,
        L.zio.streams,
        L.grpc.api,
        L.grpc.netty
      )
    }
  }

  object auth extends ScribeModule {
    override def ivyDeps = Agg(
      L.zio.zio,
      L.sttp.zio,
      L.sttp.okhttp,
      L.ujson,
      L.zio.streams
    )

    override def moduleDeps = Seq(
      `app-blocks`.config,
      `app-blocks`.`safe-equals`
    )

    object test extends ScribeTests
  }

  /* Idiomatic zio module to talk to ledger */
  object `zio-daml` extends ScribeModule {
    override def ivyDeps = Agg(
      L.zio.zio,
      L.zio.streams,
      L.semver,
      L.canton.archiveReader,
      L.transcode.proto,
      L.transcode.daml,
      L.oslib
    )

    override def moduleDeps = Seq(
      `app-blocks`.config,
      `app-blocks`.`feature-flag`,
      `app-blocks`.o11y,
      `app-blocks`.`safe-equals`,
      utils.`canonical-types`,
      auth,
      `ledger-api-zio`
    )

    /* A minimal, decodable DAR (single `DecodableTestModule`) built from `decodable-test-module/daml`. */
    object `decodable-test-module` extends DamlModule {
      override def damlcVersion = V.damlc
      override def deps           = Seq("daml-prim", "daml-stdlib")
    }

    object test extends ScribeTests {
      override def ivyDeps = T {
        super.ivyDeps() ++ Agg(
          L.zio.test.mock
        )
      }

      /* Expose the built DAR on the test classpath at the path PackageServiceSpec loads it from. */
      def decodableTestModuleResource = T {
        val pkgDir = T.dest / "com" / "digitalasset" / "zio" / "daml" / "ledgerapi"
        os.makeDir.all(pkgDir)
        os.copy.over(`decodable-test-module`.dar().path, pkgDir / "decodable-test-module.dar")
        PathRef(T.dest)
      }

      override def resources = T { super.resources() ++ Seq(decodableTestModuleResource()) }
    }
  }

  object utils extends Module {
    object `canonical-types` extends ScribeModule {
      override def ivyDeps = Agg(
        L.zio.zio,
        L.zio.config.magnolia,
        L.transcode.schema
      )

      override def moduleDeps = Seq(
        `app-blocks`.o11y,
        `app-blocks`.`safe-equals`
      )
    }

    object `zio-grpc-codegen` extends ScribeModule {
      override def ivyDeps = Agg(
        L.scalapb.compiler,
        L.transcode.daml
      )
    }

    object docker extends ScribeModule {
      override def moduleDeps = Seq(
        `app-blocks`.`safe-equals`
      )

      override def ivyDeps = Agg(
        L.oslib,
        L.zio.zio,
        L.zio.streams,
        L.zio.logging.logging,
        L.dockerClient.core,
        L.dockerClient.zerodepTransport,
        L.bouncyCastle,
        L.jwt
      )
    }

    object `func-test` extends ScribeModule {
      override def moduleDeps = Seq(
        utils.docker,
        `zio-daml`
      )

      override def ivyDeps = Agg(
        L.sbtTestInterface,
        L.zio.process,
        L.zio.test.test,
        L.zio.test.sbt,
        L.upickle,
        L.semver,
        L.transcode.daml
      )
    }
  }

}
