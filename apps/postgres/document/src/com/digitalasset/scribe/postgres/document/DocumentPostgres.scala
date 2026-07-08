// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.postgres.document

import com.digitalasset.canonical.ReassignmentEvent
import com.digitalasset.canonical.specific.{Event, Offset, Transaction, TreeEvent}
import com.digitalasset.scribe.backend.Datastore
import com.digitalasset.scribe.o11y.metrics.latency
import com.digitalasset.scribe.o11y.traces
import com.digitalasset.scribe.o11y.traces.given
import com.digitalasset.scribe.postgres.backend.*
import com.digitalasset.scribe.postgres.document.model.{EntityTypePk, PackagePk, Watermark}
import com.digitalasset.scribe.postgres.document.specific
import com.digitalasset.scribe.postgres.document.specific.*
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.schema.{Identifier, ChoiceName, Dictionary, PackageId}
import io.github.classgraph.ClassGraph
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.ResourceProvider
import org.flywaydb.core.api.resource.LoadableResource
import org.flywaydb.core.internal.jdbc.DriverDataSource
import ujson.Value
import zio.ZIO.{logDebug, logInfo, logTrace}
import zio.jdbc.*
import zio.jdbc.SqlFragment.{Segment, Setter}
import zio.metrics.Metric
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.stream.{ZChannel, ZPipeline, ZSink}
import zio.{Chunk, ChunkBuilder, Schedule, ZEnvironment, ZIO, ZLayer, durationInt, jdbc}

import java.io.{Reader, StringReader}
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.Using

object DocumentPostgres:
  def applySchema(
      pgCfg: PostgresConfig,
      instanceId: InstanceId,
      doBaseline: Boolean
  ): ZIO[ZConnectionPool & SqlSchema.Service, Throwable, Unit] =
    traces.span("apply schema") {
      logInfo("Applying schema") *>
        ZIO.attemptBlocking {
          Flyway
            .configure()
            .dataSource(
              DriverDataSource(
                Thread.currentThread().getContextClassLoader,
                "org.postgresql.Driver",
                s"jdbc:postgresql://${pgCfg.host}:${pgCfg.port}/${pgCfg.database}?currentSchema=${pgCfg.schema}",
                pgCfg.username,
                pgCfg.password.value,
                (sslprops(pgCfg.tls) ++ instanceIdProp(instanceId)).asJava
              )
            )
            .baselineOnMigrate(doBaseline)
            .baselineVersion("001")
            .baselineDescription("Baseline initial schema")
            .resourceProvider(new ResourceProvider {
              @SuppressWarnings(Array("org.wartremover.warts.Null"))
              def getResource(name: String): LoadableResource = null
              @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
              def getResources(prefix: String, suffixes: Array[String]): util.Collection[LoadableResource] =
                Using
                  .Manager { use =>
                    val pathPrefix = "db/migration"
                    val scanResult = use(ClassGraph().acceptPaths(pathPrefix).scan())
                    Seq(suffixes*)
                      .flatMap(suffix => scanResult.getResourcesWithExtension(suffix).asScala.toSeq)
                      .sortBy(_.getPath)
                      .map(x =>
                        new LoadableResource {
                          private val contents              = use(x).getContentAsString
                          def read(): Reader                = StringReader(contents)
                          def getAbsolutePath: String       = x.getURL.toString
                          def getAbsolutePathOnDisk: String = x.getClasspathElementFile.getAbsolutePath
                          def getFilename: String           = x.getPath.split('/').last
                          def getRelativePath: String       = x.getPath.drop(pathPrefix.length + 1)
                        }
                      )
                  }
                  .get
                  .asJava
            })
            .load()
            .migrate()
        }
    }
      *> traces.span("apply mappings") {
        logInfo("Applying mappings") *>
          ZIO.serviceWithZIO[SqlSchema.Service](schema =>
            logTrace(schema.mappings) *> transaction(schema.mappings.execute)
          )
      }
      <* logInfo("Schema and mappings applied")

  val live = ZLayer.scoped {
    traces.root("process metadata and schema") {
      for
        config     <- ZIO.service[SchemaConfig]
        poolConfig <- ZIO.service[PostgresConfig]
        instanceId <- ZIO.service[InstanceId]
        pool       <- ZIO.service[ZConnectionPool]
        schema     <- ZIO.service[SqlSchema.Service]
        codec      <- ZIO.service[Dictionary[Codec[Value]]]

        _ <- applySchema(poolConfig, instanceId, config.baseline) when config.autoApply // initialize schema if needed

        entities <- transaction {
          sql"""select p.id, ct.module_name, ct.entity_name, ct.pk as pk
              from __contract_tpe ct, __packages p
              where ct.package_name = p.name"""
            .query[(String, String, String, EntityTypePk)]
            .selectAll
        }
        entityTypesMap = entities.map((pkg, module, entity, pk) => (pkg, module, entity) -> pk).toMap
        getEntityType  = (id: Identifier) => entityTypesMap(id.packageId, id.moduleName, id.entityName)
        _ <- logInfo(s"Initialised ${entities.size} entity types")
        _ <- logDebug(pprint(entities, height = Int.MaxValue).toString)

        exercises <- transaction {
          sql"""select p.id, et.module_name, et.entity_name, et.choice, et.pk as pk
              from __exercise_tpe et, __packages p
              where et.package_name = p.name"""
            .query[(String, String, String, String, EntityTypePk)]
            .selectAll
        }
        exercisesTypesMap = exercises
          .map((pkg, module, entity, choice, pk) => (pkg, module, entity, choice) -> pk)
          .toMap
        getExerciseType = (id: Identifier, choice: ChoiceName) =>
          exercisesTypesMap(id.packageId, id.moduleName, id.entityName, choice)
        _ <- logInfo(s"Initialised ${exercises.size} exercise types")
        _ <- logDebug(pprint(exercises, height = Int.MaxValue).toString)

        implementsRelations <- transaction {
          sql"select template_pk, interface_pk from __contract_implements"
            .query[(EntityTypePk, EntityTypePk)]
            .selectAll
        }
        implementsMap = implementsRelations.groupMap(_._1)(_._2)
        getImplements = (id: Identifier) => Chunk.from(implementsMap.get(getEntityType(id))).flatten
        _ <- logInfo(s"Initialised ${implementsMap.size} contract<->interface mappings")
        _ <- logDebug(pprint(implementsMap, height = Int.MaxValue).toString)

        packages <- transaction {
          sql"select id, pk from __packages"
            .query[(String, PackagePk)]
            .selectAll
        }
        packageMap = packages.map((id, pk) => PackageId(id) -> pk).toMap
        _ <- logInfo(s"Initialised ${packages.size} packages")
        _ <- logDebug(pprint(packages).toString)

        lastId <- transaction {
          sql"""select max(pk) pk from __events"""
            .query[Long]
            .selectOne
            .someOrElse(0L)
        }
        _ <- logDebug(s"Initialised last PK in `__events` table: $lastId")
        placeholders = IdPlaceholder.factory(lastId + 1)
      yield Service(
        config,
        poolConfig,
        pool,
        schema,
        codec,
        getEntityType,
        getExerciseType,
        getImplements,
        packageMap,
        placeholders
      )
    }
  }

  final case class Service(
      config: SchemaConfig,
      poolConfig: PostgresConfig,
      pool: ZConnectionPool,
      schema: SqlSchema.Service,
      codec: Dictionary[Codec[Value]],
      getEntityPk: Identifier => EntityTypePk,
      getExercisePk: (Identifier, ChoiceName) => EntityTypePk,
      getImplementsPks: Identifier => Chunk[EntityTypePk],
      packageMap: Map[PackageId, PackagePk],
      placeholders: IdPlaceholder.Factory
  ) extends specific.Service(codec, getEntityPk, getExercisePk, getImplementsPks, packageMap, placeholders)
      with Datastore:
    import com.digitalasset.scribe.postgres.document.specific.offsetEncoder

    private val Genesis: Datastore.Checkpoint = (Offset.Genesis, 0L)
    private val env                           = ZEnvironment(pool) ++ ZEnvironment(config) ++ ZEnvironment(poolConfig)
    private val tx                            = ZLayer.succeedEnvironment(env) >>> transaction
    private val BatchEntitiesThreshold        = 10_000
    private val BatchReleaseWindow            = 200.millis

    override def registerActiveWriterAndCleanupTransactions = tx(
      sql"call __cleanup_transactions_after_watermark()".execute
    )

    override def getFirstCheckpoint = tx(
      sql"""select "offset", ix from oldest_checkpoint()""".query[Datastore.Checkpoint].selectOne.someOrElse(Genesis)
    )

    override def getLastCheckpoint = tx(
      sql"""select "offset", ix from latest_checkpoint()""".query[Datastore.Checkpoint].selectOne.someOrElse(Genesis)
    )

    override def processAcs = (
      waitPoint("pipeline_wp_acs_events", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
        >>> convertAcsEventsToStatements
        >>> waitPoint("pipeline_wp_acs_statements", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
        >>> batchStatements
        >>> waitPoint("pipeline_wp_acs_batched_statements")
        >>> prepareStatements
        >>> waitPoint("pipeline_wp_acs_prepared_statements")
        >>> executePar(16)
        >>> ZPipeline.flattenChunks
        >>> updateAcsOffsets
        >>> handleWatermarks
        >>> ZSink.drain
    ).provideEnvironment(env)

    override def processTransactions = (
      waitPoint("pipeline_wp_events", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
        >>> convertTransactionEventsToStatements(8)
        >>> waitPoint("pipeline_wp_statements", poolConfig.bufferSize, 1024 min poolConfig.bufferSize)
        >>> batchStatements
        >>> waitPoint("pipeline_wp_batched_statements")
        >>> prepareStatements
        >>> waitPoint("pipeline_wp_prepared_statements")
        >>> executeParUnordered(poolConfig.maxConnections)
        >>> waitPoint("pipeline_wp_watermarks", 1024)
        >>> reorderCheckpoints
        >>> handleWatermarks
        >>> ZSink.drain
    ).provideEnvironment(env)

    // privates

    /** Process ACS events */
    private def convertAcsEventsToStatements =
      val trackConvert = latency("pipeline_convert_acs_event", "Latency of converting ACS events")
      ZPipeline[Event.Created | Offset]
        .mapChunksZIO(chunk =>
          ZIO.whenCase(chunk.headOption) {
            case Some(Offset.Genesis) =>
              tx(model.Model.prepareStatement(Chunk(model.Transaction(specific.Transaction(Genesis._2, Genesis._1)))))
                .as(Chunk.empty)
          } *> ZIO.attempt {
            chunk.collect {
              case evt: Event.Created      => insertEvent(Genesis._2, evt)
              case offset: Offset.Absolute => Chunk(model.Watermark(Genesis._2, offset, Seq.empty))
            }
          } @@ trackConvert
        )
        .tap(x => logDebug(s"Converted ${x.length} ACS events to SQL fragments"))

    /** Process transaction stream events */
    private def convertTransactionEventsToStatements(n: Int) =
      val trackConvert = latency("pipeline_convert_transaction", "Latency of converting transactions")
      type TX = (Transaction[Event | TreeEvent | ReassignmentEvent], Datastore.TransactionIndex)
      ZPipeline
        .fromChannel(
          ZChannel
            .identity[Throwable, Chunk[TX], Any]
            .mapOutZIOPar(n)(chunk =>
              chunk.mapZIO { (tx, ix) =>
                for
                  _      <- tx.span.addEvent("converting canonical transaction to domain model")
                  result <- ZIO.attempt { convertTransactionToSqlStatements(tx, ix) } @@ trackConvert
                  _      <- tx.span.addEvent("converted canonical transaction to domain model")
                yield result
              }
            )
        )
        .tap(x => logDebug(s"Converted ${x.length} transaction events to SQL fragments"))

    /** Groups multiple SQL actions into large batches of SQL IO to be executed in single transactions unordered. */
    private def batchStatements =
      ZPipeline[Chunk[model.Model]]
        .aggregateAsyncWithin(
          ZSink.foldChunks( // start with:
            ChunkBuilder.make[model.Model]() -> 0
          ) { // continue while:
            (acc, size) => size < BatchEntitiesThreshold
          } { // accumulate:
            case ((acc, size), in) =>
              var s = size
              for chunk <- in; elem <- chunk do { acc.addOne(elem); s += 1 }
              (acc, s)
          },
          Schedule.spaced(BatchReleaseWindow) // release batch regularly even if not full
        )
        .map(_._1.result())
        .tap { models =>
          ZIO.foreachDiscard(models.onlyTransactions())(_.ifTraced(_.addEvent("released transaction model into batch")))
        }
        .tap(x => logDebug(s"Aggregated ${x.length} SQL fragments into single batch"))

    private def prepareStatements =
      val trackPrepare = latency("pipeline_prepare_batch_latency", "Latency of preparing batches of statements")
      val trackExecute = latency("pipeline_execute_batch_latency", "Latency of executing batches of statements")
      ZPipeline[Chunk[model.Model]].mapChunksZIO { chunk =>
        ZIO.foreach(chunk) { models =>
          val onlyTxs = models.onlyTransactions()
          ZIO.attempt {
            traces.span("execute batch") {
              model.Model.prepareStatement(models)
                @@ trackExecute
                @@ traces.attributes("scribe.batch.models_count" -> models.length.toLong)
                <* ZIO.foreachDiscard(onlyTxs) { tx =>
                  tx.ifTraced(
                    _.linkFromCurrentSpan(
                      "target" -> "↥ incoming transaction",
                      "offset" -> (tx.offset.toSqlValue)
                    )
                  )
                }
            }
          } <* ZIO.foreachDiscard(onlyTxs)(_.ifTraced(_.addEvent("prepared SQL statements for transaction model")))
        } @@ trackPrepare
      }

    /** Upstream statements were executed out of order, this pipeline restores the consecutive order of indexes */
    private def reorderCheckpoints =
      type AccumulatorChannel =
        ZChannel[Any, Nothing, Chunk[Chunk[model.Watermark]], Any, Nothing, Chunk[model.Watermark], Unit]
      def accumulator(state: mutable.ArrayBuffer[model.Watermark]): AccumulatorChannel = ZChannel.readWithCause(
        in => {
          for chunk <- in do state.addAll(chunk)
          state.sortInPlace()
          val consecutive = (state.view zip state.view.drop(1)).takeWhile { (prev, next) => prev.ix + 1 == next.ix }
          consecutive.lastOption match
            case Some((_, value)) =>
              // Gather all span refs (to individual txs & batches) up to advancing watermark
              // ignoring head of `state` since it had already advanced by now
              val advancing = state.view.slice(1, consecutive.size + 1)
              val seenAts   = advancing.map(_.seenAts).fold(Seq.empty)(_ ++ _)
              val txs       = advancing.map(_.txSpans).fold(Seq.empty)(_ ++ _)
              val batches   = advancing.map(_.persistSpans).fold(Seq.empty)(_ ++ _).distinct
              // `value` becomes the new head of `state` :)
              state.remove(0, consecutive.size)
              val effectiveWatermark = value.copy(seenAts = seenAts, txSpans = txs, persistSpans = batches)
              ZChannel.write(Chunk(effectiveWatermark)) *> accumulator(state)
            case None =>
              accumulator(state)
        },
        err => ZChannel.refailCause(err),
        _ => ZChannel.unit
      )
      ZPipeline.unwrap(
        getLastCheckpoint
          .map(cp => model.Watermark(cp._2, cp._1, Seq.empty))
          .map(start =>
            ZPipeline.fromChannel[Any, Nothing, Chunk[model.Watermark], model.Watermark](
              accumulator(mutable.ArrayBuffer(start))
            )
          )
      )

    /** Update watermarks */
    private def handleWatermarks =
      val trackWatermark = latency("pipeline_progress_watermark", "Latency of watermark progression")
      val watermarkIx = Metric
        .gauge("watermark_ix", "Current watermark index (transaction ordinal number for consistent reads)")
        .contramap[Long](_.toDouble)
      val txProcessingLatency = Metric
        .histogram(
          "total_tx_handling_latency",
          "Total transaction handling latency in scribe",
          Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
        )
        .contramap[Long](_.toDouble / 1e9)
      ZPipeline[model.Watermark].mapZIO(wm =>
        traces.span("advance datastore watermark") {
          trackWatermark(updateWatermark(wm))
            @@ traces.attributes(
              "scribe.watermark.offset" -> wm.offset.toSqlValue,
              "scribe.watermark.ix"     -> wm.ix
            )
            *> ZIO.foreachDiscard(wm.txSpans) { s =>
              s.linkToCurrentSpan("target" -> "↧ advance watermark")
                *> s.addEvent(
                  "advanced datastore watermark",
                  "offset" -> wm.offset.toSqlValue,
                  "index"  -> wm.ix
                )
                *> s.end()
            }
            *> ZIO.foreachDiscard(wm.persistSpans) { s =>
              ZIO.unit @@ traces.link(s, "target" -> "↥ persist to datastore")
            }
            *> zio.Clock.nanoTime.flatMap(now =>
              ZIO.foreachDiscard(wm.seenAts) { seenAt => txProcessingLatency.update(now - seenAt) }
            )
            *> watermarkIx.update(wm.ix)
            *> logInfo(s"Advanced watermark: ix = ${wm.ix}, offset = ${wm.offset.toSqlValue}")
        }
      )

    private def updateWatermark(wm: Watermark) =
      tx(sql"""update __watermark set "offset" = ${wm.offset.toSqlValue}, ix = ${wm.ix};""".update)
        .filterOrFail(_ == 1)(RuntimeException("Failed to update watermark."))

    private def updateAcsOffsets =
      ZPipeline[model.Watermark].tap(wm =>
        tx(sql"""update __transactions set "offset" = ${wm.offset.toSqlValue} where ix = ${Genesis._2};""".update)
      )

    private implicit def stringSetter[T <: String | Offset]: Setter[T] =
      Setter(
        (stmt, ix, value) => stmt.setObject(ix, value.toString),
        (stmt, ix) => stmt.setNull(ix, java.sql.Types.VARCHAR)
      )

    private implicit def idSetter: Setter[IdPlaceholder] = Setter(
      (stmt, ix, value) => stmt.setLong(ix, value.id),
      (stmt, ix) => stmt.setNull(ix, java.sql.Types.BIGINT)
    )

  end Service
end DocumentPostgres
