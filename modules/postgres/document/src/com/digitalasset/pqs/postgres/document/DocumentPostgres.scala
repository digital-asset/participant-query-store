// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical
import com.digitalasset.canonical.{ContractId, ReassignmentEvent}
import com.digitalasset.canonical.specific.{Offset, TreeEvent}
import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.pqs.o11y.metrics.latency
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.given
import com.digitalasset.pqs.postgres.backend.*
import com.digitalasset.pqs.postgres.document.model.{EntityTypePk, PackagePk, Watermark}
import com.digitalasset.pqs.postgres.document.specific.*
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml.DamlSchema
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

final case class DocumentPostgres(
    config: SchemaConfig,
    poolConfig: PostgresConfig,
    pool: ZConnectionPool,
    schema: SqlSchema,
    codec: Dictionary[Codec[Value]],
    entityPkMap: Map[Identifier, EntityTypePk],
    exercisePkMap: Map[(Identifier, ChoiceName), EntityTypePk],
    implementsPkMap: Map[Identifier, Chunk[EntityTypePk]],
    packageMap: Map[PackageId, PackagePk],
    placeholders: IdPlaceholder.Factory
) extends Datastore:
  import com.digitalasset.pqs.postgres.document.specific.offsetEncoder

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
    ZPipeline[canonical.specific.Event.Created | Offset]
      .mapChunksZIO(chunk =>
        ZIO.whenCase(chunk.headOption) {
          case Some(Offset.Genesis) =>
            tx(model.Model.prepareStatement(Chunk(model.Transaction(specific.Transaction(Genesis._2, Genesis._1)))))
              .as(Chunk.empty)
        } *> ZIO.attempt {
          chunk.collect {
            case evt: canonical.specific.Event.Created => insertEvent(Genesis._2, evt)
            case offset: Offset.Absolute               => Chunk(model.Watermark(Genesis._2, offset, Seq.empty))
          }
        } @@ trackConvert
      )
      .tap(x => logDebug(s"Converted ${x.length} ACS events to SQL fragments"))

  /** Process transaction stream events */
  private def convertTransactionEventsToStatements(n: Int) =
    val trackConvert = latency("pipeline_convert_transaction", "Latency of converting transactions")
    type TX = (
        canonical.specific.Transaction[canonical.specific.Event | TreeEvent | ReassignmentEvent],
        Datastore.TransactionIndex
    )
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
              @@ traces.attributes("pqs.batch.models_count" -> models.length.toLong)
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
        "Total transaction handling latency in pqs",
        Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
      )
      .contramap[Long](_.toDouble / 1e9)
    ZPipeline[model.Watermark].mapZIO(wm =>
      traces.span("advance datastore watermark") {
        trackWatermark(updateWatermark(wm))
          @@ traces.attributes(
            "pqs.watermark.offset" -> wm.offset.toSqlValue,
            "pqs.watermark.ix"     -> wm.ix
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

  private def convertTransactionToSqlStatements(
      tx: canonical.specific.Transaction[canonical.specific.Event | TreeEvent | ReassignmentEvent],
      txIx: Long
  ): Chunk[model.Model] =
    val insertTx = model.Transaction(
      Transaction(
        txIx,
        tx.offset,
        Some(tx.transactionId),
        Some(tx.effectiveAt),
        tx.domainId,
        Some(tx.workflowId),
        tx.remoteSpan,
        tx.externalTransactionHash,
        tx.paidTrafficCost
      ),
      Some(tx.span)
    )
    val insertEvents    = tx.events.flatMap(evt => insertEvent(txIx, evt))
    val insertWatermark = model.Watermark(txIx, tx.offset, Seq(tx.seenAt))
    insertTx +: insertEvents :+ insertWatermark

  private def insertEvent(
      txIx: Long,
      event: canonical.specific.Event | TreeEvent | ReassignmentEvent
  ): Chunk[model.Model] = {
    val pk = placeholders.mk

    def mkArchives(eventPk: IdPlaceholder, txIx: Long, contractId: ContractId, templateId: Identifier) =
      val templateType = entityPkMap(templateId)
      val interfaces   = implementsPkMap.getOrElse(templateId, Chunk.empty)
      (interfaces :+ templateType).map { entityType =>
        model.Archive(
          templateId.qualifiedName,
          entityType,
          eventPk,
          txIx,
          contractId,
          packageMap(templateId.packageId)
        )
      }

    event match
      case canonical.specific.Event.Created(
            eid,
            rpId,
            templateQualifiedName,
            cid,
            contractKey,
            contractKeyHash,
            payloads,
            signatories,
            observers,
            witnesses,
            created_at,
            metadata,
            acsDelta,
            creationPackageId
          ) =>
        val evt = model.Event(
          Event(
            pk = pk,
            txIx = txIx,
            eventId = eid,
            eventType = model.EventType.Create
          )
        )
        val contracts = payloads.map((entityId, value) =>
          model.Contract(
            Contract(
              qualifiedName = templateQualifiedName,
              entityType = entityPkMap(entityId),
              createEventPk = pk,
              createdAtIx = txIx,
              contractId = cid,
              signatories = signatories,
              observers = observers,
              witnesses = witnesses,
              payload = codec.template(entityId).fromDynamicValue(value),
              // A create yields a row per payload: one for the template, one per interface view. Only a keyed
              // template has a key codec, so checking templateKey codec drops both contractKey and contractKeyHash.
              contractKey = (codec.getTemplateKey(entityId) zip contractKey).map(_ `fromDynamicValue` _),
              contractKeyHash = codec.getTemplateKey(entityId).flatMap(_ => contractKeyHash),
              metadata = metadata,
              acsDelta = acsDelta,
              packagePk = packageMap(rpId),
              creationPackageId = creationPackageId
            )
          )
        )
        contracts :+ evt

      case canonical.specific.Event.Archived(eid, tid, cid) =>
        val evt = model.Event(
          Event(
            pk = pk,
            txIx = txIx,
            eventId = eid,
            eventType = model.EventType.Archive
          )
        )
        val archives = mkArchives(pk, txIx, cid, tid)
        archives :+ evt

      case canonical.specific.Event.Exercised(
            eid,
            tid,
            entityId,
            choice,
            consuming,
            cid,
            arg,
            result,
            controllers,
            witnesses,
            lastDescendant
          ) =>
        val choiceRef = entityId.copy(entityName = EntityName(choice))
        val evt = model.Event(
          Event(
            pk = pk,
            txIx = txIx,
            eventId = eid,
            eventType = model.EventType.Exercise
          )
        )
        val exercise = model.Exercise(
          Exercise(
            qualifiedName = tid.qualifiedName,
            entityType = exercisePkMap(entityId, choice),
            contractEntityType = entityPkMap(entityId),
            exerciseEventPk = pk,
            exercisedAt = txIx,
            contractId = cid,
            choiceName = choice,
            argument = codec.choiceArgument(entityId, choice).fromDynamicValue(arg),
            result = codec.choiceResult(entityId, choice).fromDynamicValue(result),
            controllers = controllers,
            witnesses = witnesses,
            lastDescendant = lastDescendant,
            packagePk = packageMap(tid.packageId)
          )
        )
        val archives = if consuming then mkArchives(pk, txIx, cid, tid) else Chunk.empty
        archives :+ exercise :+ evt

      case rs: ReassignmentEvent =>
        Chunk( /*TODO*/ )
  }
end DocumentPostgres

object DocumentPostgres:
  def applySchema(
      pgCfg: PostgresConfig,
      instanceId: InstanceId,
      doBaseline: Boolean
  ): ZIO[ZConnectionPool & SqlSchema, Throwable, Unit] =
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
          ZIO.serviceWithZIO[SqlSchema](schema => logTrace(schema.mappings) *> transaction(schema.mappings.execute))
      }
      <* logInfo("Schema and mappings applied")

  val live = ZLayer.scoped {
    traces.root("process metadata and schema") {
      for
        damlSchema <- ZIO.service[DamlSchema]
        config     <- ZIO.service[SchemaConfig]
        poolConfig <- ZIO.service[PostgresConfig]
        instanceId <- ZIO.service[InstanceId]
        pool       <- ZIO.service[ZConnectionPool]
        schema     <- ZIO.service[SqlSchema]
        codec      <- ZIO.service[Dictionary[Codec[Value]]]

        _ <- applySchema(poolConfig, instanceId, config.baseline) when config.autoApply // initialize schema if needed

        entities <- transaction {
          sql"""select p.id, ct.module_name, ct.entity_name, ct.pk as pk
              from __contract_tpe ct, __packages p
              where ct.package_name = p.name"""
            .query[(String, String, String, Long)]
            .selectAll
        }
        entityPks <- ZIO.foreach(entities)((pkg, m, e, pk) =>
          damlSchema.toIdentifier(pkg, m, e).map(_ -> EntityTypePk(pk))
        )
        entityPkMap: Map[Identifier, EntityTypePk] = entityPks.toMap
        _ <- logInfo(s"Initialised ${entities.size} entity types")
        _ <- logDebug(pprint(entities, height = Int.MaxValue).toString)

        exercises <- transaction {
          sql"""select p.id, et.module_name, et.entity_name, et.choice, et.pk as pk
              from __exercise_tpe et, __packages p
              where et.package_name = p.name"""
            .query[(String, String, String, String, Long)]
            .selectAll
        }
        exercisePks <-
          ZIO.foreach(exercises)((pkg, m, e, c, pk) =>
            damlSchema.toIdentifier(pkg, m, e).map(id => (id, ChoiceName(c)) -> EntityTypePk(pk))
          )
        _ <- logInfo(s"Initialised ${exercises.size} exercise types")
        _ <- logDebug(pprint(exercises, height = Int.MaxValue).toString)

        implementsRelations <- transaction {
          sql"select template_pk, interface_pk from __contract_implements"
            .query[(Long, Long)]
            .selectAll
        }
        implementsMap = implementsRelations
          .groupMap((template, _) => EntityTypePk(template))((_, interface) => EntityTypePk(interface))
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
      yield DocumentPostgres(
        config,
        poolConfig,
        pool,
        schema,
        codec,
        entityPkMap,
        exercisePks.toMap,
        entityPkMap.flatMap((id, pk) => implementsMap.get(pk).map(id -> _)),
        packageMap,
        placeholders
      )
    }
  }
end DocumentPostgres
