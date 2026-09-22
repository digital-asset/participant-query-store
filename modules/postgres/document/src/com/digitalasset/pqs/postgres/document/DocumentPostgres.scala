// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical
import com.digitalasset.canonical.{ContractId, Offset, Party, SynchronizerId}
import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.pqs.o11y.metrics.latency
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.traces.given
import com.digitalasset.pqs.postgres.backend.*
import com.digitalasset.transcode.Codec
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml.{DamlSchema, JsonCodecs}
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
import java.time.Instant
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.Using

final case class DocumentPostgres(
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
  private given JdbcDecoder[Offset] = (ix, rs) => (ix, Offset.Absolute(rs.getLong(ix)))

  private val Genesis: Datastore.Checkpoint = (Offset.Genesis, 0L)
  private val env                           = ZEnvironment(pool) ++ ZEnvironment(poolConfig)
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
    ZPipeline[canonical.Event.Created | Offset]
      .mapChunksZIO(chunk =>
        ZIO.whenCase(chunk.headOption) {
          case Some(Offset.Genesis) =>
            tx(Model.prepareStatement(Chunk(Transaction(Genesis._2, Genesis._1))))
              .as(Chunk.empty)
        } *> ZIO.attempt {
          chunk.collect {
            case evt: canonical.Event.Created => insertEvent(Genesis._2, evt)
            case offset: Offset.Absolute      => Chunk(Watermark(Genesis._2, offset, Seq.empty))
          }
        } @@ trackConvert
      )
      .tap(x => logDebug(s"Converted ${x.length} ACS events to SQL fragments"))

  /** Process transaction stream events */
  private def convertTransactionEventsToStatements(n: Int) =
    val trackConvert = latency("pipeline_convert_transaction", "Latency of converting transactions")
    type TX = (
        canonical.Transaction[canonical.Event],
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
    ZPipeline[Chunk[Model]]
      .aggregateAsyncWithin(
        ZSink.foldChunks( // start with:
          ChunkBuilder.make[Model]() -> 0
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
    ZPipeline[Chunk[Model]].mapChunksZIO { chunk =>
      ZIO.foreach(chunk) { models =>
        val onlyTxs = models.onlyTransactions()
        ZIO.attempt {
          traces.span("execute batch") {
            Model.prepareStatement(models)
              @@ trackExecute
              @@ traces.attributes("pqs.batch.models_count" -> models.length.toLong)
              <* ZIO.foreachDiscard(onlyTxs) { tx =>
                tx.ifTraced(
                  _.linkFromCurrentSpan(
                    "target" -> "↥ incoming transaction",
                    "offset" -> (tx.offset.toLong)
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
      ZChannel[Any, Nothing, Chunk[Chunk[Watermark]], Any, Nothing, Chunk[Watermark], Unit]
    def accumulator(state: mutable.ArrayBuffer[Watermark]): AccumulatorChannel = ZChannel.readWithCause(
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
        .map(cp => Watermark(cp._2, cp._1, Seq.empty))
        .map(start =>
          ZPipeline.fromChannel[Any, Nothing, Chunk[Watermark], Watermark](
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
    ZPipeline[Watermark].mapZIO(wm =>
      traces.span("advance datastore watermark") {
        trackWatermark(updateWatermark(wm))
          @@ traces.attributes(
            "pqs.watermark.offset" -> wm.offset.toLong,
            "pqs.watermark.ix"     -> wm.ix
          )
          *> ZIO.foreachDiscard(wm.txSpans) { s =>
            s.linkToCurrentSpan("target" -> "↧ advance watermark")
              *> s.addEvent(
                "advanced datastore watermark",
                "offset" -> wm.offset.toLong,
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
          *> logInfo(s"Advanced watermark: ix = ${wm.ix}, offset = ${wm.offset.toLong}")
      }
    )

  private def updateWatermark(wm: Watermark) =
    tx(sql"""update __watermark set "offset" = ${wm.offset.toLong}, ix = ${wm.ix};""".update)
      .filterOrFail(_ == 1)(RuntimeException("Failed to update watermark."))

  private def updateAcsOffsets =
    ZPipeline[Watermark].tap(wm =>
      tx(sql"""update __transactions set "offset" = ${wm.offset.toLong} where ix = ${Genesis._2};""".update)
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
      tx: canonical.Transaction[canonical.Event],
      txIx: Long
  ): Chunk[Model] =
    val insertTx = Transaction(
      txIx,
      tx.offset,
      Some(tx.transactionId),
      tx.effectiveAt,
      Some(tx.synchronizerId),
      Some(tx.workflowId),
      tx.remoteSpan,
      tx.externalTransactionHash,
      tx.paidTrafficCost,
      Some(tx.span)
    )
    val insertEvents    = tx.events.flatMap(evt => insertEvent(txIx, evt))
    val insertWatermark = Watermark(txIx, tx.offset, Seq(tx.seenAt))
    insertTx +: insertEvents :+ insertWatermark

  private def insertEvent(
      txIx: Long,
      event: canonical.Event
  ): Chunk[Model] = {
    val pk = placeholders.mk

    event match
      case c: canonical.Event.Created =>
        val evt = Event(pk, txIx, c.eventId, EventType.Create)
        val contracts = mkContracts(
          eventPk = pk,
          txIx,
          c.contract,
          c.synchronizerId,
          reassignmentCounter = 0,
          isCreate = true
        )
        contracts :+ evt

      case a: canonical.Event.Archived =>
        val evt      = Event(pk, txIx, a.eventId, EventType.Archive)
        val archives = mkDeactivatedContracts(pk, txIx, a.contractId, a.templateId, a.synchronizerId, isArchive = true)
        archives :+ evt

      case e: canonical.Event.Exercised =>
        val event = Event(pk, txIx, e.eventId, EventType.Exercise)
        val exercise = Exercise(
          qualifiedName = e.templateId.qualifiedName,
          entityType = exercisePkMap(e.entityId, e.choice),
          contractEntityType = entityPkMap(e.entityId),
          exerciseEventPk = pk,
          exercisedAt = txIx,
          contractId = e.contractId,
          choiceName = e.choice,
          argument = codec.choiceArgument(e.entityId, e.choice).fromDynamicValue(e.arg),
          result = codec.choiceResult(e.entityId, e.choice).fromDynamicValue(e.result),
          controllers = e.controllers,
          witnesses = e.witnesses,
          lastDescendant = e.lastDescendant,
          packagePk = packageMap(e.templateId.packageId)
        )
        val archives =
          if e.consuming then
            mkDeactivatedContracts(pk, txIx, e.contractId, e.templateId, e.synchronizerId, isArchive = true)
          else Chunk.empty
        archives :+ exercise :+ event

      case u: canonical.Event.Unassigned =>
        val event = Event(pk, txIx, u.eventId, EventType.Unassign)
        val unassignedContracts =
          mkDeactivatedContracts(pk, txIx, u.contractId, u.templateId, u.synchronizerId, isArchive = false)
        val reassignment = mkReassignment(
          eventPk = pk,
          txIx = txIx,
          reassignmentType = ReassignmentType.Unassign,
          templateId = u.templateId,
          contractId = u.contractId,
          reassignmentId = u.reassignmentId,
          source = u.source,
          target = u.target,
          submitter = u.submitter,
          reassignmentCounter = u.reassignmentCounter,
          witnesses = u.witnesses,
          assignmentExclusivity = u.assignmentExclusivity
        )
        unassignedContracts :+ reassignment :+ event

      case e: canonical.Event.Assigned =>
        val event     = Event(pk, txIx, e.eventId, EventType.Assign)
        val contracts = mkContracts(pk, txIx, e.contract, e.synchronizerId, e.reassignmentCounter, isCreate = false)
        val reassignment = mkReassignment(
          eventPk = pk,
          txIx = txIx,
          reassignmentType = ReassignmentType.Assign,
          templateId = e.contract.templateId,
          contractId = e.contract.contractId,
          reassignmentId = e.reassignmentId,
          source = e.source,
          target = e.target,
          submitter = e.submitter,
          reassignmentCounter = e.reassignmentCounter,
          witnesses = e.contract.witnesses,
          assignmentExclusivity = None
        )
        contracts :+ reassignment :+ event
  }

  private def mkContracts(
      eventPk: IdPlaceholder,
      txIx: Long,
      contract: canonical.Contract,
      synchronizerId: SynchronizerId,
      reassignmentCounter: Long,
      isCreate: Boolean
  ): Chunk[Contract] =
    contract.payloads.map((entityId, value) =>
      Contract(
        qualifiedName = contract.templateId.qualifiedName,
        entityType = entityPkMap(entityId),
        createEventPk = Option.when(isCreate)(eventPk),
        createdAtIx = Option.when(isCreate)(txIx),
        assignEventPk = Option.when(!isCreate)(eventPk),
        assignedAtIx = Option.when(!isCreate)(txIx),
        contractId = contract.contractId,
        synchronizerId = synchronizerId,
        reassignmentCounter = reassignmentCounter,
        signatories = contract.signatories,
        observers = contract.observers,
        witnesses = contract.witnesses,
        payload = codec.template(entityId).fromDynamicValue(value),
        // A create yields a row per payload: one for the template, one per interface view. Only a keyed
        // template has a key codec, so checking templateKey codec drops both contractKey and contractKeyHash.
        contractKey = codec.getTemplateKey(entityId).zip(contract.contractKey).map(_.fromDynamicValue(_)),
        contractKeyHash = codec.getTemplateKey(entityId).flatMap(_ => contract.contractKeyHash),
        metadata = contract.metadata,
        acsDelta = contract.acsDelta,
        packagePk = packageMap(contract.representativePackageId),
        creationPackageId = contract.creationPackageId
      )
    )

  private def mkDeactivatedContracts(
      eventPk: IdPlaceholder,
      txIx: Long,
      contractId: ContractId,
      templateId: Identifier,
      synchronizerId: SynchronizerId,
      isArchive: Boolean
  ) =
    val templateType = entityPkMap(templateId)
    val interfaces   = implementsPkMap.getOrElse(templateId, Chunk.empty)
    (interfaces :+ templateType).map { entityType =>
      DeactivatedContract(
        templateId.qualifiedName,
        entityType,
        contractId,
        archiveEventPk = Option.when(isArchive)(eventPk),
        archivedAtIx = Option.when(isArchive)(txIx),
        unassignEventPk = Option.when(!isArchive)(eventPk),
        unassignedAtIx = Option.when(!isArchive)(txIx),
        synchronizerId
      )
    }

  private def mkReassignment(
      eventPk: IdPlaceholder,
      txIx: Long,
      reassignmentType: ReassignmentType,
      templateId: Identifier,
      contractId: ContractId,
      reassignmentId: String,
      source: SynchronizerId,
      target: SynchronizerId,
      submitter: Option[Party],
      reassignmentCounter: Long,
      witnesses: Chunk[Party],
      assignmentExclusivity: Option[Instant]
  ) =
    Reassignment(
      entityType = entityPkMap(templateId),
      reassignmentEventPk = eventPk,
      reassignedAtIx = txIx,
      reassignmentType = reassignmentType,
      contractId = contractId,
      reassignmentId = reassignmentId,
      source = source,
      target = target,
      submitter = submitter,
      reassignmentCounter = reassignmentCounter,
      witnesses = witnesses,
      assignmentExclusivity = assignmentExclusivity
    )
end DocumentPostgres

object DocumentPostgres:
  def applySchema(
      pgCfg: PostgresConfig,
      doBaseline: Boolean
  ): ZIO[InstanceId & ZConnectionPool & SqlSchema, Throwable, Unit] =
    traces.span("apply schema") {
      for
        _          <- logInfo("Applying schema")
        instanceId <- ZIO.service[InstanceId]
        _          <- ZIO.attemptBlocking(migrateSchema(pgCfg, instanceId, doBaseline))
      yield ()
    } *> traces.span("apply mappings") {
      logInfo("Applying mappings") *>
        ZIO.serviceWithZIO[SqlSchema](schema => logTrace(schema.mappings) *> transaction(schema.mappings.execute))
    } <* logInfo("Schema and mappings applied")

  val live = ZLayer.scoped {
    traces.root("process metadata and schema") {
      for
        damlSchema <- ZIO.service[DamlSchema]
        config     <- ZIO.service[SchemaConfig]
        poolConfig <- ZIO.service[PostgresConfig]
        pool       <- ZIO.service[ZConnectionPool]
        schema     <- ZIO.service[SqlSchema]
        codec      <- ZIO.service[JsonCodecs]

        _ <- applySchema(poolConfig, config.baseline) when config.autoApply // initialize schema if needed

        entities <- transaction {
          sql"""select p.id, ct.module_name, ct.entity_name, ct.pk as pk
              from __contract_tpe ct, __packages p
              where ct.package_name = p.name"""
            .query[(String, String, String, EntityTypePk)]
            .selectAll
        }
        entityPks <- ZIO
          .foreach(entities) { (pkg, m, e, pk) =>
            // Skip invalid rows silently: Joining on package_name may pair a template with a packageId that doesn't define it
            damlSchema.toIdentifier(pkg, m, e).option.map(_.map(_ -> pk))
          }
          .map(_.flatten)
        entityPkMap: Map[Identifier, EntityTypePk] = entityPks.toMap
        _ <- logInfo(s"Initialised ${entities.size} entity types")
        _ <- logDebug(pprint(entities, height = Int.MaxValue).toString)

        exercises <- transaction {
          sql"""select p.id, et.module_name, et.entity_name, et.choice, et.pk as pk
              from __exercise_tpe et, __packages p
              where et.package_name = p.name"""
            .query[(String, String, String, String, EntityTypePk)]
            .selectAll
        }
        exercisePks <-
          ZIO
            .foreach(exercises) { (pkg, m, e, c, pk) =>
              // Skip invalid rows silently: Joining on package_name may pair a choice with a packageId that doesn't define it
              damlSchema
                .toIdentifier(pkg, m, e)
                .option
                .map(_.map(id => (id, ChoiceName(c)) -> pk))
            }
            .map(_.flatten)
        _ <- logInfo(s"Initialised ${exercises.size} exercise types")
        _ <- logDebug(pprint(exercises, height = Int.MaxValue).toString)

        implementsRelations <- transaction {
          sql"select template_pk, interface_pk from __contract_implements"
            .query[(EntityTypePk, EntityTypePk)]
            .selectAll
        }
        implementsMap = implementsRelations
          .groupMap((template, _) => template)((_, interface) => interface)
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

  private def migrateSchema(pgCfg: PostgresConfig, instanceId: InstanceId, doBaseline: Boolean): Unit =
    Flyway
      .configure()
      .dataSource(
        DriverDataSource(
          Thread.currentThread().getContextClassLoader,
          "org.postgresql.Driver",
          s"jdbc:postgresql://${pgCfg.host}:${pgCfg.port}/${pgCfg.database}?currentSchema=${pgCfg.schema}",
          pgCfg.username,
          pgCfg.password.value,
          (sslprops(pgCfg.tls) ++ instanceIdProp(instanceId) ++ pgCfg.properties.view.mapValues(_.value)).asJava
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
                .flatMap(suffix => scanResult.getResourcesWithExtension(suffix).asScala)
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
end DocumentPostgres
