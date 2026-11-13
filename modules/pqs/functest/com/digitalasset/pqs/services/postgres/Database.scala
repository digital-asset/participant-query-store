// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.postgres.backend.{InstanceId, PostgresConfig}
import zio.*
import zio.jdbc.*
import zio.test.*

final case class Database(
    name: String,
    config: PostgresConfig,
    instanceId: InstanceId,
    connectionPool: ZConnectionPool
):
  def transaction = connectionPool.transaction

  /** Runs the supplied ZIO on a connection with `autoCommit = true`, outside any explicit transaction block. Required
    * for statements Postgres forbids inside a transaction (e.g. `CREATE DATABASE`).
    */
  def autoCommit[R: Tag, A](query: => ZIO[ZConnection & R, Throwable, A]): ZIO[R, Throwable, A] =
    connectionPool.transaction(
      ZIO.serviceWithZIO[ZConnection].apply(_.access(_.setAutoCommit(true))) *> query
    )

object Database:
  /** Replaces the connection pool of the `Database` with a new one, with the same config and a new instance id. Use it
    * after a schema migration to discard all server-side prepared statements whose result type may have changed.
    */
  def freshConnectionPool: ZLayer[Database, Throwable, Database] =
    config ++ backend.instanceId
      >+> backend.connectionPool
      >>> ZLayer.fromFunction((db: Database, instanceId: InstanceId, pool: ZConnectionPool) =>
        db.copy(instanceId = instanceId, connectionPool = pool)
      )

  val config         = ZLayer.fromFunction((d: Database) => d.config)
  val connectionPool = ZLayer.fromFunction((d: Database) => d.connectionPool)
  val instanceId     = ZLayer.fromFunction((d: Database) => d.instanceId)
  val transaction    = connectionPool.project(_.transaction).flatten

  def autoCommit[R: Tag, A](query: => ZIO[ZConnection & R, Throwable, A]) =
    ZIO.service[Database].flatMap(_.autoCommit(query))

  private given Conversion[Option[String], SqlFragment] = _.map(x => sql"$x").getOrElse(sql"")

  def __packages(orderBy: SqlFragment = sql"""order by version, name""") = query(
    sql"""select pk, name, version, id
          from __packages
          where name <> 'AdminWorkflows'
          $orderBy"""
  )

  def __contract_tpe() = query(
    sql"""select pk, template_fqn, payload_type, aliases
          from __contract_tpe
          where template_fqn not like 'AdminWorkflows:%'
          order by pk"""
  )

  def __exercise_tpe() = query(
    sql"""select pk, template_fqn, choice_fqn, choice, consuming, aliases
          from __exercise_tpe
          where template_fqn not like 'AdminWorkflows:%'
          order by pk"""
  )

  def __contracts(extraColumns: Seq[String] = Seq.empty) =
    val select = SqlFragment.select((Seq("package_pk", "tpe_pk", "contract_id", "life_ix") ++ extraColumns)*)
    query(sql"$select from __contracts order by activated_at_ix, tpe_pk")

  def __exercises() = query(
    sql"""select package_pk, tpe_pk, contract_tpe_pk, contract_id, argument ->> 'newLabel'
          from __exercises order by exercised_at_ix, tpe_pk"""
  )

  def __reassignments() = query(
    sql"""select tpe.template_fqn, r."type"::text, r.contract_id, r.reassignment_id,
          r.source_synchronizer_id, r.target_synchronizer_id, r.submitter, r.reassignment_counter
          from __reassignments r join __contract_tpe tpe on tpe.pk = r.contract_tpe_pk
          order by r.reassigned_at_ix, tpe.template_fqn, r.reassign_event_pk"""
  )

  def activeAtOffset(offset: Long, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"active(null, $offset)", extraColumns)

  def active(qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"active($qname)", extraColumns)

  def archives(qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"archives($qname)", extraColumns)

  def creates(qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"creates($qname)", extraColumns)

  def lookupContract(contractId: String, qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"lookup_contract($contractId, $qname)", extraColumns)

  def exercises(qname: Option[String] = None) = query(
    sql"""select package_id, template_fqn, choice_fqn, choice, contract_id, argument ->> 'newLabel'
          from exercises($qname) order by exercised_at_ix, template_fqn, choice_fqn"""
  )

  def summaryActiveAtOffset(offset: Long) = query(sql"select * from summary_active($offset)")

  def getLastIx =
    get[Long](sql"select ix from __transactions order by ix desc limit 1")

  def getOffset(ix: Long) =
    get[Long](sql"""select "offset" from __transactions where ix=$ix""")

  def transactionCount() =
    query(sql"select count(*) from __transactions".query[Long].selectOne).someOrElse(0L)

  def checkpoint = call(sql"checkpoint")

  def call(sql: SqlFragment): ZIO[Database, Throwable, Unit] =
    Database.autoCommit(sql.execute)

  def get[A: JdbcDecoder](sql: SqlFragment): ZIO[Database, Throwable, A] =
    Database.transaction(sql.query[A].selectOne).someOrFailException

  def getAll[A: JdbcDecoder](sql: SqlFragment): ZIO[Database, Throwable, Chunk[A]] =
    Database.transaction(sql.query[A].selectAll)

  def query(sql: SqlFragment): ZIO[Database, Throwable, Table] =
    Database.transaction(sql.query[Row].selectAll).map(Table.build)

  def query[A](sql: ZIO[ZConnection, Throwable, A]): ZIO[Database, Throwable, A] =
    Database.transaction(sql)

  def tables: ZIO[Database, Throwable, Chunk[(String, String)]] =
    Database.transaction(
      sql"""select distinct table_schema, table_name from information_schema.columns"""
        .query[(String, String)]
        .selectAll
    )
  def hasTable(
      tableName: String,
      schema: String = "public"
  ): ZIO[Database, Throwable, TestResult] =
    tables.map(x => assertTrue(x.contains((schema, tableName))))
  def lacksTable(
      tableName: String,
      schema: String
  ): ZIO[Database, Throwable, TestResult] =
    tables.map(x => assertTrue(!x.contains((schema, tableName))))

  /** Simulates a gap in the `__transactions` Table by deleting the ones with the supplied indexes and adjusting the
    * `__watermark` Table appropriately. This is useful for simulation of concurrently writing transactions out of
    * order.
    * @param txIndices
    *   the indexes of the transactions to delete
    * @return
    *   the number of affected rows
    */
  def makeGap(txIndices: Range): ZIO[Database, Throwable, Long] =
    query {
      sql"""update __watermark
            set "offset" = t."offset", ix = t.ix, instance_id = 'make-gap'
            from __transactions t
            where t.ix = ${txIndices.head - 1};""".update
    } *> query {
      sql"delete from __contracts where activated_at_ix in (${txIndices.toList})".delete *>
        sql"""update __contracts
              set deactivated_at_ix = null, deactivate_event_pk = null, deactivation_type = null
              where deactivated_at_ix in (${txIndices.toList})""".update
        *> sql"delete from __exercises where exercised_at_ix in (${txIndices.toList})".delete
        *> sql"delete from __reassignments where reassigned_at_ix in (${txIndices.toList})".delete
        *> sql"delete from __events where tx_ix in (${txIndices.toList})".delete
        *> sql"delete from __transactions where ix in (${txIndices.toList})".delete
    }

  /** Simulates non-contiguous offsets by shifting all offsets from the given transaction index onward. This models the
    * real-world scenario where a participant doesn't observe every global offset or observed transactions have been
    * pruned, creating gaps in the offset sequence without removing any data.
    * @param fromIx
    *   the transaction index from which offsets are shifted (inclusive)
    * @param shift
    *   the amount to add to each offset
    */
  def shiftOffsets(fromIx: Long, shift: Long): ZIO[Database, Throwable, Unit] =
    query(
      sql"""update __transactions set "offset" = "offset" + $shift where ix >= $fromIx""".update
        *> sql"""update __watermark set "offset" = "offset" + $shift where ix >= $fromIx""".update
    ).unit

  /** Reverses watermark to the offset of the given transaction (id) Simulates a situations where watermarking is behind
    * committed transactions (happens under a load).
    */
  def reverseWatermark(transactionIx: Long): ZIO[Database, Throwable, Long] = query(
    sql"""update __watermark set "offset" = tx."offset", ix = tx.ix, instance_id = 'reverse-watermark' from __transactions tx where tx.ix = $transactionIx""".update
  )

  /** Session's "pre-processing" statements that are run before query execution */
  case class Session(statements: List[SqlFragment])
  object Session:

    /** Initialises layer that provides session's steps "storage". */
    def init: ULayer[Session] = ZLayer.scoped {
      ZIO.logDebug(s"Initialising session") *> ZIO.succeed(Session(List.empty))
    }

    /** Sets the session's oldest offset (lower history bound) to the supplied value.
      * @param offset
      *   the offset to use (use `null` to clear previously set value)
      * @return
      *   the updated session
      */
    def setOldest(offset: String): RLayer[Session, Session] =
      val stmt = sql"""select set_oldest($offset::bigint);"""
      ZLayer.scoped {
        ZIO.logDebug(s"Setting session's oldest offset to $offset") *>
          ZIO.serviceWith[Session](prep => prep.copy(statements = prep.statements.appended(stmt)))
      }

    def setOldest(offset: Long): RLayer[Session, Session] =
      setOldest(offset.toString)

    /** Sets the session's oldest offset (lower history bound) to the value of the transaction with the supplied index.
      * @param txIndex
      *   the index of the transaction to use
      * @return
      *   the updated session
      */
    def setOldest(txIndex: Int): RLayer[Session, Session] =
      val stmt = sql"""select set_oldest((select "offset" from __transactions where ix = $txIndex));"""
      ZLayer.scoped {
        ZIO.logDebug(s"Setting session's oldest offset matching transaction at index $txIndex") *>
          ZIO.serviceWith[Session](prep => prep.copy(statements = prep.statements.appended(stmt)))
      }

    /** Sets the session's latest offset (upper history bound) to the supplied value.
      * @param offset
      *   the offset to use (use `null` to clear previously set value)
      * @return
      *   the updated session
      */
    def setLatest(offset: String): RLayer[Session, Session] =
      val stmt = sql"select set_latest($offset::bigint);"
      ZLayer.scoped {
        ZIO.logDebug(s"Setting session's latest offset to $offset") *>
          ZIO.serviceWith[Session](prep => prep.copy(statements = prep.statements.appended(stmt)))
      }

    def setLatest(offset: Long): RLayer[Session, Session] =
      setLatest(offset.toString)

    /** Sets the session's latest offset (upper history bound) to the value of the transaction with the supplied index.
      * @param txIndex
      *   the index of the transaction to use
      * @return
      *   the updated session
      */
    def setLatest(txIndex: Int): RLayer[Session, Session] =
      val stmt = sql"""select set_latest((select "offset" from __transactions where ix = $txIndex));"""
      ZLayer.scoped {
        ZIO.logDebug(s"Setting session's latest offset matching transaction at index $txIndex") *>
          ZIO.serviceWith[Session](prep => prep.copy(statements = prep.statements.appended(stmt)))
      }

    /** Runs the supplied SQL statement in a session "pre-processed" by a series of additional statements (whose return
      * values are discarded).
      * @param sql
      *   the SQL to execute
      * @return
      *   the results Table
      */
    def query(sql: SqlFragment): ZIO[Database & Session, Throwable, Table] =
      queryVerbose(sql).map(_.lastOption.getOrElse(Table.empty))

    /** Runs the supplied SQL statement in a session "pre-processed" by a series of additional statements (all
      * statements' results are captured as a collection of tables).
      * @param sql
      *   the SQL to execute
      * @return
      *   the results Table
      */
    def queryVerbose(
        sql: SqlFragment
    ): ZIO[Database & Session, Throwable, List[Table]] =
      Database
        .transaction {
          ZIO.serviceWithZIO[Session](x => ZIO.foreach(x.statements.appended(sql))(_.query[Row].selectAll))
        }
        .map(_.map(Table.build))

  private def selectContracts(Table: SqlFragment, extraColumns: Seq[String]) =
    val select = SqlFragment.select((Seq("package_id", "template_fqn", "payload_type", "contract_id") ++ extraColumns)*)
    query(sql"$select from $Table order by activated_at_ix, template_fqn, payload_type desc")
