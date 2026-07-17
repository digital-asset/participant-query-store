// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

import com.digitalasset.pqs.docker.{Docker, Service}
import com.digitalasset.pqs.functest.FTEnv
import com.digitalasset.pqs.functest.table.{Cell, Row, Table}
import com.digitalasset.pqs.specific.offsetSqlFragment
import org.postgresql.PGProperty
import zio.*
import zio.ZIO.{acquireRelease, attemptBlocking, logDebug}
import zio.jdbc.*
import zio.jdbc.SqlFragment.Segment.Syntax
import zio.test.*

import scala.collection.mutable

/** Docker Postgres Service */
sealed class Postgres(
    val service: Service[Postgres],
    val adminDatabase: Database
):
  export service.*

object Postgres:
  val port = 5432

  // Snapshot of the Docker image checksums
  private val imageChecksums = Map(
    "14" -> "sha256:9e279cc7fc6e908da071befe389c576bd6752dbd295a9c078f96a75bab03e54c",
    "15" -> "sha256:bcab099bfaab33333a73a2ebe8c1d615c9f4c2402dd43452f989a36c6da9a5ba",
    "16" -> "sha256:be01cf82fc7dbba824acf0a82e150b4b360f3ff93c6631d7844af431e841a95c",
    "17" -> "sha256:0af65001d05296a2ead57ac4a6412433d8913d1bb5d0c88435a7d1e1ee5cb04b",
    "18" -> "sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4"
  )

  ////////////
  // Layers //
  ////////////

  val database: ZLayer[Docker & Postgres, Throwable, Database] = ZLayer
    .scoped(
      for
        postgres  <- ZIO.service[Postgres]
        dbCounter <- Docker.share("db_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
        dbName = s"ft_db_$dbCounter"
        _ <- postgres.adminDatabase.transaction(sql"""create database "${Syntax(dbName)}"""".execute)
        _ <- logDebug(s"Using database [$dbName]")
      yield initDatabase(dbName, postgres.service)
    )
    .flatten

  /** Docker Instances Service wrapped into Layer */
  val instance: ZLayer[FTEnv & Docker, Throwable, Postgres] = ZLayer
    .fromZIO(
      for
        cnt <- Docker.share("pg_cnt")(Ref.Synchronized.make(0)).flatMap(_.updateAndGet(_ + 1))
        hostname = s"postgres$cnt"
        ca      <- Docker.certificateAuthority
        cert    <- ca.generate(hostname, Seq(hostname, "localhost"))
        version <- ZIO.service[FTEnv].map(_.config.postgresVersion)
        checksum <- ZIO
          .fromOption(imageChecksums.get(version))
          .orElseFail(RuntimeException(s"missing checksum for postgres:$version"))
      yield Docker
        .service[Postgres](
          image = s"postgres:$version@$checksum",
          exposePorts = Set(port),
          env = Map("POSTGRES_PASSWORD" -> "postgres"),
          prepopulateFiles = Seq(
            os.root / "tls" / "root-ca.crt"  -> ca.certificate.crt,
            os.root / "tls" / "postgres.pem" -> cert.certificate.pem,
            os.root / "tls" / "postgres.crt" -> cert.certificate.crt,
            os.root / "tls" / "pg_hba.conf" ->
              """# TYPE  DATABASE        USER            ADDRESS                 METHOD
                |local   all             all                                     trust
                |hostssl postgres        postgres        all                     md5
                |hostssl all             all             all                     md5 clientcert=verify-ca
                |""".stripMargin
          ),
          hostname = Some(hostname),
          user = Some(999),
          suppressOutput = true
        )(
          "-c",
          "ssl=on",
          "-c",
          "ssl_ca_file=/tls/root-ca.crt",
          "-c",
          "ssl_cert_file=/tls/postgres.crt",
          "-c",
          "ssl_key_file=/tls/postgres.pem",
          "-c",
          "hba_file=/tls/pg_hba.conf",
          "-c",
          "max_connections=200",
          // TODO (#883) Comment and fix leaking connections
          "-c",
          "tcp_keepalives_idle=10",
          "-c",
          "tcp_keepalives_interval=5",
          "-c",
          "tcp_keepalives_count=2"
        )
        .tap(_.get.blockUntilStdErr(_.contains("database system is ready to accept connections")))
    )
    .flatten
    .project(svc => initDatabase("postgres", svc).project(Postgres(svc, _)))
    .flatten

  //////////
  // APIs //
  //////////

  def query(sql: SqlFragment): ZIO[Database, Throwable, Table] =
    transact(sql.query[Row].selectAll).map(Table.apply)

  def query[A](sql: ZIO[ZConnection, Throwable, A]): ZIO[Database, Throwable, A] =
    transact(sql)

  def tables: ZIO[Database, Throwable, Chunk[(String, String)]] =
    transact(
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

  def columnsIn(tableName: String): ZIO[Database, Throwable, Table] =
    query(
      sql"""select column_name, is_nullable, data_type
          from information_schema.columns
          where table_name like $tableName
          order by ordinal_position"""
    )

  def columnsStorageIn(tableName: String): ZIO[Database, Throwable, Table] =
    query(
      sql"""select att.attname as column,
                case att.attstorage
                    when 'p' then 'plain'
                    when 'm' then 'main'
                    when 'e' then 'external'
                    when 'x' then 'extended'
                    end as storage
          from pg_attribute att
          join pg_class tbl on tbl.oid = att.attrelid
          join pg_namespace ns on ns.oid = tbl.relnamespace
          where tbl.relname like $tableName
            and not att.attisdropped
            and attname not in ('tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid')
          order by attnum"""
    )

  /** Simulates a gap in the `__transactions` table by deleting the ones with the supplied indexes and adjusting the
    * `__watermark` table appropriately. This is useful for simulation of concurrently writing transactions out of
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
      sql"delete from __contracts where created_at_ix in (${txIndices.toList})".delete *>
        sql"update __contracts set archived_at_ix = null, archive_event_pk = null where archived_at_ix in (${txIndices.toList})".update *>
        sql"delete from __exercises where exercised_at_ix in (${txIndices.toList})".delete *>
        sql"delete from __events where tx_ix in (${txIndices.toList})".delete *>
        sql"delete from __transactions where ix in (${txIndices.toList})".delete
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

  /////////////////
  // Session API //
  ////////////////

  /** Session's "pre-processing" statements that are run before query execution */
  case class Session(statements: List[SqlFragment])
  object Session:

    /** Initialises layer that provides session's steps "storage". */
    def init: ULayer[Session] = ZLayer.scoped {
      logDebug(s"Initialising session") *> ZIO.succeed(Session(List.empty))
    }

    /** Sets the session's oldest offset (lower history bound) to the supplied value.
      * @param offset
      *   the offset to use (use `null` to clear previously set value)
      * @return
      *   the updated session
      */
    def setOldest(offset: String): RLayer[Session, Session] =
      val stmt = sql"""select set_oldest(""" ++ offsetSqlFragment(offset) ++ sql""");"""
      ZLayer.scoped {
        logDebug(s"Setting session's oldest offset to $offset") *>
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
        logDebug(s"Setting session's oldest offset matching transaction at index $txIndex") *>
          ZIO.serviceWith[Session](prep => prep.copy(statements = prep.statements.appended(stmt)))
      }

    /** Sets the session's latest offset (upper history bound) to the supplied value.
      * @param offset
      *   the offset to use (use `null` to clear previously set value)
      * @return
      *   the updated session
      */
    def setLatest(offset: String): RLayer[Session, Session] =
      val stmt = sql"""select set_latest(""" ++ offsetSqlFragment(offset) ++ sql""");"""
      ZLayer.scoped {
        logDebug(s"Setting session's latest offset to $offset") *>
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
        logDebug(s"Setting session's latest offset matching transaction at index $txIndex") *>
          ZIO.serviceWith[Session](prep => prep.copy(statements = prep.statements.appended(stmt)))
      }

    /** Runs the supplied SQL statement in a session "pre-processed" by a series of additional statements (whose return
      * values are discarded).
      * @param sql
      *   the SQL to execute
      * @return
      *   the results table
      */
    def query(sql: SqlFragment): ZIO[Database & Session, Throwable, Table] =
      queryVerbose(sql).map(_.lastOption.getOrElse(Table.empty))

    /** Runs the supplied SQL statement in a session "pre-processed" by a series of additional statements (all
      * statements' results are captured as a collection of tables).
      * @param sql
      *   the SQL to execute
      * @return
      *   the results table
      */
    def queryVerbose(
        sql: SqlFragment
    ): ZIO[Database & Session, Throwable, List[Table]] =
      transact {
        ZIO.serviceWithZIO[Session](x => ZIO.foreach(x.statements.appended(sql))(_.query[Row].selectAll))
      }.map(_.map(Table.apply))

  ///////////////
  // Internals //
  ///////////////

  private val connectionPoolConfig = ZConnectionPoolConfig(1, 4, Schedule.stop, 300.seconds)
  private def initDatabase(dbName: String, svc: Service[?]) = ZLayer
    .scoped(
      for
        ca   <- Docker.certificateAuthority
        cert <- ca.generate("postgresclient")
        rootCert <- acquireRelease(attemptBlocking(os.temp(ca.certificate.crt)))(f =>
          attemptBlocking(os.remove(f)).ignore
        )
        sslPem <- acquireRelease(attemptBlocking(os.temp(cert.certificate.der)))(f =>
          attemptBlocking(os.remove(f)).ignore
        )
        sslCrt <- acquireRelease(attemptBlocking(os.temp(cert.certificate.crt)))(f =>
          attemptBlocking(os.remove(f)).ignore
        )
        _ <- logDebug(s"Created PG connection pool @${svc.exposedAddress}:${svc.exposedPorts(port)}")
      yield ZLayer.succeed(connectionPoolConfig) >>>
        ZConnectionPool
          .postgres(
            svc.exposedAddress,
            svc.exposedPorts(port),
            dbName,
            Map(
              PGProperty.USER.getName           -> "postgres",
              PGProperty.PASSWORD.getName       -> "postgres",
              PGProperty.CURRENT_SCHEMA.getName -> "public",
              PGProperty.SSL_ROOT_CERT.getName  -> rootCert.toString,
              PGProperty.SSL_KEY.getName        -> sslPem.toString,
              PGProperty.SSL_CERT.getName       -> sslCrt.toString
            )
          )
          .project(Database(dbName, _))
    )
    .flatten
  private val transact = ZLayer.fromFunction((d: Database) => d.transaction).flatten

  private implicit val rowDecoder: JdbcDecoder[Row] = (ix, rs) =>
    val cells = mutable.Buffer.empty[Cell]
    for ix <- 1 to rs.getMetaData.getColumnCount do {
      val value = rs.getMetaData.getColumnType(ix) match
        case java.sql.Types.BOOLEAN => rs.getBoolean(ix)
        case java.sql.Types.BIT     => rs.getBoolean(ix)
        case java.sql.Types.VARCHAR => rs.getString(ix)
        case java.sql.Types.INTEGER => rs.getInt(ix)
        case java.sql.Types.BIGINT  => rs.getLong(ix)
        case other                  => rs.getString(ix)
      cells.append(Cell(value))
    }
    (rs.getMetaData.getColumnCount, Row(cells.toSeq))

end Postgres
