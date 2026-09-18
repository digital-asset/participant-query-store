// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

import com.digitalasset.pqs.postgres.backend.{InstanceId, PostgresConfig}
import zio.*
import zio.jdbc.*

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
  val config         = ZLayer.fromFunction((d: Database) => d.config)
  val connectionPool = ZLayer.fromFunction((d: Database) => d.connectionPool)
  val instanceId     = ZLayer.fromFunction((d: Database) => d.instanceId)
  val transaction    = connectionPool.project(_.transaction).flatten

  def autoCommit[R: Tag, A](query: => ZIO[ZConnection & R, Throwable, A]) =
    ZIO.service[Database].flatMap(_.autoCommit(query))

  private given Conversion[Option[String], SqlFragment] = _.map(x => sql"$x").getOrElse(sql"")

  def __packages(orderBy: SqlFragment = sql"""order by version, name""") = Postgres `query`
    sql"""select pk, name, version, id
          from __packages
          where name <> 'AdminWorkflows'
          $orderBy"""

  def __contract_tpe() = Postgres `query`
    sql"""select pk, template_fqn, payload_type, aliases
          from __contract_tpe
          where template_fqn not like 'AdminWorkflows:%'
          order by pk"""

  def __exercise_tpe() = Postgres `query`
    sql"""select pk, template_fqn, choice_fqn, choice, consuming, aliases
          from __exercise_tpe
          where template_fqn not like 'AdminWorkflows:%'
          order by pk"""

  def __contracts(extraColumns: Seq[String] = Seq.empty) =
    val select = SqlFragment.select((Seq("package_pk", "tpe_pk", "contract_id", "life_ix") ++ extraColumns)*)
    Postgres.query(sql"$select from __contracts order by created_at_ix")

  def __exercises() = Postgres `query`
    sql"""select package_pk, tpe_pk, contract_tpe_pk, contract_id, argument ->> 'newLabel' from __exercises order by exercised_at_ix, tpe_pk"""

  def activeAtOffset(offset: Long, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"active(null, $offset)", extraColumns)

  def active(qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"active($qname)", extraColumns)

  def archives(qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"archives($qname)", extraColumns)

  def creates(qname: Option[String] = None, extraColumns: Seq[String] = Seq.empty) =
    selectContracts(sql"creates($qname)", extraColumns)

  def exercises(qname: Option[String] = None) = Postgres `query`
    sql"""select package_id, template_fqn, choice_fqn, choice, contract_id, argument ->> 'newLabel' from exercises($qname) order by exercised_at_ix, template_fqn, choice_fqn"""

  def transactionCount() =
    Postgres.query(sql"select count(*) from __transactions".query[Long].selectOne).someOrElse(0L)

  private def selectContracts(table: SqlFragment, extraColumns: Seq[String]) =
    val select = SqlFragment.select((Seq("package_id", "template_fqn", "payload_type", "contract_id") ++ extraColumns)*)
    Postgres.query(sql"$select from $table order by created_at_ix, template_fqn, payload_type desc")
