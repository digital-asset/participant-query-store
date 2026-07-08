// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.services.postgres

import zio.jdbc.*

final class Database(val name: String, val connectionPool: ZConnectionPool):
  export connectionPool.transaction

object Database:
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

  def __contracts() = Postgres `query`
    sql"""select package_pk, tpe_pk, contract_id, life_ix, payload ->> 'label' from __contracts order by created_at_ix, tpe_pk"""

  def __exercises() = Postgres `query`
    sql"""select package_pk, tpe_pk, contract_tpe_pk, contract_id, argument ->> 'newLabel' from __exercises order by exercised_at_ix, tpe_pk"""

  def active(qname: Option[String] = None, additionalColumns: Seq[String] = Seq.empty) = Postgres `query` {
    val select =
      SqlFragment.select((Seq("package_id", "template_fqn", "payload_type", "contract_id") ++ additionalColumns)*)
    sql"""$select from active($qname) order by created_at_ix, template_fqn, payload_type desc"""
  }

  def archives(qname: Option[String] = None) = Postgres `query`
    sql"""select package_id, template_fqn, payload_type, contract_id from archives($qname) order by created_at_ix, template_fqn, payload_type desc"""

  def creates(qname: Option[String] = None) = Postgres `query`
    sql"""select package_id, template_fqn, payload_type, contract_id from creates($qname) order by created_at_ix, template_fqn, payload_type desc"""

  def exercises(qname: Option[String] = None) = Postgres `query`
    sql"""select package_id, template_fqn, choice_fqn, choice, contract_id, argument ->> 'newLabel' from exercises($qname) order by exercised_at_ix, template_fqn, choice_fqn"""

  def transactionCount() =
    Postgres.query(sql"select count(*) from __transactions".query[Long].selectOne).someOrElse(0L)
