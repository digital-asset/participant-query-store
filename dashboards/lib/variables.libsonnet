// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

local g = import '../lib-shared/g.libsonnet';
local v = import '../lib-shared/variables.libsonnet';

local var = g.dashboard.variable;
local q = var.query;

{
  pg_relname:
    q.new('pg_relname')
    + q.generalOptions.withLabel('Table')
    + q.generalOptions.withDescription('Postgres table name')
    + q.withDatasourceFromVariable(v.datasource)
    + q.refresh.onTime()
    + q.queryTypes.withLabelValues(
      'relname',
      'pg_custom_table_io_stats_seq_scan_total',
    ),

  k6_persona:
    q.new('k6_persona')
    + q.generalOptions.withLabel('Persona')
    + q.generalOptions.withDescription('K6 personas')
    + q.withDatasourceFromVariable(v.datasource)
    + q.refresh.onTime()
    + q.queryTypes.withLabelValues(
      'persona',
      'k6_group_duration_seconds',
    ),

  k6_group:
    q.new('k6_group')
    + q.generalOptions.withLabel('Group')
    + q.generalOptions.withDescription('K6 groups')
    + q.withDatasourceFromVariable(v.datasource)
    + q.refresh.onTime()
    + q.queryTypes.withLabelValues(
      'group',
      'k6_group_duration_seconds{persona="$k6_persona"}',
    ),
}
