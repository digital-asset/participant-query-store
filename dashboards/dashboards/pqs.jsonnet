// Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

// Company-wide baseline
local d = import '../lib-shared/dashboard.libsonnet';
local g = import '../lib-shared/g.libsonnet';
local p = import '../lib-shared/panels.libsonnet';
local q = import '../lib-shared/queries.libsonnet';
local v = import '../lib-shared/variables.libsonnet';

// Project-local overrides
local annotations = import '../lib/annotations.libsonnet';
local panels = import '../lib/panels.libsonnet';
local queries = import '../lib/queries.libsonnet';

local db = g.dashboard;
local row = g.panel.row;
local grid = g.util.grid;
local ptsOpts = g.panel.timeSeries.panelOptions;
local phmOpts = g.panel.heatmap.panelOptions;
local pts = p.timeSeries;
local phm = p.heatmap;
local ptb = p.table;
local qs = queries.pqs;

db.new('Participant Query Store (PQS)')
+ db.withUid('digital-asset-pqs')
+ db.withDescription('Dashboard for monitoring PQS application')
+ db.withTags(['pqs', 'query store'])
+ d.settings

+ db.withVariables([
  v.datasource,
  v.namespace('jvm_memory_used_bytes'),
  v.pod('jvm_memory_used_bytes'),
  v.container('jvm_memory_used_bytes'),
  v.jvm,
  v.jvm_mempool,
])

+ db.withAnnotations([
  annotations.grpc.down(qs.down.grpc),
  annotations.jdbc.down(qs.down.jdbc, 'dark-red'),
])

+ db.withPanels([
  row.new('Environment')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    ptb.settings('Settings', qs.info),
    ptb.launchArgs('Launch arguments', qs.info),
  ], panelWidth=12)),

  row.new('Contracts')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    panels.pqs.timeSeries.contractsChurn('Churn', qs.contracts.churn.all),
    panels.pqs.timeSeries.activeContracts('Active *', qs.contracts.active),
  ], panelWidth=24)),

  row.new('Throughput')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    p.gauge('Current watermark throughput', qs.watermark.throughput, width=6, height=4),
    p.gauge('Current events throughput', qs.pipeline.throughput.events, width=6, height=4),
    p.stat('Ledger transactions ingested *', qs.watermark.index, width=6, height=4)
    + ptsOpts.withDescription('\\* as observed since last restart'),
    p.stat('Ledger events ingested *', qs.pipeline.count.all_events, width=6, height=4)
    + ptsOpts.withDescription('\\* as observed since last restart'),
    pts.latency('Transaction lag', qs.tx_lag)
    + ptsOpts.withDescription('Delta between command completion as determined by transaction’s effective_at attribute and ingestion by PQS pipeline as determined by wall clock'),
    pts.throughput('Watermark history', qs.watermark.throughput)
    + ptsOpts.withDescription('Rate of ledger transactions becoming available for querying by Read API functions'),
    pts.throughput('Transactions and events', qs.pipeline.throughput.all_entities)
    + ptsOpts.withDescription('Shape of ingested traffic in terms of transactions and events dimensions'),
    pts.throughputStacked('Events breakdown', qs.pipeline.throughput.all_events)
    + ptsOpts.withDescription('Breakdown of event types inside transactions'),
    pts.throughput('Waitpoints - ACS *', qs.pipeline.waitpoints.throughput.acs.all)
    + ptsOpts.withDescription('Throughput of items passing via internal pipeline stages<br><br>* only during seeding from the ActiveContractSet Ledger API service'),
    pts.throughput('Waitpoints - streaming', qs.pipeline.waitpoints.throughput.stream.all)
    + ptsOpts.withDescription('Throughput of items passing via internal pipeline stages'),
  ], panelWidth=12)),

  row.new('Queues sizes - ACS')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    panels.pqs.heatmap.queueSize('Events', qs.pipeline.waitpoints.queue_size.acs.events)
    + phmOpts.withDescription('\\* only during seeding from the ActiveContractSet Ledger API service'),
    panels.pqs.heatmap.queueSize('Statements', qs.pipeline.waitpoints.queue_size.acs.statements)
    + phmOpts.withDescription('\\* only during seeding from the ActiveContractSet Ledger API service'),
    panels.pqs.heatmap.queueSize('Batched statements', qs.pipeline.waitpoints.queue_size.acs.batches)
    + phmOpts.withDescription('\\* only during seeding from the ActiveContractSet Ledger API service'),
    panels.pqs.heatmap.queueSize('Prepared statements', qs.pipeline.waitpoints.queue_size.acs.prepared_statements)
    + phmOpts.withDescription('\\* only during seeding from the ActiveContractSet Ledger API service'),
  ], panelWidth=12)),

  row.new('Queues sizes - streaming')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    panels.pqs.heatmap.queueSize('Events', qs.pipeline.waitpoints.queue_size.stream.events),
    panels.pqs.heatmap.queueSize('Statements', qs.pipeline.waitpoints.queue_size.stream.statements),
    panels.pqs.heatmap.queueSize('Batched statements', qs.pipeline.waitpoints.queue_size.stream.batches),
    panels.pqs.heatmap.queueSize('Prepared statements', qs.pipeline.waitpoints.queue_size.stream.prepared_statements),
    panels.pqs.heatmap.queueSize('Watermarks', qs.pipeline.waitpoints.queue_size.stream.watermarks, width=24),
  ], panelWidth=12)),

  row.new('Latency')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    pts.percentiles(
      'Convert event - ACS *',
      qs.pipeline.stages.convert_acs_event.latency.all_quantiles
      + [qs.pipeline.stages.convert_acs_event.throughput]
    )
    + ptsOpts.withDescription('\\* only during seeding from the ActiveContractSet Ledger API service'),
    phm.latency('Convert event - ACS *', qs.pipeline.stages.convert_acs_event.latency.heatmap)
    + phmOpts.withDescription('\\* only during seeding from the ActiveContractSet Ledger API service'),
    pts.percentiles(
      'Convert transaction - streaming',
      qs.pipeline.stages.convert_transaction.latency.all_quantiles
      + [qs.pipeline.stages.convert_transaction.throughput]
    ),
    phm.latency('Convert transaction - streaming', qs.pipeline.stages.convert_transaction.latency.heatmap),
    pts.percentiles(
      'Prepare batch',
      qs.pipeline.stages.prepare_batch.latency.all_quantiles
      + [qs.pipeline.stages.prepare_batch.throughput]
    ),
    phm.latency('Prepare batch', qs.pipeline.stages.prepare_batch.latency.heatmap),
    pts.percentiles(
      'Execute batch',
      qs.pipeline.stages.execute_batch.latency.all_quantiles
      + [qs.pipeline.stages.execute_batch.throughput]
    ),
    phm.latency('Execute batch', qs.pipeline.stages.execute_batch.latency.heatmap),
    pts.percentiles(
      'Progress watermark',
      qs.pipeline.stages.progress_watermark.latency.all_quantiles
      + [qs.pipeline.stages.progress_watermark.throughput]
    ),
    phm.latency('Progress watermark', qs.pipeline.stages.progress_watermark.latency.heatmap),
    pts.percentiles(
      'JDBC connection',
      qs.pipeline.stages.connection_use.latency.all_quantiles
      + [qs.pipeline.stages.connection_use.throughput]
    ),
    phm.latency('JDBC connection', qs.pipeline.stages.connection_use.latency.heatmap),
    pts.percentiles(
      'JDBC connection (validity check)',
      qs.pipeline.stages.connection_isvalid.latency.all_quantiles
      + [qs.pipeline.stages.connection_isvalid.throughput]
    ),
    phm.latency('JDBC connection (validity check)', qs.pipeline.stages.connection_isvalid.latency.heatmap),
    pts.percentiles(
      'JDBC connection (commit)',
      qs.pipeline.stages.connection_commit.latency.all_quantiles
      + [qs.pipeline.stages.connection_commit.throughput]
    ),
    phm.latency('JDBC connection (commit)', qs.pipeline.stages.connection_commit.latency.heatmap),
    pts.percentiles(
      'Total Transaction Handling Latency',
      qs.pipeline.stages.total_tx_handling.latency.all_quantiles
      + [qs.pipeline.stages.total_tx_handling.throughput]
    )
    + ptsOpts.withDescription('Time taken by entire pipeline between receipt from Ledger API to being committed to Postgres'),
    phm.latency('Total Transaction Handling Latency', qs.pipeline.stages.total_tx_handling.latency.heatmap)
    + phmOpts.withDescription('Time taken by entire pipeline between receipt from Ledger API to being committed to Postgres'),
  ], panelWidth=12)),

  row.new('JVM metrics')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    pts.short('CPUs utilisation', [
      q.jvm.cpus.available,
      q.jvm.cpus.used,
      q.jvm.cpus.system_load_1m,
      q.jvm.diagnostics.micrometer.cpus.available,
      q.jvm.diagnostics.micrometer.cpus.used,
      q.jvm.diagnostics.micrometer.cpus.system_load_1m,
    ], width=24),
    pts.memoryUsage('Heap memory', q.jvm.memory.heap.all + q.jvm.diagnostics.micrometer.memory.heap.all),
    pts.garbageCollection('Garbage collection', q.jvm.gc().all + q.jvm.diagnostics.micrometer.gc.all),
    pts.shortStacked('Threads', [q.jvm.threads, q.jvm.diagnostics.micrometer.threads]),
    pts.memoryPoolsUsage('Memory pool - $jvm_mempool', q.jvm.memory.pools.all + q.jvm.diagnostics.micrometer.memory.pools.all),
  ], panelWidth=8)),

])
