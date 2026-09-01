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
local variables = import '../lib/variables.libsonnet';

local db = g.dashboard;
local row = g.panel.row;
local grid = g.util.grid;
local pts = p.timeSeries;
local phm = p.heatmap;
local ptb = p.table;
local qs = queries.pqs;
local qp = queries.postgres;
local qc = queries.container;
local qr = queries.readapi;

db.new('PQS full-stack lens')
+ db.withUid('digital-asset-pqs-full-stack-lens')
+ db.withDescription('Dashboard for monitoring metrics across entire stack')
+ d.settings
+ db.time.withFrom('now-5m')

+ db.withVariables([
  v.datasource,
  v.namespace('jvm_memory_used_bytes'),
  v.pod('jvm_memory_used_bytes'),
  v.container('jvm_memory_used_bytes'),
  v.jvm,
  v.jvm_mempool,
  variables.pg_relname,
  variables.k6_persona,
  variables.k6_group,
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

  row.new('Test Execution')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    pts.throughput('PQS throughput', qs.watermark.throughput, width=12),
    p.gauge('Current throughput', qs.watermark.throughput),
    p.stat('Ingested so far', qs.watermark.index),
  ], panelWidth=6)),

  row.new('PQS Pipeline')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    panels.pqs.timeSeries.contractsChurn('Contract Churn', qs.contracts.churn.all, width=24),
    panels.pqs.timeSeries.activeContracts('Active Contracts *', qs.contracts.active, width=24),
    pts.latency('Transaction lag', [qs.tx_lag, qp.tx_lag]),
    pts.short('Watermark behind highest transaction index', [qp.watermark_lag]),
    pts.throughput('Transactions and Events Throughput', qs.pipeline.throughput.all_entities),
    pts.throughputStacked('Events Breakdown', qs.pipeline.throughput.all_events),
    pts.throughput('Transaction Waitpoints: Throughput', qs.pipeline.waitpoints.throughput.stream.all),
    pts.throughput('ACS Waitpoints: Throughput', qs.pipeline.waitpoints.throughput.acs.all),
    panels.pqs.heatmap.queueSize('ACS: Events Queue', qs.pipeline.waitpoints.queue_size.acs.events),
    panels.pqs.heatmap.queueSize('ACS: Statements Queue', qs.pipeline.waitpoints.queue_size.acs.statements),
    panels.pqs.heatmap.queueSize('ACS: Batched Statements Queue', qs.pipeline.waitpoints.queue_size.acs.batches),
    panels.pqs.heatmap.queueSize('ACS: Prepared Statements Queue', qs.pipeline.waitpoints.queue_size.acs.prepared_statements),
    panels.pqs.heatmap.queueSize('Events Queue', qs.pipeline.waitpoints.queue_size.stream.events),
    panels.pqs.heatmap.queueSize('Statements Queue', qs.pipeline.waitpoints.queue_size.stream.statements),
    panels.pqs.heatmap.queueSize('Batched Statements Queue', qs.pipeline.waitpoints.queue_size.stream.batches),
    panels.pqs.heatmap.queueSize('Prepared Statements Queue', qs.pipeline.waitpoints.queue_size.stream.prepared_statements),
    panels.pqs.heatmap.queueSize('Watermarks Queue', qs.pipeline.waitpoints.queue_size.stream.watermarks, width=24),
    pts.percentiles(
      'ACS: Convert Event Latency',
      qs.pipeline.stages.convert_acs_event.latency.all_quantiles
      + [qs.pipeline.stages.convert_acs_event.throughput]
    ),
    phm.latency('ACS: Convert Event Latency', qs.pipeline.stages.convert_acs_event.latency.heatmap),
    pts.percentiles(
      'Convert Transaction Latency',
      qs.pipeline.stages.convert_transaction.latency.all_quantiles
      + [qs.pipeline.stages.convert_transaction.throughput]
    ),
    phm.latency('Convert Transaction Latency', qs.pipeline.stages.convert_transaction.latency.heatmap),
    pts.percentiles(
      'Prepare Batch Latency',
      qs.pipeline.stages.prepare_batch.latency.all_quantiles
      + [qs.pipeline.stages.prepare_batch.throughput]
    ),
    phm.latency('Prepare Batch Latency', qs.pipeline.stages.prepare_batch.latency.heatmap),
    pts.percentiles(
      'Execute Batch Latency',
      qs.pipeline.stages.execute_batch.latency.all_quantiles
      + [qs.pipeline.stages.execute_batch.throughput]
    ),
    phm.latency('Execute Batch Latency', qs.pipeline.stages.execute_batch.latency.heatmap),
    pts.percentiles(
      'Progress Watermark Latency',
      qs.pipeline.stages.progress_watermark.latency.all_quantiles
      + [qs.pipeline.stages.progress_watermark.throughput]
    ),
    phm.latency('Progress Watermark Latency', qs.pipeline.stages.progress_watermark.latency.heatmap),
    pts.percentiles(
      'JDBC Connection Latency',
      qs.pipeline.stages.connection_use.latency.all_quantiles
      + [qs.pipeline.stages.connection_use.throughput]
    ),
    phm.latency('JDBC Connection Latency', qs.pipeline.stages.connection_use.latency.heatmap),
    pts.percentiles(
      'JDBC Connection Latency (validity check)',
      qs.pipeline.stages.connection_isvalid.latency.all_quantiles
      + [qs.pipeline.stages.connection_isvalid.throughput]
    ),
    phm.latency('JDBC Connection Latency (validity check)', qs.pipeline.stages.connection_isvalid.latency.heatmap),
    pts.percentiles(
      'JDBC Connection Latency (commit)',
      qs.pipeline.stages.connection_commit.latency.all_quantiles
      + [qs.pipeline.stages.connection_commit.throughput]
    ),
    phm.latency('JDBC Connection Latency (commit)', qs.pipeline.stages.connection_commit.latency.heatmap),
    pts.percentiles(
      'Total Transaction Handling Latency',
      qs.pipeline.stages.total_tx_handling.latency.all_quantiles
      + [qs.pipeline.stages.total_tx_handling.throughput]
    ),
    phm.latency('Total Transaction Handling Latency', qs.pipeline.stages.total_tx_handling.latency.heatmap),
  ], panelWidth=12)),

  row.new('Resources')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    pts.cpuStacked('CPU Usage per Container', qc.cpu.perContainer),
    pts.cpu('CPU Usage per Container', qc.cpu.perContainer),
  ], panelWidth=24)),

  row.new('Other Resources')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    pts.bytes('Memory Usage', qc.memory.usage),
    pts.bytes('Memory Residential', qc.memory.rss),
    pts.bytes('Swap', qc.memory.swap),
    pts.cpuThrottling('CPU Throttling', qc.cpu.throttling),
    pts.io('Disk IOps', qc.io.diskIops, unit='iops'),
    pts.io('Disk IO', qc.io.disk),
    pts.io('Network', qc.io.network),
  ], panelWidth=12)),

  row.new('PQS Read API')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    pts.throughput('Scenarios completions', qr.scenarios.completions),
    pts.short('Concurrent scenarios (and limits)', [qr.vus, qr.vus_max, qr.scenarios.concurrent]),
    pts.percentiles('[$k6_persona] Scenario iteration latency', qr.scenarios.iteration.latency.all_quantiles),
    pts.percentiles('[$k6_persona$k6_group] SQL latency', qr.scenarios.group.latency.all_quantiles),
    pts.short('[$k6_persona] Scenario results retrieval', qr.scenarios.results),
    pts.throughput('Scenario SQL rates', [qr.scenarios.all_throughput, qr.scenarios.persona_throughput]),
    panels.k6.timeSeries.botSleepReasons('[eve] Bot sleep reasons', [qr.scenarios.bot.no_new_transactions, qr.scenarios.bot.no_new_monitored_contracts]),
    pts.percentiles('[eve] Batch processing latency', qr.scenarios.bot.batch_processing('eve').latency.all_quantiles),
    pts.short('Non-OK checks rate', qr.scenarios.nok),
  ], panelWidth=12)),

  row.new('Postgres [$jvm]')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    panels.postgres.timeSeries.checkpointsTime('Checkpoint Stats', [
      qp.bgwriter.checkpoints.write_time,
      qp.bgwriter.checkpoints.sync_time,
    ], description='Total amount of time that has been spent in the portion of checkpoint processing where files are written/sync-ed to disk.', width=24),
    panels.postgres.timeSeries.checkpointsCount('Checkpoint Stats', [
      qp.bgwriter.checkpoints.requested,
      qp.bgwriter.checkpoints.scheduled,
    ], description='Number of requested/scheduled checkpoints that have been performed'),
    panels.postgres.timeSeries.buffers('Buffers (bgwriter)', qp.bgwriter.buffers.all),
    panels.postgres.timeSeries.stat('seq_scan', qp.seq_scan, description='Number of sequential scans initiated on this table'),
    panels.postgres.timeSeries.stat('seq_tup_read', qp.seq_tup_read, description='Number of live rows fetched by sequential scans'),
    panels.postgres.timeSeries.stat('idx_scan', qp.idx_scan, description='Number of index scans initiated on this table'),
    panels.postgres.timeSeries.stat('idx_tup_fetch', qp.idx_tup_fetch, description='Number of live rows fetched by index scans'),
    panels.postgres.timeSeries.stat('heap_blks_read', qp.heap_blks_read, description='Number of disk blocks read from this table'),
    panels.postgres.timeSeries.stat('heap_blks_hit', qp.heap_blks_hit, description='Number of buffer hits in this table'),
    panels.postgres.timeSeries.stat('idx_blks_read', qp.idx_blks_read, description='Number of disk blocks read from all indexes on this table'),
    panels.postgres.timeSeries.stat('idx_blks_hit', qp.idx_blks_hit, description='Number of buffer hits in all indexes on this table'),
    panels.postgres.timeSeries.ratio('heap_blks_ratio', qp.heap_blks_ratio, description='Ratio of buffer hits vs all reads (buffer + disk)'),
    panels.postgres.timeSeries.ratio('idx_blks_ratio', qp.idx_blks_ratio, description='Ratio of buffer hits vs all reads (buffer + disk) [indexes]'),
    panels.postgres.timeSeries.condensed('pg_custom_table_io_stats - $pg_relname', qp.all),
    panels.postgres.timeSeries.candidates('pg_custom_table_index_candidates', qp.index_candidates),
    panels.postgres.timeSeries.tuples('Dead / Live Tuples', [qp.tuples.dead, qp.tuples.live], description='Reasonably accurate estimates'),
    panels.postgres.timeSeries.deadTuplesRatio('Dead Tuples %', qp.tuples.dead_ratio, description='Reasonably accurate estimates'),
  ], panelWidth=12)),

  row.new('Postgres [$jvm] - custom metrics')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    pts.latency('Scraping latency', qp.scrape.latency),
    panels.postgres.timeSeries.scrapeStatus('Scraping HTTP status code', qp.scrape.status),
    panels.postgres.table.topQueries('Queries leaderboard', qp.top_queries.leaderboard, width=24),
    panels.postgres.table.topQueries('Queries execution time per query', qp.top_queries.executionTime, width=24),
  ], panelWidth=12)),

  row.new('GRPC Replay')
  + row.withCollapsed(true)
  + row.withPanels(grid.makeGrid([
    pts.short('Queue size', queries.grpcproxy.queue_size),
    pts.throughput('Queue ops rate', [queries.grpcproxy.enqueue, queries.grpcproxy.dequeue])
    + g.panel.timeSeries.standardOptions.withMin(null),
  ], panelWidth=12)),

  row.new('PQS - tech')
  + row.withCollapsed(true)
  + row.withPanels(grid.wrapPanels([
    pts.short('CPUs utilisation', [
      q.jvm.cpus.available,
      q.jvm.cpus.used,
      q.jvm.cpus.system_load_1m,
    ], width=24),
    pts.memoryUsage('Heap memory', q.jvm.memory.heap.all),
    pts.garbageCollection('Garbage collection', q.jvm.gc().all),
    pts.shortStacked('Threads', q.jvm.threads),
    pts.memoryPoolsUsage('Memory pool - $jvm_mempool', q.jvm.memory.pools.all),
  ], panelWidth=8)),

])
