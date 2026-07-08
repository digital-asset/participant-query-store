// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

local g = import '../lib-shared/g.libsonnet';
local q = import '../lib-shared/queries.libsonnet';

{
  scribe: {
    info:
      q.simple('target_info{job="$jvm"}', '')
      + { format: 'table', instant: true, range: false, hide: false, editorMode: 'code', exemplar: false },

    tx_lag: q.simple('tx_lag_from_ledger_wallclock{job="$jvm"}', 'command->scribe'),

    down: {
      grpc: q.simple('(grpc_up{job="$jvm"} < 1) + 1', 'Ledger down'),
      jdbc: q.simple('(jdbc_conn_pool_up{job="$jvm"} < 1) + 1', 'Datastore down'),
    },

    watermark: {
      throughput: q.simple('rate(watermark_ix{job="$jvm"}[$__rate_interval])', 'watermark index'),
      index: q.simple('watermark_ix{job="$jvm"}'),
    },

    pipeline: {
      count: {
        all_events:
          q.simple('sum(pipeline_events_total{job="$jvm", type=~"(create|archive|exercise)"})', 'events'),
      },
      throughput: {
        sums(type, label, type_op='='):
          q.simple('sum(rate(pipeline_events_total{job="$jvm", type%s"%s"}[$__rate_interval]))' % [type_op, type], label),
        transactions:
          q.simple('rate(pipeline_events_total{job="$jvm", type="transaction"}[$__rate_interval])', 'transactions'),
        events: self.sums('(create|archive|exercise)', 'events', '=~'),
        creates: self.sums('create', 'creates'),
        archives: self.sums('archive', 'archives'),
        exercises: self.sums('exercise', 'exercises'),
        all_entities: [self.transactions, self.events],
        all_events: [self.creates, self.archives, self.exercises],
      },

      waitpoints: {
        throughput: {
          local this = self,
          base(wp_type, label):
            q.simple('rate(pipeline_wp_%s_total{job="$jvm"}[$__rate_interval])' % wp_type, label),
          stream: {
            events: this.base('events', 'ledger events'),
            statements: this.base('statements', 'statements'),
            batches: this.base('batched_statements', 'batches'),
            prepared_statements: this.base('prepared_statements', 'prepared statements'),
            watermarks: this.base('watermarks', 'watermarks'),
            all: [self.events, self.statements, self.batches, self.prepared_statements, self.watermarks],
          },
          acs: {
            events: this.base('acs_events', 'ledger events'),
            statements: this.base('acs_statements', 'statements'),
            batches: this.base('acs_batched_statements', 'batches'),
            prepared_statements: this.base('acs_prepared_statements', 'prepared statements'),
            all: [self.events, self.statements, self.batches, self.prepared_statements],
          },
        },
        queue_size: {
          local this = self,
          base(wp_type):
            q.simple('rate(pipeline_wp_%s_size_bucket{job="$jvm"}[$__rate_interval])' % wp_type, '{{label_name}}')
            + { format: 'heatmap' },
          stream: {
            events: this.base('events'),
            statements: this.base('statements'),
            batches: this.base('batched_statements'),
            prepared_statements: this.base('prepared_statements'),
            watermarks: this.base('watermarks'),
          },
          acs: {
            events: this.base('acs_events'),
            statements: this.base('acs_statements'),
            batches: this.base('acs_batched_statements'),
            prepared_statements: this.base('acs_prepared_statements'),
          },
        },
      },

      stages: {
        local this = self,
        quantile(source, pct, label):
          q.simple('histogram_quantile(%.2f, sum by(le) (rate(%s_bucket{job="$jvm"}[$__rate_interval])))' % [pct, source], label),
        heatmap(source):
          q.simple('rate(%s_bucket{job="$jvm"}[$__rate_interval])' % source, '{{label_name}}')
          + { format: 'heatmap' },
        wrap(source): {
          latency: {
            p50: this.quantile(source, 0.5, 'p50'),
            p90: this.quantile(source, 0.9, 'p90'),
            p95: this.quantile(source, 0.95, 'p95'),
            p99: this.quantile(source, 0.99, 'p99'),
            all_quantiles: [self.p50, self.p90, self.p95, self.p99],
            heatmap: this.heatmap(source),
          },
          throughput:
            q.simple('rate(%s_count[$__rate_interval])' % source, 'throughput'),
        },
        convert_transaction: self.wrap('pipeline_convert_transaction'),
        prepare_batch: self.wrap('pipeline_prepare_batch_latency'),
        execute_batch: self.wrap('pipeline_execute_batch_latency'),
        progress_watermark: self.wrap('pipeline_progress_watermark'),
        connection_use: self.wrap('jdbc_conn_use'),
        connection_isvalid: self.wrap('jdbc_conn_isvalid'),
        connection_commit: self.wrap('jdbc_conn_commit'),
        convert_acs_event: self.wrap('pipeline_convert_acs_event'),
        total_tx_handling: self.wrap('total_tx_handling_latency'),
      },
    },

    contracts: {
      churn: {
        base(type, mult=1):
          q.simple('label_replace(rate(pipeline_events_total{job="$jvm", type="%s"}[$__rate_interval]) * %d, "short_template", "$1", "template", ".*?:(.*)")' % [type, mult], '{{short_template}}'),
        creates: self.base('create'),
        archives: self.base('archive', -1),
        all: [self.creates, self.archives],
      },
      active:
        q.simple('label_replace(sum without(type) (pipeline_events_total{job="$jvm", type="create"}) - ((sum without(type) (pipeline_events_total{job="$jvm", type="archive"})) or (sum without(type) (pipeline_events_total{job="$jvm", type="create"}) * 0)), "short_template", "$1", "template", ".*?:(.*)")', '{{short_template}}'),
    },
  },

  container: {
    local this = self,
    base(source):
      q.simple('container_%s{name!=""}' % source, '{{name}}'),
    rate(source, negative=false, labels=[]):
      q.simple('%sirate(container_%s_total{%s}[$__rate_interval])' % [
        if negative then '-' else '',
        source,
        std.join(',', ['name!=""'] + labels),
      ], '{{name}}'),

    memory: {
      usage: this.base('memory_usage_bytes'),
      rss: this.base('memory_rss'),
      swap: this.base('memory_swap'),
    },

    cpu: {
      perContainer:
        q.simple('sum(rate(container_cpu_usage_seconds_total{name=~".+"}[$__rate_interval])) by (name) * 100', '{{name}}')
        + g.query.prometheus.withIntervalFactor(2),
      throttling: this.rate('cpu_cfs_throttled_seconds'),
    },

    io: {
      diskIops: [
        this.rate('fs_reads'),
        this.rate('fs_writes', negative=true),
      ],
      disk: [
        this.rate('fs_reads_bytes'),
        this.rate('fs_writes_bytes', negative=true),
      ],
      network: [
        this.rate('network_transmit_bytes', labels=['interface="eth0"']),
        this.rate('network_receive_bytes', negative=true, labels=['interface="eth0"']),
      ],
    },
  },

  postgres: {
    local this = self,
    base(query, source, labels=[], filterZeroes=true, legend):
      q.simple(query % [source, std.join(',', labels), if filterZeroes then '> 0' else ''], legend),
    stat(source, labels=[], filterZeroes=true, legend='{{relname}}'):
      self.base('pg_custom_table_io_stats_%s{%s} %s', source, labels, filterZeroes, legend),
    statRate(source, labels=[], filterZeroes=true, legend='{{relname}}'):
      self.base('irate(pg_custom_table_io_stats_%s_total{%s}[$__rate_interval]) %s', source, labels, filterZeroes, legend),
    candidate(source, labels=[], filterZeroes=true, legend='{{relname}}/%s' % source):
      self.base('pg_custom_table_index_candidates_%s{%s} %s', source, labels, filterZeroes, legend),
    candidateRate(source, labels=[], filterZeroes=true, legend='{{relname}}/%s' % source):
      self.base('irate(pg_custom_table_index_candidates_%s_total{%s}[$__rate_interval]) %s', source, labels, filterZeroes, legend),

    seq_scan: self.statRate('seq_scan'),
    seq_tup_read: self.statRate('seq_tup_read'),
    idx_scan: self.statRate('idx_scan'),
    idx_tup_fetch: self.statRate('idx_tup_fetch'),
    heap_blks_read: self.statRate('heap_blks_read'),
    heap_blks_hit: self.statRate('heap_blks_hit'),
    idx_blks_read: self.statRate('idx_blks_read'),
    idx_blks_hit: self.statRate('idx_blks_hit'),
    heap_blks_ratio: self.stat('heap_blks_ratio'),
    idx_blks_ratio: self.stat('idx_blks_ratio'),

    tx_lag: q.simple('pg_lag_tracker_delta_wallclock_time{job="postgres-scribe-exporter"}', 'command->watermark'),
    watermark_lag: q.simple('pg_lag_tracker_delta_tx_index{job="postgres-scribe-exporter"}', 'delta'),

    all: [
      self.statRate('seq_scan', ['relname=~"$pg_relname"'], false, 'seq_scan'),
      self.statRate('seq_tup_read', ['relname=~"$pg_relname"'], false, 'seq_tup_read'),
      self.statRate('idx_scan', ['relname=~"$pg_relname"'], false, 'idx_scan'),
      self.statRate('idx_tup_fetch', ['relname=~"$pg_relname"'], false, 'idx_tup_fetch'),
      self.statRate('heap_blks_read', ['relname=~"$pg_relname"'], false, 'heap_blks_read'),
      self.statRate('heap_blks_hit', ['relname=~"$pg_relname"'], false, 'heap_blks_hit'),
      self.statRate('idx_blks_read', ['relname=~"$pg_relname"'], false, 'idx_blks_read'),
      self.statRate('idx_blks_hit', ['relname=~"$pg_relname"'], false, 'idx_blks_hit'),
      self.stat('heap_blks_ratio', ['relname=~"$pg_relname"'], false, 'heap_blks_ratio'),
      self.stat('idx_blks_ratio', ['relname=~"$pg_relname"'], false, 'idx_blks_ratio'),
    ],

    index_candidates: [
      self.candidateRate('seq_scan'),
      self.candidateRate('seq_tup_read'),
      self.candidateRate('idx_scan'),
      self.candidate('avg'),
    ],

    tuples: {
      live: q.simple('pg_custom_rows_estimates_n_live_tup', '[L] {{relname}}'),
      dead: q.simple('pg_custom_rows_estimates_n_dead_tup', '[D] {{relname}}'),
      dead_ratio: q.simple('pg_custom_rows_estimates_dead_percentage', '{{relname}}'),
    },

    bgwriter: {
      local this2 = self,
      stat(source, labels=['job="postgres-scribe-exporter"'], filterZeroes=false, legend=source):
        this.base('irate(pg_stat_bgwriter_%s_total{%s}[$__rate_interval]) %s', source, labels, filterZeroes, legend),
      checkpoints: {
        write_time: this2.stat('checkpoint_write_time', legend='write time'),
        sync_time: this2.stat('checkpoint_sync_time', legend='sync time'),
        requested: this2.stat('checkpoints_req', legend='requested'),
        scheduled: this2.stat('checkpoints_timed', legend='timed'),
      },
      buffers: {
        backend: this2.stat('buffers_backend'),
        alloc: this2.stat('buffers_alloc'),
        backend_fsync: this2.stat('buffers_backend_fsync'),
        checkpoint: this2.stat('buffers_checkpoint'),
        clean: this2.stat('buffers_clean'),
        all: [self.backend, self.alloc, self.backend_fsync, self.checkpoint, self.clean],
      },
    },

    scrape: {
      latency: q.simple('pg_exporter_last_scrape_duration_seconds{job="postgres-scribe-exporter"}', '{{role}}'),
      status: q.simple('irate(promhttp_metric_handler_requests_total{job="postgres-scribe-exporter"}[$__rate_interval])', '[{{role}}] {{code}}'),
    },

    top_queries: {
      leaderboard:
        q.simple('(max_over_time(pg_custom_top_queries_exec_time_total{}[$__range]) - min_over_time(pg_custom_top_queries_exec_time_total{}[$__range])) > 0', '{{query}}')
        + { format: 'table', instant: true },
      executionTime:
        q.simple('(max_over_time(pg_custom_top_queries_exec_time_total{}[$__range]) - min_over_time(pg_custom_top_queries_exec_time_total{}[$__range])) / (max_over_time(pg_custom_top_queries_calls_total{}[$__range]) - min_over_time(pg_custom_top_queries_calls_total{}[$__range])) > 0', '{{query}}')
        + { format: 'table', instant: true },
    },
  },

  grpcproxy: {
    queue_size: q.simple('queue_size{job="participant-proxy"}', '{{queue}}'),
    enqueue: q.simple('rate(queue_size_inc_total{job="participant-proxy"}[$__rate_interval])', 'enqueue'),
    dequeue: q.simple('-rate(queue_size_dec_total{job="participant-proxy"}[$__rate_interval])', 'dequeue'),
  },

  readapi: {
    vus: q.simple('k6_vus', '{{__name__}}'),
    vus_max: q.simple('k6_vus_max', '{{__name__}}'),

    scenarios: {
      local this = self,
      quantile(source, pct, labels, legend):
        q.simple('histogram_quantile(%.2f, sum(rate(k6_%s_seconds{%s}[$__rate_interval])))' % [pct, source, std.join(',', labels)], legend),
      wrap(source, labels): {
        latency: {
          p50: this.quantile(source, 0.5, labels, 'p50'),
          p90: this.quantile(source, 0.9, labels, 'p90'),
          p95: this.quantile(source, 0.95, labels, 'p95'),
          p99: this.quantile(source, 0.99, labels, 'p99'),
          all_quantiles: [self.p50, self.p90, self.p95, self.p99],
        },
      },
      iteration: self.wrap('iteration_duration', ['persona="$k6_persona"']),
      group: self.wrap('group_duration', ['persona="$k6_persona"', 'group="$k6_group"']),

      bot: {
        batch_processing(persona): this.wrap('custom_bot_batch_processing_latency', ['persona="%s"' % persona]),
        no_new_transactions: q.simple('irate(k6_custom_bot_sleeping_total{type="no_new_transactions"}[$__rate_interval])', '{{type}}'),
        no_new_monitored_contracts: q.simple('irate(k6_custom_bot_sleeping_total{type="no_new_monitored_contracts"}[$__rate_interval])', '{{type}}'),
      },

      completions: q.simple('irate(k6_iterations_total[$__rate_interval])', '{{persona}}'),
      concurrent: q.simple('histogram_count(sum by(persona) (rate(k6_group_duration_seconds[$__rate_interval])))', '{{persona}}'),
      results: q.simple('{__name__=~"k6_custom_.*_retrieved_total", persona="$k6_persona"}', '{{__name__}}'),

      persona_throughput: q.simple('histogram_count(sum by(group) (rate(k6_group_duration_seconds{persona="$k6_persona"}[$__rate_interval])))', '{{group}}'),
      all_throughput: q.simple('histogram_count(sum(rate(k6_group_duration_seconds[$__rate_interval])))', 'all SQL groups'),

      nok: q.simple('k6_checks_rate < 1', '[{{persona}}] {{check}}'),
    },
  },
}
