// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

local g = import '../lib-shared/g.libsonnet';
local p = import '../lib-shared/panels.libsonnet';

{
  scribe: {
    timeSeries: {
      local ts = g.panel.timeSeries,
      local options = ts.options,

      contractsChurn(title, targets, width=null):
        p.timeSeries.throughput(title, targets, width)
        + ts.panelOptions.withDescription('Per-template activity (creates/archives) on the ledger')
        + ts.standardOptions.withMin(null)
        + options.legend.withDisplayMode('table')
        + options.legend.withPlacement('right'),

      activeContracts(title, targets, width=null):
        p.timeSeries.short(title, targets, width)
        + ts.panelOptions.withDescription('Per-template active contracts count<br><br>* as observed since last restart')
        + ts.standardOptions.withDecimals(null)
        + options.legend.withCalcs(['lastNotNull', 'max'])
        + options.legend.withDisplayMode('table')
        + options.legend.withPlacement('right'),
    },

    heatmap: {
      local hm = g.panel.heatmap,
      local options = hm.options,

      queueSize(title, targets, width=null):
        p.heatmap.base(title, targets, width)
        + hm.standardOptions.withMin(0)
        + hm.standardOptions.withMax(32)
        + (
          if p.utils.isPreV11() then
            options.color.HeatmapColorOptions.withScale('linear')
            + options.color.HeatmapColorOptions.withSteps(32)
            + options.color.HeatmapColorOptions.withMax(32)
            + options.color.HeatmapColorOptions.withMin(0)
          else
            options.color.withScale('linear')
            + options.color.withSteps(32)
            + options.color.withMax(32)
            + options.color.withMin(0)
        )
        + options.withCellGap(0)
        + options.withFilterValues({ le: 0 })
        + options.yAxis.withAxisPlacement('hidden'),
    },
  },

  postgres: {
    timeSeries: {
      local ts = g.panel.timeSeries,
      local fieldOverride = ts.fieldOverride,
      local custom = ts.fieldConfig.defaults.custom,
      local options = ts.options,

      stat(title, targets, width=null, description=null):
        p.timeSeries.throughput(title, targets, width)
        + ts.panelOptions.withDescription(description)
        + ts.standardOptions.withDecimals(null)
        + ts.standardOptions.withMin(0)
        + custom.withSpanNulls(false)
        + custom.withShowPoints('auto'),

      ratio(title, targets, width=null, description=null):
        self.stat(title, targets, width, description)
        + ts.standardOptions.withUnit('percent')
        + ts.standardOptions.withMin(null),

      condensed(title, targets, width=null):
        self.stat(title, targets, width)
        + ts.standardOptions.withOverrides([
          fieldOverride.byQuery.new('I')
          + fieldOverride.byQuery.withProperty('unit', 'percent')
          + fieldOverride.byQuery.withProperty('min', null),
          fieldOverride.byQuery.new('J')
          + fieldOverride.byQuery.withProperty('unit', 'percent')
          + fieldOverride.byQuery.withProperty('min', null),
        ]),

      candidates(title, targets, width=null):
        self.stat(title, targets, width)
        + ts.standardOptions.withOverrides([
          fieldOverride.byQuery.new('D')
          + fieldOverride.byQuery.withProperty('unit', null),
        ]),

      checkpointsTime(title, targets, width=null, description=null):
        self.stat(title, targets, width, description)
        + ts.standardOptions.withUnit('ms')
        + options.legend.withDisplayMode('table')
        + options.legend.withPlacement('bottom')
        + options.legend.withCalcs(['mean', 'lastNotNull', 'max', 'min']),

      checkpointsCount(title, targets, width=null, description=null):
        self.stat(title, targets, width, description)
        + ts.standardOptions.withUnit(null),

      buffers(title, targets, width=null, description=null):
        self.stat(title, targets, width, description)
        + ts.standardOptions.withUnit(null)
        + options.legend.withDisplayMode('table')
        + options.legend.withPlacement('bottom')
        + options.legend.withCalcs(['mean', 'lastNotNull', 'max', 'min']),

      tuples(title, targets, width=null, description=null):
        self.stat(title, targets, width, description)
        + ts.standardOptions.withDecimals(0)
        + ts.standardOptions.withUnit(null)
        + ts.standardOptions.withOverrides([
          fieldOverride.byQuery.new('B')
          + fieldOverride.byQuery.withProperty('custom.axisPlacement', 'right'),
        ])
        + options.legend.withPlacement('right')
        + options.withTooltip({ mode: 'single' }),

      deadTuplesRatio(title, targets, width=null, description=null):
        self.stat(title, targets, width, description)
        + ts.standardOptions.withUnit('percent')
        + options.legend.withPlacement('right'),

      scrapeStatus(title, targets, width=null):
        p.timeSeries.base(title, targets, width)
        + ts.standardOptions.withMin(null)
        + ts.standardOptions.withUnit(null)
        + ts.standardOptions.withDecimals(null)
        + custom.withShowPoints(null)
        + custom.withFillOpacity(0)
        + custom.withSpanNulls(false),
    },

    table: {
      local tb = g.panel.table,
      local fieldOverride = tb.fieldOverride,
      local custom = tb.fieldConfig.defaults.custom,
      local tf = tb.queryOptions.transformation,

      topQueries(title, targets, width=null):
        tb.new(title)
        + custom.withFilterable(true)
        + tb.queryOptions.withTargets(targets)
        + tb.panelOptions.withGridPos(w=width)
        + tb.options.withSortBy({ desc: true, displayName: 'Value' })
        + tb.standardOptions.withUnit('ms')
        + tb.standardOptions.color.withMode('fixed')
        + tb.standardOptions.color.withFixedColor('green')
        + tb.standardOptions.withOverrides([
          fieldOverride.byName.new('Value')
          + fieldOverride.byName.withProperty('custom.cellOptions', { mode: 'lcd', type: 'gauge' })
          + fieldOverride.byName.withProperty('custom.width', 300),
          fieldOverride.byName.new('queryid')
          + fieldOverride.byName.withProperty('custom.hidden', true),
          fieldOverride.byName.new('toplevel')
          + fieldOverride.byName.withProperty('custom.width', 60),
        ])
        + tb.queryOptions.withTransformations([
          tf.withId('organize')
          + tf.withOptions({
            excludeByName: { Time: true, __name__: true, component: true, instance: true, job: true, role: true, server: true },
          }),
        ]),
    },
  },

  k6: {
    timeSeries: {
      local ts = g.panel.timeSeries,
      local custom = ts.fieldConfig.defaults.custom,

      botSleepReasons(title, targets, width=null):
        p.timeSeries.short(title, targets, width)
        + ts.standardOptions.withDecimals(null)
        + custom.withSpanNulls(false),
    },
  },
}
