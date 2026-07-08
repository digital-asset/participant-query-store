// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

local g = import '../lib-shared/g.libsonnet';

local ann = g.dashboard.annotation;

{
  local this = self,
  down(name, query, color):
    ann.withName(name)
    + ann.withEnable()
    + ann.withHide(false)
    + ann.withIconColor(color)
    + {
      datasource: query.datasource,
      expr: query.expr,
      titleFormat: query.legendFormat,
    },

  grpc: {
    down(query, color='red'):
      this.down('Ledger availability', query, color),
  },

  jdbc: {
    down(query, color='red'):
      this.down('Datastore availability', query, color),
  },
}
