-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


alter table __contracts
  add column assign_event_pk bigint,
  add column assigned_at_ix bigint,
  add column unassign_event_pk bigint,
  add column unassigned_at_ix bigint,
  add column reassignment_counter bigint,
  add column synchronizer_id text,
  -- Postgres 16 and older does not support altering expression of generated column
  -- Instead we drop and re-create the column atomically
  drop column life_ix,
  add column life_ix int8range not null generated always as (
    int8range(
      coalesce(created_at_ix, assigned_at_ix),
      coalesce(archived_at_ix, unassigned_at_ix)
    )
  ) stored;

-- re-create index on life_ix
create index __contracts_life_ix_idx
    on __contracts using gist (life_ix)
    include (tpe_pk)
    where not divulged_only;

