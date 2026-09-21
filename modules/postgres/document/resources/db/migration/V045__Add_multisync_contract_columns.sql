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
create index if not exists __contracts_life_ix_idx
    on __contracts using gist (life_ix)
    include (tpe_pk)
    where not divulged_only;

alter type contract
    add attribute assign_event_pk bigint,
    add attribute assign_event_id event_id,
    add attribute assigned_at_ix bigint,
    add attribute assigned_at_offset bigint,
    add attribute unassign_event_pk bigint,
    add attribute unassign_event_id event_id,
    add attribute unassigned_at_ix bigint,
    add attribute unassigned_at_offset bigint,
    add attribute reassignment_counter bigint,
    add attribute synchronizer_id text;

create table if not exists __tmp_deactivated_contracts
(
  tpe_pk bigint not null,
  contract_id text not null,
  archive_event_pk bigint,
  archived_at_ix bigint,
  unassign_event_pk bigint,
  unassigned_at_ix bigint,
  synchronizer_id text not null,
  deactivated_at_ix bigint not null generated always as (coalesce(archived_at_ix, unassigned_at_ix)) stored
);

create index if not exists __tmp_deactivated_contracts_ix_idx
  on __tmp_deactivated_contracts
  using btree (deactivated_at_ix);

drop table if exists __tmp_archived_contracts;

drop function if exists __insert_archive_fn();

alter type exercise
    add attribute synchronizer_id text;
