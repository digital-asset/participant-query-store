-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type activation_type as enum ('create', 'assign');
create type deactivation_type as enum ('archive', 'unassign');

alter table __contracts rename column create_event_pk to activate_event_pk;
alter table __contracts rename column created_at_ix to activated_at_ix;
alter table __contracts rename column archive_event_pk to deactivate_event_pk;
alter table __contracts rename column archived_at_ix to deactivated_at_ix;

alter table __contracts
  add column activation_type activation_type not null default 'create',
  -- keep null values in legacy archived rows to avoid the table rewrite
  add column deactivation_type deactivation_type,
  add column reassignment_counter bigint not null default 0,
  add column synchronizer_id text;

alter type contract
    add attribute activate_event_pk bigint,
    add attribute activate_event_id event_id,
    add attribute activated_at_ix bigint,
    add attribute activated_at_offset bigint,
    add attribute activation_type activation_type,
    add attribute deactivate_event_pk bigint,
    add attribute deactivate_event_id event_id,
    add attribute deactivated_at_ix bigint,
    add attribute deactivated_at_offset bigint,
    add attribute deactivation_type deactivation_type,
    add attribute reassignment_counter bigint,
    add attribute synchronizer_id text,
    -- tpe_pk is added to optimize deduplication in active function
    add attribute tpe_pk bigint;

create table if not exists __tmp_deactivated_contracts
(
  tpe_pk bigint not null,
  contract_id text not null,
  deactivate_event_pk bigint not null,
  deactivated_at_ix bigint not null,
  deactivation_type deactivation_type not null,
  synchronizer_id text not null
);

create index if not exists __tmp_deactivated_contracts_ix_idx
  on __tmp_deactivated_contracts using btree (tpe_pk, deactivated_at_ix);

drop table if exists __tmp_archived_contracts;

drop trigger if exists __insert_archive_trg on __archives;

drop function if exists __insert_archive_fn();

drop view if exists __archives;

alter type exercise
    add attribute synchronizer_id text;
