-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type __activation_type as enum ('create', 'assign');
create type __deactivation_type as enum ('archive', 'unassign');

alter table __contracts rename column create_event_pk to activate_event_pk;
alter table __contracts rename column created_at_ix to activated_at_ix;
alter table __contracts rename column archive_event_pk to deactivate_event_pk;
alter table __contracts rename column archived_at_ix to deactivated_at_ix;

alter table __contracts
  -- keep null values to not seq-scan rewrite the table
  add column activation_type __activation_type,
  add column deactivation_type __deactivation_type,
  add column reassignment_counter bigint,
  add column synchronizer_id text;

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
    add attribute synchronizer_id text,
    add attribute activated_at_ix bigint,
    add attribute activation_type __activation_type,
    add attribute deactivated_at_ix bigint,
    add attribute deactivation_type __deactivation_type,
    -- tpe_pk is added to optimize deduplication in active function
    add attribute tpe_pk bigint;

create table if not exists __tmp_deactivated_contracts
(
  tpe_pk bigint not null,
  contract_id text not null,
  deactivate_event_pk bigint not null,
  deactivated_at_ix bigint not null,
  deactivation_type __deactivation_type not null,
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
