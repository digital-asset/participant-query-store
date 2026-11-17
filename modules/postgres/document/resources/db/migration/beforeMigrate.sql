-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- A hook that runs before any Flyway migration

-- V001's immutable, database-wide type check can find an unrelated schema (#240).
-- Bootstrap its initial types only for this collision; existing stores are unchanged.
do
$$
    begin
        -- postgres has no "create type if not exists" shorthand
        if not exists (
            select 1 from pg_type
            where typname = '__event_type' and typnamespace = current_schema()::regnamespace
        ) and exists (
            select 1 from pg_type
            where typname = '__event_type' and typnamespace <> current_schema()::regnamespace
        ) then
            create type __event_type as enum ('create', 'archive', 'exercise');
            create type __payload_type as enum ('template', 'interface');
            create type checkpoint as
            (
                "offset" text,
                ix       bigint
            );
            create type contract as
            (
                template_fqn       text,
                payload_type       __payload_type,
                create_event_pk    bigint,
                create_event_id    text,
                created_at_ix      bigint,
                created_at_offset  text,
                archive_event_pk   bigint,
                archive_event_id   text,
                archived_at_ix     bigint,
                archived_at_offset text,
                life_ix            int8range,
                contract_id        text,
                witnesses          text[],
                payload            jsonb,
                contract_key       jsonb,
                metadata           bytea
            );
            create type contract_summary as
            (
                template_fqn text,
                payload_type __payload_type,
                count        bigint
            );
            create type exercise as
            (
                template_fqn        text,
                choice_fqn          text,
                choice              text,
                consuming           bool,
                exercise_event_pk   bigint,
                exercise_event_id   text,
                exercised_at_ix     bigint,
                exercised_at_offset text,
                parent_event_id     text,
                contract_id         text,
                witnesses           text[],
                argument            jsonb,
                result              jsonb
            );
        end if;
    end;
$$ language plpgsql;

-- Fix #1015: Update the instance_id to ensure Flyway can update watermark in V40 migration
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_name = '__watermark' and column_name = 'instance_id' and table_schema = current_schema()
  ) then
    update __watermark set instance_id = current_setting('scribe.instance');
  end if;
end $$;
