-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------
-- __contracts table partition index --
---------------------------------------

create index __contracts_life_ix_idx
    on __contracts using gist (life_ix) include (tpe_pk);

create index __contracts_created_at_ix_idx
    on __contracts (created_at_ix) include (tpe_pk);

create index __contracts_create_event_pk_idx
    on __contracts (create_event_pk);

create index __contracts_archived_at_ix_idx
    on __contracts (archived_at_ix) include (tpe_pk);

create index __contracts_archive_event_pk_idx
    on __contracts (archive_event_pk);

create index __contracts_contract_id_idx
    on __contracts using hash (contract_id);

create index __contracts_package_pk_idx
    on __contracts (package_pk);

--------------------------------
-- Table partitioning routine --
--------------------------------

drop procedure __initialize_contract_tpe;
create procedure __initialize_contract_tpe(
    package_name text,
    module_name text,
    entity_name text,
    payload_type __payload_type
) as
$$
declare
    new_tpe_pk bigint;
begin
    select pk
    from __contract_tpe tpe
    where tpe.template_fqn =
          __initialize_contract_tpe.package_name
              || ':' || __initialize_contract_tpe.module_name
              || ':' || __initialize_contract_tpe.entity_name
    into new_tpe_pk;

    if new_tpe_pk is null then
        insert into __contract_tpe(package_name, module_name, entity_name, payload_type, aliases)
        values (package_name, module_name, entity_name, payload_type,
                __make_aliases(package_name, module_name, entity_name))
        returning pk into new_tpe_pk;

        execute format(
                'create table %I partition of __contracts for values in(%L)',
                '__contracts_' || new_tpe_pk,
                new_tpe_pk
                );
        execute format(
                'alter table %I alter column metadata set storage external',
                '__contracts_' || new_tpe_pk
                );
    end if;
end;
$$ language plpgsql;

----------------------------------------------------------------------
-- if indexes already exist, rename them to include tpe_pk standard --
----------------------------------------------------------------------

do $$
declare
    rec record;
begin
    for rec in
        select
            indexname,
            tablename,
            case
                when indexname like '%_life_ix_idx' then replace(indexname, '_life_ix_idx', '_life_ix_tpe_pk_idx')
                when indexname like '%_created_at_ix_idx' then replace(indexname, '_created_at_ix_idx', '_created_at_ix_tpe_pk_idx')
                when indexname like '%_archived_at_ix_idx' then replace(indexname, '_archived_at_ix_idx', '_archived_at_ix_tpe_pk_idx')
            end as new_indexname
        from pg_indexes pi
        where pi.schemaname = current_schema()
            and pi.indexname ~ '^__contracts_\d+_(life_ix|created_at_ix|archived_at_ix)_idx$'
            and exists (
                select 1
                from pg_index pgi
                join pg_class pic on pgi.indexrelid = pic.oid
                where pic.relname = pi.indexname
                    and pgi.indnatts > 0
                    and exists (
                        select 1
                        from pg_get_indexdef(pgi.indexrelid) as indexdef
                        where indexdef ilike '%include (tpe_pk)%'
                    )
            )
    loop
        execute format('alter index %I rename to %I', rec.indexname, rec.new_indexname);
    end loop;
end
$$;

---------------------------------------
-- __exercises table partition index --
---------------------------------------

create index __exercises_exercised_at_ix_idx
    on __exercises (exercised_at_ix) include (tpe_pk);

create index __exercises_exercise_event_pk_idx
    on __exercises (exercise_event_pk);

create index __exercises_contract_id_idx
    on __exercises using hash (contract_id);

create index __exercises_package_pk_idx
    on __exercises (package_pk);

--------------------------------
-- Table partitioning routine --
--------------------------------

drop procedure __initialize_exercise_tpe;
create procedure __initialize_exercise_tpe(
    package_name text,
    module_name text,
    entity_name text,
    choice text,
    consuming bool
) as
$$
declare
    new_tpe_pk bigint;
begin
    select pk
    from __exercise_tpe tpe
    where tpe.choice_fqn =
          __initialize_exercise_tpe.package_name
              || ':' || __initialize_exercise_tpe.module_name
              || ':' || __initialize_exercise_tpe.entity_name
              || ':' || __initialize_exercise_tpe.choice
    into new_tpe_pk;

    if new_tpe_pk is null then
        insert into __exercise_tpe(package_name, module_name, entity_name, choice, consuming, aliases)
        values (package_name, module_name, entity_name, choice, consuming,
                __make_aliases(package_name, module_name, entity_name, choice))
        returning pk into new_tpe_pk;

        execute format(
                'create table %I partition of __exercises for values in(%L)',
                '__exercises_' || new_tpe_pk,
                new_tpe_pk
                );
    end if;
end;
$$ language plpgsql;

--------------------------------------------------------------------
-- if index already exist, rename them to include tpe_pk standard --
--------------------------------------------------------------------

do $$
declare
    rec record;
begin
    for rec in
        select
            indexname,
            tablename,
            case
                when indexname like '%_exercised_at_ix_idx' then replace(indexname, '_exercised_at_ix_idx', '_exercised_at_ix_tpe_pk_idx')
            end as new_indexname
        from pg_indexes pi
        where pi.schemaname = current_schema()
          and pi.indexname ~ '^__exercises_\d+_exercised_at_ix_idx$'
          and exists (
              select 1
              from pg_index pgi
              join pg_class pic on pgi.indexrelid = pic.oid
              where pic.relname = pi.indexname
                and pgi.indnatts > 0
                and exists (
                    select 1
                    from pg_get_indexdef(pgi.indexrelid) as indexdef
                    where indexdef ilike '%include (tpe_pk)%'
                )
          )
    loop
        execute format('alter index %I rename to %I', rec.indexname, rec.new_indexname);
    end loop;
end
$$;
