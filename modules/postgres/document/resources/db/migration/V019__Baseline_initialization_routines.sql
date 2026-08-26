-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

do
$$
    declare
        pending_record_exists bigint;
    begin
        select 1
        from __contracts
        where package_pk = 0
        union all
        select 1
        from __exercises
        where package_pk = 0
        union all
        select 1
        from __contract_tpe
        where template_fqn != package_name || ':' || module_name || ':' || entity_name
        union all
        select 1
        from __exercise_tpe
        where choice_fqn != package_name || ':' || module_name || ':' || entity_name || ':' || choice
        limit 1
        into pending_record_exists;

        if pending_record_exists is not null then
            raise exception 'There are items that were not migrated to package-name dependent addressing scheme. Please upgrade to scribe version 0.4.5 (schema version V013) first.';
        end if;

        delete from __packages where pk = 0;
    end;
$$ language plpgsql;

---------------------------------
-- Table partitioning routines --
---------------------------------

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
                'create index %I on %I using gist(life_ix) include (tpe_pk)',
                '__contracts_' || new_tpe_pk || '_life_ix_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(created_at_ix) include (tpe_pk)',
                '__contracts_' || new_tpe_pk || '_created_at_ix_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(create_event_pk)',
                '__contracts_' || new_tpe_pk || '_create_event_pk_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(archived_at_ix) include (tpe_pk)',
                '__contracts_' || new_tpe_pk || '_archived_at_ix_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(archive_event_pk)',
                '__contracts_' || new_tpe_pk || '_archive_event_pk_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using hash(contract_id)',
                '__contracts_' || new_tpe_pk || '_contract_id_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(package_pk)',
                '__contracts_' || new_tpe_pk || '_package_pk_idx',
                '__contracts_' || new_tpe_pk
                );
        execute format(
                'alter table %I alter column metadata set storage external',
                '__contracts_' || new_tpe_pk
                );
    end if;
end;
$$ language plpgsql;

drop function __initialize_exercise_tpe;
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
        execute format(
                'create index %I on %I using btree(exercised_at_ix) include (tpe_pk)',
                '__exercises_' || new_tpe_pk || '_exercised_at_ix_idx',
                '__exercises_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(exercise_event_pk)',
                '__exercises_' || new_tpe_pk || '_exercise_event_pk_idx',
                '__exercises_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using hash(contract_id)',
                '__exercises_' || new_tpe_pk || '_contract_id_idx',
                '__exercises_' || new_tpe_pk
                );
        execute format(
                'create index %I on %I using btree(package_pk)',
                '__exercises_' || new_tpe_pk || '_package_pk_idx',
                '__exercises_' || new_tpe_pk
                );
    end if;
end;
$$ language plpgsql;
