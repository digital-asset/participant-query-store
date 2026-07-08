-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------
-- Table partitioning routines --
---------------------------------

drop procedure __initialize_contract_tpe;
create procedure __initialize_contract_tpe(package_id text, package_name text, module_name text, entity_name text,
                                           payload_type __payload_type) as
$$
declare
    old_tpe_pk            bigint;
    new_tpe_pk            bigint;
    qname                 text;
    _package_pk           bigint;
    pending_record_exists int;
begin
    qname := module_name || ':' || entity_name;
    select pk
    from __contract_tpe tpe
    where tpe.template_fqn = __initialize_contract_tpe.package_name || ':' || qname
    into new_tpe_pk;
    select pk from __contract_tpe tpe where tpe.template_fqn = package_id || ':' || qname into old_tpe_pk;

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

    if old_tpe_pk is not null then
        select pk from __packages where id = package_id into _package_pk;
        insert into __contracts (tpe_pk, create_event_pk, created_at_ix, archive_event_pk, archived_at_ix, contract_id,
                                 payload, contract_key, metadata, redaction_id, package_pk, signatories, observers)
        select new_tpe_pk,
               create_event_pk,
               created_at_ix,
               archive_event_pk,
               archived_at_ix,
               contract_id,
               payload,
               contract_key,
               metadata,
               redaction_id,
               _package_pk,
               signatories,
               observers
        from __contracts
        where tpe_pk = old_tpe_pk;

        update __exercises set contract_tpe_pk = new_tpe_pk where contract_tpe_pk = old_tpe_pk;
        update __tmp_archived_contracts
        set tpe_pk     = new_tpe_pk,
            package_pk = _package_pk
        where tpe_pk = old_tpe_pk;

        if payload_type = 'template' then
            delete from __contract_implements where template_pk = old_tpe_pk;
        end if;

        if payload_type = 'interface' then
            delete from __contract_implements where interface_pk = old_tpe_pk;
        end if;

        begin
            execute format(
                    'alter table __contracts detach partition %I',
                    '__contracts_' || old_tpe_pk
                    );

            execute format(
                    'drop table %I',
                    '__contracts_' || old_tpe_pk
                    );
        exception
            when undefined_table then
        end;
        delete from __contract_tpe where pk = old_tpe_pk;
    end if;

    select 1
    from __contracts
    where package_pk = 0
    union all
    select 1
    from __exercises
    where package_pk = 0
    limit 1
    into pending_record_exists;

    if pending_record_exists is null then
        alter table __contracts
            alter column package_pk drop default;
        alter table __exercises
            alter column package_pk drop default;
        alter table __tmp_archived_contracts
            alter column package_pk drop default;
        delete from __packages where pk = 0;
    end if;
end;
$$ language plpgsql;

drop function __initialize_exercise_tpe;
create function __initialize_exercise_tpe(package_id text, package_name text, module_name text, entity_name text,
                                          choice text,
                                          consuming bool) returns bigint as
$$
declare
    old_tpe_pk            bigint;
    new_tpe_pk            bigint;
    choice_qname          text;
    pending_record_exists int;
begin
    choice_qname := module_name || ':' || choice;
    select pk
    from __exercise_tpe tpe
    where tpe.choice_fqn = __initialize_exercise_tpe.package_name || ':' || choice_qname
    into new_tpe_pk;
    select pk from __exercise_tpe tpe where tpe.choice_fqn = package_id || ':' || choice_qname into old_tpe_pk;

    if new_tpe_pk is null then
        insert into __exercise_tpe(package_name, module_name, entity_name, choice, consuming, aliases)
        values (package_name, module_name, entity_name, choice, consuming,
                __make_aliases(package_name, module_name, choice))
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

    if old_tpe_pk is not null then
        insert into __exercises(tpe_pk, contract_tpe_pk, exercise_event_pk, exercised_at_ix, contract_id,
                                argument, result, redaction_id, package_pk)
        select new_tpe_pk,
               contract_tpe_pk,
               exercise_event_pk,
               exercised_at_ix,
               contract_id,
               argument,
               result,
               redaction_id,
               (select pk from __packages where id = package_id)
        from __exercises
        where tpe_pk = old_tpe_pk;
        begin
            -- may not exist because it was already detached by initializing contract_tpe with same choice_fqn
            execute format(
                    'alter table __exercises detach partition %I',
                    '__exercises_' || old_tpe_pk
                    );
            execute format(
                    'drop table %I',
                    '__exercises_' || old_tpe_pk
                    );
        exception
            when undefined_table then
        end;
        delete from __exercise_tpe where pk = old_tpe_pk;
    end if;

    select 1
    from __contracts
    where package_pk = 0
    union all
    select 1
    from __exercises
    where package_pk = 0
    limit 1
    into pending_record_exists;

    if pending_record_exists is null then
        alter table __contracts
            alter column package_pk drop default;
        alter table __exercises
            alter column package_pk drop default;
        alter table __tmp_archived_contracts
            alter column package_pk drop default;
        delete from __packages where pk = 0;
    end if;
    return new_tpe_pk;
end;
$$ language plpgsql;

