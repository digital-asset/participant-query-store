-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __exercise_tpe
    drop column choice_fqn,
    add column choice_fqn text not null generated always as (package_name || ':' || module_name || ':' || entity_name || ':' || choice) stored;

-- overloaded function
drop function if exists __make_aliases(text, text, text, text);
create function __make_aliases(
    package_name text,
    module_name text,
    entity_name text,
    choice_name text
) returns text[] as
$$
begin
    return array [
        package_name || ':' || module_name || ':' || entity_name || ':' || choice_name,
        module_name || ':' || entity_name || ':' || choice_name,
        entity_name || ':' || choice_name,
        choice_name
        ];
end;
$$ language plpgsql immutable
                    parallel safe
                    strict;

drop function __initialize_exercise_tpe;
create function __initialize_exercise_tpe(
    package_id text,
    package_name text,
    module_name text,
    entity_name text,
    choice text,
    consuming bool
) returns bigint as
$$
declare
    old_tpe_pk            bigint;
    new_tpe_pk            bigint;
    contract_tpe          bigint;
    package_id_pk         bigint;
    choice_qname          text;
    remaining_rows        int;
    pending_record_exists int;
begin
    choice_qname := package_name || ':' || module_name || ':' || entity_name || ':' || choice;
    select pk from __exercise_tpe tpe where tpe.choice_fqn = choice_qname into new_tpe_pk;

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

    select pk
    from __exercise_tpe tpe
    where tpe.choice_fqn =
          __initialize_exercise_tpe.package_id || ':' || __initialize_exercise_tpe.module_name || ':' ||
          __initialize_exercise_tpe.entity_name || ':' || __initialize_exercise_tpe.choice
    into old_tpe_pk;

    if old_tpe_pk is not null then
        select ctpe.pk
        from __contract_tpe ctpe
        where ctpe.package_name = __initialize_exercise_tpe.package_name
          and ctpe.module_name = __initialize_exercise_tpe.module_name
          and ctpe.entity_name = __initialize_exercise_tpe.entity_name
        into contract_tpe;

        select pk from __packages where id = package_id into package_id_pk;

        with removed as (
            delete from __exercises
                where tpe_pk = old_tpe_pk
                    and contract_tpe_pk = contract_tpe
                returning
                    new_tpe_pk,
                    contract_tpe_pk,
                    exercise_event_pk,
                    exercised_at_ix,
                    contract_id,
                    argument,
                    result,
                    redaction_id,
                    package_id_pk)
        insert
        into __exercises(tpe_pk, contract_tpe_pk, exercise_event_pk, exercised_at_ix, contract_id,
                         argument, result, redaction_id, package_pk)
        select *
        from removed;

        select 1 from __exercises where tpe_pk = old_tpe_pk limit 1 into remaining_rows;
        if remaining_rows is null then
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


-- Migrate existing 'Archive' choices:
update __exercise_tpe
set aliases = __make_aliases(package_name, module_name, entity_name, choice);

update __exercise_tpe
set package_name = '!defunct',
    module_name  = '!defunct',
    entity_name  = '!defunct'
where choice = 'Archive';

with data as (select ctpe.package_name,
                     ctpe.module_name,
                     ctpe.entity_name
              from __contract_tpe ctpe)
select 'Migrating archive from ' || data.package_name || ':' || data.module_name || ':' || data.entity_name as description,
       __initialize_exercise_tpe(
               '',
               data.package_name,
               data.module_name,
               data.entity_name,
               'Archive',
               true
       ) as exercise_tpe_pk
from data;

with removed as (
    delete from __exercises where tpe_pk in (select pk
                                             from __exercise_tpe
                                             where choice_fqn = '!defunct:!defunct:!defunct:Archive')
        returning contract_tpe_pk, exercise_event_pk, exercised_at_ix, contract_id, argument, result, redaction_id, package_pk)
insert
into __exercises (tpe_pk, contract_tpe_pk, exercise_event_pk, exercised_at_ix, contract_id,
                  argument, result, redaction_id, package_pk)
select etpe.pk as tpe_pk,
       r.contract_tpe_pk,
       r.exercise_event_pk,
       r.exercised_at_ix,
       r.contract_id,
       r.argument,
       r.result,
       r.redaction_id,
       r.package_pk
from removed r,
     __contract_tpe ctpe,
     __exercise_tpe etpe
where r.contract_tpe_pk = ctpe.pk
  and etpe.template_fqn = ctpe.template_fqn
  and etpe.choice = 'Archive';

delete
from __exercise_tpe
where choice_fqn = '!defunct:!defunct:!defunct:Archive';

