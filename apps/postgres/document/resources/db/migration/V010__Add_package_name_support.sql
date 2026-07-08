-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

--------------------
-- Private Schema --
--------------------

create table __packages
(
    pk      bigserial primary key,
    name    text not null,
    version text not null,
    id      text not null,
    unique (name, version)
);

create index __packages_id_idx on __packages using hash (id);

-- insert default `baseline` package
insert into __packages(pk, name, version, id)
values (0, 'baseline', '0.0.0', '0');
alter table __contracts
    add package_pk bigint not null default 0 references __packages (pk);
alter table __exercises
    add package_pk bigint not null default 0 references __packages (pk);
alter table __tmp_archived_contracts
    add package_pk bigint not null default 0 references __packages (pk);

alter type contract
    add attribute package_name text,
    add attribute package_version text,
    add attribute package_id text;
alter type exercise
    add attribute package_name text,
    add attribute package_version text,
    add attribute package_id text;


alter table __contract_tpe
    add column package_name text not null default 'baseline',
    add column module_name  text not null default 'baseline',
    add column entity_name  text not null default 'baseline';

update __contract_tpe
set package_name = split_part(template_fqn, ':', 1),
    module_name  = split_part(template_fqn, ':', 2),
    entity_name  = split_part(template_fqn, ':', 3);

alter table __contract_tpe
    drop column template_fqn,
    add column template_fqn text not null generated always as (package_name || ':' || module_name || ':' || entity_name) stored;

alter table __contract_tpe
    alter column package_name drop default,
    alter column module_name drop default,
    alter column entity_name drop default;

alter table __exercise_tpe
    add column package_name text not null default 'baseline',
    add column module_name  text not null default 'baseline',
    add column entity_name  text not null default 'baseline';

update __exercise_tpe
set package_name = split_part(template_fqn, ':', 1),
    module_name  = split_part(template_fqn, ':', 2),
    entity_name  = split_part(template_fqn, ':', 3);

alter table __exercise_tpe
    drop column template_fqn,
    drop column choice_fqn,
    add column template_fqn text not null generated always as (package_name || ':' || module_name || ':' || entity_name) stored,
    add column choice_fqn   text not null generated always as (package_name || ':' || module_name || ':' || choice) stored;

alter table __exercise_tpe
    alter column package_name drop default,
    alter column module_name drop default,
    alter column entity_name drop default;

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
                                 witnesses, payload, contract_key, metadata, redaction_id, package_pk)
        select new_tpe_pk,
               create_event_pk,
               created_at_ix,
               archive_event_pk,
               archived_at_ix,
               contract_id,
               witnesses,
               payload,
               contract_key,
               metadata,
               redaction_id,
               _package_pk
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
        insert into __exercises(tpe_pk, contract_tpe_pk, exercise_event_pk, exercised_at_ix, contract_id, witnesses,
                                argument, result, redaction_id, package_pk)
        select new_tpe_pk,
               contract_tpe_pk,
               exercise_event_pk,
               exercised_at_ix,
               contract_id,
               witnesses,
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

---------------
-- Utilities --
---------------
create procedure __initialize_package(package_name text, package_version text, package_id text) as
$$
declare
    pkg bigint;
begin
    select pk
    from __packages pkgs
    where pkgs.name = package_name
      and pkgs.version = package_version
      and pkgs.id = package_id
    into pkg;
    if pkg is null then
        insert into __packages(name, version, id) values (package_name, package_version, package_id);
    end if;
end;
$$ language plpgsql;

create function __make_aliases(package_name text, module_name text, entity_name text) returns text[] as
$$
declare
    q_name  text;
    fq_name text;
begin
    q_name := module_name || ':' || entity_name;
    fq_name := package_name || ':' || q_name;
    return array [fq_name, q_name, entity_name];
end;
$$ language plpgsql immutable
                    parallel safe
                    strict;

drop function __contracts;
create function __contracts(qname text default null) returns setof contract
as
$$
select tpe.template_fqn,
       tpe.payload_type,
       c.create_event_pk,
       ce.event_id,
       c.created_at_ix,
       ct."offset",
       c.archive_event_pk,
       ae.event_id,
       c.archived_at_ix,
       at."offset",
       c.life_ix,
       c.contract_id,
       c.witnesses,
       c.payload,
       c.contract_key,
       c.metadata,
       ct.effective_at,
       at.effective_at,
       c.redaction_id,
       p.name,
       p.version,
       p.id
from __contracts c
         left join __contract_tpe tpe on tpe.pk = c.tpe_pk
         left join __transactions ct on c.created_at_ix = ct.ix
         left join __transactions at on c.archived_at_ix = at.ix
         left join __events ce on ce.pk = c.create_event_pk
         left join __events ae on ae.pk = c.archive_event_pk
         left join __packages p on c.package_pk = p.pk
where qname is null
   or c.tpe_pk = __contract_tpe4name(qname)
$$ language sql stable
                parallel safe;

drop function __exercises;
create function __exercises(qname text default null) returns setof exercise
as
$$
select tpe.template_fqn,
       tpe.choice_fqn,
       tpe.choice,
       tpe.consuming,
       e.exercise_event_pk,
       ee.event_id,
       e.exercised_at_ix,
       t."offset",
       pe.event_id as parent_event_id,
       e.contract_id,
       e.witnesses,
       e.argument,
       e.result,
       t.effective_at,
       e.redaction_id,
       p.name,
       p.version,
       p.id
from __exercises e
         left join __exercise_tpe tpe on tpe.pk = e.tpe_pk
         left join __transactions t on t.ix = e.exercised_at_ix
         left join __events ee on ee.pk = e.exercise_event_pk
         left join __events pe on pe.pk = ee.parent_event_pk
         left join __packages p on e.package_pk = p.pk
where qname is null
   or e.tpe_pk = __exercise_tpe4name(qname)
$$ language sql stable
                parallel safe;


-------------------------
-- Ingestion utilities --
-------------------------
drop view __archives;
create view __archives as
select c.archive_event_pk as archive_event_pk,
       c.archived_at_ix   as archived_at_ix,
       c.contract_id      as contract_id,
       c.tpe_pk           as tpe_pk,
       c.package_pk       as package_pk
from __contracts c;

drop function __insert_archive_fn;
create function __insert_archive_fn() returns trigger as
$$
declare
    updated_rows int;
begin
    with updated as (
        update __contracts c
            set archive_event_pk = new.archive_event_pk,
                archived_at_ix = new.archived_at_ix
            where c.tpe_pk = new.tpe_pk and c.contract_id = new.contract_id
            returning 1)
    select count(*)
    from updated
    into updated_rows;
    if updated_rows = 0 then -- avoid contention, defer to when watermark is updated
        insert into __tmp_archived_contracts(contract_id, archive_event_pk, archived_at_ix, tpe_pk, package_pk)
        values (new.contract_id, new.archive_event_pk, new.archived_at_ix, new.tpe_pk, new.package_pk);
    end if;
    return new;
end;
$$ language plpgsql;
create trigger __insert_archive_trg
    instead of insert
    on __archives
    for each row
execute function __insert_archive_fn();

comment on function active is $$Returns payload and metadata for active contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-name>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts that are active at particular offset are considered.$$;

comment on function creates is $$Returns payload and metadata for created contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-name>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts within [from_offset; to_offset] range are considered.$$;

comment on function archives is $$Returns payload and metadata for archived contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-name>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Only contracts within [from_offset; to_offset] range are considered.$$;

comment on function exercises is $$Returns argument, result and metadata for exercised events of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-name>:<module-path>:<choice-name>'
- partially qualified, e.g. '<module-path>:<choice-name>' or '<choice-name>'
Only events within [from_offset; to_offset] range are considered.$$;


