-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- NB: See Public API section below

-----------
-- Types --
-----------

do
$$
    begin
        -- postgres has no "create type if not exists" shorthand
        if not exists (select 1 from pg_type where typname = '__event_type') then
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

--------------------
-- Private Schema --
--------------------

create table if not exists __watermark
(
    singleton bool   not null primary key default true,
    ix        bigint not null,
    "offset"  text   not null,
    constraint singleton_watermark check (singleton)
);

create table if not exists __transactions
(
    ix             bigint not null primary key,
    "offset"       text   not null,
    transaction_id text,
    effective_at   timestamp with time zone,
    workflow_id    text,
    domain_id      text
);
create unique index if not exists __transactions_offset_ix_idx on __transactions using btree ("offset", ix);
create index if not exists __transactions_transaction_id_idx on __transactions using hash (transaction_id);
create index if not exists __transactions_transaction_effective_at_idx on __transactions using brin (effective_at);

create table if not exists __contract_tpe
(
    pk           bigserial primary key,
    template_fqn text           not null,
    payload_type __payload_type not null,
    aliases      text[]         not null
);
create index if not exists __contract_tpe_aliases_ix on __contract_tpe using gin (aliases array_ops);
create table if not exists __contract_implements
(
    template_pk  bigint not null references __contract_tpe,
    interface_pk bigint not null references __contract_tpe,
    primary key (template_pk, interface_pk)
);
create table if not exists __exercise_tpe
(
    pk           bigserial primary key,
    template_fqn text   not null,
    choice_fqn   text   not null,
    choice       text   not null,
    consuming    bool   not null,
    aliases      text[] not null
);
create index if not exists __exercise_tpe_aliases_ix on __exercise_tpe using gin (aliases array_ops);

create table if not exists __events
(
    pk              bigint       not null primary key,
    tx_ix           bigint       not null references __transactions on delete cascade,
    event_id        text         not null,
    type            __event_type not null,
    parent_event_pk bigint references __events on delete set null deferrable initially deferred,
    witnesses       text[]       not null
);
create index if not exists __events_event_id_idx on __events using hash (event_id);
create index if not exists __events_tx_ix_idx on __events using btree (tx_ix);

create table if not exists __contracts
(
    tpe_pk           bigint    not null references __contract_tpe,
    create_event_pk  bigint references __events on delete cascade,
    created_at_ix    bigint references __transactions on delete cascade,
    archive_event_pk bigint    references __events on delete set null,
    archived_at_ix   bigint    references __transactions on delete set null,
    life_ix          int8range not null generated always as (int8range(created_at_ix, archived_at_ix)) stored,
    contract_id      text      not null,
    witnesses        text[]    not null,
    payload          jsonb     not null,
    contract_key     jsonb,
    metadata         bytea
) partition by list (tpe_pk);

create table if not exists __exercises
(
    tpe_pk            bigint not null references __exercise_tpe,
    contract_tpe_pk   bigint not null references __contract_tpe,
    exercise_event_pk bigint references __events on delete cascade,
    exercised_at_ix   bigint references __transactions on delete cascade,
    contract_id       text   not null,
    witnesses         text[] not null,
    argument          jsonb  not null,
    result            jsonb  not null
) partition by list (tpe_pk);

create table if not exists __tmp_archived_contracts
(
    contract_id      text,
    archive_event_pk bigint,
    archived_at_ix   bigint,
    tpe_pk           bigint
);
create index if not exists __tmp_archived_contracts_ix_idx on __tmp_archived_contracts using btree (archived_at_ix);

---------------------------------
-- Table partitioning routines --
---------------------------------

create or replace procedure __initialize_contract_tpe(template_fqn text, payload_type __payload_type) as
$$
declare
    tpe_pk   bigint;
    segments text[];
begin
    select pk from __contract_tpe tpe where tpe.template_fqn = __initialize_contract_tpe.template_fqn into tpe_pk;
    if tpe_pk is null then
        segments := regexp_split_to_array(template_fqn, E':');
        insert into __contract_tpe(template_fqn, payload_type, aliases)
        values (template_fqn, payload_type, array [
            segments[1] || ':' || segments[2] || ':' || segments[3],
            segments[2] || ':' || segments[3],
            segments[3]
            ])
        returning pk into tpe_pk;

        execute format(
                'create table %I partition of __contracts for values in(%L)',
                '__contracts_' || tpe_pk,
                tpe_pk
                );
        execute format(
                'create index %I on %I using gist(life_ix) include (tpe_pk)',
                '__contracts_' || tpe_pk || '_life_ix_idx',
                '__contracts_' || tpe_pk
                );
        execute format(
                'create index %I on %I using btree(created_at_ix) include (tpe_pk)',
                '__contracts_' || tpe_pk || '_created_at_ix_idx',
                '__contracts_' || tpe_pk
                );
        execute format(
                'create index %I on %I using btree(create_event_pk)',
                '__contracts_' || tpe_pk || '_create_event_pk_idx',
                '__contracts_' || tpe_pk
                );
        execute format(
                'create index %I on %I using btree(archived_at_ix) include (tpe_pk)',
                '__contracts_' || tpe_pk || '_archived_at_ix_idx',
                '__contracts_' || tpe_pk
                );
        execute format(
                'create index %I on %I using btree(archive_event_pk)',
                '__contracts_' || tpe_pk || '_archive_event_pk_idx',
                '__contracts_' || tpe_pk
                );
        execute format(
                'create index %I on %I using hash(contract_id)',
                '__contracts_' || tpe_pk || '_contract_id_idx',
                '__contracts_' || tpe_pk
                );
        execute format(
                'alter table %I alter column metadata set storage external;',
                '__contracts_' || tpe_pk
                );
    end if;
end;
$$ language plpgsql;

create or replace procedure __initialize_contract_implements(template_fqn text, interface_fqn text) as
$$
declare
    template_pk  bigint;
    interface_pk bigint;
begin
    select pk
    from __contract_tpe t
    where t.template_fqn = __initialize_contract_implements.template_fqn
    into template_pk;
    select pk
    from __contract_tpe t
    where t.template_fqn = __initialize_contract_implements.interface_fqn
    into interface_pk;
    insert into __contract_implements (template_pk, interface_pk) values (template_pk, interface_pk);
end;
$$ language plpgsql;

create or replace function __initialize_exercise_tpe(template_fqn text, choice_fqn text, choice text, consuming bool) returns bigint as
$$
declare
    tpe_pk   bigint;
    segments text[];
begin
    select pk from __exercise_tpe tpe where tpe.choice_fqn = __initialize_exercise_tpe.choice_fqn into tpe_pk;
    if tpe_pk is null then
        segments := regexp_split_to_array(choice_fqn, E':');
        insert into __exercise_tpe(template_fqn, choice_fqn, choice, consuming, aliases)
        values (template_fqn, choice_fqn, choice, consuming, array [
            segments[1] || ':' || segments[2] || ':' || segments[3],
            segments[2] || ':' || segments[3],
            segments[3]
            ])
        returning pk into tpe_pk;

        execute format(
                'create table %I partition of __exercises for values in(%L)',
                '__exercises_' || tpe_pk,
                tpe_pk
                );
        execute format(
                'create index %I on %I using btree(exercised_at_ix) include (tpe_pk)',
                '__exercises_' || tpe_pk || '_exercised_at_ix_idx',
                '__exercises_' || tpe_pk
                );
        execute format(
                'create index %I on %I using btree(exercise_event_pk)',
                '__exercises_' || tpe_pk || '_exercise_event_pk_idx',
                '__exercises_' || tpe_pk
                );
        execute format(
                'create index %I on %I using hash(contract_id)',
                '__exercises_' || tpe_pk || '_contract_id_idx',
                '__exercises_' || tpe_pk
                );
    end if;
    return tpe_pk;
end;
$$ language plpgsql;


-------------------------
-- Ingestion utilities --
-------------------------

create or replace view __archives as
select c.archive_event_pk as archive_event_pk,
       c.archived_at_ix   as archived_at_ix,
       c.contract_id      as contract_id,
       c.tpe_pk           as tpe_pk
from __contracts c;
create or replace function __insert_archive_fn() returns trigger as
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
        insert into __tmp_archived_contracts(contract_id, archive_event_pk, archived_at_ix, tpe_pk)
        values (new.contract_id, new.archive_event_pk, new.archived_at_ix, new.tpe_pk);
    end if;
    return new;
end;
$$ language plpgsql;
drop trigger if exists __insert_archive_trg on __archives;
create trigger __insert_archive_trg
    instead of insert
    on __archives
    for each row
execute function __insert_archive_fn();

create or replace function __update_watermark_fn() returns trigger as
$$
declare
    stuff __tmp_archived_contracts%rowtype;
begin
    with deleted as (delete from __tmp_archived_contracts where archived_at_ix <= new.ix returning *)
    update __contracts c
    set archive_event_pk = deleted.archive_event_pk,
        archived_at_ix   = deleted.archived_at_ix
    from deleted
    where c.tpe_pk = deleted.tpe_pk and c.contract_id = deleted.contract_id;
    return new;
end;
$$ language plpgsql parallel safe;
drop trigger if exists __update_watermark_trg on __watermark;
create trigger __update_watermark_trg
    before update of ix
    on __watermark
    for each row
execute function __update_watermark_fn();
drop trigger if exists __insert_watermark_trg on __watermark;
create trigger __insert_watermark_trg
    before insert
    on __watermark
    for each row
execute function __update_watermark_fn();


---------------
-- Utilities --
---------------

create or replace function __contract_tpe4name(qname text) returns __contract_tpe.pk%type as
$$
declare
    result bigint;
begin
    case (select count(*) from __contract_tpe where array [qname] <@ aliases)
    when 0 then
        raise exception 'Identifier not found: %' , qname;
    when 1 then
        select pk from __contract_tpe where array [qname] <@ aliases into result;
    else
        raise exception 'Ambiguous identifier: %' , qname;
    end case;
    return result;
end
$$ language plpgsql immutable -- immutable because __contract_tpe rarely changes.
                    parallel safe
                    strict;

create or replace function __exercise_tpe4name(qname text) returns __exercise_tpe.pk%type as
$$
declare
    result bigint;
begin
    case (select count(*) from __exercise_tpe where array [qname] <@ aliases)
    when 0 then
        raise exception 'Identifier not found: %' , qname;
    when 1 then
        select pk from __exercise_tpe where array [qname] <@ aliases into result;
    else
        raise exception 'Ambiguous identifier: %' , qname;
    end case;
    return result;
end
$$ language plpgsql immutable
                    parallel safe
                    strict;

create or replace function __nearest_ix("offset" checkpoint."offset"%type) returns checkpoint.ix%type as
$$
declare
    result bigint;
begin
    if __nearest_ix."offset" > latest_offset() then
        raise exception 'Offset % is after the latest known offset %' , __nearest_ix."offset", latest_offset();
    end if;
    if __nearest_ix."offset" < oldest_offset() then
        raise exception 'Offset % is before the oldest known offset %' , __nearest_ix."offset", oldest_offset();
    end if;
    select ix
    from __transactions t
    where t."offset" <= __nearest_ix."offset"
    order by t."offset" desc
    limit 1
    into result;
    return result;
end
$$ language plpgsql immutable
                    parallel safe;

create or replace function __contracts(qname text) returns setof contract
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
       c.metadata
from __contracts c
    left join __contract_tpe tpe on tpe.pk = c.tpe_pk
    left join __transactions ct on c.created_at_ix = ct.ix
    left join __transactions at on c.archived_at_ix = at.ix
    left join __events ce on ce.pk = c.create_event_pk
    left join __events ae on ae.pk = c.archive_event_pk
where qname is null or c.tpe_pk = __contract_tpe4name(qname)
$$ language sql stable
                parallel safe;

create or replace function __exercises(qname text default null) returns setof exercise
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
       e.result
from __exercises e
    join __exercise_tpe tpe on tpe.pk = e.tpe_pk
    left join __transactions t on t.ix = e.exercised_at_ix
    left join __events ee on ee.pk = e.exercise_event_pk
    left join __events pe on pe.pk = ee.parent_event_pk
where qname is null or e.tpe_pk = __exercise_tpe4name(qname)
$$ language sql stable
                parallel safe;

/*******************
********************
**** Public API ****
********************
*******************/

--------------------------------------
-- Checkpoints, offsets and indexes --
--------------------------------------

create or replace function oldest_checkpoint() returns setof checkpoint
as
$$
select "offset", ix from __transactions order by "offset" limit 1;
$$ language sql rows 1
                parallel safe;
comment on function oldest_checkpoint is 'Returns the oldest (earliest) checkpoint in the history.';

create or replace function latest_checkpoint() returns setof checkpoint
as
$$
select "offset", ix from __watermark;
$$ language sql rows 1
                parallel safe;
comment on function latest_checkpoint() is 'Returns the latest (newest) checkpoint in the history.';

create or replace function validate_offset_exists("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as $$
declare
    first_offset  text;
    latest_offset text;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select c."offset" from oldest_checkpoint() c);
    latest_offset := (select c."offset" from latest_checkpoint() c);

    if "offset" < first_offset then
        raise exception 'Illegal offset % is outside lower bounds of contiguous history', "offset";
    end if;
    if "offset" > latest_offset then
        raise exception 'Illegal offset % is beyond upper bounds of contiguous history', "offset";
    end if;
    return "offset";
end;
$$ language plpgsql stable
                    strict;
comment on function validate_offset_exists is 'Validate that the offset in question is part of ingested contiguous history.';

create or replace function set_oldest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as $$
select set_config('public.session_offset_oldest', validate_offset_exists("offset"), false);
$$ language sql stable;
comment on function set_oldest(text) is $$Sets 'public.session_offset_oldest' to the given offset, or clears it if null.$$;

create or replace function set_latest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as $$
select set_config('public.session_offset_latest', validate_offset_exists("offset"), false);
$$ language sql stable;
comment on function set_latest(text) is $$Sets 'public.session_offset_latest' to the given offset, or clears it if null.$$;

create or replace function oldest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('public.session_offset_oldest', true), '') = ''
               then (select "offset" from oldest_checkpoint())
           else current_setting('public.session_offset_oldest', false)
       end;
$$ language sql stable
                parallel safe;
comment on function oldest_offset is 'Returns the oldest (earliest) offset in the history.';

create or replace function latest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('public.session_offset_latest', true), '') = ''
               then (select "offset" from latest_checkpoint())
           else current_setting('public.session_offset_latest', false)
       end;
$$ language sql stable
                parallel safe;
comment on function latest_offset is 'Returns the latest (newest) offset in the history.';

create or replace function nearest_offset(cutoff_ts timestamptz) returns text as $$
declare
    result text;
begin
    raise info 'Finding closest offset prior to cut-off timestamp: % (UTC)', cutoff_ts;
    select max("offset") from __transactions where effective_at <= cutoff_ts into result;
    raise info 'Determined closest offset: %', result;
    return result;
end;
$$ language plpgsql stable
                    strict;
comment on function nearest_offset(timestamptz) is
    'Returns the closest offset prior to the given cut-off timestamp.';

create or replace function nearest_offset(cutoff interval) returns text as $$
declare
    result text;
begin
    raise info 'Current time: % (UTC)', now();
    raise info 'Finding closest offset prior to cut-off interval ago: %', cutoff;
    select nearest_offset(now() - cutoff) into result;
    return result;
end;
$$ language plpgsql stable
                    strict;
comment on function nearest_offset(interval) is
    'Returns the closest offset prior to the given cut-off interval ago.';

---------------
-- Contracts --
---------------

create or replace function active(
    qname text default null,
    at_offset __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
select c.*
from __contracts(qname) c
where c.life_ix @> __nearest_ix(at_offset)
$$ language sql stable
                parallel safe;
comment on function active is $$Returns payload and metadata for active contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts that are active at particular offset are considered.$$;

create or replace function creates(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
select c.*
from __contracts(qname) c
where c.created_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable
                parallel safe;
comment on function creates is $$Returns payload and metadata for created contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts within [from_offset; to_offset] range are considered.$$;


create or replace function archives(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
select c.*
from __contracts(qname) c
where c.archived_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable
                parallel safe;
comment on function archives is $$Returns payload and metadata for archived contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Only contracts within [from_offset; to_offset] range are considered.$$;

create or replace function lookup_contract(
    contract_id __contracts.contract_id%type,
    qname text default null
) returns setof contract as
$$
select c.*
from __contracts(qname) c
where c.contract_id = lookup_contract.contract_id
$$ language sql stable;
comment on function lookup_contract is 'Lookup contract and its interface views data by contract ID.';

create or replace function summary_active(
    at_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix @> __nearest_ix(at_offset)
               group by c.tpe_pk)
select tpe.template_fqn, tpe.payload_type, stats.count
from stats
    join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_active is 'Returns the number of active contracts per Daml fully qualified name at particular offset.';

create or replace function summary_creates(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.created_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
               group by c.tpe_pk)
select tpe.template_fqn, tpe.payload_type, stats.count
from stats
    join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_creates is 'Returns the number of created contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

create or replace function summary_archives(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.archived_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
               group by c.tpe_pk)
select tpe.template_fqn, tpe.payload_type, stats.count
from stats
    join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_archives is 'Returns the number of archived contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

create or replace function summary_transients(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix <@ int8range(__nearest_ix(from_offset), __nearest_ix(to_offset))
               group by c.tpe_pk)
select tpe.template_fqn, tpe.payload_type, stats.count
from stats
    join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_transients is 'Returns the number of transient contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

create or replace function summary_updates(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
)
    returns table
            (
                template_fqn text,
                payload_type __payload_type,
                creates      bigint,
                archives     bigint
            )
as
$$
with creates as (select c.tpe_pk as tpe_pk, count(*) as count
                 from __contracts c
                 where c.created_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
                 group by c.tpe_pk),
     archives as (select c.tpe_pk as tpe_pk, count(*) as count
                  from __contracts c
                  where c.archived_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
                  group by c.tpe_pk)
select tpe.template_fqn,
       tpe.payload_type,
       coalesce(creates.count, 0)  as creates,
       coalesce(archives.count, 0) as archives
from (creates full outer join archives on creates.tpe_pk = archives.tpe_pk)
    join __contract_tpe tpe on creates.tpe_pk = tpe.pk or archives.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_updates is 'Returns the summary of creates and archives per Daml fully qualified name in the [from_offset, to_offset] range.';

create or replace function exercises(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof exercise
as
$$
select e.*
from __exercises(qname) e
where e.exercised_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable
                parallel safe;
comment on function exercises is $$Returns argument, result and metadata for exercised events of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<choice-name>'
- partially qualified, e.g. '<module-path>:<choice-name>' or '<choice-name>'
Only events within [from_offset; to_offset] range are considered.$$;

create or replace function lookup_exercises(
    contract_id __exercises.contract_id%type,
    qname text default null
) returns setof exercise as
$$
select e.*
from __exercises(qname) e
where e.contract_id = lookup_exercises.contract_id
$$ language sql stable
                parallel safe;
comment on function lookup_exercises is 'Lookup choice exercises by contract ID.';

create or replace function summary_exercises(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
)
    returns table
            (
                template_fqn __exercise_tpe.template_fqn%type,
                choice_fqn   __exercise_tpe.choice_fqn%type,
                choice       text,
                consuming    bool,
                count        bigint
            )
as
$$
with stats as (select e.tpe_pk as tpe_pk, count(*) as count
               from __exercises e
               where e.exercised_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
               group by e.tpe_pk)
select tpe.template_fqn, tpe.choice_fqn, tpe.choice, tpe.consuming, count
from stats
    join __exercise_tpe tpe on tpe.pk = stats.tpe_pk
$$ language sql stable
                parallel safe;
comment on function summary_exercises is 'Returns the number of exercised events per Daml coordinates in the [from_offset, to_offset] range.';

-------------
-- Pruning --
-------------

create or replace function validate_pruning_offset(min_offset text)
    returns table
            (
                squash_inclusive      text,
                new_oldest            text,
                affected_transactions integer
            )
as $$
declare
    first_offset  text;
    latest_offset text;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());
    squash_inclusive := (select min("offset") from __transactions where "offset" >= min_offset);
    new_oldest := (select min("offset") from __transactions where "offset" > squash_inclusive);

    if min_offset < first_offset then
        raise exception 'Illegal pruning offset % is outside lower bounds of contiguous history', min_offset;
    end if;
    if squash_inclusive = latest_offset then
        raise exception 'Illegal pruning offset % coincides with latest consistent checkpoint of contiguous history', min_offset;
    end if;
    if min_offset > latest_offset then
        raise exception 'Illegal pruning offset % is beyond upper bounds of contiguous history', min_offset;
    end if;

    -- Defensively ensure that there is a new_oldest - this error should never be raised
    if new_oldest is null then
        raise exception 'No offset found after offset %, aborting pruning operation', squash_inclusive;
    end if;

    affected_transactions := (select count(*) from __transactions where "offset" <= squash_inclusive);

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function validate_pruning_offset(text)
    is $$This function validates the offset for pruning and returns the minimum offset for pruning,
the new genesis offset, and the expected number of transactions to be affected by the pruning operation.$$;

create or replace function prune_to_offset(min_offset text)
    returns table
            (
                squash_inclusive      text,
                new_oldest            text,
                affected_transactions integer
            )
as $$
declare
    squash_inclusive text;
    new_oldest       text;
    new_parent_tx    bigint;
begin
    -- log the offset
    raise notice 'Pruning to offset: %', min_offset;

    -- Validate the provided offset
    select validation.squash_inclusive, validation.new_oldest
    into squash_inclusive, new_oldest
    from validate_pruning_offset(min_offset) as validation;

    select ix from __transactions where "offset" = new_oldest into new_parent_tx;

    -- move offset of active contracts (only) to the "new genesis", i.e. the oldest offset excluded from pruning
    with event_pks as (
        update __contracts
            set created_at_ix = new_parent_tx
            where created_at_ix < new_parent_tx and
                  (archived_at_ix is null or archived_at_ix >= new_parent_tx)
            returning create_event_pk)
    update __events
    set tx_ix = new_parent_tx
    from event_pks
    where pk = create_event_pk;

    -- delete everything before the min_offset (bar the active contracts that have been moved forward)
    with deleted_transactions as (delete from __transactions where ix < new_parent_tx returning 1)
    select count(*)
    into affected_transactions
    from deleted_transactions;

    raise log 'Pruned % transactions', affected_transactions;

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$ language plpgsql strict;

comment on function prune_to_offset(text)
    is $$Prunes the database to a specified ledger offset.
Squashes all active contracts up to, and inclusive of, the given offset into the next known transaction,
while removing all archived contracts and choice payloads in the same offset range.$$;


create or replace function validate_reset_offset(max_offset text)
    returns table
            (
                new_latest            text,
                affected_transactions integer
            )
as $$
declare
    first_offset  text;
    latest_offset text;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());

    if max_offset < first_offset then
        raise exception 'Illegal reset offset % is outside lower bounds of contiguous history', max_offset;
    end if;
    if max_offset > latest_offset then
        raise exception 'Illegal reset offset % is beyond upper bounds of contiguous history', max_offset;
    end if;

    affected_transactions := (select count(*) from __transactions where "offset" > max_offset);

    return query select max_offset, affected_transactions;
end;
$$ language plpgsql strict;

comment on function validate_reset_offset(text)
    is $$This function validates the offset for reset and returns the expected number of transactions to be affected
by the reset operation.$$;

create or replace function reset_to_offset(max_offset text)
    returns table
            (
                new_latest            text,
                affected_transactions integer
            )
as $$
declare
    new_latest text;
begin
    -- log the offset
    raise notice 'Reset to offset: %', max_offset;

    -- Validate the provided offset
    select validation.new_latest into new_latest from validate_reset_offset(max_offset) as validation;

    -- delete everything after the max_offset
    with deleted_transactions as (delete from __transactions where "offset" > new_latest returning 1)
    select count(*)
    into affected_transactions
    from deleted_transactions;

    -- adjust watermark
    with new_latest_ix as (select ix from __transactions where "offset" = new_latest)
    update __watermark
    set "offset" = new_latest,
        ix       = new_latest_ix.ix
    from new_latest_ix;

    raise log 'Reset % transactions', affected_transactions;

    return query select new_latest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function reset_to_offset(text) is $$Reset the database to a specified ledger offset.$$;

--------------------
-- Custom indexes --
--------------------

create or replace procedure create_index_for_contract(name text, qname text, expression text, index_type text,
                                                      index_opclass text default '') as
$$
declare
    tpe_pk bigint;
begin
    select __contract_tpe4name(qname) tpe into tpe_pk;
    execute format(
            'create index if not exists %I on %I using %s((%s) %s)',
            '__contracts_' || tpe_pk || '_' || name || '_idx',
            '__contracts_' || tpe_pk,
            index_type,
            expression,
            index_opclass
            );
end;
$$ language plpgsql;
comment on procedure create_index_for_contract is 'Create index over payload on table partition for corresponding qualified Daml entity.';