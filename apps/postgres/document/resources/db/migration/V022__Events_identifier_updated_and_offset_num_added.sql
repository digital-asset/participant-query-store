-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

------------------- drop functions/procedures to be recreated after change of types -------------------
drop procedure __validate_redaction_exercise(__events.event_id%type);
drop function redact_exercise(__events.event_id%type, __exercises.redaction_id%type);
drop function validate_offset_exists(checkpoint."offset"%type);
drop function set_oldest(checkpoint."offset"%type);
drop function set_latest(checkpoint."offset"%type);
drop function validate_pruning_offset(checkpoint."offset"%type);
drop function validate_reset_offset(checkpoint."offset"%type);
drop function reset_to_offset(checkpoint."offset"%type);
drop function prune_to_offset(checkpoint."offset"%type);
drop function nearest_offset(timestamptz);
drop function nearest_offset(interval);
------------------- drop functions depending on latest_offset and oldest_offset -------------------
drop function __nearest_ix(checkpoint."offset"%type);
drop function active(text, __transactions."offset"%type);
drop function summary_active(__transactions."offset"%type);
drop function creates(text, __transactions."offset"%type, __transactions."offset"%type);
drop function summary_creates(__transactions."offset"%type, __transactions."offset"%type);
drop function archives(text, __transactions."offset"%type, __transactions."offset"%type);
drop function summary_archives(__transactions."offset"%type, __transactions."offset"%type);
drop function summary_transients(__transactions."offset"%type, __transactions."offset"%type);
drop function exercises(text, __transactions."offset"%type, __transactions."offset"%type);
drop function summary_exercises(__transactions."offset"%type, __transactions."offset"%type);
drop function summary_updates(__transactions."offset"%type, __transactions."offset"%type);
drop function latest_offset();
drop function oldest_offset();

------------------- __transactions -------------------
alter table __transactions add column offset_num bigint;
update __transactions set offset_num = ('x' || lpad(ltrim("offset", '0'), 16, '0'))::bit(64)::bigint;
drop index __transactions_offset_ix_idx;
alter table __transactions alter column "offset" drop not null;
update __transactions set "offset" = null;
alter table __transactions alter column "offset" type bigint using "offset"::bigint;
update __transactions set "offset" = offset_num;
alter table __transactions alter column "offset" set not null;
alter table __transactions drop column offset_num;
create unique index __transactions_offset_ix_idx on __transactions using btree ("offset", ix);

------------------- event_id type -------------------
create type event_id as
    (
    "offset" bigint,
    node_id  int
    );
comment on type event_id is 'Identification of a transaction''s event';

------------------- __events -------------------
create function __resolve_new_event_id(tx_ix __events.tx_ix%type, old_event_id text) returns event_id as
$$
declare
    _offset event_id."offset"%type;
    _node_id event_id.node_id%type;
begin
    if (tx_ix = 0) then
        raise exception 'Current data was seeded from ACS, partial data may is present. Please, drop existing database and seed a new.';
    end if;

    select t."offset" into _offset from __transactions t where t.ix = tx_ix limit 1;
    select cast(regexp_replace(e.event_id, '^.*:', '') as int) into _node_id from __events e where e.event_id = old_event_id limit 1;
    return (_offset, _node_id);
end;
$$ language plpgsql stable parallel safe;

alter table __events add column _event_id event_id;
update __events set _event_id = __resolve_new_event_id(tx_ix, event_id);
alter table __events alter column event_id drop not null;
update __events set event_id = null;
drop index __events_event_id_idx;
alter table __events alter column event_id type event_id using event_id::event_id;
update __events set event_id = _event_id;
alter table __events alter column event_id set not null;
alter table __events drop column _event_id;
create index if not exists __events_event_id_idx on __events using btree (event_id);

drop function __resolve_new_event_id(__events.tx_ix%type, text);

------------------- drop functions to modify contract and exercise types -------------------
drop function __contracts(text);
drop function __exercises(text);

------------------- contract -------------------
alter type contract
    alter attribute create_event_id type event_id,
    alter attribute created_at_offset type bigint,
    alter attribute archive_event_id type event_id,
    alter attribute archived_at_offset type bigint;

------------------- exercise -------------------
alter type exercise
    alter attribute exercise_event_id type event_id,
    alter attribute exercised_at_offset type bigint,
    drop attribute  parent_event_id;

------------------- __contracts -------------------
create function __contracts(qname text default null) returns setof contract
as $$
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
       c.payload,
       c.contract_key,
       c.metadata,
       ct.effective_at,
       at.effective_at,
       c.redaction_id,
       p.name,
       p.version,
       p.id,
       c.signatories,
       c.observers
from __contracts c
    left join __contract_tpe tpe on tpe.pk = c.tpe_pk
    left join __transactions ct on c.created_at_ix = ct.ix
    left join __transactions at on c.archived_at_ix = at.ix
    left join __events ce on ce.pk = c.create_event_pk
    left join __events ae on ae.pk = c.archive_event_pk
    left join __packages p on c.package_pk = p.pk
where qname is null or c.tpe_pk = __contract_tpe4name(qname)
    $$ language sql stable
    parallel safe;

------------------- __exercises -------------------
create function __exercises(qname text default null) returns setof exercise
as $$
select tpe.template_fqn,
       tpe.choice_fqn,
       tpe.choice,
       tpe.consuming,
       e.exercise_event_pk,
       ee.event_id,
       e.exercised_at_ix,
       t."offset",
       e.contract_id,
       e.argument,
       e.result,
       t.effective_at,
       e.redaction_id,
       p.name,
       p.version,
       p.id,
       c.signatories,
       c.observers
from __exercises e
         left join __contracts c on c.contract_id = e.contract_id and c.tpe_pk = e.contract_tpe_pk
         left join __exercise_tpe tpe on tpe.pk = e.tpe_pk
         left join __transactions t on t.ix = e.exercised_at_ix
         left join __events ee on ee.pk = e.exercise_event_pk
         left join __packages p on e.package_pk = p.pk
where qname is null or e.tpe_pk = __exercise_tpe4name(qname)
    $$ language sql stable
    parallel safe;

------------------- __validate_redaction_exercise -------------------
create procedure __validate_redaction_exercise(event_id __events.event_id%type) as
$$
declare
    found_count integer;
begin
    select count(*) into found_count
    from exercises()
    where
        exercise_event_id = event_id and
        redaction_id is not null;
    if found_count > 0 then
        raise exception 'Cannot redact exercise with event ID % because it is already redacted', event_id;
    end if;
end;
$$ language plpgsql;

------------------- redact_exercise -------------------
create function redact_exercise(event_id __events.event_id%type, redaction_id __exercises.redaction_id%type)
    returns void as
$$
declare
    updated_exercises int;
begin
    raise notice 'Redacting argument and result from exercise with event ID: %', event_id;
    call __validate_redaction_exercise(event_id);
    with affected_exercises as
             (
                 update __exercises ex
                     set argument = null,
                         result = null,
                         redaction_id = redact_exercise.redaction_id
                     from __events ae
                     where
                         ae.pk = ex.exercise_event_pk and
                         ae.event_id = redact_exercise.event_id and
                         ex.redaction_id is null
                     returning 1
             )
    select count(*)
    into updated_exercises
    from affected_exercises;
    if updated_exercises = 0
    then raise exception 'Cannot find exercise with event ID %', event_id;
    end if;
end;
$$ language plpgsql strict;
comment on function redact_exercise(__events.event_id%type, __exercises.redaction_id%type) is
    'Assign a redaction_id to an exercise by event ID and redact its argument and result';

------------------- __watermark -------------------
alter table __watermark add column offset_num bigint;
update __watermark set offset_num = ('x' || lpad(ltrim("offset", '0'), 16, '0'))::bit(64)::bigint;
alter table __watermark alter column "offset" drop not null;
update __watermark set "offset" = null;
alter table __watermark alter column "offset" type bigint using "offset"::bigint;
update __watermark set "offset" = offset_num;
alter table __watermark alter column "offset" set not null;
alter table __watermark drop column offset_num;

------------------- drop functions to modify checkpoint type -------------------
drop function oldest_checkpoint();
drop function latest_checkpoint();

------------------- checkpoint -------------------
alter type checkpoint alter attribute "offset" type bigint;

------------------- oldest_checkpoint -------------------
create function oldest_checkpoint() returns setof checkpoint as
$$
    select "offset", ix from __transactions order by "offset" limit 1;
$$ language sql rows 1 stable parallel safe;
comment on function oldest_checkpoint is 'Returns the oldest (earliest) checkpoint in the history.';

------------------- latest_checkpoint -------------------
create function latest_checkpoint() returns setof checkpoint as
$$
    select "offset", ix from __watermark;
$$ language sql rows 1 stable parallel safe;
comment on function latest_checkpoint() is 'Returns the latest (newest) checkpoint in the history.';

------------------- validate_offset_exists -------------------
create function validate_offset_exists("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as
$$
declare
    first_offset  bigint;
    latest_offset bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset  := (select c."offset" from oldest_checkpoint() c);
    latest_offset := (select c."offset" from latest_checkpoint() c);

    if "offset" < first_offset then
        raise exception 'Illegal offset % is outside lower bounds of contiguous history', "offset";
    end if;
    if "offset" > latest_offset then
        raise exception 'Illegal offset % is beyond upper bounds of contiguous history', "offset";
    end if;
    return "offset";
end;
$$ language plpgsql stable strict;
comment on function validate_offset_exists(checkpoint."offset"%type) is 'Validate that the offset in question is part of ingested contiguous history.';

------------------- set_oldest -------------------
create function set_oldest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as
$$
    select set_config('public.session_offset_oldest', validate_offset_exists("offset")::text, false);
    select "offset";
$$ language sql stable;
comment on function set_oldest(checkpoint."offset"%type) is $$Sets 'public.session_offset_oldest' to the given offset, or clears it if null.$$;

------------------- set_latest -------------------
create function set_latest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as
$$
    select set_config('public.session_offset_latest', validate_offset_exists("offset")::text, false);
    select "offset";
$$ language sql stable;
comment on function set_latest(checkpoint."offset"%type) is $$Sets 'public.session_offset_latest' to the given offset, or clears it if null.$$;

------------------- validate_pruning_offset -------------------
create function validate_pruning_offset(min_offset checkpoint."offset"%type)
    returns table
            (
                squash_inclusive      bigint,
                new_oldest            bigint,
                affected_transactions integer
            )
as $$
declare
    first_offset  bigint;
    latest_offset bigint;
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
comment on function validate_pruning_offset(checkpoint."offset"%type)
    is $$This function validates the offset for pruning and returns the minimum offset for pruning,
the new genesis offset, and the expected number of transactions to be affected by the pruning operation.$$;

------------------- validate_reset_offset -------------------
create function validate_reset_offset(max_offset checkpoint."offset"%type)
    returns table
            (
                new_latest            bigint,
                affected_transactions integer
            )
as $$
declare
    first_offset  bigint;
    latest_offset bigint;
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
comment on function validate_reset_offset(checkpoint."offset"%type)
    is $$This function validates the offset for reset and returns the expected number of transactions to be affected
by the reset operation.$$;

------------------- reset_to_offset -------------------
create function reset_to_offset(max_offset checkpoint."offset"%type)
    returns table
            (
                new_latest            bigint,
                affected_transactions integer
            )
as $$
declare
    cutoff_ix  checkpoint.ix%type;
    new_latest bigint;
begin
    -- log the offset
    raise notice 'Reset to offset: %', max_offset;

    -- Validate the provided offset
    select validation.new_latest into new_latest from validate_reset_offset(max_offset) as validation;
    select ix into cutoff_ix from __transactions where "offset" = new_latest;

    -- delete everything after the max_offset
    select count(*) into affected_transactions from __transactions where ix > cutoff_ix;
    call __delete_transactions_after(cutoff_ix);

    -- adjust watermark
    update __watermark
    set "offset" = new_latest,
        ix       = cutoff_ix;

    raise log 'Reset % transactions', affected_transactions;

    return query select new_latest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function reset_to_offset(checkpoint."offset"%type) is $$Reset the database to a specified ledger offset.$$;

------------------- prune_to_offset -------------------
create function prune_to_offset(min_offset checkpoint."offset"%type)
    returns table
            (
                squash_inclusive      bigint,
                new_oldest            bigint,
                affected_transactions integer
            )
as $$
declare
    cutoff_ix        checkpoint.ix%type;
    squash_inclusive bigint;
    new_oldest       bigint;
begin
    -- log the offset
    raise notice 'Pruning to offset: %', min_offset;

    -- Validate the provided offset
    select validation.squash_inclusive, validation.new_oldest
    into squash_inclusive, new_oldest
    from validate_pruning_offset(min_offset) as validation;

    select ix into cutoff_ix from __transactions where "offset" = new_oldest;

    -- move offset of active contracts (only) to the "new genesis", i.e. the oldest offset excluded from pruning
    select count(*) into affected_transactions from __transactions where ix < cutoff_ix;
    call __delete_transactions_before(cutoff_ix);

    raise log 'Pruned % transactions', affected_transactions;

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function prune_to_offset(checkpoint."offset"%type) is $$Prunes the database to a specified ledger offset.
Squashes all active contracts up to, and inclusive of, the given offset into the next known transaction,
while removing all archived contracts and choice payloads in the same offset range.$$;

------------------- nearest_offset -------------------
create function nearest_offset(cutoff_ts timestamptz) returns checkpoint."offset"%type as $$
declare
    result bigint;
begin
    raise info 'Finding closest offset prior to cut-off timestamp: % (UTC)', cutoff_ts;
    select max("offset") into result from __transactions where effective_at <= cutoff_ts;
    raise info 'Determined closest offset: %', result;
    return result;
end;
$$ language plpgsql stable strict;
comment on function nearest_offset(timestamptz) is 'Returns the closest offset prior to the given cut-off timestamp.';


create function nearest_offset(cutoff interval) returns bigint as $$
begin
    raise info 'Current time: % (UTC)', now();
    raise info 'Finding closest offset prior to cut-off interval ago: %', cutoff;
    return nearest_offset(now() - cutoff);
end;
$$ language plpgsql stable strict;
comment on function nearest_offset(interval) is 'Returns the closest offset prior to the given cut-off interval ago.';

------------------- latest_offset -------------------
create function latest_offset() returns checkpoint."offset"%type as
$$
    select
        case
           when coalesce(current_setting('public.session_offset_latest', true), '') = ''
                then (select "offset" from latest_checkpoint())
           else current_setting('public.session_offset_latest', false)::bigint
        end;
$$ language sql stable parallel safe;
comment on function latest_offset is 'Returns the latest (newest) offset in the history.';

------------------- oldest_offset -------------------
create function oldest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('public.session_offset_oldest', true), '') = ''
               then (select "offset" from oldest_checkpoint())
           else current_setting('public.session_offset_oldest', false)::bigint
        end;
$$ language sql stable parallel safe;
comment on function oldest_offset is 'Returns the oldest (earliest) offset in the history.';

------------------- __nearest_ix -------------------
create function __nearest_ix("offset" checkpoint."offset"%type) returns checkpoint.ix%type as
$$
declare
    result bigint;
begin
    if "offset" > latest_offset() then
        raise exception 'Offset % is after the latest known offset %' , "offset", latest_offset();
    end if;
    if "offset" < oldest_offset() then
        raise exception 'Offset % is before the oldest known offset %' , "offset", oldest_offset();
    end if;
    select ix into result from __transactions t
    where t."offset" <= __nearest_ix."offset" order by t."offset" desc limit 1;
    return result;
end
$$ language plpgsql immutable parallel safe;

------------------- active -------------------
create function active(
    qname text default null,
    "offset" __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
    select c.*
    from __contracts(qname) c
    where c.life_ix @> __nearest_ix("offset")
$$ language sql stable parallel safe;
comment on function active(text, __transactions."offset"%type) is $$Returns payload and metadata for active contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts that are active at particular offset are considered.$$;

------------------- summary_active -------------------
create function summary_active(
    "offset" __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
    with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix @> __nearest_ix("offset")
               group by c.tpe_pk)
    select tpe.template_fqn, tpe.payload_type, stats.count
    from stats
        join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable parallel safe;
comment on function summary_active(__transactions."offset"%type) is 'Returns the number of active contracts per Daml fully qualified name at particular offset.';

------------------- creates -------------------
create function creates(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
    select c.*
    from __contracts(qname) c
    where c.created_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable parallel safe;
comment on function creates(text, __transactions."offset"%type, __transactions."offset"%type) is $$Returns payload and metadata for created contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts within [from_offset; to_offset] range are considered.$$;

------------------- summary_creates -------------------
create function summary_creates(
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
$$ language sql stable parallel safe;
comment on function summary_creates(__transactions."offset"%type, __transactions."offset"%type) is 'Returns the number of created contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

------------------- archives -------------------
create function archives(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
    select c.*
    from __contracts(qname) c
    where c.archived_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable parallel safe;
comment on function archives(text, __transactions."offset"%type, __transactions."offset"%type) is $$Returns payload and metadata for archived contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Only contracts within [from_offset; to_offset] range are considered.$$;

------------------- summary_archives -------------------
create function summary_archives(
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
$$ language sql stable parallel safe;
comment on function summary_archives(__transactions."offset"%type, __transactions."offset"%type) is 'Returns the number of archived contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

------------------- summary_transients -------------------
create function summary_transients(
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
$$ language sql stable parallel safe;
comment on function summary_transients(__transactions."offset"%type, __transactions."offset"%type)
    is 'Returns the number of transient contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

------------------- exercises -------------------
create function exercises(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof exercise as
$$
    select e.*
    from __exercises(qname) e
    where e.exercised_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable parallel safe;
comment on function exercises(text, __transactions."offset"%type, __transactions."offset"%type)
    is $$Returns argument, result and metadata for exercised events of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<choice-name>'
- partially qualified, e.g. '<module-path>:<choice-name>' or '<choice-name>'
Only events within [from_offset; to_offset] range are considered.$$;

------------------- summary_exercises -------------------
create function summary_exercises(
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
            ) as
$$
    with stats as (select e.tpe_pk as tpe_pk, count(*) as count
               from __exercises e
               where e.exercised_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
               group by e.tpe_pk)
    select tpe.template_fqn, tpe.choice_fqn, tpe.choice, tpe.consuming, count
    from stats
         join __exercise_tpe tpe on tpe.pk = stats.tpe_pk
$$ language sql stable parallel safe;
comment on function summary_exercises(__transactions."offset"%type, __transactions."offset"%type)
    is 'Returns the number of exercised events per Daml coordinates in the [from_offset, to_offset] range.';

------------------- summary_updates -------------------
create function summary_updates(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
)
    returns table
            (
                template_fqn text,
                payload_type __payload_type,
                creates      bigint,
                archives     bigint
            ) as
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
$$ language sql stable parallel safe;
comment on function summary_updates(__transactions."offset"%type, __transactions."offset"%type)
    is 'Returns the summary of creates and archives per Daml fully qualified name in the [from_offset, to_offset] range.';
