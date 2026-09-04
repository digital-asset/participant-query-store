-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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

create or replace function __contract_tpe_from_exercise_name(qname text) returns __contract_tpe.pk%type as
$$
select __contract_tpe4name(template_fqn)
from __exercise_tpe
where pk = __exercise_tpe4name(qname);
$$ language sql immutable
                parallel safe
                strict;

create or replace function __nearest_ix_floor("offset" checkpoint."offset"%type) returns checkpoint.ix%type as
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
    where t."offset" <= __nearest_ix_floor."offset" order by t."offset" desc limit 1;
    return result;
end
$$ language plpgsql immutable parallel safe;

create or replace function __nearest_ix_ceil("offset" checkpoint."offset"%type) returns checkpoint.ix%type as
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
    where t."offset" >= __nearest_ix_ceil."offset" order by t."offset" asc limit 1;
    return result;
end
$$ language plpgsql immutable parallel safe;

create or replace function __contracts(qname text default null) returns setof contract
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
       c.observers,
       c.witnesses,
       c.divulged_only,
       -- Storage optimization and backward compatibility:
       -- We don't store the creation package id on the contract if it is the same as the representative package,
       -- which is the common case.
       -- It was also not stored before the creation_package_id column was added. In those cases, the package id
       -- was always the creation package id.
       COALESCE(c.creation_package_id, p.id) as creation_package_id,
       c.contract_key_hash
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
       e.contract_id,
       e.argument,
       e.result,
       t.effective_at,
       e.redaction_id,
       p.name,
       p.version,
       p.id,
       c.signatories,
       c.observers,
       e.controllers,
       e.last_descendant_node_id,
       e.witnesses
from __exercises e
         left join __contracts c on c.contract_id = e.contract_id and c.tpe_pk = e.contract_tpe_pk
         left join __exercise_tpe tpe on tpe.pk = e.tpe_pk
         left join __transactions t on t.ix = e.exercised_at_ix
         left join __events ee on ee.pk = e.exercise_event_pk
         left join __packages p on e.package_pk = p.pk
where qname is null
   or (e.tpe_pk = __exercise_tpe4name(qname) and e.contract_tpe_pk = __contract_tpe_from_exercise_name(qname))
$$ language sql stable
                parallel safe;


create or replace procedure __initialize_contract_implements(template_fqn text, interface_fqn text) as
$$
declare
    pk_template    bigint;
    pk_interface   bigint;
    already_exists boolean;
begin
    select pk
    from __contract_tpe t
    where t.template_fqn = __initialize_contract_implements.template_fqn
    into pk_template;
    select pk
    from __contract_tpe t
    where t.template_fqn = __initialize_contract_implements.interface_fqn
    into pk_interface;
    select exists (select 1
                   from __contract_implements t
                   where t.template_pk = pk_template
                     and t.interface_pk = pk_interface)
    into already_exists;
    if already_exists = false then
        insert into __contract_implements (template_pk, interface_pk) values (pk_template, pk_interface);
    end if;
end;
$$ language plpgsql;


create or replace procedure __initialize_contract_tpe(
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


create or replace procedure __initialize_exercise_tpe(
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


create or replace procedure __initialize_package(package_name text, package_version text, package_id text) as
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


create or replace function __make_aliases(package_name text, module_name text, entity_name text) returns text[] as
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

create or replace function __make_aliases(
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


create or replace function oldest_checkpoint() returns setof checkpoint as
$$
    select "offset", ix from __transactions order by "offset" limit 1;
$$ language sql rows 1 stable parallel safe;
comment on function oldest_checkpoint is 'Returns the oldest (earliest) checkpoint in the history.';

create or replace function latest_checkpoint() returns setof checkpoint as
$$
    select "offset", ix from __watermark where "offset" is not null and ix is not null;
$$ language sql rows 1 stable parallel safe;
comment on function latest_checkpoint() is 'Returns the latest (newest) checkpoint in the history.';

create or replace function validate_offset_exists("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as
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

create or replace function set_oldest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as
$$
select set_config('pqs.session_offset_oldest', validate_offset_exists("offset")::text, false);
select "offset";
$$ language sql stable;
comment on function set_oldest(checkpoint."offset"%type) is $$Sets 'session_offset_oldest' to the given offset, or clears it if null.$$;

create or replace function set_latest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as
$$
select set_config('pqs.session_offset_latest', validate_offset_exists("offset")::text, false);
select "offset";
$$ language sql stable;
comment on function set_latest(checkpoint."offset"%type) is $$Sets 'session_offset_latest' to the given offset, or clears it if null.$$;

create or replace function latest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('pqs.session_offset_latest', true), '') = ''
               then (select "offset" from latest_checkpoint())
           else current_setting('pqs.session_offset_latest', false)::bigint
           end;
$$ language sql stable
                parallel safe;
comment on function latest_offset is 'Returns the latest (newest) offset in the history.';

create or replace function oldest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('pqs.session_offset_oldest', true), '') = ''
               then (select "offset" from oldest_checkpoint())
           else current_setting('pqs.session_offset_oldest', false)::bigint
           end;
$$ language sql stable
                parallel safe;
comment on function oldest_offset is 'Returns the oldest (earliest) offset in the history.';

create or replace function pruned_offset() returns checkpoint."offset"%type as
$$
    select pruned_offset from __pruning_metadata;
$$ language sql stable parallel safe;
comment on function pruned_offset is 'Returns the offset of the last pruning operation, or NULL if never pruned.';


create or replace function nearest_offset(cutoff_ts timestamptz) returns checkpoint."offset"%type as $$
declare
    result bigint;
begin
    raise info 'Finding closest offset prior to cut-off timestamp: % (UTC)', cutoff_ts;
    select max(tx."offset") into result from __transactions tx, __watermark wm where tx.effective_at <= cutoff_ts and tx."offset" <= wm."offset";
    raise info 'Determined closest offset: %', result;
    return result;
end;
$$ language plpgsql stable strict;
comment on function nearest_offset(timestamptz) is
        'Returns the closest offset prior to the given cut-off timestamp.';

create or replace function nearest_offset(cutoff interval) returns bigint as $$
begin
    raise info 'Current time: % (UTC)', now();
    raise info 'Finding closest offset prior to cut-off interval ago: %', cutoff;
    return nearest_offset(now() - cutoff);
end;
$$ language plpgsql stable strict;
comment on function nearest_offset(interval) is 'Returns the closest offset prior to the given cut-off interval ago.';


create or replace function validate_pruning_offset(min_offset checkpoint."offset"%type)
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
    last_pruned   bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());
    squash_inclusive := (select min("offset") from __transactions where "offset" >= min_offset);
    new_oldest := (select min("offset") from __transactions where "offset" > squash_inclusive);
    last_pruned := (select pruned_offset from __pruning_metadata);

    -- Legacy prune_to_offset persists the first surviving offset as pruned_offset,
    -- so only strictly earlier offsets are already pruned: treat as no-op
    if last_pruned is not null and min_offset < last_pruned then
        raise warning 'Already pruned past offset %, nothing to do.', last_pruned;
        return;
    end if;

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

create or replace function __validate_max_pruned_offset(max_pruned_offset checkpoint."offset"%type)
    returns table
            (
                pruning_boundary_offset bigint
            )
as $$
declare
    first_offset  bigint;
    latest_offset bigint;
    last_pruned   bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());
    pruning_boundary_offset := (select min("offset") from __transactions where "offset" > max_pruned_offset);
    last_pruned := (select pruned_offset from __pruning_metadata);

    -- Already pruned past this offset: treat as no-op (return empty set)
    if last_pruned is not null and max_pruned_offset <= last_pruned then
        raise warning 'Already pruned past offset %, nothing to do.', last_pruned;
        return;
    end if;

    if max_pruned_offset < first_offset then
        raise exception 'Illegal pruning offset % is outside lower bounds of contiguous history', max_pruned_offset;
    end if;
    if max_pruned_offset = latest_offset then
        raise exception 'Illegal pruning offset % coincides with latest consistent checkpoint of contiguous history', max_pruned_offset;
    end if;
    if max_pruned_offset > latest_offset then
        raise exception 'Illegal pruning offset % is beyond upper bounds of contiguous history', max_pruned_offset;
    end if;

    -- Defensively ensure that there is a pruning_boundary_offset - this error should never be raised
    if pruning_boundary_offset is null then
        raise exception 'No offset found after offset %, aborting pruning operation', max_pruned_offset;
    end if;

    return query select pruning_boundary_offset;
end;
$$ language plpgsql strict;
comment on function __validate_max_pruned_offset(checkpoint."offset"%type) is
$$This function validates the offset for pruning and returns the pruning boundary.
The pruning boundary is the first offset that will not be pruned.$$;

create or replace function prune_archived_to_offset(max_pruned_offset checkpoint."offset"%type)
    returns table
            (
                pruning_boundary_offset bigint,
                deleted_contracts integer,
                deleted_exercises integer,
                deleted_events integer,
                deleted_transactions integer
            )
as $$
declare
    cutoff_ix checkpoint.ix%type;
    new_oldest bigint;
begin
    -- Lock __pruning_metadata to prevent concurrent pruning or resetting.
    lock table __pruning_metadata in share update exclusive mode;

    -- Log the offset
    raise log 'Pruning up to and including offset: %', max_pruned_offset;

    -- Validate the provided offset (returns empty set if already pruned)
    select validation.pruning_boundary_offset into pruning_boundary_offset
    from __validate_max_pruned_offset(max_pruned_offset) as validation;

    -- Already pruned: return zeroed stats
    if pruning_boundary_offset is null then
        return query select null::bigint, 0, 0, 0, 0;
        return;
    end if;

    select ix into cutoff_ix from __transactions where "offset" = pruning_boundary_offset;

    with deleted_contracts as (
        delete from __contracts
        where
            -- prune contracts that were archived prior to cutoff
            archived_at_ix < cutoff_ix
            -- prune divulged-only contracts that existed prior to cutoff
            or (divulged_only and created_at_ix < cutoff_ix)
        returning create_event_pk, archive_event_pk
    ),
    -- prune exercises that happened prior to cutoff
    deleted_exercises as (
        delete from __exercises where exercised_at_ix < cutoff_ix
        returning exercise_event_pk
    ),
    -- prune create, archive and exercise events
    deleted_events as (
        delete from __events
        where tx_ix < cutoff_ix
        and pk in (
            select create_event_pk from deleted_contracts
            union all
            select archive_event_pk from deleted_contracts where archive_event_pk is not null
            union all
            select exercise_event_pk from deleted_exercises
        )
        returning 1
    )
    select
        (select count(*) from deleted_contracts),
        (select count(*) from deleted_exercises),
        (select count(*) from deleted_events)
    into deleted_contracts, deleted_exercises, deleted_events;

    -- after we have removed the archived contracts, we can remove the orphaned transactions
    with deleted_transactions as (
        delete from __transactions
        where ix < cutoff_ix and not exists (
            select 1 from __contracts where __contracts.created_at_ix = __transactions.ix
        )
        returning 1
    )
    select count(*) into deleted_transactions from deleted_transactions;

    -- Persist the pruning offset
    update __pruning_metadata set pruned_offset = max_pruned_offset;

    raise log 'Pruned % contracts, % exercises, % events and % transactions',
        deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;

    return query select pruning_boundary_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;
end;
$$ language plpgsql strict;
comment on function prune_archived_to_offset(checkpoint."offset"%type) is
$$Prunes archived ledger data up to the specified offset while maintaining referential integrity.
- Removes archived contracts and their associated create/archive events.
- Removes exercises and their corresponding exercise events.
- Prunes transactions only if they no longer reference any active contracts.
- Guarantees that all currently active contracts and their history remain intact.$$;

create or replace function prune_archived_to_offset_dry_run(max_pruned_offset checkpoint."offset"%type)
    returns table
            (
                pruning_boundary_offset bigint,
                deleted_contracts integer,
                deleted_exercises integer,
                deleted_events integer,
                deleted_transactions integer
            )
as $$
declare
    cutoff_ix checkpoint.ix%type;
    new_oldest bigint;
begin
    -- Log the offset
    raise notice 'DRY-RUN: pruning to offset: %', max_pruned_offset;

    -- Validate the provided offset (returns empty set if already pruned)
    select validation.pruning_boundary_offset into pruning_boundary_offset
    from __validate_max_pruned_offset(max_pruned_offset) as validation;

    -- Already pruned: return zeroed stats
    if pruning_boundary_offset is null then
        return query select null::bigint, 0, 0, 0, 0;
        return;
    end if;

    select ix into cutoff_ix from __transactions where "offset" = pruning_boundary_offset;

    with deleted_contracts as (
        select create_event_pk, archive_event_pk
        from __contracts
        where
            -- prune contracts that were archived prior to cutoff
            archived_at_ix < cutoff_ix
            -- prune divulged-only contracts that existed prior to cutoff
            or (divulged_only and created_at_ix < cutoff_ix)
    ),
    -- prune exercises that happened prior to cutoff
    deleted_exercises as (
        select exercise_event_pk from __exercises where exercised_at_ix < cutoff_ix
    ),
    -- prune create, archive and exercise events
    deleted_events as (
        select 1 from __events
        where tx_ix < cutoff_ix
        and pk in (
            select create_event_pk from deleted_contracts
            union all
            select archive_event_pk from deleted_contracts where archive_event_pk is not null
            union all
            select exercise_event_pk from deleted_exercises
        )
    )
    select
        (select count(*) from deleted_contracts),
        (select count(*) from deleted_exercises),
        (select count(*) from deleted_events)
    into deleted_contracts, deleted_exercises, deleted_events;

    -- prune orphaned transactions
    select count(*) into deleted_transactions
    from __transactions
    where ix < cutoff_ix and not exists (
        select 1 from __contracts
        where __contracts.created_at_ix = __transactions.ix
        -- the contract is not divulged
        and not __contracts.divulged_only
        -- the contract is not archived or it is archived after the cutoff
        and (__contracts.archived_at_ix is null or __contracts.archived_at_ix >= cutoff_ix)
    );

    raise notice 'DRY-RUN: pruning % contracts, % exercises, % events and % transactions',
        deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;

    return query select pruning_boundary_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;
end;
$$ language plpgsql strict;
comment on function prune_archived_to_offset_dry_run(checkpoint."offset"%type) is
$$Dry-run of prune_archived_to_offset.$$;

create or replace function prune_to_offset(min_offset checkpoint."offset"%type)
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
    -- Lock __pruning_metadata to prevent concurrent pruning or resetting.
    lock table __pruning_metadata in share update exclusive mode;

    raise warning 'Function prune_to_offset(%) is DEPRECATED.', min_offset
    using detail = 'This implementation is not thread-safe and contains performance bottlenecks.',
        hint = 'Use prune_archived_to_offset(offset) instead for better safety and speed.';

    -- Validate the provided offset (returns empty set if already pruned)
    select validation.squash_inclusive, validation.new_oldest
    into squash_inclusive, new_oldest
    from validate_pruning_offset(min_offset) as validation;

    -- Already pruned: return zeroed stats
    if squash_inclusive is null then
        return query select null::bigint, null::bigint, 0;
        return;
    end if;

    select ix into cutoff_ix from __transactions where "offset" = new_oldest;

    -- move offset of active contracts (only) to the "new genesis", i.e. the oldest offset excluded from pruning
    select count(*) into affected_transactions from __transactions where ix < cutoff_ix;
    call __delete_transactions_before(cutoff_ix);

    -- Persist the pruning offset
    -- Use new_oldest (not squash_inclusive) because prune_to_offset physically deletes
    -- all transactions before the cutoff. Setting pruned_offset = squash_inclusive would
    -- point to a deleted offset, breaking archives()/exercises() which pass
    -- coalesce(pruned_offset(), oldest_offset()) to __nearest_ix().
    update __pruning_metadata set pruned_offset = new_oldest;

    raise log 'Pruned % transactions', affected_transactions;

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function prune_to_offset(checkpoint."offset"%type) is
$$@deprecated - Use prune_archived_to_offset instead.
Prunes the database to a specified ledger offset.
Squashes all active contracts up to, and inclusive of, the given offset into the next known transaction.
WARNING: This function is not thread-safe and has significant performance bottlenecks when handling large datasets.$$;


create or replace procedure __validate_redaction_contract(contract_id __contracts.contract_id%type)
as $$
declare
    active_count integer;
    redacted_count integer;
begin
    select count(*) filter (where archived_at_ix is null) as active_count,
           count(*) filter (where redaction_id is not null) as redacted_count
        into active_count, redacted_count
        from lookup_contract(contract_id);

    if active_count > 0 then
        raise exception 'Cannot redact contract % because it is active', contract_id;
    end if;

    if redacted_count > 0 then
        raise exception 'Cannot redact contract % because it is already redacted', contract_id;
    end if;
end;
$$ language plpgsql;

create or replace function redact_contract(contract_id __contracts.contract_id%type, redaction_id __contracts.redaction_id%type)
   returns integer
as $$
declare
    updated_contracts int;
begin
    raise notice 'Redacting payload and contract key from contract: %', contract_id;
    call __validate_redaction_contract(contract_id);
    with affected_contracts as
        (
            update __contracts c
                set payload = null,
                    contract_key = null,
                    contract_key_hash = null,
                    redaction_id = redact_contract.redaction_id
                where c.contract_id = redact_contract.contract_id and c.redaction_id is null
                returning 1
        )
        select count(*)
            into updated_contracts
            from affected_contracts;
    if updated_contracts = 0 then
        raise exception 'Cannot find contract %', contract_id;
    end if;
    return updated_contracts;
end;
$$ language plpgsql strict;
comment on function redact_contract is
    'Assign a redaction_id to an archived contract by contract ID and redact its payload and contract key';

create or replace procedure __validate_redaction_exercise(event_id __events.event_id%type) as
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

create or replace function redact_exercise(event_id __events.event_id%type, redaction_id __exercises.redaction_id%type)
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


create or replace function validate_reset_offset(max_offset checkpoint."offset"%type)
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

    -- Resolve to the nearest surviving transaction (the exact offset may have been pruned away)
    new_latest := (select "offset" from __transactions where "offset" <= max_offset order by "offset" desc limit 1);
    affected_transactions := (select count(*) from __transactions where "offset" > new_latest);

    return query select new_latest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function validate_reset_offset(checkpoint."offset"%type)
    is $$This function validates the offset for reset and returns the expected number of transactions to be affected
by the reset operation.$$;

create or replace function reset_to_offset(max_offset checkpoint."offset"%type)
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
    -- try locking __watermark table or fail fast
    -- it is unsafe to run reset_to_offset while __watermark is being updated by another process
    lock table __watermark in exclusive mode nowait;
    -- also lock __pruning_metadata to prevent concurrent prune or reset from writing stale state
    lock table __pruning_metadata in share update exclusive mode nowait;

    -- log the offset
    raise notice 'Reset to offset: %', max_offset;

    -- Validate the provided offset and resolve to the nearest surviving transaction
    select validation.new_latest into new_latest from validate_reset_offset(max_offset) as validation;
    select ix into cutoff_ix from __transactions where "offset" = new_latest;

    -- delete everything after the max_offset
    select count(*) into affected_transactions from __transactions where ix > cutoff_ix;
    call __delete_transactions_after(cutoff_ix);

    -- adjust watermark and invalidate the previous PQS instance
    update __watermark
    set "offset" = new_latest,
        ix       = cutoff_ix,
        -- update the instance_id to bypass the __ensure_writer_valid check and to invalidate the previous writer
        instance_id = gen_random_uuid();

    -- adjust pruning metadata if reset goes below the pruned offset
    update __pruning_metadata
    set pruned_offset = case
            when pruned_offset is not null and new_latest < pruned_offset then new_latest
            else pruned_offset
        end;

    raise log 'Reset % transactions', affected_transactions;

    return query select new_latest, affected_transactions;
end;
$$ language plpgsql strict;
comment on function reset_to_offset(checkpoint."offset"%type) is $$Reset the database to a specified ledger offset.$$;

create or replace procedure __delete_transactions_after(cutoff_ix checkpoint.ix%type) as
$$
declare
    work_exists boolean;
begin
    select exists(select ix from __transactions where ix > cutoff_ix) into work_exists;

    if work_exists then
        delete from __contracts where created_at_ix > cutoff_ix;
        update __contracts set archived_at_ix = null, archive_event_pk = null where archived_at_ix > cutoff_ix;
        delete from __exercises where exercised_at_ix > cutoff_ix;
        delete from __events where tx_ix > cutoff_ix;
        delete from __tmp_archived_contracts where archived_at_ix > cutoff_ix;
        delete from __transactions where ix > cutoff_ix;
    end if;
end
$$ language plpgsql;

create or replace procedure __delete_transactions_before(cutoff_ix checkpoint.ix%type) as
$$
declare
    work_exists boolean;
begin
    select exists(select ix from __transactions where ix < cutoff_ix) into work_exists;

    if work_exists then
        delete from __contracts where
            -- prune disclosed contracts that were archived prior to cutoff
            archived_at_ix < cutoff_ix or
            -- prune divulged-only contracts that existed prior to cutoff
            divulged_only and created_at_ix < cutoff_ix;
        -- prune exercises that happened prior to cutoff
        delete from __exercises where exercised_at_ix < cutoff_ix;

        with event_pks as (
            update __contracts set created_at_ix = cutoff_ix where created_at_ix < cutoff_ix returning create_event_pk)
        update __events
        set tx_ix = cutoff_ix
        from event_pks
        where pk = create_event_pk;

        delete from __events where tx_ix < cutoff_ix;
        delete from __transactions where ix < cutoff_ix;
    end if;
end
$$ language plpgsql;

create or replace procedure __cleanup_transactions_after_watermark() as
$$
declare
    latest_ix checkpoint.ix%type;
begin
    lock table __watermark in exclusive mode;
    select ix from latest_checkpoint() into latest_ix;
    call __delete_transactions_after(coalesce(latest_ix, 0));
    update __watermark set instance_id = current_setting('scribe.instance');
end
$$ language plpgsql;

create or replace procedure create_index_for_contract(
    name text,
    qname text,
    expression text,
    index_type text,
    index_opclass text default ''
) as $$
declare
    tpe_pk bigint;
begin
    select __contract_tpe4name(qname) tpe into tpe_pk;
    execute format(
            'create index concurrently if not exists %I on %I using %s(%s %s)',
            '__contracts_' || tpe_pk || '_' || name || '_idx',
            '__contracts_' || tpe_pk,
            index_type,
            expression,
            index_opclass
            );
end;
$$ language plpgsql;
comment on procedure create_index_for_contract is 'Create index over payload on table partition for corresponding qualified Daml entity.';

create or replace function creates(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract as
$$
    select c.*
    from __contracts(qname) c
    where c.created_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
$$ language sql stable parallel safe;
comment on function creates(text, __transactions."offset"%type, __transactions."offset"%type) is $$Returns payload and metadata for created contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts within [from_offset; to_offset] range are considered.$$;

create or replace function summary_creates(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
    with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.created_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
               group by c.tpe_pk)
    select tpe.template_fqn, tpe.payload_type, stats.count
    from stats
        join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable parallel safe;
comment on function summary_creates(__transactions."offset"%type, __transactions."offset"%type) is 'Returns the number of created contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

create or replace function exercises(
    qname text default null,
    from_offset __transactions."offset"%type default coalesce(pruned_offset(), oldest_offset()),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof exercise as
$$
    select e.*
    from __exercises(qname) e
    where e.exercised_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
$$ language sql stable parallel safe;
comment on function exercises(text, __transactions."offset"%type, __transactions."offset"%type)
    is $$Returns argument, result and metadata for exercised events of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<choice-name>'
- partially qualified, e.g. '<module-path>:<choice-name>' or '<choice-name>'
Only events within [from_offset; to_offset] range are considered.$$;

create or replace function summary_exercises(
    from_offset __transactions."offset"%type default coalesce(pruned_offset(), oldest_offset()),
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
               where e.exercised_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
               group by e.tpe_pk)
    select tpe.template_fqn, tpe.choice_fqn, tpe.choice, tpe.consuming, count
    from stats
         join __exercise_tpe tpe on tpe.pk = stats.tpe_pk
$$ language sql stable parallel safe;
comment on function summary_exercises(__transactions."offset"%type, __transactions."offset"%type)
    is 'Returns the number of exercised events per Daml coordinates in the [from_offset, to_offset] range.';

create or replace function active(
    qname text default null,
    "offset" __transactions."offset"%type default latest_offset()
) returns setof contract
as
$$
select c.*
from __contracts(qname) c
where c.life_ix @> (select __nearest_ix_floor("offset"))
  and not c.divulged_only -- exclude contracts that were merely divulged
$$ language sql stable
                parallel safe;
comment on function active is $$Returns payload and metadata for active contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts that are active at particular offset are considered.
Contracts that were only divulged are excluded from the result set.$$;

create or replace function summary_active(
    "offset" __transactions."offset"%type default latest_offset()
) returns setof contract_summary
as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix @> (select __nearest_ix_floor("offset"))
                 and not c.divulged_only -- exclude contracts that were merely divulged
               group by c.tpe_pk)
select tpe.template_fqn,
       tpe.payload_type,
       stats.count
from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_active is $$Returns the number of active contracts per Daml fully qualified name at particular offset.
Contracts that were only divulged are excluded from consideration.$$;

create or replace function archives(
    qname text default null,
    from_offset __transactions."offset"%type default coalesce(pruned_offset(), oldest_offset()),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract
as
$$
select c.template_fqn,
       c.payload_type,
       c.create_event_pk,
       c.create_event_id,
       c.created_at_ix,
       c.created_at_offset,
       c.archive_event_pk,
       c.archive_event_id,
       c.archived_at_ix,
       c.archived_at_offset,
       c.life_ix,
       c.contract_id,
       c.payload,
       c.contract_key,
       c.metadata,
       c.created_effective_at,
       c.archived_effective_at,
       c.redaction_id,
       c.package_name,
       c.package_version,
       c.package_id,
       c.signatories,
       c.observers,
       '{}'::text[], -- prevent propagation of witnesses information
       c.divulged_only,
       c.creation_package_id,
       c.contract_key_hash
from __contracts(qname) c
where c.archived_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
$$ language sql stable
                parallel safe;
comment on function archives is $$Returns payload and metadata for archived contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts within [from_offset; to_offset] range are considered.$$;

create or replace function summary_archives(
    from_offset __transactions."offset"%type default coalesce(pruned_offset(), oldest_offset()),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary as
$$
    with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.archived_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
               group by c.tpe_pk)
    select tpe.template_fqn, tpe.payload_type, stats.count
    from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable parallel safe;
comment on function summary_archives(__transactions."offset"%type, __transactions."offset"%type) is 'Returns the number of archived contracts per Daml fully qualified name in the [from_offset, to_offset] range.';

create or replace function summary_transients(
    from_offset __transactions."offset"%type default coalesce(pruned_offset(), oldest_offset()),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary
as
$$
with bounds as (
    select 
        __nearest_ix_ceil(from_offset) as lower_ix,
        __nearest_ix_floor(to_offset)  as upper_ix
),
stats as (
    select c.tpe_pk as tpe_pk, count(*) as count
    from __contracts c
            cross join bounds b
    where c.life_ix <@ int8range(
            b.lower_ix,
            -- Gap-only windows can round to lower_ix > upper_ix; clamp to empty range.
            greatest(b.lower_ix, b.upper_ix)
            )
        and not c.divulged_only -- exclude contracts that were merely divulged
    group by c.tpe_pk
)
select tpe.template_fqn,
       tpe.payload_type,
       stats.count
from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_transients is $$Returns the number of transient contracts per Daml fully qualified name in the [from_offset, to_offset] range.
Contracts that were only divulged are excluded from consideration.$$;

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
            ) as
$$
    with creates as (select c.tpe_pk as tpe_pk, count(*) as count
                 from __contracts c
                 where c.created_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
                 group by c.tpe_pk),
         archives as (select c.tpe_pk as tpe_pk, count(*) as count
                 from __contracts c
                 where c.archived_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
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

create or replace function stakeholders(c contract) returns text[]
as
$$
select array_agg(distinct x)
from unnest(c.signatories || c.observers) t(x);
$$ language sql immutable
                strict
                parallel safe;
comment on function stakeholders(contract) is $$Helper function to simplify reference to stakeholders of a contract.$$;

create or replace function stakeholders(e exercise) returns text[]
as
$$
select array_agg(distinct x)
from unnest(e.signatories || e.observers) t(x);
$$ language sql immutable
                strict
                parallel safe;
comment on function stakeholders(exercise) is $$Helper function to simplify reference to stakeholders of an exercise's contract.$$;

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


-- Clean up the old single-function form that was replaced by __nearest_ix_floor / __nearest_ix_ceil.
DROP FUNCTION IF EXISTS __nearest_ix(bigint);
