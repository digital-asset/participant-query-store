-- Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

------------------- __validate_max_pruned_offset -------------------

create function __validate_max_pruned_offset(max_pruned_offset checkpoint."offset"%type)
    returns table
            (
                pruning_boundary_offset bigint
            )
as $$
declare
    first_offset  bigint;
    latest_offset bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());
    pruning_boundary_offset := (select min("offset") from __transactions where "offset" > max_pruned_offset);

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

------------------- prune_archived_to_offset -------------------

create function prune_archived_to_offset(max_pruned_offset checkpoint."offset"%type)
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
    raise log 'Pruning up to and including offset: %', max_pruned_offset;

    -- Validate the provided offset
    select validation.pruning_boundary_offset into pruning_boundary_offset
    from __validate_max_pruned_offset(max_pruned_offset) as validation;

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

------------------- prune_archived_to_offset_dry_run -------------------

create function prune_archived_to_offset_dry_run(max_pruned_offset checkpoint."offset"%type)
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

    -- Validate the provided offset
    select validation.pruning_boundary_offset into pruning_boundary_offset
    from __validate_max_pruned_offset(max_pruned_offset) as validation;

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

------------------- Deprecation of prune_to_offset -------------------

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
    raise warning 'Function prune_to_offset(%) is DEPRECATED.', min_offset
    using detail = 'This implementation is not thread-safe and contains performance bottlenecks.',
        hint = 'Use prune_archived_to_offset(offset) instead for better safety and speed.';

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
comment on function prune_to_offset(checkpoint."offset"%type) is
$$@deprecated - Use prune_archived_to_offset instead.
Prunes the database to a specified ledger offset.
Squashes all active contracts up to, and inclusive of, the given offset into the next known transaction.
WARNING: This function is not thread-safe and has significant performance bottlenecks when handling large datasets.$$;