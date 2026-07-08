-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create or replace procedure __disable_referential_checks() as
$$
begin
    alter table __events
        drop constraint __events_parent_event_pk_fkey,
        drop constraint __events_tx_ix_fkey;

    alter table __contracts
        drop constraint __contracts_create_event_pk_fkey,
        drop constraint __contracts_created_at_ix_fkey,
        drop constraint __contracts_archive_event_pk_fkey,
        drop constraint __contracts_archived_at_ix_fkey;

    alter table __exercises
        drop constraint __exercises_exercise_event_pk_fkey,
        drop constraint __exercises_exercised_at_ix_fkey;

    alter table __tmp_archived_contracts
        drop constraint __tmp_archived_contracts_archived_at_ix_fkey;
end
$$ language plpgsql;

create or replace procedure __enable_referential_checks() as
$$
begin
    alter table __events
        add constraint __events_parent_event_pk_fkey
            foreign key (parent_event_pk) references __events,
        add constraint __events_tx_ix_fkey
            foreign key (tx_ix) references __transactions;

    alter table __contracts
        add constraint __contracts_create_event_pk_fkey
            foreign key (create_event_pk) references __events,
        add constraint __contracts_created_at_ix_fkey
            foreign key (created_at_ix) references __transactions,
        add constraint __contracts_archive_event_pk_fkey
            foreign key (archive_event_pk) references __events,
        add constraint __contracts_archived_at_ix_fkey
            foreign key (archived_at_ix) references __transactions;

    alter table __exercises
        add constraint __exercises_exercise_event_pk_fkey
            foreign key (exercise_event_pk) references __events,
        add constraint __exercises_exercised_at_ix_fkey
            foreign key (exercised_at_ix) references __transactions;

    alter table __tmp_archived_contracts
        add constraint __tmp_archived_contracts_archived_at_ix_fkey
            foreign key (archived_at_ix) references __transactions;
end
$$ language plpgsql;

-- REMOVE previous definitions
alter table __events
    drop constraint __events_parent_event_pk_fkey,
    drop constraint __events_tx_ix_fkey;

alter table __contracts
    drop constraint __contracts_create_event_pk_fkey,
    drop constraint __contracts_created_at_ix_fkey,
    drop constraint __contracts_archive_event_pk_fkey,
    drop constraint __contracts_archived_at_ix_fkey;

alter table __exercises
    drop constraint __exercises_exercise_event_pk_fkey,
    drop constraint __exercises_exercised_at_ix_fkey;

alter table __tmp_archived_contracts
    drop constraint __tmp_archived_at_ix_to_transactions_fkey;

-- Enable referential checks
call __enable_referential_checks();

create or replace procedure __delete_transactions_after(cutoff_ix checkpoint.ix%type) as
$$
declare
    work_exists boolean;
begin
    select exists(select ix from __transactions where ix > cutoff_ix) into work_exists;

    if work_exists then
        call __disable_referential_checks();
        delete from __contracts where created_at_ix > cutoff_ix;
        update __contracts set archived_at_ix = null, archive_event_pk = null where archived_at_ix > cutoff_ix;
        delete from __exercises where exercised_at_ix > cutoff_ix;
        delete from __events where tx_ix > cutoff_ix;
        delete from __tmp_archived_contracts where archived_at_ix > cutoff_ix;
        delete from __transactions where ix > cutoff_ix;
        call __enable_referential_checks();
    end if;
end
$$ language plpgsql;

create procedure __delete_transactions_before(cutoff_ix checkpoint.ix%type) as
$$
declare
    work_exists boolean;
begin
    select exists(select ix from __transactions where ix < cutoff_ix) into work_exists;

    if work_exists then
        call __disable_referential_checks();

        delete from __contracts where archived_at_ix < cutoff_ix;
        delete from __exercises where exercised_at_ix < cutoff_ix;

        with event_pks as (
            update __contracts set created_at_ix = cutoff_ix where created_at_ix < cutoff_ix returning create_event_pk)
        update __events
        set tx_ix = cutoff_ix
        from event_pks
        where pk = create_event_pk;

        with event_pks as (
            delete from __events where tx_ix < cutoff_ix returning pk)
        update __events
        set parent_event_pk = null
        from event_pks
        where parent_event_pk = event_pks.pk;

        delete from __transactions where ix < cutoff_ix;

        call __enable_referential_checks();
    end if;
end
$$ language plpgsql;

drop procedure __delete_transactions;
create procedure __cleanup_transactions_after_watermark() as
$$
declare
    latest_ix checkpoint.ix%type;
begin
    lock table __transactions;
    lock table __watermark;
    select ix from latest_checkpoint() into latest_ix;
    call __delete_transactions_after(coalesce(latest_ix, 0));
    update __watermark set instance_id = current_setting('scribe.instance');
end
$$ language plpgsql;

create or replace function reset_to_offset(max_offset text)
    returns table
            (
                new_latest            text,
                affected_transactions integer
            )
as $$
declare
    cutoff_ix  checkpoint.ix%type;
    new_latest text;
begin
    -- log the offset
    raise notice 'Reset to offset: %', max_offset;

    -- Validate the provided offset
    select validation.new_latest into new_latest from validate_reset_offset(max_offset) as validation;
    select ix from __transactions where "offset" = new_latest into cutoff_ix;

    -- delete everything after the max_offset
    select count(*) from __transactions where ix > cutoff_ix into affected_transactions;
    call __delete_transactions_after(cutoff_ix);

    -- adjust watermark
    update __watermark
    set "offset" = new_latest,
        ix       = cutoff_ix;

    raise log 'Reset % transactions', affected_transactions;

    return query select new_latest, affected_transactions;
end;
$$ language plpgsql strict;

create or replace function prune_to_offset(min_offset text)
    returns table
            (
                squash_inclusive      text,
                new_oldest            text,
                affected_transactions integer
            )
as $$
declare
    cutoff_ix        checkpoint.ix%type;
    squash_inclusive text;
    new_oldest       text;
begin
    -- log the offset
    raise notice 'Pruning to offset: %', min_offset;

    -- Validate the provided offset
    select validation.squash_inclusive, validation.new_oldest
    into squash_inclusive, new_oldest
    from validate_pruning_offset(min_offset) as validation;

    select ix from __transactions where "offset" = new_oldest into cutoff_ix;

    -- move offset of active contracts (only) to the "new genesis", i.e. the oldest offset excluded from pruning
    select count(*) from __transactions where ix < cutoff_ix into affected_transactions;
    call __delete_transactions_before(cutoff_ix);

    raise log 'Pruned % transactions', affected_transactions;

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$ language plpgsql strict;
