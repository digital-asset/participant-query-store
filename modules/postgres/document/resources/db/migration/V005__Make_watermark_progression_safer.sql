-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop trigger __update_watermark_trg on __watermark;
drop trigger __insert_watermark_trg on __watermark;
drop function __update_watermark_fn();
drop function reset_to_offset(text);
alter table __watermark add column instance_id text;

create procedure __delete_transactions(cutoff_ix checkpoint.ix%type) as
$$
begin
    delete from __transactions where ix > cutoff_ix;
    update __watermark set instance_id = current_setting('scribe.instance');
end
$$ language plpgsql;

create function __current_writer() returns __watermark.instance_id%type
as $$ select instance_id from __watermark limit 1 $$
language sql;

create procedure __ensure_writer_valid() as
$$
declare
    current_writer __watermark.instance_id%type;
    session_writer __watermark.instance_id%type;
begin
    select __current_writer() into current_writer;
    if current_writer is not null then
        session_writer := current_setting('scribe.instance');
        if current_writer != session_writer then
            raise exception 'Scribe writer instance has changed (old = %, new = %). Aborting...' , session_writer, current_writer;
        end if;
    end if;
end
$$ language plpgsql;

create function __update_watermark_fn() returns trigger as
$$
begin
    call __ensure_writer_valid();

    with deleted as (delete from __tmp_archived_contracts where archived_at_ix <= new.ix returning *)
    update __contracts c
    set archive_event_pk = deleted.archive_event_pk,
        archived_at_ix   = deleted.archived_at_ix
    from deleted
    where c.tpe_pk = deleted.tpe_pk and c.contract_id = deleted.contract_id;
    return new;
end;
$$ language plpgsql parallel safe;

create trigger __update_watermark_trg
    before update of ix
    on __watermark
    for each row
execute function __update_watermark_fn();

create trigger __insert_watermark_trg
    before insert
    on __watermark
    for each row
execute function __update_watermark_fn();

create function reset_to_offset(max_offset text)
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

    -- trick to preserve current writer to allow interference with the watermark below
    perform set_config('scribe.instance', (select __current_writer()), true);

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
