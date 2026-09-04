-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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
        insert into __tmp_archived_contracts(contract_id, archive_event_pk, archived_at_ix, tpe_pk, package_pk)
        values (new.contract_id, new.archive_event_pk, new.archived_at_ix, new.tpe_pk, new.package_pk);
    end if;
    return new;
end;
$$ language plpgsql;

create or replace function __current_writer() returns __watermark.instance_id%type
as $$ select instance_id from __watermark limit 1 $$
language sql;

create or replace procedure __ensure_writer_valid() as
$$
declare
    current_writer __watermark.instance_id%type;
    session_writer __watermark.instance_id%type;
begin
    select __current_writer() into current_writer;
    if current_writer is not null then
        session_writer := current_setting('scribe.instance');
        if current_writer != session_writer then
            raise exception 'PQS writer instance has changed (old = %, new = %). Aborting...' , session_writer, current_writer;
        end if;
    end if;
end
$$ language plpgsql;

create or replace function __update_watermark_fn() returns trigger as
$$
declare
    tpe_curs cursor for select distinct tpe_pk from __tmp_archived_contracts where archived_at_ix <= new.ix;
begin
    -- Bypass the writer check when instance_id is being updated explicitly.
    -- This is used by reset_to_offset to reset the watermark and invalidate the previous writer.
    if new.instance_id = old.instance_id then
        call __ensure_writer_valid();
    end if;

    if new.ix is null or new."offset" is null then
        raise exception '__watermark.ix and __watermark.offset must not be null';
    end if;

    for tpe in tpe_curs
        loop
            with deleted as (
                delete from __tmp_archived_contracts where archived_at_ix <= new.ix and tpe_pk = tpe.tpe_pk returning *)
            update __contracts c
            set archive_event_pk = deleted.archive_event_pk,
                archived_at_ix   = deleted.archived_at_ix
            from deleted
            where c.tpe_pk = tpe.tpe_pk and c.contract_id = deleted.contract_id;
        end loop;
    return new;
end;
$$ language plpgsql;

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

create or replace view __archives as
select c.archive_event_pk as archive_event_pk,
       c.archived_at_ix   as archived_at_ix,
       c.contract_id      as contract_id,
       c.tpe_pk           as tpe_pk,
       c.package_pk       as package_pk
from __contracts c;

drop trigger if exists __insert_archive_trg on __archives;
create trigger __insert_archive_trg
    instead of insert
    on __archives
    for each row
execute function __insert_archive_fn();

create or replace view transactions as
select t.ix,
       t."offset",
       t.transaction_id,
       t.effective_at,
       t.workflow_id,
       t.trace_context,
       t.external_transaction_hash,
       t.paid_traffic_cost
from __transactions t
where t."offset" between oldest_offset() and latest_offset();
