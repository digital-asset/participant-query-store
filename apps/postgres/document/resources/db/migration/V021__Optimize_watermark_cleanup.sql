-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create or replace function __update_watermark_fn() returns trigger as
$$
declare
    tpe_curs cursor for select distinct tpe_pk from __tmp_archived_contracts where archived_at_ix <= new.ix;
begin
    call __ensure_writer_valid();

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
