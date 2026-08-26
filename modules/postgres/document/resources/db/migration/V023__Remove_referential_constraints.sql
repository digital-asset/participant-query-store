-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Remove toggling of the referential checks surrounding the operations if work exists
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
    end if;
end
$$ language plpgsql;

-- Remove locking of the __transactions table
create or replace procedure __cleanup_transactions_after_watermark() as
$$
declare
    latest_ix checkpoint.ix%type;
begin
    lock table __watermark;
    select ix from latest_checkpoint() into latest_ix;
    call __delete_transactions_after(coalesce(latest_ix, 0));
    update __watermark set instance_id = current_setting('scribe.instance');
end
$$ language plpgsql;

-- Remove the referential checks
call __disable_referential_checks();
drop procedure __disable_referential_checks;
drop procedure __enable_referential_checks;
