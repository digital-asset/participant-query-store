-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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

        -- daml2 version (patch V023) also makes orphans (set parent_event_pk = null) of those events
        -- whose parents are deleted as part of pruning
        delete from __events where tx_ix < cutoff_ix;

        delete from __transactions where ix < cutoff_ix;
    end if;
end
$$ language plpgsql;

alter table __events drop column parent_event_pk;
