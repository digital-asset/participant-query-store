-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop procedure create_index_for_contract;

create procedure create_index_for_contract(
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
            'create index if not exists %I on %I using %s(%s %s)',
            '__contracts_' || tpe_pk || '_' || name || '_idx',
            '__contracts_' || tpe_pk,
            index_type,
            expression,
            index_opclass
            );
end;
$$ language plpgsql;
comment on procedure create_index_for_contract is 'Create index over payload on table partition for corresponding qualified Daml entity.';