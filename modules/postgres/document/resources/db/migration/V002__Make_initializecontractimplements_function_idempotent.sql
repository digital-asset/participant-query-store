-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop procedure __initialize_contract_implements(text, text);

create procedure __initialize_contract_implements(template_fqn text, interface_fqn text) as
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
