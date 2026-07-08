-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop function __exercises(text);

alter table __exercises
    add column controllers text[] not null default '{}';

alter type exercise
    add attribute controllers text[];

create function __exercises(qname text default null) returns setof exercise
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
       e.controllers
from __exercises e
         left join __contracts c on c.contract_id = e.contract_id and c.tpe_pk = e.contract_tpe_pk
         left join __exercise_tpe tpe on tpe.pk = e.tpe_pk
         left join __transactions t on t.ix = e.exercised_at_ix
         left join __events ee on ee.pk = e.exercise_event_pk
         left join __packages p on e.package_pk = p.pk
where qname is null
   or e.tpe_pk = __exercise_tpe4name(qname)
$$ language sql stable
                parallel safe;
