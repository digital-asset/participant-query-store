-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


drop function __contracts(text);
drop function __exercises(text);

alter type contract
    add attribute created_effective_at timestamp with time zone,
    add attribute archived_effective_at timestamp with time zone;

alter type exercise add attribute exercised_effective_at timestamp with time zone;

create function __contracts(qname text default null) returns setof contract
as
$$
select tpe.template_fqn,
       tpe.payload_type,
       c.create_event_pk,
       ce.event_id,
       c.created_at_ix,
       ct."offset",
       c.archive_event_pk,
       ae.event_id,
       c.archived_at_ix,
       at."offset",
       c.life_ix,
       c.contract_id,
       c.witnesses,
       c.payload,
       c.contract_key,
       c.metadata,
       ct.effective_at,
       at.effective_at
from __contracts c
    left join __contract_tpe tpe on tpe.pk = c.tpe_pk
    left join __transactions ct on c.created_at_ix = ct.ix
    left join __transactions at on c.archived_at_ix = at.ix
    left join __events ce on ce.pk = c.create_event_pk
    left join __events ae on ae.pk = c.archive_event_pk
where qname is null or c.tpe_pk = __contract_tpe4name(qname)
    $$ language sql stable
    parallel safe;

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
       pe.event_id as parent_event_id,
       e.contract_id,
       e.witnesses,
       e.argument,
       e.result,
       t.effective_at
from __exercises e
    join __exercise_tpe tpe on tpe.pk = e.tpe_pk
    left join __transactions t on t.ix = e.exercised_at_ix
    left join __events ee on ee.pk = e.exercise_event_pk
    left join __events pe on pe.pk = ee.parent_event_pk
where qname is null or e.tpe_pk = __exercise_tpe4name(qname)
    $$ language sql stable
    parallel safe;
