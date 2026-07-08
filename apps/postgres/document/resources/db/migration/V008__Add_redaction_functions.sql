-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __contracts
    alter column payload       drop not null,
    alter column contract_key  drop not null,
    add column   redaction_id  text;

alter table __exercises
    alter column argument     drop not null,
    alter column result       drop not null,
    add column   redaction_id text;

alter type contract add attribute redaction_id text;
alter type exercise add attribute redaction_id text;

drop function __contracts(text);
drop function __exercises(text);

create function __contracts(qname text default null) returns setof contract
as $$
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
       (case when c.redaction_id is null then c.payload else null end) as payload,
       (case when c.redaction_id is null then c.contract_key else null end) as contract_key,
       c.metadata,
       ct.effective_at,
       at.effective_at,
       c.redaction_id
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
as $$
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
       (case when e.redaction_id is null then e.argument else null end) as argument,
       (case when e.redaction_id is null then e.result else null end) as result,
       t.effective_at,
       e.redaction_id
from __exercises e
    left join __exercise_tpe tpe on tpe.pk = e.tpe_pk
    left join __transactions t on t.ix = e.exercised_at_ix
    left join __events ee on ee.pk = e.exercise_event_pk
    left join __events pe on pe.pk = ee.parent_event_pk
where qname is null or e.tpe_pk = __exercise_tpe4name(qname)
    $$ language sql stable
    parallel safe;


create function redact_contract(contract_id __contracts.contract_id%type, redaction_id __contracts.redaction_id%type)
   returns integer
as $$
declare
    updated_contracts int;
begin
    raise notice 'Redacting payload and contract key from contract: %', contract_id;
    call __validate_redaction_contract(contract_id);
    with affected_contracts as
        (
            update __contracts c
                set payload = null,
                    contract_key = null,
                    redaction_id = redact_contract.redaction_id
                where c.contract_id = redact_contract.contract_id and c.redaction_id is null
                returning 1
        )
        select count(*)
            into updated_contracts
            from affected_contracts;
    if updated_contracts = 0 then
        raise exception 'Cannot find contract %', contract_id;
    end if;
    return updated_contracts;
end;
$$ language plpgsql strict;
comment on function redact_contract is
    'Assign a redaction_id to an archived contract by contract ID and redact its payload and contract key';
       
create procedure __validate_redaction_contract(contract_id __contracts.contract_id%type)
as $$
declare
    active_count integer;
    redacted_count integer;
begin
    select count(*) filter (where archived_at_ix is null) as active_count,
           count(*) filter (where redaction_id is not null) as redacted_count
        into active_count, redacted_count
        from lookup_contract(contract_id);

    if active_count > 0 then
        raise exception 'Cannot redact contract % because it is active', contract_id;
    end if;

    if redacted_count > 0 then
        raise exception 'Cannot redact contract % because it is already redacted', contract_id;
    end if;
end;
$$ language plpgsql;
   
   
create function redact_exercise(event_id __events.event_id%type, redaction_id __exercises.redaction_id%type)
    returns void
as $$
declare
    updated_exercises int;
begin
    raise notice 'Redacting argument and result from exercise with event ID: %', event_id;
    call __validate_redaction_exercise(event_id);
    with affected_exercises as
        (
            update __exercises ex
            set argument = null,
                result = null,
                redaction_id = redact_exercise.redaction_id
            from __events ae
            where
                ae.pk = ex.exercise_event_pk and
                ae.event_id = redact_exercise.event_id and
                ex.redaction_id is null
            returning 1
        )
        select count(*)
           into updated_exercises
           from affected_exercises;
    if updated_exercises = 0 then
      raise exception 'Cannot find exercise with event ID %', event_id;
    end if;
end;
$$ language plpgsql strict;
comment on function redact_exercise is
    'Assign a redaction_id to an exercise by event ID and redact its argument and result';

create procedure __validate_redaction_exercise(event_id __events.event_id%type)
as $$
declare
    found_count integer;
begin
    select count(*) into found_count
        from exercises()
        where
            exercise_event_id = __validate_redaction_exercise.event_id and
            redaction_id is not null;
    if found_count > 0 then
        raise exception 'Cannot redact exercise with event ID % because it is already redacted', event_id;
    end if;
end;
$$ language plpgsql;