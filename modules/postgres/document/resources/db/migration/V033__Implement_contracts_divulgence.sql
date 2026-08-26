-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop function archives(text, __transactions."offset"%type, __transactions."offset"%type);
drop function active(text, __transactions."offset"%type);
drop function summary_active(__transactions."offset"%type);
drop function summary_transients(__transactions."offset"%type, __transactions."offset"%type);
drop function __contracts(text);
drop function __exercises(text);

alter table __contracts
    add column witnesses text[] not null default '{}',
    add column divulged_only boolean not null default false;

comment on column __contracts.divulged_only is $$Indicates whether the contract was only divulged (true), or properly disclosed (false).
'Divulged' means that creation of the contract was shared with a participant node, but there will be no explicit archive event for it in the future.
'Disclosed' means that creation of the contract was shared with a participant node (because one of its parties is a stakeholder), and it will be notified of its archival in the future.$$;

drop index __contracts_life_ix_idx;
create index __contracts_life_ix_idx
    on __contracts using gist (life_ix)
    include (tpe_pk)
    where not divulged_only;

alter table __exercises
    add column witnesses text[] not null default '{}';

alter type contract
    add attribute witnesses text[],
    add attribute divulged_only boolean;

alter type exercise
    add attribute witnesses text[];

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
       c.payload,
       c.contract_key,
       c.metadata,
       ct.effective_at,
       at.effective_at,
       c.redaction_id,
       p.name,
       p.version,
       p.id,
       c.signatories,
       c.observers,
       c.witnesses,
       c.divulged_only
from __contracts c
         left join __contract_tpe tpe on tpe.pk = c.tpe_pk
         left join __transactions ct on c.created_at_ix = ct.ix
         left join __transactions at on c.archived_at_ix = at.ix
         left join __events ce on ce.pk = c.create_event_pk
         left join __events ae on ae.pk = c.archive_event_pk
         left join __packages p on c.package_pk = p.pk
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
       e.controllers,
       e.last_descendant_node_id,
       e.witnesses
from __exercises e
         left join __contracts c on c.contract_id = e.contract_id and c.tpe_pk = e.contract_tpe_pk
         left join __exercise_tpe tpe on tpe.pk = e.tpe_pk
         left join __transactions t on t.ix = e.exercised_at_ix
         left join __events ee on ee.pk = e.exercise_event_pk
         left join __packages p on e.package_pk = p.pk
where qname is null
   or (e.tpe_pk = __exercise_tpe4name(qname) and e.contract_tpe_pk = __contract_tpe_from_exercise_name(qname))
$$ language sql stable
                parallel safe;

create function stakeholders(c contract) returns text[]
as
$$
select array_agg(distinct x)
from unnest(c.signatories || c.observers) t(x);
$$ language sql immutable
                strict
                parallel safe;
comment on function stakeholders(contract) is $$Helper function to simplify reference to stakeholders of a contract.$$;

create function stakeholders(e exercise) returns text[]
as
$$
select array_agg(distinct x)
from unnest(e.signatories || e.observers) t(x);
$$ language sql immutable
                strict
                parallel safe;
comment on function stakeholders(exercise) is $$Helper function to simplify reference to stakeholders of an exercise's contract.$$;

create function archives(
    qname text default null,
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract
as
$$
select c.template_fqn,
       c.payload_type,
       c.create_event_pk,
       c.create_event_id,
       c.created_at_ix,
       c.created_at_offset,
       c.archive_event_pk,
       c.archive_event_id,
       c.archived_at_ix,
       c.archived_at_offset,
       c.life_ix,
       c.contract_id,
       c.payload,
       c.contract_key,
       c.metadata,
       c.created_effective_at,
       c.archived_effective_at,
       c.redaction_id,
       c.package_name,
       c.package_version,
       c.package_id,
       c.signatories,
       c.observers,
       '{}'::text[], -- prevent propagation of witnesses information
       c.divulged_only
from __contracts(qname) c
where c.archived_at_ix between __nearest_ix(from_offset) and __nearest_ix(to_offset)
$$ language sql stable
                parallel safe;
comment on function archives is $$Returns payload and metadata for archived contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts within [from_offset; to_offset] range are considered.$$;

create function active(
    qname text default null,
    "offset" __transactions."offset"%type default latest_offset()
) returns setof contract
as
$$
select c.*
from __contracts(qname) c
where c.life_ix @> __nearest_ix("offset")
  and not c.divulged_only -- exclude contracts that were merely divulged
$$ language sql stable
                parallel safe;
comment on function active is $$Returns payload and metadata for active contracts of the given Daml qualified name.
Qualified name can be:
- fully qualified, e.g. '<package-id>:<module-path>:<template-name>'
- partially qualified, e.g. '<module-path>:<template-name>' or '<template-name>'
Qualified name cannot be ambiguous.
Only contracts that are active at particular offset are considered.
Contracts that were only divulged are excluded from the result set.$$;

create function summary_active(
    "offset" __transactions."offset"%type default latest_offset()
) returns setof contract_summary
as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix @> __nearest_ix("offset")
                 and not c.divulged_only -- exclude contracts that were merely divulged
               group by c.tpe_pk)
select tpe.template_fqn,
       tpe.payload_type,
       stats.count
from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_active is $$Returns the number of active contracts per Daml fully qualified name at particular offset.
Contracts that were only divulged are excluded from consideration.$$;

create function summary_transients(
    from_offset __transactions."offset"%type default oldest_offset(),
    to_offset __transactions."offset"%type default latest_offset()
) returns setof contract_summary
as
$$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix <@ int8range(__nearest_ix(from_offset), __nearest_ix(to_offset))
                 and not c.divulged_only -- exclude contracts that were merely divulged
               group by c.tpe_pk)
select tpe.template_fqn,
       tpe.payload_type,
       stats.count
from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$ language sql stable
                parallel safe;
comment on function summary_transients is $$Returns the number of transient contracts per Daml fully qualified name in the [from_offset, to_offset] range.
Contracts that were only divulged are excluded from consideration.$$;

create or replace procedure __delete_transactions_before(cutoff_ix checkpoint.ix%type) as
$$
declare
    work_exists boolean;
begin
    select exists(select ix from __transactions where ix < cutoff_ix) into work_exists;

    if work_exists then
        delete from __contracts where
            -- prune disclosed contracts that were archived prior to cutoff
            archived_at_ix < cutoff_ix or
            -- prune divulged-only contracts that existed prior to cutoff
            divulged_only and created_at_ix < cutoff_ix;
        -- prune exercises that happened prior to cutoff
        delete from __exercises where exercised_at_ix < cutoff_ix;

        with event_pks as (
            update __contracts set created_at_ix = cutoff_ix where created_at_ix < cutoff_ix returning create_event_pk)
        update __events
        set tx_ix = cutoff_ix
        from event_pks
        where pk = create_event_pk;

        delete from __events where tx_ix < cutoff_ix;
        delete from __transactions where ix < cutoff_ix;
    end if;
end
$$ language plpgsql;
