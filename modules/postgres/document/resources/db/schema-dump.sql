-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

--
-- PostgreSQL database dump
--

\restrict onlyfortestingpqs

-- Dumped from database version 17.10 (Debian 17.10-1.pgdg13+1)
-- Dumped by pg_dump version 17.10 (Debian 17.10-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: __event_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.__event_type AS ENUM (
    'create',
    'archive',
    'exercise'
);


--
-- Name: __payload_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.__payload_type AS ENUM (
    'template',
    'interface'
);


--
-- Name: checkpoint; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.checkpoint AS (
	"offset" bigint,
	ix bigint
);


--
-- Name: event_id; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.event_id AS (
	"offset" bigint,
	node_id integer
);


--
-- Name: contract; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.contract AS (
	template_fqn text,
	payload_type public.__payload_type,
	create_event_pk bigint,
	create_event_id public.event_id,
	created_at_ix bigint,
	created_at_offset bigint,
	archive_event_pk bigint,
	archive_event_id public.event_id,
	archived_at_ix bigint,
	archived_at_offset bigint,
	life_ix int8range,
	contract_id text,
	payload jsonb,
	contract_key jsonb,
	metadata bytea,
	created_effective_at timestamp with time zone,
	archived_effective_at timestamp with time zone,
	redaction_id text,
	package_name text,
	package_version text,
	package_id text,
	signatories text[],
	observers text[],
	witnesses text[],
	divulged_only boolean,
	creation_package_id text,
	contract_key_hash bytea
);


--
-- Name: contract_summary; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.contract_summary AS (
	template_fqn text,
	payload_type public.__payload_type,
	count bigint
);


--
-- Name: exercise; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.exercise AS (
	template_fqn text,
	choice_fqn text,
	choice text,
	consuming boolean,
	exercise_event_pk bigint,
	exercise_event_id public.event_id,
	exercised_at_ix bigint,
	exercised_at_offset bigint,
	contract_id text,
	argument jsonb,
	result jsonb,
	exercised_effective_at timestamp with time zone,
	redaction_id text,
	package_name text,
	package_version text,
	package_id text,
	signatories text[],
	observers text[],
	controllers text[],
	last_descendant_node_id integer,
	witnesses text[]
);


--
-- Name: trace_context; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.trace_context AS (
	trace_parent text,
	trace_state text
);


--
-- Name: __cleanup_transactions_after_watermark(); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__cleanup_transactions_after_watermark()
    LANGUAGE plpgsql
    AS $$
declare
    latest_ix checkpoint.ix%type;
begin
    lock table __watermark in exclusive mode;
    select ix from latest_checkpoint() into latest_ix;
    call __delete_transactions_after(coalesce(latest_ix, 0));
    update __watermark set instance_id = current_setting('scribe.instance');
end
$$;


--
-- Name: __contract_tpe4name(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__contract_tpe4name(qname text) RETURNS bigint
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
declare
    result bigint;
begin
    case (select count(*) from __contract_tpe where array [qname] <@ aliases)
    when 0 then
        raise exception 'Identifier not found: %' , qname;
    when 1 then
        select pk from __contract_tpe where array [qname] <@ aliases into result;
    else
        raise exception 'Ambiguous identifier: %' , qname;
    end case;
    return result;
end
$$;


--
-- Name: __contract_tpe_from_exercise_name(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__contract_tpe_from_exercise_name(qname text) RETURNS bigint
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
select __contract_tpe4name(template_fqn)
from __exercise_tpe
where pk = __exercise_tpe4name(qname);
$$;


--
-- Name: __contracts(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__contracts(qname text DEFAULT NULL::text) RETURNS SETOF public.contract
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
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
       c.divulged_only,
       -- Storage optimization and backward compatibility:
       -- We don't store the creation package id on the contract if it is the same as the representative package,
       -- which is the common case.
       -- It was also not stored before the creation_package_id column was added. In those cases, the package id
       -- was always the creation package id.
       COALESCE(c.creation_package_id, p.id) as creation_package_id,
       c.contract_key_hash
from __contracts c
         left join __contract_tpe tpe on tpe.pk = c.tpe_pk
         left join __transactions ct on c.created_at_ix = ct.ix
         left join __transactions at on c.archived_at_ix = at.ix
         left join __events ce on ce.pk = c.create_event_pk
         left join __events ae on ae.pk = c.archive_event_pk
         left join __packages p on c.package_pk = p.pk
where qname is null or c.tpe_pk = __contract_tpe4name(qname)
$$;


--
-- Name: __current_writer(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__current_writer() RETURNS text
    LANGUAGE sql
    AS $$ select instance_id from __watermark limit 1 $$;


--
-- Name: __delete_transactions_after(bigint); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__delete_transactions_after(IN cutoff_ix bigint)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __delete_transactions_before(bigint); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__delete_transactions_before(IN cutoff_ix bigint)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __ensure_writer_valid(); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__ensure_writer_valid()
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __exercise_tpe4name(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__exercise_tpe4name(qname text) RETURNS bigint
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
declare
    result bigint;
begin
    case (select count(*) from __exercise_tpe where array [qname] <@ aliases)
    when 0 then
        raise exception 'Identifier not found: %' , qname;
    when 1 then
        select pk from __exercise_tpe where array [qname] <@ aliases into result;
    else
        raise exception 'Ambiguous identifier: %' , qname;
    end case;
    return result;
end
$$;


--
-- Name: __exercises(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__exercises(qname text DEFAULT NULL::text) RETURNS SETOF public.exercise
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
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
$$;


--
-- Name: __initialize_contract_implements(text, text); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__initialize_contract_implements(IN template_fqn text, IN interface_fqn text)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __initialize_contract_tpe(text, text, text, public.__payload_type); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__initialize_contract_tpe(IN package_name text, IN module_name text, IN entity_name text, IN payload_type public.__payload_type)
    LANGUAGE plpgsql
    AS $$
declare
    new_tpe_pk bigint;
begin
    select pk
    from __contract_tpe tpe
    where tpe.template_fqn =
          __initialize_contract_tpe.package_name
              || ':' || __initialize_contract_tpe.module_name
              || ':' || __initialize_contract_tpe.entity_name
    into new_tpe_pk;

    if new_tpe_pk is null then
        insert into __contract_tpe(package_name, module_name, entity_name, payload_type, aliases)
        values (package_name, module_name, entity_name, payload_type,
                __make_aliases(package_name, module_name, entity_name))
        returning pk into new_tpe_pk;

        execute format(
                'create table %I partition of __contracts for values in(%L)',
                '__contracts_' || new_tpe_pk,
                new_tpe_pk
                );
        execute format(
                'alter table %I alter column metadata set storage external',
                '__contracts_' || new_tpe_pk
                );
    end if;
end;
$$;


--
-- Name: __initialize_exercise_tpe(text, text, text, text, boolean); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__initialize_exercise_tpe(IN package_name text, IN module_name text, IN entity_name text, IN choice text, IN consuming boolean)
    LANGUAGE plpgsql
    AS $$
declare
    new_tpe_pk bigint;
begin
    select pk
    from __exercise_tpe tpe
    where tpe.choice_fqn =
          __initialize_exercise_tpe.package_name
              || ':' || __initialize_exercise_tpe.module_name
              || ':' || __initialize_exercise_tpe.entity_name
              || ':' || __initialize_exercise_tpe.choice
    into new_tpe_pk;

    if new_tpe_pk is null then
        insert into __exercise_tpe(package_name, module_name, entity_name, choice, consuming, aliases)
        values (package_name, module_name, entity_name, choice, consuming,
                __make_aliases(package_name, module_name, entity_name, choice))
        returning pk into new_tpe_pk;

        execute format(
                'create table %I partition of __exercises for values in(%L)',
                '__exercises_' || new_tpe_pk,
                new_tpe_pk
                );
    end if;
end;
$$;


--
-- Name: __initialize_package(text, text, text); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__initialize_package(IN package_name text, IN package_version text, IN package_id text)
    LANGUAGE plpgsql
    AS $$
declare
    pkg bigint;
begin
    select pk
    from __packages pkgs
    where pkgs.name = package_name
      and pkgs.version = package_version
      and pkgs.id = package_id
    into pkg;
    if pkg is null then
        insert into __packages(name, version, id) values (package_name, package_version, package_id);
    end if;
end;
$$;


--
-- Name: __insert_archive_fn(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__insert_archive_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __make_aliases(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__make_aliases(package_name text, module_name text, entity_name text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
declare
    q_name  text;
    fq_name text;
begin
    q_name := module_name || ':' || entity_name;
    fq_name := package_name || ':' || q_name;
    return array [fq_name, q_name, entity_name];
end;
$$;


--
-- Name: __make_aliases(text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__make_aliases(package_name text, module_name text, entity_name text, choice_name text) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
begin
    return array [
        package_name || ':' || module_name || ':' || entity_name || ':' || choice_name,
        module_name || ':' || entity_name || ':' || choice_name,
        entity_name || ':' || choice_name,
        choice_name
        ];
end;
$$;


--
-- Name: __nearest_ix_ceil(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__nearest_ix_ceil("offset" bigint) RETURNS bigint
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
declare
    result bigint;
begin
    if "offset" > latest_offset() then
        raise exception 'Offset % is after the latest known offset %' , "offset", latest_offset();
    end if;
    if "offset" < oldest_offset() then
        raise exception 'Offset % is before the oldest known offset %' , "offset", oldest_offset();
    end if;
    select ix into result from __transactions t
    where t."offset" >= __nearest_ix_ceil."offset" order by t."offset" asc limit 1;
    return result;
end
$$;


--
-- Name: __nearest_ix_floor(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__nearest_ix_floor("offset" bigint) RETURNS bigint
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
declare
    result bigint;
begin
    if "offset" > latest_offset() then
        raise exception 'Offset % is after the latest known offset %' , "offset", latest_offset();
    end if;
    if "offset" < oldest_offset() then
        raise exception 'Offset % is before the oldest known offset %' , "offset", oldest_offset();
    end if;
    select ix into result from __transactions t
    where t."offset" <= __nearest_ix_floor."offset" order by t."offset" desc limit 1;
    return result;
end
$$;


--
-- Name: __update_watermark_fn(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__update_watermark_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __validate_max_pruned_offset(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.__validate_max_pruned_offset(max_pruned_offset bigint) RETURNS TABLE(pruning_boundary_offset bigint)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    first_offset  bigint;
    latest_offset bigint;
    last_pruned   bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());
    pruning_boundary_offset := (select min("offset") from __transactions where "offset" > max_pruned_offset);
    last_pruned := (select pruned_offset from __pruning_metadata);

    -- Already pruned past this offset: treat as no-op (return empty set)
    if last_pruned is not null and max_pruned_offset <= last_pruned then
        raise warning 'Already pruned past offset %, nothing to do.', last_pruned;
        return;
    end if;

    if max_pruned_offset < first_offset then
        raise exception 'Illegal pruning offset % is outside lower bounds of contiguous history', max_pruned_offset;
    end if;
    if max_pruned_offset = latest_offset then
        raise exception 'Illegal pruning offset % coincides with latest consistent checkpoint of contiguous history', max_pruned_offset;
    end if;
    if max_pruned_offset > latest_offset then
        raise exception 'Illegal pruning offset % is beyond upper bounds of contiguous history', max_pruned_offset;
    end if;

    -- Defensively ensure that there is a pruning_boundary_offset - this error should never be raised
    if pruning_boundary_offset is null then
        raise exception 'No offset found after offset %, aborting pruning operation', max_pruned_offset;
    end if;

    return query select pruning_boundary_offset;
end;
$$;


--
-- Name: __validate_redaction_contract(text); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__validate_redaction_contract(IN contract_id text)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: __validate_redaction_exercise(public.event_id); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.__validate_redaction_exercise(IN event_id public.event_id)
    LANGUAGE plpgsql
    AS $$
declare
    found_count integer;
begin
    select count(*) into found_count
    from exercises()
    where
        exercise_event_id = event_id and
        redaction_id is not null;
    if found_count > 0 then
        raise exception 'Cannot redact exercise with event ID % because it is already redacted', event_id;
    end if;
end;
$$;


--
-- Name: latest_offset(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.latest_offset() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
select case
           when coalesce(current_setting('pqs.session_offset_latest', true), '') = ''
               then (select "offset" from latest_checkpoint())
           else current_setting('pqs.session_offset_latest', false)::bigint
           end;
$$;


--
-- Name: active(text, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.active(qname text DEFAULT NULL::text, "offset" bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
select c.*
from __contracts(qname) c
where c.life_ix @> (select __nearest_ix_floor("offset"))
  and not c.divulged_only -- exclude contracts that were merely divulged
$$;


--
-- Name: oldest_offset(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.oldest_offset() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
select case
           when coalesce(current_setting('pqs.session_offset_oldest', true), '') = ''
               then (select "offset" from oldest_checkpoint())
           else current_setting('pqs.session_offset_oldest', false)::bigint
           end;
$$;


--
-- Name: pruned_offset(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.pruned_offset() RETURNS bigint
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select pruned_offset from __pruning_metadata;
$$;


--
-- Name: archives(text, bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.archives(qname text DEFAULT NULL::text, from_offset bigint DEFAULT COALESCE(public.pruned_offset(), public.oldest_offset()), to_offset bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
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
       c.divulged_only,
       c.creation_package_id,
       c.contract_key_hash
from __contracts(qname) c
where c.archived_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
$$;


--
-- Name: create_index_for_contract(text, text, text, text, text); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.create_index_for_contract(IN name text, IN qname text, IN expression text, IN index_type text, IN index_opclass text DEFAULT ''::text)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: creates(text, bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.creates(qname text DEFAULT NULL::text, from_offset bigint DEFAULT public.oldest_offset(), to_offset bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select c.*
    from __contracts(qname) c
    where c.created_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
$$;


--
-- Name: exercises(text, bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.exercises(qname text DEFAULT NULL::text, from_offset bigint DEFAULT COALESCE(public.pruned_offset(), public.oldest_offset()), to_offset bigint DEFAULT public.latest_offset()) RETURNS SETOF public.exercise
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    select e.*
    from __exercises(qname) e
    where e.exercised_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
$$;


--
-- Name: latest_checkpoint(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.latest_checkpoint() RETURNS SETOF public.checkpoint
    LANGUAGE sql STABLE ROWS 1 PARALLEL SAFE
    AS $$
    select "offset", ix from __watermark where "offset" is not null and ix is not null;
$$;


--
-- Name: lookup_contract(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.lookup_contract(contract_id text, qname text DEFAULT NULL::text) RETURNS SETOF public.contract
    LANGUAGE sql STABLE
    AS $$
select c.*
from __contracts(qname) c
where c.contract_id = lookup_contract.contract_id
$$;


--
-- Name: lookup_exercises(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.lookup_exercises(contract_id text, qname text DEFAULT NULL::text) RETURNS SETOF public.exercise
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
select e.*
from __exercises(qname) e
where e.contract_id = lookup_exercises.contract_id
$$;


--
-- Name: nearest_offset(interval); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.nearest_offset(cutoff interval) RETURNS bigint
    LANGUAGE plpgsql STABLE STRICT
    AS $$
begin
    raise info 'Current time: % (UTC)', now();
    raise info 'Finding closest offset prior to cut-off interval ago: %', cutoff;
    return nearest_offset(now() - cutoff);
end;
$$;


--
-- Name: nearest_offset(timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.nearest_offset(cutoff_ts timestamp with time zone) RETURNS bigint
    LANGUAGE plpgsql STABLE STRICT
    AS $$
declare
    result bigint;
begin
    raise info 'Finding closest offset prior to cut-off timestamp: % (UTC)', cutoff_ts;
    select max(tx."offset") into result from __transactions tx, __watermark wm where tx.effective_at <= cutoff_ts and tx."offset" <= wm."offset";
    raise info 'Determined closest offset: %', result;
    return result;
end;
$$;


--
-- Name: oldest_checkpoint(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.oldest_checkpoint() RETURNS SETOF public.checkpoint
    LANGUAGE sql STABLE ROWS 1 PARALLEL SAFE
    AS $$
    select "offset", ix from __transactions order by "offset" limit 1;
$$;


--
-- Name: prune_archived_to_offset(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prune_archived_to_offset(max_pruned_offset bigint) RETURNS TABLE(pruning_boundary_offset bigint, deleted_contracts integer, deleted_exercises integer, deleted_events integer, deleted_transactions integer)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    cutoff_ix checkpoint.ix%type;
    new_oldest bigint;
begin
    -- Lock __pruning_metadata to prevent concurrent pruning or resetting.
    lock table __pruning_metadata in share update exclusive mode;

    -- Log the offset
    raise log 'Pruning up to and including offset: %', max_pruned_offset;

    -- Validate the provided offset (returns empty set if already pruned)
    select validation.pruning_boundary_offset into pruning_boundary_offset
    from __validate_max_pruned_offset(max_pruned_offset) as validation;

    -- Already pruned: return zeroed stats
    if pruning_boundary_offset is null then
        return query select null::bigint, 0, 0, 0, 0;
        return;
    end if;

    select ix into cutoff_ix from __transactions where "offset" = pruning_boundary_offset;

    with deleted_contracts as (
        delete from __contracts
        where
            -- prune contracts that were archived prior to cutoff
            archived_at_ix < cutoff_ix
            -- prune divulged-only contracts that existed prior to cutoff
            or (divulged_only and created_at_ix < cutoff_ix)
        returning create_event_pk, archive_event_pk
    ),
    -- prune exercises that happened prior to cutoff
    deleted_exercises as (
        delete from __exercises where exercised_at_ix < cutoff_ix
        returning exercise_event_pk
    ),
    -- prune create, archive and exercise events
    deleted_events as (
        delete from __events
        where tx_ix < cutoff_ix
        and pk in (
            select create_event_pk from deleted_contracts
            union all
            select archive_event_pk from deleted_contracts where archive_event_pk is not null
            union all
            select exercise_event_pk from deleted_exercises
        )
        returning 1
    )
    select
        (select count(*) from deleted_contracts),
        (select count(*) from deleted_exercises),
        (select count(*) from deleted_events)
    into deleted_contracts, deleted_exercises, deleted_events;

    -- after we have removed the archived contracts, we can remove the orphaned transactions
    with deleted_transactions as (
        delete from __transactions
        where ix < cutoff_ix and not exists (
            select 1 from __contracts where __contracts.created_at_ix = __transactions.ix
        )
        returning 1
    )
    select count(*) into deleted_transactions from deleted_transactions;

    -- Persist the pruning offset
    update __pruning_metadata set pruned_offset = max_pruned_offset;

    raise log 'Pruned % contracts, % exercises, % events and % transactions',
        deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;

    return query select pruning_boundary_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;
end;
$$;


--
-- Name: prune_archived_to_offset_dry_run(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prune_archived_to_offset_dry_run(max_pruned_offset bigint) RETURNS TABLE(pruning_boundary_offset bigint, deleted_contracts integer, deleted_exercises integer, deleted_events integer, deleted_transactions integer)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    cutoff_ix checkpoint.ix%type;
    new_oldest bigint;
begin
    -- Log the offset
    raise notice 'DRY-RUN: pruning to offset: %', max_pruned_offset;

    -- Validate the provided offset (returns empty set if already pruned)
    select validation.pruning_boundary_offset into pruning_boundary_offset
    from __validate_max_pruned_offset(max_pruned_offset) as validation;

    -- Already pruned: return zeroed stats
    if pruning_boundary_offset is null then
        return query select null::bigint, 0, 0, 0, 0;
        return;
    end if;

    select ix into cutoff_ix from __transactions where "offset" = pruning_boundary_offset;

    with deleted_contracts as (
        select create_event_pk, archive_event_pk
        from __contracts
        where
            -- prune contracts that were archived prior to cutoff
            archived_at_ix < cutoff_ix
            -- prune divulged-only contracts that existed prior to cutoff
            or (divulged_only and created_at_ix < cutoff_ix)
    ),
    -- prune exercises that happened prior to cutoff
    deleted_exercises as (
        select exercise_event_pk from __exercises where exercised_at_ix < cutoff_ix
    ),
    -- prune create, archive and exercise events
    deleted_events as (
        select 1 from __events
        where tx_ix < cutoff_ix
        and pk in (
            select create_event_pk from deleted_contracts
            union all
            select archive_event_pk from deleted_contracts where archive_event_pk is not null
            union all
            select exercise_event_pk from deleted_exercises
        )
    )
    select
        (select count(*) from deleted_contracts),
        (select count(*) from deleted_exercises),
        (select count(*) from deleted_events)
    into deleted_contracts, deleted_exercises, deleted_events;

    -- prune orphaned transactions
    select count(*) into deleted_transactions
    from __transactions
    where ix < cutoff_ix and not exists (
        select 1 from __contracts
        where __contracts.created_at_ix = __transactions.ix
        -- the contract is not divulged
        and not __contracts.divulged_only
        -- the contract is not archived or it is archived after the cutoff
        and (__contracts.archived_at_ix is null or __contracts.archived_at_ix >= cutoff_ix)
    );

    raise notice 'DRY-RUN: pruning % contracts, % exercises, % events and % transactions',
        deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;

    return query select pruning_boundary_offset, deleted_contracts, deleted_exercises, deleted_events, deleted_transactions;
end;
$$;


--
-- Name: prune_to_offset(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prune_to_offset(min_offset bigint) RETURNS TABLE(squash_inclusive bigint, new_oldest bigint, affected_transactions integer)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    cutoff_ix        checkpoint.ix%type;
    squash_inclusive bigint;
    new_oldest       bigint;
begin
    -- Lock __pruning_metadata to prevent concurrent pruning or resetting.
    lock table __pruning_metadata in share update exclusive mode;

    raise warning 'Function prune_to_offset(%) is DEPRECATED.', min_offset
    using detail = 'This implementation is not thread-safe and contains performance bottlenecks.',
        hint = 'Use prune_archived_to_offset(offset) instead for better safety and speed.';

    -- Validate the provided offset (returns empty set if already pruned)
    select validation.squash_inclusive, validation.new_oldest
    into squash_inclusive, new_oldest
    from validate_pruning_offset(min_offset) as validation;

    -- Already pruned: return zeroed stats
    if squash_inclusive is null then
        return query select null::bigint, null::bigint, 0;
        return;
    end if;

    select ix into cutoff_ix from __transactions where "offset" = new_oldest;

    -- move offset of active contracts (only) to the "new genesis", i.e. the oldest offset excluded from pruning
    select count(*) into affected_transactions from __transactions where ix < cutoff_ix;
    call __delete_transactions_before(cutoff_ix);

    -- Persist the pruning offset
    -- Use new_oldest (not squash_inclusive) because prune_to_offset physically deletes
    -- all transactions before the cutoff. Setting pruned_offset = squash_inclusive would
    -- point to a deleted offset, breaking archives()/exercises() which pass
    -- coalesce(pruned_offset(), oldest_offset()) to __nearest_ix().
    update __pruning_metadata set pruned_offset = new_oldest;

    raise log 'Pruned % transactions', affected_transactions;

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$;


--
-- Name: redact_contract(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.redact_contract(contract_id text, redaction_id text) RETURNS integer
    LANGUAGE plpgsql STRICT
    AS $$
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
                    contract_key_hash = null,
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
$$;


--
-- Name: redact_exercise(public.event_id, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.redact_exercise(event_id public.event_id, redaction_id text) RETURNS void
    LANGUAGE plpgsql STRICT
    AS $$
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
    if updated_exercises = 0
    then raise exception 'Cannot find exercise with event ID %', event_id;
    end if;
end;
$$;


--
-- Name: reset_to_offset(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reset_to_offset(max_offset bigint) RETURNS TABLE(new_latest bigint, affected_transactions integer)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    cutoff_ix  checkpoint.ix%type;
    new_latest bigint;
begin
    -- try locking __watermark table or fail fast
    -- it is unsafe to run reset_to_offset while __watermark is being updated by another process
    lock table __watermark in exclusive mode nowait;
    -- also lock __pruning_metadata to prevent concurrent prune or reset from writing stale state
    lock table __pruning_metadata in share update exclusive mode nowait;

    -- log the offset
    raise notice 'Reset to offset: %', max_offset;

    -- Validate the provided offset and resolve to the nearest surviving transaction
    select validation.new_latest into new_latest from validate_reset_offset(max_offset) as validation;
    select ix into cutoff_ix from __transactions where "offset" = new_latest;

    -- delete everything after the max_offset
    select count(*) into affected_transactions from __transactions where ix > cutoff_ix;
    call __delete_transactions_after(cutoff_ix);

    -- adjust watermark and invalidate the previous PQS instance
    update __watermark
    set "offset" = new_latest,
        ix       = cutoff_ix,
        -- update the instance_id to bypass the __ensure_writer_valid check and to invalidate the previous writer
        instance_id = gen_random_uuid();

    -- adjust pruning metadata if reset goes below the pruned offset
    update __pruning_metadata
    set pruned_offset = case
            when pruned_offset is not null and new_latest < pruned_offset then new_latest
            else pruned_offset
        end;

    raise log 'Reset % transactions', affected_transactions;

    return query select new_latest, affected_transactions;
end;
$$;


--
-- Name: set_latest(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_latest("offset" bigint) RETURNS bigint
    LANGUAGE sql STABLE
    AS $$
select set_config('pqs.session_offset_latest', validate_offset_exists("offset")::text, false);
select "offset";
$$;


--
-- Name: set_oldest(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_oldest("offset" bigint) RETURNS bigint
    LANGUAGE sql STABLE
    AS $$
select set_config('pqs.session_offset_oldest', validate_offset_exists("offset")::text, false);
select "offset";
$$;


--
-- Name: stakeholders(public.contract); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.stakeholders(c public.contract) RETURNS text[]
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
select array_agg(distinct x)
from unnest(c.signatories || c.observers) t(x);
$$;


--
-- Name: stakeholders(public.exercise); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.stakeholders(e public.exercise) RETURNS text[]
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
select array_agg(distinct x)
from unnest(e.signatories || e.observers) t(x);
$$;


--
-- Name: summary_active(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.summary_active("offset" bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract_summary
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.life_ix @> (select __nearest_ix_floor("offset"))
                 and not c.divulged_only -- exclude contracts that were merely divulged
               group by c.tpe_pk)
select tpe.template_fqn,
       tpe.payload_type,
       stats.count
from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$;


--
-- Name: summary_archives(bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.summary_archives(from_offset bigint DEFAULT COALESCE(public.pruned_offset(), public.oldest_offset()), to_offset bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract_summary
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.archived_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
               group by c.tpe_pk)
    select tpe.template_fqn, tpe.payload_type, stats.count
    from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$;


--
-- Name: summary_creates(bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.summary_creates(from_offset bigint DEFAULT public.oldest_offset(), to_offset bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract_summary
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    with stats as (select c.tpe_pk as tpe_pk, count(*) as count
               from __contracts c
               where c.created_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
               group by c.tpe_pk)
    select tpe.template_fqn, tpe.payload_type, stats.count
    from stats
        join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$;


--
-- Name: summary_exercises(bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.summary_exercises(from_offset bigint DEFAULT COALESCE(public.pruned_offset(), public.oldest_offset()), to_offset bigint DEFAULT public.latest_offset()) RETURNS TABLE(template_fqn text, choice_fqn text, choice text, consuming boolean, count bigint)
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    with stats as (select e.tpe_pk as tpe_pk, count(*) as count
               from __exercises e
               where e.exercised_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
               group by e.tpe_pk)
    select tpe.template_fqn, tpe.choice_fqn, tpe.choice, tpe.consuming, count
    from stats
         join __exercise_tpe tpe on tpe.pk = stats.tpe_pk
$$;


--
-- Name: summary_transients(bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.summary_transients(from_offset bigint DEFAULT COALESCE(public.pruned_offset(), public.oldest_offset()), to_offset bigint DEFAULT public.latest_offset()) RETURNS SETOF public.contract_summary
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
with bounds as (
    select 
        __nearest_ix_ceil(from_offset) as lower_ix,
        __nearest_ix_floor(to_offset)  as upper_ix
),
stats as (
    select c.tpe_pk as tpe_pk, count(*) as count
    from __contracts c
            cross join bounds b
    where c.life_ix <@ int8range(
            b.lower_ix,
            -- Gap-only windows can round to lower_ix > upper_ix; clamp to empty range.
            greatest(b.lower_ix, b.upper_ix)
            )
        and not c.divulged_only -- exclude contracts that were merely divulged
    group by c.tpe_pk
)
select tpe.template_fqn,
       tpe.payload_type,
       stats.count
from stats
         join __contract_tpe tpe on stats.tpe_pk = tpe.pk
$$;


--
-- Name: summary_updates(bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.summary_updates(from_offset bigint DEFAULT public.oldest_offset(), to_offset bigint DEFAULT public.latest_offset()) RETURNS TABLE(template_fqn text, payload_type public.__payload_type, creates bigint, archives bigint)
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
    with creates as (select c.tpe_pk as tpe_pk, count(*) as count
                 from __contracts c
                 where c.created_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
                 group by c.tpe_pk),
         archives as (select c.tpe_pk as tpe_pk, count(*) as count
                 from __contracts c
                 where c.archived_at_ix between (select __nearest_ix_ceil(from_offset)) and (select __nearest_ix_floor(to_offset))
                 group by c.tpe_pk)
    select tpe.template_fqn,
           tpe.payload_type,
           coalesce(creates.count, 0)  as creates,
           coalesce(archives.count, 0) as archives
    from (creates full outer join archives on creates.tpe_pk = archives.tpe_pk)
         join __contract_tpe tpe on creates.tpe_pk = tpe.pk or archives.tpe_pk = tpe.pk
$$;


--
-- Name: validate_offset_exists(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_offset_exists("offset" bigint) RETURNS bigint
    LANGUAGE plpgsql STABLE STRICT
    AS $$
declare
    first_offset  bigint;
    latest_offset bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset  := (select c."offset" from oldest_checkpoint() c);
    latest_offset := (select c."offset" from latest_checkpoint() c);

    if "offset" < first_offset then
        raise exception 'Illegal offset % is outside lower bounds of contiguous history', "offset";
    end if;
    if "offset" > latest_offset then
        raise exception 'Illegal offset % is beyond upper bounds of contiguous history', "offset";
    end if;
    return "offset";
end;
$$;


--
-- Name: validate_pruning_offset(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_pruning_offset(min_offset bigint) RETURNS TABLE(squash_inclusive bigint, new_oldest bigint, affected_transactions integer)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    first_offset  bigint;
    latest_offset bigint;
    last_pruned   bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());
    squash_inclusive := (select min("offset") from __transactions where "offset" >= min_offset);
    new_oldest := (select min("offset") from __transactions where "offset" > squash_inclusive);
    last_pruned := (select pruned_offset from __pruning_metadata);

    -- Legacy prune_to_offset persists the first surviving offset as pruned_offset,
    -- so only strictly earlier offsets are already pruned: treat as no-op
    if last_pruned is not null and min_offset < last_pruned then
        raise warning 'Already pruned past offset %, nothing to do.', last_pruned;
        return;
    end if;

    if min_offset < first_offset then
        raise exception 'Illegal pruning offset % is outside lower bounds of contiguous history', min_offset;
    end if;
    if squash_inclusive = latest_offset then
        raise exception 'Illegal pruning offset % coincides with latest consistent checkpoint of contiguous history', min_offset;
    end if;
    if min_offset > latest_offset then
        raise exception 'Illegal pruning offset % is beyond upper bounds of contiguous history', min_offset;
    end if;

    -- Defensively ensure that there is a new_oldest - this error should never be raised
    if new_oldest is null then
        raise exception 'No offset found after offset %, aborting pruning operation', squash_inclusive;
    end if;

    affected_transactions := (select count(*) from __transactions where "offset" <= squash_inclusive);

    return query select squash_inclusive, new_oldest, affected_transactions;
end;
$$;


--
-- Name: validate_reset_offset(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_reset_offset(max_offset bigint) RETURNS TABLE(new_latest bigint, affected_transactions integer)
    LANGUAGE plpgsql STRICT
    AS $$
declare
    first_offset  bigint;
    latest_offset bigint;
begin
    -- Retrieve latest and first checkpoints
    first_offset := (select "offset" from oldest_checkpoint());
    latest_offset := (select "offset" from latest_checkpoint());

    if max_offset < first_offset then
        raise exception 'Illegal reset offset % is outside lower bounds of contiguous history', max_offset;
    end if;
    if max_offset > latest_offset then
        raise exception 'Illegal reset offset % is beyond upper bounds of contiguous history', max_offset;
    end if;

    -- Resolve to the nearest surviving transaction (the exact offset may have been pruned away)
    new_latest := (select "offset" from __transactions where "offset" <= max_offset order by "offset" desc limit 1);
    affected_transactions := (select count(*) from __transactions where "offset" > new_latest);

    return query select new_latest, affected_transactions;
end;
$$;


SET default_tablespace = '';

--
-- Name: __contracts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__contracts (
    tpe_pk bigint NOT NULL,
    create_event_pk bigint,
    created_at_ix bigint,
    archive_event_pk bigint,
    archived_at_ix bigint,
    life_ix int8range GENERATED ALWAYS AS (int8range(created_at_ix, archived_at_ix)) STORED NOT NULL,
    contract_id text NOT NULL,
    payload jsonb,
    contract_key jsonb,
    metadata bytea,
    redaction_id text,
    package_pk bigint DEFAULT 0 NOT NULL,
    signatories text[] DEFAULT '{}'::text[] NOT NULL,
    observers text[] DEFAULT '{}'::text[] NOT NULL,
    witnesses text[] DEFAULT '{}'::text[] NOT NULL,
    divulged_only boolean DEFAULT false NOT NULL,
    creation_package_id text,
    contract_key_hash bytea
)
PARTITION BY LIST (tpe_pk);


--
-- Name: __archives; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.__archives AS
 SELECT archive_event_pk,
    archived_at_ix,
    contract_id,
    tpe_pk,
    package_pk
   FROM public.__contracts c;


SET default_table_access_method = heap;

--
-- Name: __contract_implements; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__contract_implements (
    template_pk bigint NOT NULL,
    interface_pk bigint NOT NULL
);


--
-- Name: __contract_tpe; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__contract_tpe (
    pk bigint NOT NULL,
    payload_type public.__payload_type NOT NULL,
    aliases text[] NOT NULL,
    package_name text NOT NULL,
    module_name text NOT NULL,
    entity_name text NOT NULL,
    template_fqn text GENERATED ALWAYS AS (((((package_name || ':'::text) || module_name) || ':'::text) || entity_name)) STORED NOT NULL
);


--
-- Name: __contract_tpe_pk_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.__contract_tpe_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __contract_tpe_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.__contract_tpe_pk_seq OWNED BY public.__contract_tpe.pk;


--
-- Name: __contracts_1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__contracts_1 (
    tpe_pk bigint NOT NULL,
    create_event_pk bigint,
    created_at_ix bigint,
    archive_event_pk bigint,
    archived_at_ix bigint,
    life_ix int8range GENERATED ALWAYS AS (int8range(created_at_ix, archived_at_ix)) STORED NOT NULL,
    contract_id text NOT NULL,
    payload jsonb,
    contract_key jsonb,
    metadata bytea,
    redaction_id text,
    package_pk bigint DEFAULT 0 NOT NULL,
    signatories text[] DEFAULT '{}'::text[] NOT NULL,
    observers text[] DEFAULT '{}'::text[] NOT NULL,
    witnesses text[] DEFAULT '{}'::text[] NOT NULL,
    divulged_only boolean DEFAULT false NOT NULL,
    creation_package_id text,
    contract_key_hash bytea
);
ALTER TABLE ONLY public.__contracts_1 ALTER COLUMN metadata SET STORAGE EXTERNAL;


--
-- Name: __events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__events (
    pk bigint NOT NULL,
    tx_ix bigint NOT NULL,
    event_id public.event_id NOT NULL,
    type public.__event_type NOT NULL
);


--
-- Name: __exercise_tpe; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__exercise_tpe (
    pk bigint NOT NULL,
    choice text NOT NULL,
    consuming boolean NOT NULL,
    aliases text[] NOT NULL,
    package_name text NOT NULL,
    module_name text NOT NULL,
    entity_name text NOT NULL,
    template_fqn text GENERATED ALWAYS AS (((((package_name || ':'::text) || module_name) || ':'::text) || entity_name)) STORED NOT NULL,
    choice_fqn text GENERATED ALWAYS AS (((((((package_name || ':'::text) || module_name) || ':'::text) || entity_name) || ':'::text) || choice)) STORED NOT NULL
);


--
-- Name: __exercise_tpe_pk_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.__exercise_tpe_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __exercise_tpe_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.__exercise_tpe_pk_seq OWNED BY public.__exercise_tpe.pk;


--
-- Name: __exercises; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__exercises (
    tpe_pk bigint NOT NULL,
    contract_tpe_pk bigint NOT NULL,
    exercise_event_pk bigint,
    exercised_at_ix bigint,
    contract_id text NOT NULL,
    argument jsonb,
    result jsonb,
    redaction_id text,
    package_pk bigint DEFAULT 0 NOT NULL,
    controllers text[] DEFAULT '{}'::text[] NOT NULL,
    last_descendant_node_id integer DEFAULT '-1'::integer NOT NULL,
    witnesses text[] DEFAULT '{}'::text[] NOT NULL
)
PARTITION BY LIST (tpe_pk);


--
-- Name: __exercises_1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__exercises_1 (
    tpe_pk bigint NOT NULL,
    contract_tpe_pk bigint NOT NULL,
    exercise_event_pk bigint,
    exercised_at_ix bigint,
    contract_id text NOT NULL,
    argument jsonb,
    result jsonb,
    redaction_id text,
    package_pk bigint DEFAULT 0 NOT NULL,
    controllers text[] DEFAULT '{}'::text[] NOT NULL,
    last_descendant_node_id integer DEFAULT '-1'::integer NOT NULL,
    witnesses text[] DEFAULT '{}'::text[] NOT NULL
);


--
-- Name: __packages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__packages (
    pk bigint NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    id text NOT NULL
);


--
-- Name: __packages_pk_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.__packages_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: __packages_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.__packages_pk_seq OWNED BY public.__packages.pk;


--
-- Name: __pruning_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__pruning_metadata (
    singleton boolean DEFAULT true NOT NULL,
    pruned_offset bigint,
    CONSTRAINT singleton_pruning CHECK (singleton)
);


--
-- Name: __tmp_archived_contracts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__tmp_archived_contracts (
    contract_id text,
    archive_event_pk bigint,
    archived_at_ix bigint,
    tpe_pk bigint,
    package_pk bigint DEFAULT 0 NOT NULL
);


--
-- Name: __transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__transactions (
    ix bigint NOT NULL,
    "offset" bigint NOT NULL,
    transaction_id text,
    effective_at timestamp with time zone,
    workflow_id text,
    domain_id text,
    trace_context public.trace_context,
    external_transaction_hash bytea,
    paid_traffic_cost bigint
);
ALTER TABLE ONLY public.__transactions ALTER COLUMN external_transaction_hash SET STORAGE EXTERNAL;


--
-- Name: __watermark; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.__watermark (
    singleton boolean DEFAULT true NOT NULL,
    ix bigint,
    "offset" bigint,
    instance_id text,
    CONSTRAINT singleton_watermark CHECK (singleton)
);


--
-- Name: flyway_schema_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.flyway_schema_history (
    installed_rank integer NOT NULL,
    version character varying(50),
    description character varying(200) NOT NULL,
    type character varying(20) NOT NULL,
    script character varying(1000) NOT NULL,
    checksum integer,
    installed_by character varying(100) NOT NULL,
    installed_on timestamp without time zone DEFAULT now() NOT NULL,
    execution_time integer NOT NULL,
    success boolean NOT NULL
);


--
-- Name: transactions; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.transactions AS
 SELECT ix,
    "offset",
    transaction_id,
    effective_at,
    workflow_id,
    trace_context,
    external_transaction_hash,
    paid_traffic_cost
   FROM public.__transactions t
  WHERE (("offset" >= public.oldest_offset()) AND ("offset" <= public.latest_offset()));


--
-- Name: __contracts_1; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__contracts ATTACH PARTITION public.__contracts_1 FOR VALUES IN ('1');


--
-- Name: __exercises_1; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__exercises ATTACH PARTITION public.__exercises_1 FOR VALUES IN ('1');


--
-- Name: __contract_tpe pk; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__contract_tpe ALTER COLUMN pk SET DEFAULT nextval('public.__contract_tpe_pk_seq'::regclass);


--
-- Name: __exercise_tpe pk; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__exercise_tpe ALTER COLUMN pk SET DEFAULT nextval('public.__exercise_tpe_pk_seq'::regclass);


--
-- Name: __packages pk; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__packages ALTER COLUMN pk SET DEFAULT nextval('public.__packages_pk_seq'::regclass);


--
-- Name: __contract_implements __contract_implements_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__contract_implements
    ADD CONSTRAINT __contract_implements_pkey PRIMARY KEY (template_pk, interface_pk);


--
-- Name: __contract_tpe __contract_tpe_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__contract_tpe
    ADD CONSTRAINT __contract_tpe_pkey PRIMARY KEY (pk);


--
-- Name: __events __events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__events
    ADD CONSTRAINT __events_pkey PRIMARY KEY (pk);


--
-- Name: __exercise_tpe __exercise_tpe_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__exercise_tpe
    ADD CONSTRAINT __exercise_tpe_pkey PRIMARY KEY (pk);


--
-- Name: __packages __packages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__packages
    ADD CONSTRAINT __packages_pkey PRIMARY KEY (pk);


--
-- Name: __pruning_metadata __pruning_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__pruning_metadata
    ADD CONSTRAINT __pruning_metadata_pkey PRIMARY KEY (singleton);


--
-- Name: __transactions __transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__transactions
    ADD CONSTRAINT __transactions_pkey PRIMARY KEY (ix);


--
-- Name: __watermark __watermark_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__watermark
    ADD CONSTRAINT __watermark_pkey PRIMARY KEY (singleton);


--
-- Name: flyway_schema_history flyway_schema_history_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.flyway_schema_history
    ADD CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank);


--
-- Name: __contract_tpe_aliases_ix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contract_tpe_aliases_ix ON public.__contract_tpe USING gin (aliases);


--
-- Name: __contracts_archive_event_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_archive_event_pk_idx ON ONLY public.__contracts USING btree (archive_event_pk);


--
-- Name: __contracts_1_archive_event_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_archive_event_pk_idx ON public.__contracts_1 USING btree (archive_event_pk);


--
-- Name: __contracts_archived_at_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_archived_at_ix_idx ON ONLY public.__contracts USING btree (archived_at_ix) INCLUDE (tpe_pk);


--
-- Name: __contracts_1_archived_at_ix_tpe_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_archived_at_ix_tpe_pk_idx ON public.__contracts_1 USING btree (archived_at_ix) INCLUDE (tpe_pk);


--
-- Name: __contracts_contract_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_contract_id_idx ON ONLY public.__contracts USING hash (contract_id);


--
-- Name: __contracts_1_contract_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_contract_id_idx ON public.__contracts_1 USING hash (contract_id);


--
-- Name: __contracts_create_event_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_create_event_pk_idx ON ONLY public.__contracts USING btree (create_event_pk);


--
-- Name: __contracts_1_create_event_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_create_event_pk_idx ON public.__contracts_1 USING btree (create_event_pk);


--
-- Name: __contracts_created_at_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_created_at_ix_idx ON ONLY public.__contracts USING btree (created_at_ix) INCLUDE (tpe_pk);


--
-- Name: __contracts_1_created_at_ix_tpe_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_created_at_ix_tpe_pk_idx ON public.__contracts_1 USING btree (created_at_ix) INCLUDE (tpe_pk);


--
-- Name: __contracts_life_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_life_ix_idx ON ONLY public.__contracts USING gist (life_ix) INCLUDE (tpe_pk) WHERE (NOT divulged_only);


--
-- Name: __contracts_1_life_ix_tpe_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_life_ix_tpe_pk_idx ON public.__contracts_1 USING gist (life_ix) INCLUDE (tpe_pk) WHERE (NOT divulged_only);


--
-- Name: __contracts_package_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_package_pk_idx ON ONLY public.__contracts USING btree (package_pk);


--
-- Name: __contracts_1_package_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __contracts_1_package_pk_idx ON public.__contracts_1 USING btree (package_pk);


--
-- Name: __events_event_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __events_event_id_idx ON public.__events USING btree (event_id);


--
-- Name: __events_tx_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __events_tx_ix_idx ON public.__events USING btree (tx_ix);


--
-- Name: __exercise_tpe_aliases_ix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercise_tpe_aliases_ix ON public.__exercise_tpe USING gin (aliases);


--
-- Name: __exercises_contract_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_contract_id_idx ON ONLY public.__exercises USING hash (contract_id);


--
-- Name: __exercises_1_contract_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_1_contract_id_idx ON public.__exercises_1 USING hash (contract_id);


--
-- Name: __exercises_exercise_event_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_exercise_event_pk_idx ON ONLY public.__exercises USING btree (exercise_event_pk);


--
-- Name: __exercises_1_exercise_event_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_1_exercise_event_pk_idx ON public.__exercises_1 USING btree (exercise_event_pk);


--
-- Name: __exercises_exercised_at_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_exercised_at_ix_idx ON ONLY public.__exercises USING btree (exercised_at_ix) INCLUDE (tpe_pk);


--
-- Name: __exercises_1_exercised_at_ix_tpe_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_1_exercised_at_ix_tpe_pk_idx ON public.__exercises_1 USING btree (exercised_at_ix) INCLUDE (tpe_pk);


--
-- Name: __exercises_package_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_package_pk_idx ON ONLY public.__exercises USING btree (package_pk);


--
-- Name: __exercises_1_package_pk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __exercises_1_package_pk_idx ON public.__exercises_1 USING btree (package_pk);


--
-- Name: __packages_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __packages_id_idx ON public.__packages USING hash (id);


--
-- Name: __tmp_archived_contracts_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __tmp_archived_contracts_ix_idx ON public.__tmp_archived_contracts USING btree (archived_at_ix);


--
-- Name: __transactions_ext_tx_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __transactions_ext_tx_hash_idx ON public.__transactions USING hash (external_transaction_hash) WHERE (external_transaction_hash IS NOT NULL);


--
-- Name: __transactions_offset_ix_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX __transactions_offset_ix_idx ON public.__transactions USING btree ("offset", ix);


--
-- Name: __transactions_transaction_effective_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __transactions_transaction_effective_at_idx ON public.__transactions USING brin (effective_at);


--
-- Name: __transactions_transaction_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX __transactions_transaction_id_idx ON public.__transactions USING hash (transaction_id);


--
-- Name: flyway_schema_history_s_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX flyway_schema_history_s_idx ON public.flyway_schema_history USING btree (success);


--
-- Name: __contracts_1_archive_event_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_archive_event_pk_idx ATTACH PARTITION public.__contracts_1_archive_event_pk_idx;


--
-- Name: __contracts_1_archived_at_ix_tpe_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_archived_at_ix_idx ATTACH PARTITION public.__contracts_1_archived_at_ix_tpe_pk_idx;


--
-- Name: __contracts_1_contract_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_contract_id_idx ATTACH PARTITION public.__contracts_1_contract_id_idx;


--
-- Name: __contracts_1_create_event_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_create_event_pk_idx ATTACH PARTITION public.__contracts_1_create_event_pk_idx;


--
-- Name: __contracts_1_created_at_ix_tpe_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_created_at_ix_idx ATTACH PARTITION public.__contracts_1_created_at_ix_tpe_pk_idx;


--
-- Name: __contracts_1_life_ix_tpe_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_life_ix_idx ATTACH PARTITION public.__contracts_1_life_ix_tpe_pk_idx;


--
-- Name: __contracts_1_package_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__contracts_package_pk_idx ATTACH PARTITION public.__contracts_1_package_pk_idx;


--
-- Name: __exercises_1_contract_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__exercises_contract_id_idx ATTACH PARTITION public.__exercises_1_contract_id_idx;


--
-- Name: __exercises_1_exercise_event_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__exercises_exercise_event_pk_idx ATTACH PARTITION public.__exercises_1_exercise_event_pk_idx;


--
-- Name: __exercises_1_exercised_at_ix_tpe_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__exercises_exercised_at_ix_idx ATTACH PARTITION public.__exercises_1_exercised_at_ix_tpe_pk_idx;


--
-- Name: __exercises_1_package_pk_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.__exercises_package_pk_idx ATTACH PARTITION public.__exercises_1_package_pk_idx;


--
-- Name: __archives __insert_archive_trg; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER __insert_archive_trg INSTEAD OF INSERT ON public.__archives FOR EACH ROW EXECUTE FUNCTION public.__insert_archive_fn();


--
-- Name: __watermark __insert_watermark_trg; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER __insert_watermark_trg BEFORE INSERT ON public.__watermark FOR EACH ROW EXECUTE FUNCTION public.__update_watermark_fn();


--
-- Name: __watermark __update_watermark_trg; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER __update_watermark_trg BEFORE UPDATE OF ix ON public.__watermark FOR EACH ROW EXECUTE FUNCTION public.__update_watermark_fn();


--
-- Name: __contract_implements __contract_implements_interface_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__contract_implements
    ADD CONSTRAINT __contract_implements_interface_pk_fkey FOREIGN KEY (interface_pk) REFERENCES public.__contract_tpe(pk);


--
-- Name: __contract_implements __contract_implements_template_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__contract_implements
    ADD CONSTRAINT __contract_implements_template_pk_fkey FOREIGN KEY (template_pk) REFERENCES public.__contract_tpe(pk);


--
-- Name: __contracts __contracts_package_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.__contracts
    ADD CONSTRAINT __contracts_package_pk_fkey FOREIGN KEY (package_pk) REFERENCES public.__packages(pk);


--
-- Name: __contracts __contracts_tpe_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.__contracts
    ADD CONSTRAINT __contracts_tpe_pk_fkey FOREIGN KEY (tpe_pk) REFERENCES public.__contract_tpe(pk);


--
-- Name: __exercises __exercises_contract_tpe_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.__exercises
    ADD CONSTRAINT __exercises_contract_tpe_pk_fkey FOREIGN KEY (contract_tpe_pk) REFERENCES public.__contract_tpe(pk);


--
-- Name: __exercises __exercises_package_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.__exercises
    ADD CONSTRAINT __exercises_package_pk_fkey FOREIGN KEY (package_pk) REFERENCES public.__packages(pk);


--
-- Name: __exercises __exercises_tpe_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.__exercises
    ADD CONSTRAINT __exercises_tpe_pk_fkey FOREIGN KEY (tpe_pk) REFERENCES public.__exercise_tpe(pk);


--
-- Name: __tmp_archived_contracts __tmp_archived_contracts_package_pk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__tmp_archived_contracts
    ADD CONSTRAINT __tmp_archived_contracts_package_pk_fkey FOREIGN KEY (package_pk) REFERENCES public.__packages(pk);


--
-- PostgreSQL database dump complete
--

\unrestrict onlyfortestingpqs

