-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- `pqs.` prefix below are not references to a schema, it's just a prefix to avoid conflicts

create or replace function set_oldest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as $$
select set_config('pqs.session_offset_oldest', validate_offset_exists("offset")::text, false);
select "offset";
$$ language sql stable;
comment on function set_oldest(checkpoint."offset"%type) is $$Sets 'session_offset_oldest' to the given offset, or clears it if null.$$;

create or replace function set_latest("offset" checkpoint."offset"%type) returns checkpoint."offset"%type as $$
select set_config('pqs.session_offset_latest', validate_offset_exists("offset")::text, false);
select "offset";
$$ language sql stable;
comment on function set_latest(checkpoint."offset"%type) is $$Sets 'session_offset_latest' to the given offset, or clears it if null.$$;

create or replace function oldest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('pqs.session_offset_oldest', true), '') = ''
               then (select "offset" from oldest_checkpoint())
           else current_setting('pqs.session_offset_oldest', false)::bigint
           end;
$$ language sql stable
                parallel safe;
comment on function oldest_offset is 'Returns the oldest (earliest) offset in the history.';

create or replace function latest_offset() returns checkpoint."offset"%type as
$$
select case
           when coalesce(current_setting('pqs.session_offset_latest', true), '') = ''
               then (select "offset" from latest_checkpoint())
           else current_setting('pqs.session_offset_latest', false)::bigint
           end;
$$ language sql stable
                parallel safe;
comment on function latest_offset is 'Returns the latest (newest) offset in the history.';
