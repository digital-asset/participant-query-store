-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop function nearest_offset(timestamptz);

create function nearest_offset(cutoff_ts timestamptz) returns checkpoint."offset"%type as $$
declare
    result bigint;
begin
    raise info 'Finding closest offset prior to cut-off timestamp: % (UTC)', cutoff_ts;
    select max(tx."offset") into result from __transactions tx, __watermark wm where tx.effective_at <= cutoff_ts and tx."offset" <= wm."offset";
    raise info 'Determined closest offset: %', result;
    return result;
end;
$$ language plpgsql stable strict;
comment on function nearest_offset(timestamptz) is
        'Returns the closest offset prior to the given cut-off timestamp.';
