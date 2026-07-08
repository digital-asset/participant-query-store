-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop function oldest_checkpoint();
drop function latest_checkpoint();

create function oldest_checkpoint() returns setof checkpoint
as
$$
select "offset", ix from __transactions order by "offset" limit 1;
$$ language sql rows 1
                stable
                parallel safe;
comment on function oldest_checkpoint is 'Returns the oldest (earliest) checkpoint in the history.';

create function latest_checkpoint() returns setof checkpoint
as
$$
select "offset", ix from __watermark;
$$ language sql rows 1
                stable
                parallel safe;
comment on function latest_checkpoint() is 'Returns the latest (newest) checkpoint in the history.';
