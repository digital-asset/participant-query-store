-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- We change the return type of those functions, so we need to drop them first and then let R__fuctions.sql recreate them.
drop function if exists prune_archived_to_offset(bigint);
drop function if exists prune_archived_to_offset_dry_run(bigint);

create index __reassignments_reassigned_at_ix_idx
	on __reassignments (reassigned_at_ix);

