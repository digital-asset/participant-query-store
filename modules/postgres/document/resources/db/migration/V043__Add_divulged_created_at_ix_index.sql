-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- used by prune_archived_to_offset to delete divulged-only contracts via `divulged_only and created_at_ix < cutoff`.
create index __contracts_divulged_created_at_ix_idx
    on __contracts using btree (created_at_ix)
    where divulged_only;
