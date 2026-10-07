-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Partial index meant to be used in active() to find reassigned contracts.
-- The `reassignment_counter > 0 and not divulged_only` predicate must be repeated for the planner to use it in a query.
create index if not exists __contracts_reassigned_contract_id_idx
    on __contracts using btree (contract_id)
    where reassignment_counter > 0 and not divulged_only;

