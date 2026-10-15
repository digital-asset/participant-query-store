-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type __reassignment_type as enum ('assign', 'unassign');

create table __reassignments
(
    contract_tpe_pk        bigint              not null references __contract_tpe,
    reassign_event_pk      bigint              not null,
    reassigned_at_ix       bigint              not null,
    type                   __reassignment_type not null,
    contract_id            text                not null,
    reassignment_id        text                not null,
    source_synchronizer_id text                not null,
    target_synchronizer_id text                not null,
    submitter              text,
    reassignment_counter   bigint              not null,
    witnesses              text[]              not null,
    -- unassign-only: before this time only the unassignment's submitter may assign
    assignment_exclusivity timestamp with time zone
) partition by list (contract_tpe_pk);

-- Contract types that already exist need their partition created here; the updated
-- __initialize_contract_tpe below only covers types first seen after this migration.
do $$
declare
    tpe record;
begin
    for tpe in select pk from __contract_tpe loop
        execute format(
            'create table %I partition of __reassignments for values in(%L)',
            '__reassignments_' || tpe.pk, tpe.pk);
    end loop;
end $$;
