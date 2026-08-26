-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __tmp_archived_contracts
    add constraint __tmp_archived_at_ix_to_transactions_fkey
        foreign key (archived_at_ix)
            references __transactions (ix)
            on delete cascade;
