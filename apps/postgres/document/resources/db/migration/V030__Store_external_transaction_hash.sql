-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __transactions
    add column external_transaction_hash bytea,
    alter column external_transaction_hash set storage external;

create index __transactions_ext_tx_hash_idx
    on __transactions using hash (external_transaction_hash)
    where external_transaction_hash is not null;
