-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __transactions add column record_time timestamp with time zone;

create index __transactions_record_time_idx on __transactions using brin (record_time);
