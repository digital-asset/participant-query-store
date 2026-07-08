-- Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Make it possible to insert an initial empty watermark (null values).
-- This ensures that __watermark always contains a row and that we can register the instance_id on it.
alter table __watermark
  alter column ix drop not null,
  alter column "offset" drop not null;

-- Initialize the singleton row with null values.
insert into __watermark ("offset", ix, instance_id) values (null, null, null)
on conflict (singleton) do nothing;
