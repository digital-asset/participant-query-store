-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- A hook that runs before any Flyway migration

-- Fix #1015: Update the instance_id to ensure Flyway can update watermark in V40 migration
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_name = '__watermark' and column_name = 'instance_id' and table_schema = current_schema()
  ) then
    update __watermark set instance_id = current_setting('pqs.instance');
  end if;
end $$;
